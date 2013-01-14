#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012, GC3, University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__docformat__ = 'reStructuredText'
__version__ = '$Revision$'


import os
import paramiko
import time

# EC2 APIs
import boto
import boto.ec2.regioninfo

# GC3Pie imports
import gc3libs
import gc3libs.exceptions
import gc3libs.url
from gc3libs import Run
from gc3libs.utils import same_docstring_as
from gc3libs.backends import LRMS

class InstanceNotFound(Exception):
    pass

available_subresource_types = [gc3libs.Default.SHELLCMD_LRMS]

class EC2Lrms(LRMS):
    """
    EC2 resource.
    """
    def __init__(self, name,
                 # these parameters are inherited from the `LRMS` class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime, 
                 # these are specific of the EC2Lrms class
                 ec2_region, ec2_url,
                 keypair_name, public_key, image_id,
                 auth=None, **extra_args):
        LRMS.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth, **extra_args)

        self.free_slots = int(max_cores)
        self.user_run = 0
        self.user_queued = 0
        self.queued = 0

        self.subresource_type = self.type.split('+', 1)[1]
        if self.subresource_type not in available_subresource_types:
            raise Exception("Invalid resource type: %s" % self.type)

        self.region = ec2_region

        # Mapping of job.ec2_instance_id => LRMS
        self.resources = {}

        auth = self._auth_fn()
        self.ec2_access_key = auth.ec2_access_key
        self.ec2_secret_key = auth.ec2_secret_key
        if ec2_url:
            self.ec2_url = gc3libs.url.Url(ec2_url)
        else:
            self.ec2_url = None

        self.keypair_name = keypair_name
        self.public_key = public_key

        self.image_id = image_id
        self._parse_security_group()


        args = {'aws_access_key_id': self.ec2_access_key,
                'aws_secret_access_key': self.ec2_secret_key,
                }

        if self.ec2_url:
            region = boto.ec2.regioninfo.RegionInfo(name=ec2_region, endpoint=self.ec2_url.hostname)
            args['region'] = region
            args['port'] = self.ec2_url.port
            args['host'] = self.ec2_url.hostname
            args['path'] = self.ec2_url.path
            if self.ec2_url.scheme in ['http']:
                args['is_secure'] = False

        self._conn = boto.connect_ec2(**args)

        # `self.subresource_args` is used to create subresources
        self.subresource_args = extra_args
        self.subresource_args['type'] = self.subresource_type
        for key in ['architecture', 'max_cores', 'max_cores_per_job', 'max_memory_per_core', 'max_walltime']:
            self.subresource_args[key] = self[key]

    def _get_vm(self, vm_id):
        """
        Return the instance with id `vm_id`, if any. 
        """
        # NOTE: since `vm_id` is supposed to be unique, we assume
        # reservations only contains one element.
        reservations = self._conn.get_all_instances(instance_ids=[vm_id])
        instances = dict((i.id, i) for i in reservations[0].instances)
        if vm_id not in instances:
            raise InstanceNotFound("Instance with id %s has not found in EC2 cloud %s" % (vm_id, self.auth.ec2_auth_url))
        return instances[vm_id]

    def _make_resource(self, remote_ip):
        """
        Create a resource associated to the instance with `remote_ip`
        ip using configuration file parameters.
        """
        args = self.subresource_args.copy()
        args['frontend'] = remote_ip
        args['transport'] = "ssh"
        args['name'] = "%s@%s" % (remote_ip, self.name)
        args['auth'] = args['vm_auth']
        cfg = gc3libs.config.Configuration(*gc3libs.Default.CONFIG_FILE_LOCATIONS,
                                            **{'auto_enable_auth': True})
        resource = cfg._make_resource(args)
        time_waited = 0
        gc3libs.log.debug("Waiting for resource %s to become ready" % resource.name)
        while time_waited < gc3libs.Default.EC2_MAX_WAITING_TIME_FOR_VM_TO_BECOME_RUNNING:
            time_waited += gc3libs.Default.EC2_MIN_WAIT_TIME
            try:
                resource.transport.connect()
                break
            except Exception, ex:
                gc3libs.log.debug("Ignoring error while trying to connect to the instance %s: %s" % (remote_ip, ex))
                time.sleep(gc3libs.Default.EC2_MIN_WAIT_TIME)
        return resource
            
        

    def _get_remote_resource(self, vm_id):
        """
        Return the resource associated to the virtual machine with `vm_id`.
        """
        if vm_id not in self.resources:
            vm = self._get_vm(vm_id)
            self.resources[vm_id] = self._make_resource(vm.public_dns_name)
        return self.resources[vm_id]

    def _import_keypair(self):
        """
        Create a new keypair and import the public key defined in the
        configuration file.
        """
        fd = open(os.path.expanduser(self.public_key))
        try:
            key_material = fd.read()
            imported_key = self._conn.import_key_pair(self.keypair_name, key_material)

            gc3libs.log.info("Successfully imported key `%s` with fingerprint `%s`"
                             " as keypir `%s`" % (imported_key.name,
                                                  imported_key.fingerprint,
                                                  self.keypair_name))
        except Exception, ex:
            gc3libs.log.error("Error importing keypair %s: %s" % (self.keypair_name, ex))
            fd.close()
            raise ex

    def _parse_security_group(self):
        """
        Parse configuration file and set `self.security_group_rules`
        with a list of dictionaries containing the rule sets
        """
        rules = self.security_group_rules.split('\n')
        self.security_group_rules = []
        for rule in rules:
            rulesplit = rule.split(':')
            if len(rulesplit) != 4:
                gc3libs.log.warning("Invalid rule specification in `security_group_rules`: %s" % rule)
                continue
            self.security_group_rules.append(
                { 'ip_protocol': rulesplit[0],
                  'from_port':   int(rulesplit[1]),
                  'to_port':     int(rulesplit[2]),
                  'cidr_ip':     rulesplit[3]}
                )

    def _setup_security_groups(self):
        """
        Check the current configuration and set up a security group if
        it does not exist
        """
        if not self.security_group_name:
            gc3libs.log.error("Group name in `security_group_name` configuration option cannot be empty!")
            return
        security_groups = self._conn.get_all_security_groups()
        groups = dict((g.name, g) for g in security_groups)
        # Check if the security group exists already
        if self.security_group_name not in groups:
            try:
                gc3libs.log.info("Creating security group %s" %  self.security_group_name)
                security_group = self._conn.create_security_group(
                    self.security_group_name, "GC3Pie_%s" %  self.security_group_name)
            except Exception, ex:
                gc3libs.log.error("Error creating security group %s: "
                                  "%s" % (self.security_group_name, ex))
                raise Exception("Error creating security group %s: "
                                "%s" % (self.security_group_name, ex))

            for rule in self.security_group_rules:
                try:
                    gc3libs.log.debug("Adding rule %s to security group %s." % (rule, self.security_group_name))
                    security_group.authorize(**rule)
                except Exception, ex:
                    gc3libs.log.error(
                        "Ignoring error adding rule %s to security group %s: %s" \
                            % (str(rule), self.security_group_name, str(ex)))

        else:
            # Check if the security group has all the rules we want
            security_group = groups[self.security_group_name]
            current_rules = []
            for rule in security_group.rules:
                rule_dict = {'ip_protocol' : rule.ip_protocol,
                             'from_port'   : int(rule.from_port),
                             'to_port'     : int(rule.to_port),
                             'cidr_ip'     : str(rule.grants[0]),
                             }
                current_rules.append(rule_dict)

            for new_rule in self.security_group_rules:
                if new_rule not in current_rules:
                    security_group.authorize(**new_rule)

    @same_docstring_as(LRMS.cancel_job)
    def cancel_job(self, app):
        resource = self._get_remote_resource(app.ec2_instance_id)
        return resource.cancel_job(app)

    @same_docstring_as(LRMS.free)
    def free(self, app):
        """
        Free up any remote resources used for the execution of `app`.
        In particular, this should terminate any remote instance used by the job.

        Call this method when `app.execution.state` is anything other
        than `TERMINATED` results in undefined behavior and will
        likely be the cause of errors later on.  Be cautious.
        """
        # XXX: this should probably done only when no other VMs are
        # using this resource.

        # FIXME: freeing the resource from the application is probably
        # not needed since instances are not persistent.

        # resource = self.resources[app.ec2_instance_id]
        # resource.free(app)
        gc3libs.log.debug("Terminating VM with id `%s`" % app.ec2_instance_id) 
        vm = self._get_vm(app.ec2_instance_id)
        vm.terminate()

    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        self.updated = False
        for resource in self.resources.values():
            resource.get_resource_status()
        return self

    @same_docstring_as(LRMS.get_results)
    def get_results(self, job, download_dir, overwrite=False):
        resource = self._get_remote_resource(job.ec2_instance_id)
        return resource.get_results(job, download_dir, overwrite=False)

    @same_docstring_as(LRMS.update_job_state)
    def update_job_state(self, app):
        if app.ec2_instance_id not in self.resources:
            self.resources[app.ec2_instance_id] = self._get_remote_resource(app.ec2_instance_id)
        
        return self.resources[app.ec2_instance_id].update_job_state(app)
        

    def _create_instance(self):
        """
        Create an instance.
        """
        args={'key_name'  : self.keypair_name,
              'min_count' : 1,
              'max_count' : 1}
        if 'instance_type' in self:
            args['instance_type'] = self.instance_type
        if 'user_data' in self:
            args['user_data'] = self.user_data

        # Check if the desired keypair is present
        keypairs = dict((k.name, k) for k in self._conn.get_all_key_pairs())
        if self.keypair_name not in keypairs:
            gc3libs.log.info("Keypair `%s` not found: creating it using public key `%s`" \
                                 % (self.keypair_name, self.public_key))
            # Create keypair if it does not exist and give an error if it
            # exists but have different fingerprint
            self._import_keypair()
        else:
            keyfile = os.path.expanduser(self.public_key)
            if keyfile.endswith('.pub'):
                keyfile = keyfile[:-4]
            else:
                gc3libs.log.warning("`public_key` option in configuration file should contains path to a public key. Found %s instead: %s" % self.public_key)
            try:
                pkey = paramiko.DSSKey.from_private_key_file(keyfile)
            except:
                pkey = paramiko.RSAKey.from_private_key_file(keyfile)

            # Check key fingerprint
            localkey_fingerprint = ':'.join(i.encode('hex') for i in pkey.get_fingerprint())
            if localkey_fingerprint != keypairs[self.keypair_name].fingerprint:
                gc3libs.log.error("Keypair `%s` is present but has different fingerprint: "
                                  "%s != %s. Aborting!" % (self.keypair_name,
                                                           localkey_fingerprint,
                                                           keypairs[self.keypair_name].fingerprint))
                raise Exception("Keypair `%s` is present but has different fingerprint: "
                                "%s != %s. Aborting!" % (self.keypair_name,
                                                         localkey_fingerprint,
                                                         keypairs[self.keypair_name].fingerprint))

        # Setup security groups
        if 'security_group_name' in self:
            self._setup_security_groups()
            args['security_groups'] = [self.security_group_name]

        # FIXME: we should add check/creation of proper security
        # groups
        gc3libs.log.debug("Create new VM using image id `%s`" % self.image_id) 
        reservation = self._conn.run_instances(self.image_id, **args)
        vm = reservation.instances[0]
        
        # wait until the instance is ready
        gc3libs.log.debug("Waiting for VM with id `%s` to go into RUNNING state" % vm.id)
        time_waited=0
        while vm.update() == 'pending':
            if time_waited > gc3libs.Default.EC2_MAX_WAITING_TIME_FOR_VM_TO_BECOME_RUNNING:
                vm.terminate()
                raise Exception("VM %s not ready after %s seconds. Exiting." % (vm.id, time_waited))
            time.sleep(gc3libs.Default.EC2_MIN_WAIT_TIME)
            time_waited += gc3libs.Default.EC2_MIN_WAIT_TIME

        if vm.state != 'running':
            gc3libs.log.error(
                "VM with id `%s` is in %s state. Aborting" % (vm.id, vm.state))
            raise Exception("VM with id `%s` is in %s state. Aborting" % (vm.id, vm.state))

        gc3libs.log.info("VM with id `%s` is now RUNNING and has `public_dns_name` `%s`"
                         " and `private_dns_name` `%s`" % (vm.id, vm.public_dns_name, vm.private_dns_name))
        return vm

    @same_docstring_as(LRMS.submit_job)
    def submit_job(self, job):
        """
        Create an instance, create a resource related to that instance and then submit the job on that instance
        """
        # Create an instance
        # max_count and min_count are set in order to be sure that only one instance will be run

        # FIXME:
        # * check if the job has a ec2_instance_id
        # * if not, create instance
        # * check the status of the instance
        # * if it's in pending, return a temporary error
        # * if it's in error, return a permanent error
        # * if it's running, create the resource:
        #   - submit to the vm. If you have an error, return a temporary error
        #     unless the error reported by the resource is permanent
        #
        # The idea is that int this way we use Engine to retry to submit the job untile all the VMs are running.
        # This also means that we may need to use the pool_max variable...
        vm = self._create_instance()

        job.ec2_instance_id = vm.id

        self.resources[vm.id] = self._make_resource(vm.public_dns_name)
        return self.resources[vm.id].submit_job(job)

    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        job = app.execution
        assert job.has_key('lrms_execdir'), \
            "Missing attribute `lrms_execdir` on `Job` instance passed to `PbsLrms.peek`."

        if size is None:
            size = sys.maxint

        _filename_mapping = generic_filename_mapping(job.lrms_jobname, job.lrms_jobid, remote_filename)
        _remote_filename = os.path.join(job.lrms_execdir, _filename_mapping)

        try:
            self.transport.connect()
            remote_handler = self.transport.open(_remote_filename, mode='r', bufsize=-1)
            remote_handler.seek(offset)
            data = remote_handler.read(size)
        except Exception, ex:
            log.error("Could not read remote file '%s': %s: %s",
                              _remote_filename, ex.__class__.__name__, str(ex))

        try:
            local_file.write(data)
        except (TypeError, AttributeError):
            output_file = open(local_file, 'w+b')
            output_file.write(data)
            output_file.close()
        log.debug('... Done.')

    @same_docstring_as(LRMS.validate_data)
    def validate_data(self, data_file_list=None):
        """
        Supported protocols: file
        """
        for url in data_file_list:
            if not url.scheme in ['file']:
                return False
        return True

    @same_docstring_as(LRMS.close)
    def close(self):
        for resource in self.resources.values():
            resource.close()
        

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="ec2",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
