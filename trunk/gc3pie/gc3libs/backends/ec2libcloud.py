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
from libcloud.compute.types import Provider as ComputeProvider
from libcloud.compute.providers import get_driver as get_compute_driver
from libcloud.compute.types import NodeState

# GC3Pie imports
import gc3libs
import gc3libs.exceptions
import gc3libs.url
from gc3libs import Run
from gc3libs.utils import same_docstring_as
from gc3libs.backends import LRMS

class InstanceNotFound(Exception):
    pass

class ImageSizeNotFound(Exception):
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
                 pool_size_max, pool_size_min, ec2_region, ec2_url,
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

        self.pool_size_max = int(pool_size_max)
        if self.pool_size_max > 0:
            self.pool_size_max = int(self.pool_size_max)
        self.pool_size_min = int(pool_size_min)
        # Mapping of job.ec2_instance_id => LRMS
        self.resources = {}

        auth = self._auth_fn()
        self.ec2_access_key = auth.ec2_access_key
        self.ec2_secret_key = auth.ec2_secret_key
        self.ec2_url = gc3libs.url.Url(ec2_url)

        self.keypair_name = keypair_name
        self.public_key = os.path.expanduser(public_key)

        self.image_id = image_id
        self._parse_security_group()

        # ComputeProvider.EUCALYPTUS or ComputeProvider.EC2 ???
        self.driver = get_compute_driver(ComputeProvider.EUCALYPTUS)

        gc3libs.log.info("Region information for EC2 libcloud backend is NOT used.")
        self._conn = self.driver(self.ec2_access_key,
                                 self.ec2_secret_key, 
                                 host=self.ec2_url.hostname,
                                 port=self.ec2_url.port,
                                 path=self.ec2_url.path, 
                                 secure=False)



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
        vms = self._conn.list_nodes()
        matching_vms = [vm for vm in vms if vm.id == vm_id]
        if not matching_vms:
            gc3libs.log.error("No VM matching id %s found. Aborting" % vm_id)
            raise InstanceNotFound("No VM matching id %s found. Aborting" % vm_id)
        elif len(matching_vms) > 1:
            gc3libs.log.warning("More than one VM matching id %s found! This should NEVE happen. I will use the first one and continue!" % vm_id)

        return matching_vms[0]

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
            self.resources[vm_id] = self._make_resource(vm.private_ip[0])
        return self.resources[vm_id]

    def _import_keypair(self):
        """
        Create a new keypair and import the public key defined in the
        configuration file.
        """
        fd = open(self.public_key)
        try:
            key_material = fd.read()
            imported_key = self._conn.ex_import_keypair(
                self.keypair_name, self.public_key)

            gc3libs.log.info("Successfully imported key `%s` with fingerprint `%s`"
                             " as keypir `%s`" % (imported_key.name,
                                                  imported_key.fingerprint,
                                                  self.keypair_name))
        except:
            fd.close()

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

        security_groups = self._conn.ex_list_security_groups()
        # Check if the security group exists already
        if self.security_group_name not in security_groups:
            try:
                security_group = self._conn.ex_create_security_group(
                    self.security_group_name, "GC3Pie_%s" %  self.security_group_name)
            except Exception, ex:
                gc3libs.log.error("Error creating security group %s: "
                                  "%s" % (self.security_group_name, ex))
                raise Exception("Error creating security group %s: "
                                "%s" % (self.security_group_name, ex))

            for rule in self.security_group_rules:
                try:
                    self._conn.ex_authorize_security_group(
                        self.security_group_name, 
                        rule['from_port'],
                        rule['to_port'],
                        rule['cidr_ip'],
                        rule['ip_protocol'])
                except Exception, ex:
                    gc3libs.log.error(
                        "Ignoring error adding rule %s to security group %s: %s" \
                            % (str(rule), security_group, str(ex)))
                    

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
        self._conn.destroy_node(vm)

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
        args={'ex_keyname'  : self.keypair_name,
              'name' : "GC3Pie:%s" % self.name,
              'ex_mincount' : '1',
              'ex_maxcount' : '1'}

        if 'user_data' in self:
            args['ex_user_data'] = self.user_data

        # Check if the desired keypair is present
        try:
            self._conn.ex_describe_keypairs(self.keypair_name)
        except Exception, ex:
            # Check exception or return status. If it does not exist
            # we have to create one
            self._import_keypair()

        keyfile = os.path.expanduser(self.public_key)
        try:
            pkey = paramiko.DSSKey.from_private_key_file(keyfile[:-4])
        except:
            pkey = paramiko.RSAKey.from_private_key_file(keyfile[:-4])


        # Setup security groups
        if 'security_group_name' in self:
            self._setup_security_groups()
            args['ex_securitygroup'] = self.security_group_name

        # FIXME: we should add check/creation of proper security
        # groups
        gc3libs.log.debug("Create new VM using image id `%s`" % self.image_id)
        images = self._conn.list_images()
        matching_images = [image for image in images if image.id == self.image_id]
        if not matching_images:
            raise ImageNotFound("Image with id `%s` not found: please specify a different `image_id` value in the configuration file." % self.image_id)
        args['image'] = matching_images[0]

        sizes = self._conn.list_sizes()
        default_size = [size for size in sizes if size.id == 'm1.small'][0]
        if 'instance_type' in self:
            matching_sizes = [size for size in sizes if size.id==self.instance_type]
            if not matching_sizes:
                raise ImageSizeNotFound("Instance type `%s` does not exists: please specify a different `instance_type` in the configuration file. Using %s instead" % (self.instance_type, default_size.id))
            args['size'] = default_size
        else:
            gc3libs.log.info("No valid instance type has been defined in configuration file. Using %s" % default_size.id)
            args['size'] = default_size
        
        vm = self._conn.create_node(**args)

        # wait until the instance is ready
        gc3libs.log.debug("Waiting for VM with id `%s` to go into RUNNING state" % vm.id)
        time_waited=0
        vm = self._get_vm(vm.id)
        while vm.state == NodeState.PENDING or not vm.private_ip:
            if time_waited > gc3libs.Default.EC2_MAX_WAITING_TIME_FOR_VM_TO_BECOME_RUNNING:
                self._conn.destroy_node(vm)
                raise Exception("VM %s not ready after %s seconds. Exiting." % (vm.id, time_waited))
            time.sleep(gc3libs.Default.EC2_MIN_WAIT_TIME)
            time_waited += gc3libs.Default.EC2_MIN_WAIT_TIME
            vm = self._get_vm(vm.id)

        if self._get_vm(vm.id).state != NodeState.RUNNING:
            gc3libs.log.error("Instance %s is not in running state: %s" % (vm.id, self._get_vm(vm.id).state))
        else:
            gc3libs.log.info("VM with id `%s` is now RUNNING and has `public_ip` `%s`"
                             " and `private_ip` `%s`" % (vm.id, vm.public_ip, vm.private_ip))
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

        self.resources[vm.id] = self._make_resource(vm.private_ip[0])
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
