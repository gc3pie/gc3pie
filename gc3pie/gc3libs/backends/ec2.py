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
try:
    import boto
    import boto.ec2.regioninfo
except ImportError:
    from gc3libs.exceptions import ConfigurationError
    raise ConfigurationError(
        "EC2 backend has been requested but no `boto` package"
        " was found. Please, install `boto` with `pip install boto`"
        " or `easy_install boto` and try again, or update your"
        " configuration file.")

# GC3Pie imports
import gc3libs
from gc3libs.exceptions import RecoverableError, UnrecoverableError, \
    ConfigurationError
import gc3libs.url
from gc3libs import Run
from gc3libs.utils import same_docstring_as
from gc3libs.backends import LRMS

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
                 ec2_region, keypair_name, public_key,
                 image_id=None, image_name=None, ec2_url=None,
                 instance_type=None, auth=None, **extra_args):
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
            raise UnrecoverableError("Invalid resource type: %s" % self.type)

        self.region = ec2_region

        # Mapping of job.ec2_instance_id => LRMS
        self.resources = {}

        auth = self._auth_fn()
        self.ec2_access_key = auth.ec2_access_key
        self.ec2_secret_key = auth.ec2_secret_key
        if ec2_url:
            self.ec2_url = gc3libs.url.Url(ec2_url)
        else:
            self.ec2_url = os.getenv('EC2_URL')

        self.keypair_name = keypair_name
        self.public_key = public_key.strip()
        self.image_id = image_id
        self.image_name = image_name
        self.instance_type = instance_type

        self._parse_security_group()
        self._conn = None
        self._vms = {}

        # `self.subresource_args` is used to create subresources
        self.subresource_args = extra_args
        self.subresource_args['type'] = self.subresource_type
        for key in ['architecture', 'max_cores', 'max_cores_per_job',
                    'max_memory_per_core', 'max_walltime']:
            self.subresource_args[key] = self[key]
        # ShellcmdLrms by default trusts the configuration, instead of
        # checking the real amount of memory and number of cpus, but
        # we need the real values instead.
        if self.subresource_type == gc3libs.Default.SHELLCMD_LRMS:
            self.subresource_args['override'] = 'True'

        if not image_name and not image_id:
            raise ConfigurationError(
                "No `image_id` or `image_name` has been specified in the"
                " configuration file.")

    def _connect(self):
        """
        Connect to the EC2 endpoint and check that the required
        `image_id` exists.
        """
        if self._conn is not None:
            return

        args = {'aws_access_key_id': self.ec2_access_key,
                'aws_secret_access_key': self.ec2_secret_key,
                }

        if self.ec2_url:
            region = boto.ec2.regioninfo.RegionInfo(
                name=self.region,
                endpoint=self.ec2_url.hostname)
            args['region'] = region
            args['port'] = self.ec2_url.port
            args['host'] = self.ec2_url.hostname
            args['path'] = self.ec2_url.path
            if self.ec2_url.scheme in ['http']:
                args['is_secure'] = False

        self._conn = boto.connect_ec2(**args)

        all_images = self._conn.get_all_images()
        if self.image_id:
            if self.image_id not in [i.id for i in all_images]:
                raise RuntimeError(
                    "Image with id `%s` not found. Please specify a valid"
                    " image name or image id in the configuration file."
                    % self.image_id)
        else:
            images = [i for i in all_images if i.name == self.image_name]
            if not images:
                raise RuntimeError(
                    "Image with name `%s` not found. Please specify a valid"
                    " image name or image id in the configuration file."
                    % self.image_name)
            elif len(images) != 1:
                raise RuntimeError(
                    "Multiple images found with name `%s`: %s"
                    " Please specify an unique image id in configuration"
                    " file." % (self.image_name, [i.id for i in images]))
            self.image_id = images[0].id

    def _create_instance(self, image_id, instance_type=None):
        """
        Create an instance using the image `image_id` and instance
        type `instance_type`. If not `instance_type` is defined, use
        the default.

        This method will also setup the keypair and the security
        groups, if needed.
        """
        self._connect()

        args = {'key_name':  self.keypair_name,
                'min_count': 1,
                'max_count': 1}
        if instance_type:
            args['instance_type'] = instance_type

        if 'user_data' in self:
            args['user_data'] = self.user_data

        # Check if the desired keypair is present
        keypairs = dict((k.name, k) for k in self._conn.get_all_key_pairs())
        if self.keypair_name not in keypairs:
            gc3libs.log.info(
                "Keypair `%s` not found: creating it using public key `%s`"
                % (self.keypair_name, self.public_key))
            # Create keypair if it does not exist and give an error if it
            # exists but have different fingerprint
            self._import_keypair()
        else:
            keyfile = os.path.expanduser(self.public_key)
            if keyfile.endswith('.pub'):
                keyfile = keyfile[:-4]
            else:
                gc3libs.log.warning(
                    "`public_key` option in configuration file should contains"
                    " path to a public key. Found %s instead: %s",
                    self.public_key)
            try:
                pkey = paramiko.DSSKey.from_private_key_file(keyfile)
            except paramiko.PasswordRequiredException:
                raise RuntimeError("Key %s is encripted with a password. Please, use"
                             " an unencrypted key or use ssh-agent" % keyfile)
            except paramiko.SSHException, ex:
                gc3libs.log.debug("File `%s` is not a valid DSS private key:"
                                  " %s", keyfile, ex)
                try:
                    pkey = paramiko.RSAKey.from_private_key_file(keyfile)
                except paramiko.PasswordRequiredException:
                    raise RuntimeError(
                        "Key %s is encripted with a password. Please, use"
                        " an unencrypted key or use ssh-agent" % keyfile)
                except paramiko.SSHException, ex:
                    gc3libs.log.debug("File `%s` is not a valid RSA private "
                                      "key: %s", keyfile, ex)
                    raise ValueError("Public key `%s` is neither a valid "
                                     "RSA key nor a DSS key" % self.public_key)

            # Check key fingerprint
            localkey_fingerprint = str.join(
                ':', (i.encode('hex') for i in pkey.get_fingerprint()))
            if localkey_fingerprint != keypairs[self.keypair_name].fingerprint:
                gc3libs.log.error(
                    "Keypair `%s` is present but has different fingerprint: "
                    "%s != %s. Aborting!" % (
                        self.keypair_name,
                        localkey_fingerprint,
                        keypairs[self.keypair_name].fingerprint))
                raise UnrecoverableError(
                    "Keypair `%s` is present but has different fingerprint: "
                    "%s != %s. Aborting!" % (
                        self.keypair_name,
                        localkey_fingerprint,
                        keypairs[self.keypair_name].fingerprint))

        # Setup security groups
        if 'security_group_name' in self:
            self._setup_security_groups()
            args['security_groups'] = [self.security_group_name]

        # FIXME: we should add check/creation of proper security
        # groups
        gc3libs.log.debug("Create new VM using image id `%s`", image_id)
        try:
            reservation = self._conn.run_instances(image_id, **args)
        except Exception, ex:
            raise UnrecoverableError("Error starting instance: %s" % str(ex))
        vm = reservation.instances[0]

        gc3libs.log.info(
            "VM with id `%s` has been created and is in %s state.",
            vm.id, vm.state)

        return vm

    def _get_remote_resource(self, vm):
        """
        Return the resource associated to the virtual machine with
        `vm`.

        Updates the internal list of available resources if needed.
        """
        if vm.id not in self.resources:
            self.resources[vm.id] = self._make_resource(vm.public_dns_name)
        return self.resources[vm.id]

    def _get_vm(self, vm_id):
        """
        Return the instance with id `vm_id`, raises an error if there
        is no such instance with that id.
        """
        # Return cached value if we already have it
        if vm_id in self._vms:
            return self._vms[vm_id]

        self._connect()

        # NOTE: since `vm_id` is supposed to be unique, we assume
        # reservations only contains one element.
        try:
            reservations = self._conn.get_all_instances(instance_ids=[vm_id])
        except boto.exception.EC2ResponseError, ex:
            gc3libs.log.error(
                "Error getting VM %s from %s: %s", vm_id, self.ec2_url, ex)
            raise UnrecoverableError(
                "Error getting VM %s from %s: %s" % (vm_id, self.ec2_url, ex))
        if not reservations:
            raise UnrecoverableError(
                "Instance with id %s has not found in EC2 cloud %s"
                % (vm_id, self.ec2_url))

        instances = dict((i.id, i) for i in reservations[0].instances
                         if reservations)
        if vm_id not in instances:
            raise UnrecoverableError(
                "Instance with id %s has not found in EC2 cloud %s"
                % (vm_id, self.ec2_url))
        vm = instances[vm_id]
        self._vms[vm_id] = vm
        return vm

    def _import_keypair(self):
        """
        Create a new keypair and import the public key defined in the
        configuration file.
        """
        fd = open(os.path.expanduser(self.public_key))
        try:
            key_material = fd.read()
            imported_key = self._conn.import_key_pair(
                self.keypair_name, key_material)

            gc3libs.log.info(
                "Successfully imported key `%s` with fingerprint `%s`"
                " as keypir `%s`" % (imported_key.name,
                                     imported_key.fingerprint,
                                     self.keypair_name))
        except Exception, ex:
            fd.close()
            raise UnrecoverableError("Error importing keypair %s: %s"
                                     % self.keypair_name, ex)

    def _make_resource(self, remote_ip):
        """
        Create a resource associated to the instance with `remote_ip`
        ip using configuration file parameters.
        """
        if not remote_ip:
            raise ValueError(
                "_make_resource: `remote_ip` must be a valid IP or hostname.")
        gc3libs.log.info("Creating remote ShellcmdLrms resource for ip %s",
                         remote_ip)
        args = self.subresource_args.copy()
        args['frontend'] = remote_ip
        args['transport'] = "ssh"
        args['name'] = "%s@%s" % (remote_ip, self.name)
        args['auth'] = args['vm_auth']
        cfg = gc3libs.config.Configuration(
            *gc3libs.Default.CONFIG_FILE_LOCATIONS,
            **{'auto_enable_auth': True})
        resource = cfg._make_resource(args)
        return resource

    def _parse_security_group(self):
        """
        Parse configuration file and set `self.security_group_rules`
        with a list of dictionaries containing the rule sets
        """
        rules = self.security_group_rules.split(',')
        self.security_group_rules = []
        for rule in rules:
            rulesplit = rule.strip().split(':')
            if len(rulesplit) != 4:
                gc3libs.log.warning("Invalid rule specification in"
                                    " `security_group_rules`: %s" % rule)
                continue
            self.security_group_rules.append(
                {'ip_protocol': rulesplit[0],
                 'from_port':   int(rulesplit[1]),
                 'to_port':     int(rulesplit[2]),
                 'cidr_ip':     rulesplit[3],
                 })

    def _setup_security_groups(self):
        """
        Check the current configuration and set up the security group
        if it does not exist.
        """
        if not self.security_group_name:
            gc3libs.log.error("Group name in `security_group_name`"
                              " configuration option cannot be empty!")
            return
        security_groups = self._conn.get_all_security_groups()
        groups = dict((g.name, g) for g in security_groups)
        # Check if the security group exists already
        if self.security_group_name not in groups:
            try:
                gc3libs.log.info("Creating security group %s",
                                 self.security_group_name)
                security_group = self._conn.create_security_group(
                    self.security_group_name,
                    "GC3Pie_%s" % self.security_group_name)
            except Exception, ex:
                gc3libs.log.error("Error creating security group %s: %s",
                                  self.security_group_name, ex)
                raise UnrecoverableError(
                    "Error creating security group %s: %s"
                    % (self.security_group_name, ex))

            for rule in self.security_group_rules:
                try:
                    gc3libs.log.debug(
                        "Adding rule %s to security group %s.",
                        rule, self.security_group_name)
                    security_group.authorize(**rule)
                except Exception, ex:
                    gc3libs.log.error("Ignoring error adding rule %s to"
                                      " security group %s: %s", str(rule),
                                      self.security_group_name, str(ex))

        else:
            # Check if the security group has all the rules we want
            security_group = groups[self.security_group_name]
            current_rules = []
            for rule in security_group.rules:
                rule_dict = {'ip_protocol':  rule.ip_protocol,
                             'from_port':    int(rule.from_port),
                             'to_port':      int(rule.to_port),
                             'cidr_ip':      str(rule.grants[0]),
                             }
                current_rules.append(rule_dict)

            for new_rule in self.security_group_rules:
                if new_rule not in current_rules:
                    security_group.authorize(**new_rule)

    # Public methods

    @same_docstring_as(LRMS.cancel_job)
    def cancel_job(self, app):
        resource = self._get_remote_resource(self._get_vm(app.ec2_instance_id))
        return resource.cancel_job(app)

    @same_docstring_as(LRMS.free)
    def free(self, app):
        """
        Free up any remote resources used for the execution of `app`.
        In particular, this should terminate any remote instance used
        by the job.

        Call this method when `app.execution.state` is anything other
        than `TERMINATED` results in undefined behavior and will
        likely be the cause of errors later on.  Be cautious.
        """
        # XXX: this should probably done only when no other VMs are
        # using this resource.

        # FIXME: freeing the resource from the application is probably
        # not needed since instances are not persistent.

        # freeing the resource from the application is now needed as
        # the same instanc may run multiple applications
        resource = self._get_remote_resource(self._get_vm(app.ec2_instance_id))
        resource.free(app)

        # FIXME: current approach in terminating running instances:
        # if no more applications are currently running, turn the instance off
        # check with the associated resource
        resource.get_resource_status()
        if len(resource.job_infos) == 0:
            # turn VM off
            vm = self._get_vm(app.ec2_instance_id)
            gc3libs.log.info("VM instance %s at %s is no longer needed."
                             " Terminating.", vm.id, vm.public_dns_name)
            del self.resources[vm.id]
            vm.terminate()
            del self._vms[vm.id]

    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        self.updated = False
        # Since we create the resource *before* the VM is actually up
        # & running, it's possible that the `frontend` value of the
        # resources points to a non-existent hostname. Therefore, we
        # have to update them with valid public_ip, if they are
        # present.
        for vm_id, resource in self.resources.items():
            try:
                resource.get_resource_status()
            except Exception, ex:
                gc3libs.log.error("Got error while updating resource %s:"
                                  " %s", resource.name, ex)
                gc3libs.log.debug(
                    "Re-creating resource %s: it may be possible that it has "
                    "been created too early", resource.name)
                vm = self._get_vm(vm_id)
                vm.update()
                self.resources[vm.id] = self._make_resource(vm.public_dns_name)
                try:
                    resource.get_resource_status()
                except Exception, ex:
                    gc3libs.log.warning(
                        "Ignoring ERROR while updating EC2 subresource %s: %s",
                        resource.name, ex)
        return self

    @same_docstring_as(LRMS.get_results)
    def get_results(self, job, download_dir, overwrite=False):
        resource = self._get_remote_resource(self._get_vm(job.ec2_instance_id))
        return resource.get_results(job, download_dir, overwrite=False)

    @same_docstring_as(LRMS.update_job_state)
    def update_job_state(self, app):
        if app.ec2_instance_id not in self.resources:
            self.resources[app.ec2_instance_id] = self._get_remote_resource(
                self._get_vm(app.ec2_instance_id))

        return self.resources[app.ec2_instance_id].update_job_state(app)

    @same_docstring_as(LRMS.submit_job)
    def submit_job(self, job):
        """
        XXX: update doc

        This method create an instance on the cloud, then will create
        a resource to use this instance.

        Since the creation of VMs can take some time, instead of
        waiting for the VM to be ready we will raise an
        `RecoverableError`:class: every time we try to submit a job on
        this resource but the associated VM is not yet ready and we
        cannot yet connect to the it via ssh, which is not an issue
        when you call `submit_job` from the `Engine`:class:.

        When the instance is up&running and the associated resource is
        created, this method will call the `submit_job` method of the
        resource and returns its return value.
        """
        self._connect()
        # Updating resource is needed to update the subresources. This
        # is not always done before the submit_job because of issue
        # nr.  386:
        #     http://code.google.com/p/gc3pie/issues/detail?id=386
        self.get_resource_status()

        # if the job has an `ec2_instance_id` attribute, it means that
        # a VM for this job has already been created. If not, a new
        # instance is created.

        pending_vms = []
        vm = None
        if hasattr(job, 'ec2_instance_id'):
            # This job was previously submitted but we already raised
            # an exception, probably because the VM was not ready
            # yet. Let's try again!
            gc3libs.log.info("Job already allocated to VM %s.",
                             job.ec2_instance_id)
            try:
                vm = self._get_vm(job.ec2_instance_id)
            except UnrecoverableError:
                # It is possible that the VM has been terminated before
                # after this job was assigned to it, during a previous
                # submission. No problem, let's continue and submit it to
                # a new VM, if possible
                gc3libs.log.error(
                    "The VM this job was assigned to is no longer available. "
                    "Trying to submit to a different VM.")

        # XXX: There has to be a better way in order to exit from the
        # `if hasattr()` statement and skip to the next block...
        if vm:
            try:
                resource = self._get_remote_resource(vm)
            except Exception, ex:
                # XXX: It may be possible that the there is not vm
                # with this id, because it has been terminated. We
                # must be able to spot this, and run a
                # del job.ec2_instance_id
                gc3libs.log.error(
                    "Error while creating resource for vm %s: %s",
                    vm.id, ex)
                # Check if the vm exists
                raise RecoverableError(
                    "Error while creating resource for vm %s: %s" %
                    (vm.id, ex))

            try:
                resource.submit_job(job)
                gc3libs.log.info("Job successfully submitted to remote "
                                 "resource %s.", resource.name)
                return job
            except Exception, ex:
                gc3libs.log.warning(
                    "Unable to submit job to resource %s: %s",
                    resource.name, ex)
                if resource.free_slots >= job.requested_cores:
                    pending_vms.append(vm.id)
                raise RecoverableError(
                    "Remote VM `%s` not yet ready: %s" %
                    (job.ec2_instance_id, str(ex)))
        # This is the first attempt to submit a job.  First of all,
        # let's try to submit it to one of the resource we already
        # created.
        gc3libs.log.debug("First submission of job %s. Looking for a free VM "
                          "to use", job)
        for vm_id, resource in self.resources.items():
            try:
                resource.submit_job(job)
                job.ec2_instance_id = vm_id
                job.changed = True
                gc3libs.log.info(
                    "Job successfully submitted to remote resource %s.",
                    resource.name)
                return job
            except gc3libs.exceptions.LRMSSubmitError, ex:
                # Selected resource was not able to submit.
                # The associated VM could be not ready yet...
                gc3libs.log.debug(
                    "Ignoring error while submit to resource %s: %s. ",
                    resource.name, str(ex))
                if resource.free_slots >= job.requested_cores:
                    pending_vms.append(vm_id)

        if pending_vms:
            # No available resource was found, but some remote
            # resource with enough free slots was found, so let's wait
            # until the next iteration.
            gc3libs.log.info(
                "No available resource was found, but some VM is still in "
                "`pending` state. Waiting until the next iteration before "
                "creating a new VM.")
            raise RecoverableError(
                "Delaying submission until some of the VMs currently pending "
                "is ready.")

        gc3libs.log.debug("No available resource was found. Creating a new VM")
        # No available resource was found. Let's create a new VM.
        image_id = job.get('ec2_image_id', self.image_id)
        instance_type = job.get('ec2_instance_type', self.instance_type)
        vm = self._create_instance(image_id, instance_type=instance_type)

        # XXX: If we do this, we need to make sure the resource associated to the VM
        # is aware of this 'reservation'
        # otherwise in the next submit, another job could be launched on it
        # thus preventing the current job to start.
        job.ec2_instance_id = vm.id

        job.changed = True

        # get the resource associated to it or create a new one if
        # none has been created yet.
        resource = self._get_remote_resource(vm)

        if vm.state == 'running':
            # The VM is created, but it may be not ready yet.
            gc3libs.log.info(
                "VM with id `%s` is now RUNNING and has `public_dns_name` `%s`"
                " and `private_dns_name` `%s`",
                vm.id, vm.public_dns_name, vm.private_dns_name)
            # Submit the job to the remote resource
            return resource.submit_job(job)
        elif vm.state == 'pending':
            # The VM is not ready yet, retry later.
            raise RecoverableError(
                "VM with id `%s` still in PENDING state, waiting" % vm.id)
        elif vm.state == 'error':
            # The VM is in error state: exit.
            raise UnrecoverableError(
                "VM with id `%s` is in ERROR state."
                " Aborting submission of job %s" % (vm.id, str(job)))
        elif vm.state in ['shutting-down', 'terminated', 'stopped']:
            # The VM has been terminated, probably from outside GC3Pie.
            raise UnrecoverableError("VM with id `%s` is in terminal state."
                                     " Aborting submission of job %s"
                                     % (vm.id, str(job)))

    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        resource = self._get_remote_resource(
            self._get_vm(app.ec2_instance_id))
        return resource.peek(app, remote_filename, local_file, offset, size)

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
        for vm_id, resource in self.resources.items():
            try:
                resource.get_resource_status()
            except Exception, ex:
                gc3libs.log.warning(
                    "Error while updating EC2 subresource %s: %s. "
                    "Turning off associated VM.", resource.name, ex)
                if len(resource.job_infos) == 0:
                    # turn VM off
                    vm = self._get_vm(vm_id)
                    gc3libs.log.warning(
                        "VM instance %s at %s is no longer needed. "
                        "You may need to terminate it manually.",
                        vm.id, vm.public_dns_name)
            resource.close()


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="ec2",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
