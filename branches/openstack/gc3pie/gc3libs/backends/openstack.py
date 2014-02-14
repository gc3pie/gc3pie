#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012-2014, GC3, University of Zurich. All rights reserved.
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
import re

# OpenStack APIs
try:
    from novaclient import client as NovaClient
    from novaclient.exceptions import NotFound
except ImportError:
    from gc3libs.exceptions import ConfigurationError
    raise ConfigurationError(
        "OpenStack backend has been requested but no `python-novaclient`"
        " package was found. Please, install `python-novaclient` with"
        "`pip install python-novaclient` or `easy_install python-novaclient`"
        " and try again, or update your configuration file.")

# GC3Pie imports
import gc3libs
from gc3libs.exceptions import RecoverableError, UnrecoverableError, \
    ConfigurationError, LRMSSkipSubmissionToNextIteration, MaximumCapacityReached, UnrecoverableAuthError
import gc3libs.url
from gc3libs import Run
from gc3libs.utils import mkdir, same_docstring_as
from gc3libs.backends import LRMS
from gc3libs.session import Session
from gc3libs.persistence import Persistable
from gc3libs.utils import cache_for

available_subresource_types = [gc3libs.Default.SHELLCMD_LRMS]

### VMPool
# Note: this is took from ec2 backend. It would be nice to have an
# abstraction of VMPool instead!

class InstanceNotFound(UnrecoverableError):
    """Specified instance was not found"""


class VMPool(object):
    """
    Persistable container for a list of VM objects.

    Holds a list of all VM IDs of inserted VMs, and a cache of the
    actual VM objects. If information about a VM is requested, which
    is not currently in the cache, a request is made to the cloud
    provider API (through the `conn` object passed to the constructor)
    to get that information.

    The `VMPool`:class: looks like a mixture of the `set` and `dict`
    interfaces:

    * VMs are added to the container using the `add_vm` method::

        | >>> vmpool.add_vm(vm1)

      (There is no dictionary-like ``D[x]=y`` setter syntax, though,
      as that would require spelling out the VM ID.)

    * VMs can be removed via the `remove_vm` method or the `del`
      syntax; in both cases it's the VM *ID* that must be passed::

       | >>> vmpool.remove_vm(vm1)

       | >>> del vmpool[vm1]

    * Iterating over a `VMPool`:class: instance returns the VM IDs.

    * Other sequence methods work as expected: the VM info can be
      accessed with the usual ``[]`` lookup syntax from its ID, the
      ``len()`` of a `VMPool`:class: object is the total number of VM
      IDs registered, etc..

    `VMPool`:class: objects can be persisted using the
    `gc3libs.persistence`:module: framework.  Note however that the VM
    cache will be empty upon loading a `VMPool` instance from
    persistent storage.
    """

    def __init__(self, path, nova_client):
        # remove trailing `/` so that we can use the last path
        # component as a name
        if path.endswith('/'):
            self.path = path[:-1]
        else:
            self.path = path
        self.name = os.path.basename(self.path)

        if os.path.isdir(path):
            self.load()
        else:
            mkdir(self.path)
            self._vm_ids = set()

        self.client = nova_client
        self._vm_cache = {}
        self.changed = False

    def __delitem__(self, vm_id):
        """
        x.__delitem__(self, vm_id) <==> x.remove_vm(vm_id)
        """
        return self.remove_vm(vm_id)

    def __getitem__(self, vm_id):
        """
        x.__getitem__(vm_id) <==> x.get_vm(vm_id)
        """
        return self.get_vm(vm_id)

    def __getstate__(self):
        # only save path and list of IDs, the rest can be
        # reconstructed from these two (see `__setstate__`)
        return dict(
            path=self.path,
            _vm_ids=self._vm_ids,
        )

    def __iter__(self):
        """
        Iterate over the list of known VM ids.
        """
        # We need to create a new list because the _vm_ids set may be
        # updated during iteration.
        return iter(list(self._vm_ids))

    def __len__(self):
        return len(self._vm_ids)

    def __repr__(self):
        return self._vm_ids.__repr__()

    def __setstate__(self, state):
        self.path = state['path']
        self.name = os.path.basename(self.path)
        self.client = None
        self._vm_cache = {}
        self._vm_ids = state['_vm_ids']

    def __str__(self):
        return "VMPool('%s') : %s" % (self.name, self._vm_ids)

    def add_vm(self, vm):
        """
        Add a VM object to the list of VMs.
        """
        gc3libs.utils.touch(os.path.join(self.path, vm.id))
        self._vm_ids.add(vm.id)
        self._vm_cache[vm.id] = vm
        self.changed = True

    def remove_vm(self, vm_id):
        """
        Remove VM with id `vm_id` from the list of known VMs. No
        connection to the OpenStack endpoint is performed.
        """
        if os.path.exists(os.path.join(self.path, vm_id)):
            try:
                os.remove(os.path.join(self.path, vm_id))
            except OSError, err:
                if err.errno == 2: # ENOENT, "No such file or directory"
                    # ignore - some other process might have removed it
                    pass
                else:
                    raise
        if vm_id in self._vm_ids:
            self._vm_ids.remove(vm_id)
        if vm_id in self._vm_cache:
            del self._vm_cache[vm_id]
        self.changed = True

    def get_vm(self, vm_id, force_reload=False):
        """
        Return the VM object with id `vm_id`.

        If it is found in the local cache, that object is
        returned. Otherwise a new VM object is searched for in the 
        OpenStack endpoint.
        """
        # return cached info, if any
        if not force_reload and vm_id in self._vm_cache:
            return self._vm_cache[vm_id]

        # XXX: should this be an `assert` instead?
        if not self.client:
            raise UnrecoverableError(
                "No connection set for `VMPool('%s')`" % self.path)

        # contact OpenStack API to get VM info
        try:
            vm = self.client.servers.get(vm_id)
        except NotFound:
            raise UnrecoverableError(
                "No instance with id %s has been found." % vm_id)
        self._vm_cache[vm_id] = vm
        if vm_id not in self._vm_ids:
            self._vm_ids.add(vm_id)
            self.changed = True
        return vm

    def get_all_vms(self):
        """
        Return list of all known VMs.
        """
        vms = []
        for vm_id in self._vm_ids:
            try:
                vms.append(self.get_vm(vm_id))
            except UnrecoverableError as ex:
                gc3libs.log.warning(
                    "Cloud resource `%s`: ignoring error while trying to "
                    "get information on VM wiht id `%s`: %s" \
                    % (self.name, vm_id, ex))
        return vms

    def load(self):
        """Populate list of VM IDs from the data saved on disk."""
        self._vm_ids = set([
            entry for entry in os.listdir(self.path)
            if not entry.startswith('.')
        ])

    def save(self):
        """Ensure all VM IDs will be found by the next `load()` call."""
        for vm_id in self._vm_ids:
            gc3libs.utils.touch(os.path.join(self.path, vm_id))

    def update(self, remove=False):
        """
        Synchronize list of VM IDs with contents of disk storage.

        If optional argument `remove` is true, then remove VMs whose
        ID is no longer present in the on-disk storage.
        """
        ids_on_disk = set([
            entry for entry in os.listdir(self.path)
            if not entry.startswith('.')
        ])
        added = ids_on_disk - self._vm_ids
        for vm_id in added:
            self._vm_ids.add(vm_id)
        if remove:
            removed = self._vm_ids - ids_on_disk
            for vm_id in removed:
                self.remove_vm(vm_id)

ERROR_STATES = ['ERROR', 'UNNKNOWN']
PENDING_STATES = ['BUILD', 'REBUILD', 'REBOOT', 'HARD_REBOOT', 'RESIZE', 'REVERT_RESIZE']

class OpenStackLrms(LRMS):
    """
    OpenStack resource.
    """
    RESOURCE_DIR = '$HOME/.gc3/openstack.d'
    def __init__(self, name,
                 # these parameters are inherited from the `LRMS` class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime,
                 # these are specific of the OpenStackLrms class
                 keypair_name, public_key, os_region=None,
                 image_id=None, image_name=None, os_auth_url=None,
                 instance_type=None, auth=None, vm_pool_max_size=None,
                 user_data=None, **extra_args):
        LRMS.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth, **extra_args)

        self.free_slots = int(max_cores)
        self.user_run = 0
        self.user_queued = 0
        self.queued = 0
        self.vm_pool_max_size = vm_pool_max_size
        if vm_pool_max_size is not None:
            try:
                self.vm_pool_max_size = int(self.vm_pool_max_size)
            except ValueError:
                raise ConfigurationError(
                    "Value for `vm_pool_max_size` must be an integer,"
                    " was %s instead." % vm_pool_max_size)

        self.subresource_type = self.type.split('+', 1)[1]
        if self.subresource_type not in available_subresource_types:
            raise UnrecoverableError("Invalid resource type: %s" % self.type)

        # Mapping of job.os_instance_id => LRMS
        self.subresources = {}
    
        auth = self._auth_fn()
        if os_auth_url is None:
            os_auth_url = os.getenv('OS_AUTH_URL')
        if os_auth_url is None:
            raise gc3libs.exceptions.InvalidArgument(
                "Cannot connect to the OpenStack API:"
                " No 'OS_AUTH_URL' environment variable defined,"
                " and no 'os_auth_url' argument passed to the EC2 backend.")
        self.os_auth_url = os_auth_url
        self.os_username = auth.os_username
        self.os_password = auth.os_password
        self.os_tenant_name = auth.os_project_name
        self.os_region_name = os_region
        if self.os_auth_url is None:
            raise gc3libs.exceptions.InvalidArgument(
                "Cannot connect to the OpenStack API:"
                " No 'os_auth_url' argument passed to the OpenStack backend.")
        # Keypair names can only contain alphanumeric chars!
        if re.match(r'.*\W.*', keypair_name):
            raise ConfigurationError(
                "Keypair name `%s` is invalid: keypair names can only contain "
                "alphanumeric chars: [a-zA-Z0-9_]" % keypair_name)
        self.keypair_name = keypair_name
        self.public_key = os.path.expanduser(os.path.expandvars(public_key.strip()))
        self.image_id = image_id
        self.image_name = image_name
        self.instance_type = instance_type
        self.user_data = user_data

        self._parse_security_group()
        self._conn = None

        # `self.subresource_args` is used to create subresources
        self.subresource_args = extra_args
        self.subresource_args['type'] = self.subresource_type
        self.subresource_args['architecture'] = self['architecture']
        self.subresource_args['max_cores'] = self['max_cores']
        self.subresource_args['max_cores_per_job'] = self['max_cores_per_job']
        self.subresource_args['max_memory_per_core'] = self['max_memory_per_core']
        self.subresource_args['max_walltime'] = self['max_walltime']
        # ShellcmdLrms by default trusts the configuration, instead of
        # checking the real amount of memory and number of cpus, but
        # we need the real values instead.
        if self.subresource_type == gc3libs.Default.SHELLCMD_LRMS:
            self.subresource_args['override'] = 'True'

        if not image_name and not image_id:
            raise ConfigurationError(
                "No `image_id` or `image_name` has been specified in the"
                " configuration file.")

        # helper for creating sub-resources
        self._cfgobj = gc3libs.config.Configuration(
            *gc3libs.Default.CONFIG_FILE_LOCATIONS,
            auto_enable_auth=True)

        # Only api version 1.1 are tested so far.
        self.compute_api_version='1.1'

        # "Connect" to the cloud (connection is actually performed
        # only when needed by the `Client` class.
        self.client = NovaClient.Client(
            self.compute_api_version, self.os_username, self.os_password,
            self.os_tenant_name, self.os_auth_url,
            region_name=self.os_region_name)

        # Set up the VMPool persistent class. This has been delayed
        # until here because otherwise self._conn is None
        pooldir = os.path.join(os.path.expandvars(OpenStackLrms.RESOURCE_DIR),
                               'vmpool', self.name)
        self._vmpool = VMPool(pooldir, self.client)

    def _connect(self):
        self.client.authenticate()
        
    def _create_instance(self, image_id, name='gc3pie-instance', instance_type=None,
                         user_data=None):
        """
        Create an instance using the image `image_id` and instance
        type `instance_type`. If not `instance_type` is defined, use
        the default.

        This method will also setup the keypair and the security
        groups, if needed.
        """

        args = {}
        if user_data:
            args['user_data'] = user_data

        # Check if the desired keypair is present
        try:
            keypair = self._get_keypair(self.keypair_name)
        except NotFound:
            gc3libs.log.info(
                "Keypair `%s` not found: creating it using public key `%s`"
                % (self.keypair_name, self.public_key))
            # Create keypair if it does not exist and give an error if it
            # exists but have different fingerprint
            self._import_keypair()
        else:
            self._have_keypair(keypair)

        # Setup security groups
        if 'security_group_name' in self:
            self._setup_security_groups()
            args['security_groups'] = [self.security_group_name]


        flavors = self._get_available_flavors()
        flavor = flavors[0]
        if instance_type:
            try:
                flavor = self._get_flavor(instance_type)
            except:
                raise ConfigurationError(
                    "Instance type %s not found. Check configuration option "
                    "`instance_type` and try again." % instance_type)
        # FIXME: we should add check/creation of proper security
        # groups
        gc3libs.log.debug("Create new VM using image id `%s`", image_id)
        try:
            vm = self.client.servers.create(name, image_id, flavor,
                                            key_name=self.keypair_name, **args)
        except Exception, ex:
            # scrape actual error kind and message out of the
            # exception; we do this mostly for sensible logging, but
            # could be an actual improvement to Boto to provide
            # different exception classes based on the <Code>
            # element...
            # XXX: is there a more robust way of doing this?
            # fall back to normal reporting...
            raise UnrecoverableError("Error starting instance: %s" % ex)

        
        self._vmpool.add_vm(vm)
        gc3libs.log.info(
            "VM with id `%s` has been created and is in %s state.",
            vm.id, vm.status)
        return vm

    def _import_keypair(self):
        """
        Create a new keypair and import the public key defined in the
        configuration file.
        """
        fd = open(os.path.expanduser(self.public_key))
        try:
            key_material = fd.read()
            self.client.keypairs.create(self.keypair_name, key_material)
            keypair = self.client.keypairs.get(self.keypair_name)
            gc3libs.log.info(
                "Successfully imported key `%s` with fingerprint `%s`"
                " as keypair `%s`" % (self.public_key,
                                      keypair.fingerprint,
                                      self.keypair_name))
            return keypair
        except Exception, ex:
            fd.close()
            raise UnrecoverableError("Error importing keypair %s: %s"
                                     % (self.keypair_name, ex))

    def _get_subresource(self, vm):
        """
        Return the resource associated to the virtual machine `vm`.

        Updates the internal list of available resources if needed.

        """
        if vm.id not in self.subresources:
            self.subresources[vm.id] = self._make_subresource(self._get_preferred_ip(vm))
        return self.subresources[vm.id]

    def _get_vm(self, vm_id):
        """
        Return the instance with id `vm_id`, raises an error if there
        is no such instance with that id.
        """
        self._connect()
        vm = self._vmpool.get_vm(vm_id)
        return vm

    def _get_preferred_ip(self, vm):
        """
        Try to guess which is the best IP to use to connect to the VM
        """
        ip = vm.networks.get('public', vm.networks.get('private', ''))
        if ip:
            # The last ip is usually the floating ip associated to the
            # VM.
            return ip[-1]
        return ''

    @staticmethod
    def __str_fingerprint(pkey):
        """
        Print key fingerprint like SSH commands do.

        We need to convert the key fingerprint from Paramiko's
        internal representation to this colon-separated hex format
        because that's the way the fingerprint is returned from the
        OpenStack API.
        """
        return str.join(':', (i.encode('hex') for i in pkey.get_fingerprint()))

    def _have_keypair(self, keypair):
        """
        Check if the given SSH key is available locally.

        Try to locate the given SSH key first among the keys loaded
        into a running ``ssh-agent`` (if any), then in the key file
        given in the ``public_key`` configuration key.
        """
        # try with SSH agent first
        gc3libs.log.debug("Checking if keypair is registered in SSH agent...")
        agent = paramiko.Agent()
        fingerprints_from_agent = [ self.__str_fingerprint(k) for k in agent.get_keys() ]
        if keypair.fingerprint in fingerprints_from_agent:
            gc3libs.log.debug("Found remote key fingerprint in SSH agent.")
            return True

        # else, try to load from file
        keyfile = self.public_key
        if keyfile.endswith('.pub'):
            keyfile = keyfile[:-4]
        else:
            gc3libs.log.warning(
                "Option `public_key` in configuration file should contain"
                " the path to a public key file (with `.pub` ending),"
                " but '%s' was found instead. Continuing anyway.",
                self.public_key)

        pkey = None
        for format, reader in [
                ('DSS', paramiko.DSSKey.from_private_key_file),
                ('RSA', paramiko.RSAKey.from_private_key_file),
                ]:
            try:
                gc3libs.log.debug(
                    "Trying to load key file `%s` as %s key...",
                    keyfile, format)
                pkey = reader(keyfile)
                ## PasswordRequiredException < SSHException so we must check this first
            except paramiko.PasswordRequiredException:
                gc3libs.log.warning(
                    "Key %s is encripted with a password, so we cannot check if it"
                    " matches the remote keypair (maybe you should start `ssh-agent`?)."
                    " Continuing without check.",
                    keyfile)
                return False
            except paramiko.SSHException, ex:
                gc3libs.log.debug(
                    "File `%s` is not a valid %s private key: %s", 
                    keyfile, format, ex)
                # try with next format
                continue
        if pkey is None:
            raise ValueError("Public key `%s` is neither a valid"
                             " RSA key nor a DSS key" % self.public_key)

        # check key fingerprint
        localkey_fingerprint = self.__str_fingerprint(pkey)
        if localkey_fingerprint != keypair.fingerprint:
            raise UnrecoverableAuthError(
                "Keypair `%s` is present but has different fingerprint: "
                "%s != %s. Aborting!" % (
                    self.keypair_name,
                    localkey_fingerprint,
                    keypair.fingerprint,
                ),
                do_log=True)

    @cache_for(120)
    def _get_security_groups(self):
        return self.client.security_groups.list()

    @cache_for(120)
    def _get_security_group(self, name):
        groups = self._get_security_groups()
        group = [grp for grp in groups if grp.name == name]
        if not group:
            raise NotFound("Security group %s not found." % name)
        return group[0]

    def _make_subresource(self, remote_ip):
        """
        Create a resource associated to the instance with `remote_ip`
        ip using configuration file parameters.
        """
        if not remote_ip:
            raise ValueError(
                "_make_subresource: `remote_ip` must be a valid IP or hostname.")
        gc3libs.log.debug(
            "Creating remote ShellcmdLrms resource for ip %s", remote_ip)
        args = self.subresource_args.copy()
        args['frontend'] = remote_ip
        args['transport'] = "ssh"
        args['keyfile'] = self.public_key
        if args['keyfile'].endswith('.pub'):
            args['keyfile'] = args['keyfile'][:-4]
        args['ignore_ssh_host_keys'] = True
        args['name'] = "%s@%s" % (remote_ip, self.name)
        args['auth'] = args['vm_auth']
        resource = self._cfgobj._make_resource(args)
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
            self.security_group_rules.append({
                'ip_protocol': rulesplit[0],
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

        try:
            security_group = self._get_security_group(self.security_group_name)
        except NotFound:
            try:
                gc3libs.log.info("Creating security group %s",
                                 self.security_group_name)
                
                self.client.security_groups.create(
                    self.security_group_name,
                    "GC3Pie_%s" % self.security_group_name)
            except Exception, ex:
                gc3libs.log.error("Error creating security group %s: %s",
                                  self.security_group_name, ex)
                raise UnrecoverableError(
                    "Error creating security group %s: %s"
                    % (self.security_group_name, ex))

            security_group = self._get_security_group(self.security_group_name)
        # TODO: Check if the security group has all the rules we want
        # security_group = groups[self.security_group_name]
        # current_rules = []
        # for rule in security_group.rules:
        #     rule_dict = {
        #         'ip_protocol':  rule.ip_protocol,
        #         'from_port':    int(rule.from_port),
        #         'to_port':      int(rule.to_port),
        #         'cidr_ip':      str(rule.grants[0]),
        #     }
        #     current_rules.append(rule_dict)

        # for new_rule in self.security_group_rules:
        #     if new_rule not in current_rules:
        #         security_group.authorize(**new_rule)

    @cache_for(120)
    def _get_available_images(self):
        return self.client.images.list()

    @cache_for(120)
    def _get_available_flavors(self):
        return self.client.flavors.list()

    @cache_for(120)
    def _get_flavor(self, name):
        flavors = self._get_available_flavors()
        flavor = [fl for fl in flavors if fl.name == name]
        if not flavor:
            raise NotFound("Flavor `%s` not found." % name)
        return flavor[0]

    @cache_for(120)
    def _get_keypair(self, keypair_name):
        return self.client.keypairs.get(keypair_name)

    def get_image_id_for_job(self, job):
        """
        If a configuration option <application>_image_id is present,
        returns its value, otherwise returns `self.image_id`
        """
        conf_option = job.application_name + '_image_id'
        if conf_option in self:
            return self[conf_option]
        else:
            return self.image_id

    def get_instance_type_for_job(self, job):
        """
        If a configuration option <application>_instance_type is present,
        returns its value, otherwise returns `self.instance_type`
        """
        conf_option = job.application_name + '_instance_type'
        if conf_option in self:
            return self[conf_option]
        else:
            return self.instance_type

    def get_user_data_for_job(self, job):
        """
        If a configuration option <application>_user_data is present,
        returns its value, otherwise returns `self.user_data`.
        """
        conf_option = job.application_name + '_user_data'
        if conf_option in self:
            return self[conf_option]
        else:
            return self.user_data

    @same_docstring_as(LRMS.cancel_job)
    def cancel_job(self, app):
        resource = self._get_subresource(self._get_vm(app.os_instance_id))
        return resource.cancel_job(app)

    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        self.updated = False
        # Since we create the resource *before* the VM is actually up
        # & running, it's possible that the `frontend` value of the
        # resources points to a non-existent hostname. Therefore, we
        # have to update them with valid public_ip, if they are
        # present.

        # Update status of known VMs
        for vm_id in self._vmpool:
            try:
                vm = self._vmpool.get_vm(vm_id, force_reload=True)
            except UnrecoverableError, ex:
                gc3libs.log.warning(
                    "Removing stale information on VM `%s`. It has probably"
                    " been deleted from outside GC3Pie.", vm_id)
                self._vmpool.remove_vm(vm_id)
                continue

            if vm.status in PENDING_STATES:
                # If VM is still in pending state, skip creation of
                # the resource
                continue
            elif vm.status in ERROR_STATES:
                # The VM is in error state: exit.
                gc3libs.log.error(
                    "VM with id `%s` is in ERROR state."
                    " Terminating it!", vm.id)
                vm.delete()
                self._vmpool.remove_vm(vm.id)
            elif vm.status == 'DELETED':
                gc3libs.log.info(
                    "VM `%s` in DELETE state. It has probably been terminated"
                    " from outside GC3Pie. Removing it from the list of VM.",
                    vm.id)
                self._vmpool.remove_vm(vm.id)
            elif vm.status in ['SHUTOFF', 'SUSPENDED', 'RESCUE', 'VERIFY_RESIZE']:
                # The VM has probably ben stopped or shut down from
                # outside GC3Pie.
                gc3libs.log.error(
                    "VM with id `%s` is in permanent state `%s`.", vm.id, vm.state)

            # Get or create a resource associated to the vm
            resource = self._get_subresource(vm)
            try:
                resource.get_resource_status()
            except Exception, ex:
                # XXX: Actually, we should try to identify the kind of
                # error we are getting. For instance, if the
                # configuration options `username` is wrong, we will
                # create VMs but we will never be able to submit jobs
                # to them, thus causing an increasing number of
                # useless VMs created on the cloud.
                gc3libs.log.info(
                    "Ignoring error while updating resource %s. "
                    "The corresponding VM may not be ready yet. Error: %s",
                    resource.name, ex)
        return self

    @same_docstring_as(LRMS.get_results)
    def get_results(self, job, download_dir, overwrite=False):
        resource = self._get_subresource(self._get_vm(job.os_instance_id))
        return resource.get_results(job, download_dir, overwrite=False)

    @same_docstring_as(LRMS.update_job_state)
    def update_job_state(self, app):
        if app.os_instance_id not in self.subresources:
            try:
                self.subresources[app.os_instance_id] = self._get_subresource(
                    self._get_vm(app.os_instance_id))
            except InstanceNotFound, ex:
                gc3libs.log.error(
                    "Changing state of task '%s' to TERMINATED since OpenStack "
                    "instance '%s' does not exist anymore.",
                    app.execution.lrms_jobid, app.os_instance_id)
                app.execution.state = Run.State.TERMINATED
                raise ex
            except UnrecoverableError, ex:
                gc3libs.log.error(
                    "Changing state of task '%s' to UNKNOWN because of "
                    "an OpenStack API error.", app.execution.lrms_jobid)
                app.execution.state = Run.State.UNKNOWN
                raise ex

        return self.subresources[app.os_instance_id].update_job_state(app)

    def submit_job(self, job):
        """
        Submission on an OpenStack resource will usually happen in
        multiple steps, since creating a VM and attaching a resource
        to it will take some time.

        In order to return as soon as possible, the backend will raise
        a `RecoverableError` whenever submission is delayed.

        In case a permanent error is found (for instance, we cannot
        create VMs on the cloud), a `UnrecoverableError` is raised.

        More in detail, when during submission the following will
        happen:

        * First of all, the backend will try to submit the job to one
          of the already available subresources.

        * If none of them is able to sbmit the job, the backend will
          check if there is a VM in pending state, and in case there
          is one it will raise a `RecoverableError`, thus delaying
          submission.

        * If no VM in pending state is found, the `vm_pool_max_size`
          configuration option is checked. If we already reached the
          maximum number of VM, a `UnrecoverableError` is raised.

        * If no VM in pending state is found but `vm_pool_max_size` is
          still lesser than the number of VM currently created (or it
          is None, which for us means no limit), then a new VM is
          created, and `RecoverableError` is raised.

        """
        # Updating resource is needed to update the subresources. This
        # is not always done before the submit_job because of issue
        # nr.  386:
        #     http://code.google.com/p/gc3pie/issues/detail?id=386
        self.get_resource_status()

        pending_vms = set(vm.id for vm in self._vmpool.get_all_vms()
                          if vm.status in PENDING_STATES)

        image_id = self.get_image_id_for_job(job)
        # Check if the image id is valid
        if image_id not in [img.id for img in self._get_available_images()]:
            raise ConfigurationError("Image ID %s not found in cloud "
                                     "%s" % (image_id, self.os_auth_url))

        instance_type = self.get_instance_type_for_job(job)
        if instance_type not in [flv.name for flv in self._get_available_flavors()]:
            raise ConfigurationError("Instance type ID %s does not exist in "
                                     "cloud %s" % (image_id, self.os_auth_url))

        # First of all, try to submit to one of the subresources.
        for vm_id, resource in self.subresources.items():
            if not resource.updated:
                # The VM is probably still booting, let's skip to the
                # next one and add it to the list of "pending" VMs.
                pending_vms.add(vm_id)
                continue
            try:
                # Check that the required image id and instance type
                # are correct
                vm = self._get_vm(vm_id)
                flavors = self._get_available_flavors()
                flavor = [flv.name for flv in flavors if flv.id]
                if vm.image['id'] != image_id or \
                   not flavor or flavor[0] != instance_type:
                    continue
                resource.submit_job(job)
                job.os_instance_id = vm_id
                job.changed = True
                gc3libs.log.info(
                    "Job successfully submitted to remote resource %s.",
                    resource.name)
                return job
            except gc3libs.exceptions.LRMSSubmitError, ex:
                gc3libs.log.debug(
                    "Ignoring error while submit to resource %s: %s. ",
                    resource.name, str(ex))

        # Couldn't submit to any resource.
        if not pending_vms:
            # No pending VM, and no resource available. Create a new VM
            if not self.vm_pool_max_size \
                    or len(self._vmpool) < self.vm_pool_max_size:
                user_data = self.get_user_data_for_job(job)
                vm = self._create_instance(image_id,
                                           name="GC3Pie_%s_%d" % (self.name, (len(self._vmpool)+1)),
                                           instance_type=instance_type,
                                           user_data=user_data)
                pending_vms.add(vm.id)

                self._vmpool.add_vm(vm)
            else:
                raise MaximumCapacityReached(
                    "Already running the maximum number of VM on resource %s:"
                    " %d VMs started, but max %d allowed by configuration."
                    % (self.name, len(self._vmpool), self.vm_pool_max_size),
                    do_log=True)

        # If we reached this point, we are waiting for a VM to be
        # ready, so delay the submission until we wither can submit to
        # one of the available resources or until all the VMs are
        # ready.
        gc3libs.log.debug(
            "No available resource was found, but some VM is still in"
            " `pending` state. Waiting until the next iteration before"
            " creating a new VM. Pending VM ids: %s", pending_vms)
        raise LRMSSkipSubmissionToNextIteration(
            "Delaying submission until some of the VMs currently pending"
            " is ready. Pending VM ids: %s"
            % str.join(', ', pending_vms))

    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        resource = self._get_subresource(
            self._get_vm(app.os_instance_id))
        return resource.peek(app, remote_filename, local_file, offset, size)

    def validate_data(self, data_file_list=None):
        """
        Supported protocols: file
        """
        for url in data_file_list:
            if not url.scheme in ['file']:
                return False
        return True

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
        resource = self._get_subresource(self._get_vm(app.os_instance_id))
        resource.free(app)

        # FIXME: current approach in terminating running instances:
        # if no more applications are currently running, turn the instance off
        # check with the associated resource
        resource.get_resource_status()
        if len(resource.job_infos) == 0:
            # turn VM off
            vm = self._get_vm(app.os_instance_id)

            gc3libs.log.info("VM instance %s at %s is no longer needed."
                             " Terminating.", vm.id, self._get_preferred_ip(vm))
            del self.subresources[vm.id]
            vm.delete()
            del self._vmpool[vm.id]
            # self._session.save_all()

    @same_docstring_as(LRMS.close)
    def close(self):
        gc3libs.log.info("Closing connection to cloud '%s'...",
                         self.name)
        if not self.enabled:
            # The resources was most probably disabled by command
            # line. We didn't update it before, so we don't care about
            # currently running VMs now.
            return
        # Update status of VMs and remote resources
        self.get_resource_status()
        for vm_id, resource in self.subresources.items():
            if resource.updated and not resource.job_infos:
                vm = self._get_vm(vm_id)
                gc3libs.log.warning(
                    "VM instance %s at %s is no longer needed. "
                    "You may need to terminate it manually.",
                    vm.id, self._get_preferred_ip(vm))
                vm.delete()
                del self._vmpool[vm.id]
            resource.close()
        # self._session.save_all()

    def __getstate__(self):
        # Do not save `novaclient.client.Client` class as it is
        # not pickle-compliant
        state = self.__dict__.copy()
        del state['client']
        return state

    def __setstate__(self):
        self.__dict__.update(state)
        self.client = client.Client(
            self.compute_api_version, self.os_username, self.os_password,
            self.os_tenant_name, self.os_auth_url,
            region_name=self.os_region_name)


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="openstack",
                    optionflags=doctest.NORMALIZE_WHITESPACE)


# Server states
#    ACTIVE. The server is active.
#
#    BUILD. The server has not finished the original build process.
#
#    DELETED. The server is deleted.
#
#    ERROR. The server is in error.
#
#    HARD_REBOOT. The server is hard rebooting. This is equivalent to
#    pulling the power plug on a physical server, plugging it back in,
#    and rebooting it.
#
#    PASSWORD. The password is being reset on the server.
#
#    REBOOT. The server is in a soft reboot state. A reboot command
#    was passed to the operating system.
#
#    REBUILD. The server is currently being rebuilt from an image.
#
#    RESCUE. The server is in rescue mode.
#
#    RESIZE. Server is performing the differential copy of data that
#    changed during its initial copy. Server is down for this stage.
#
#    REVERT_RESIZE. The resize or migration of a server failed for
#    some reason. The destination server is being cleaned up and the
#    original source server is restarting.
#
#    SHUTOFF. The virtual machine (VM) was powered down by the user,
#    but not through the OpenStack Compute API. For example, the user
#    issued a shutdown -h command from within the server instance. If
#    the OpenStack Compute manager detects that the VM was powered
#    down, it transitions the server instance to the SHUTOFF
#    status. If you use the OpenStack Compute API to restart the
#    instance, the instance might be deleted first, depending on the
#    value in the shutdown_terminate database field on the Instance
#    model.
#
#    SUSPENDED. The server is suspended, either by request or
#    necessity. This status appears for only the following
#    hypervisors: XenServer/XCP, KVM, and ESXi. Review support tickets
#    or contact Rackspace support to determine why the server is in
#    this state.
#
#    UNKNOWN. The state of the server is unknown. Contact your cloud
#    provider.
#
#    VERIFY_RESIZE. System is awaiting confirmation that the server is
#    operational after a move or resize.


# [<SecurityGroup description=Allow all the ports, id=6, name=all_tcp_ports, rules=[{u'from_port': 1, u'group': {}, u'ip_protocol': u'tcp', u'to_port': 65000, u'parent_group_id': 6, u'ip_range': {u'cidr': u'0.0.0.0/0'}, u'id': 11}, {u'from_port': -1, u'group': {}, u'ip_protocol': u'icmp', u'to_port': -1, u'parent_group_id': 6, u'ip_range': {u'cidr': u'0.0.0.0/0'}, u'id': 12}, {u'from_port': 1, u'group': {}, u'ip_protocol': u'udp', u'to_port': 65535, u'parent_group_id': 6, u'ip_range': {u'cidr': u'0.0.0.0/0'}, u'id': 13}], tenant_id=4bdc5d18c711438f8be0ec9b70272892>,
