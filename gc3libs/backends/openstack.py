#! /usr/bin/env python

"""
Manage startup and teardown of cloud-based VMs to run applications.

This only works on clouds implementing the OpenStack Compute API.
"""

# Copyright (C) 2012-2019  University of Zurich. All rights reserved.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import, print_function, unicode_literals
__docformat__ = 'reStructuredText'


import sys
if sys.version_info < (2, 7):
    raise ImportError("GC3Pie's OpenStack backend requires Python 2.7+")

import os
import paramiko
from string import ascii_letters, digits

# OpenStack APIs
try:
    import os_client_config
except ImportError as err:
    from gc3libs.exceptions import ConfigurationError
    raise ConfigurationError(
        "The OpenStack backend is used but the `os_client_config` module"
        " cannot be loaded: {err}. Please, either install it with"
        "`pip install os-client-config` and verify that it works"
        " by running `python -c 'import os_client_config'`,"
        " or update your configuration file and"
        " disable any OpenStack-based resources."
        .format(err=err))

try:
    from novaclient.exceptions import NotFound
except ImportError as err:
    from gc3libs.exceptions import ConfigurationError
    raise ConfigurationError(
        "The OpenStack backend is used but the `novaclient` module"
        " cannot be loaded: {err}. Please, either install it with"
        "`pip install python-novaclient` and verify that it works"
        " by running `python -c 'import novaclient'`,"
        " or update your configuration file and"
        " disable any OpenStack-based resources."
        .format(err=err))


# GC3Pie imports
import gc3libs
import gc3libs.defaults
from gc3libs.exceptions import \
    ConfigurationError, \
    LRMSSubmitError, \
    MaximumCapacityReached, \
    ResourceNotReady, \
    TransportError, \
    UnrecoverableAuthError, \
    UnrecoverableDataStagingError, \
    UnrecoverableError
import gc3libs.url
from gc3libs import Run
from gc3libs.utils import same_docstring_as
from gc3libs.backends import LRMS
from gc3libs.backends.shellcmd import ShellcmdLrms
from gc3libs.backends.vmpool import VMPool, InstanceNotFound
from gc3libs.utils import cache_for, lookup
from gc3libs.quantity import MiB


available_subresource_types = [gc3libs.defaults.SHELLCMD_LRMS]

ERROR_STATES = ['ERROR', 'UNNKNOWN']
PENDING_STATES = ['BUILD', 'REBUILD', 'REBOOT', 'HARD_REBOOT',
                  'RESIZE', 'REVERT_RESIZE']


class OpenStackVMPool(VMPool):
    """
    Implementation of `VMPool` for OpenStack cloud
    """

    def _get_instance(self, vm_id):
        try:
            vm = self.conn.servers.get(vm_id)
        except NotFound:
            raise InstanceNotFound(
                "No instance with id %s has been found." % vm_id)
        return vm


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
                 keypair_name, public_key, vm_auth,
                 os_region=None, image_id=None, os_auth_url=None,
                 instance_type=None, auth=None,
                 vm_pool_max_size=None, user_data=None,
                 vm_os_overhead=gc3libs.defaults.VM_OS_OVERHEAD,
                 # extra args are used to instanciate "sub-resources"
                 **extra_args):

        # Note: this creates attributes from key/value pairs given in the
        # `extra_args` parameters. In particular, the `self.type` attribute
        # (referenced below) is set in this chained constructor...
        LRMS.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth, **extra_args)

        self.free_slots = int(max_cores)
        self.user_run = 0
        self.user_queued = 0
        self.queued = 0
        self._flavors = []
        self.vm_pool_max_size = vm_pool_max_size
        if vm_pool_max_size is not None:
            try:
                self.vm_pool_max_size = int(self.vm_pool_max_size)
            except ValueError:
                raise ConfigurationError(
                    "Value for `vm_pool_max_size` must be an integer,"
                    " was %s instead." % vm_pool_max_size)

        # pylint: disable=no-member
        self.subresource_type = self.type.split('+', 1)[1]
        if self.subresource_type not in available_subresource_types:
            raise UnrecoverableError("Invalid resource type: %s" % self.type)

        # Mapping of job.execution.instance_id => LRMS
        self.subresources = {}

        auth = self._auth_fn()
        if os_auth_url is None:
            os_auth_url = os.getenv('OS_AUTH_URL')
        if os_auth_url is None:
            raise gc3libs.exceptions.InvalidArgument(
                "Cannot connect to the OpenStack API:"
                " No 'OS_AUTH_URL' environment variable defined,"
                " and no 'os_auth_url' argument passed"
                " to the OpenStack backend.")
        self.os_auth_url = os_auth_url
        self.os_username = auth.os_username
        self.os_password = auth.os_password
        self.os_tenant_name = auth.os_project_name
        self.os_region_name = os_region
        # Keypair names can only contain alphanumeric chars!
        if not set(keypair_name).issubset(set(ascii_letters + digits + '_-.')):
            raise ConfigurationError(
                "Keypair name `{0}` is invalid:"
                " keypair names can only consist of"
                " alphanumeric chars, plus `_`, `-`, and `.`"
                .format(keypair_name))
        self.keypair_name = keypair_name
        self.public_key = os.path.expanduser(
            os.path.expandvars(public_key.strip()))
        self.image_id = image_id
        self.instance_type = instance_type
        self.user_data = user_data
        self.vm_os_overhead = gc3libs.quantity.Memory(vm_os_overhead)
        self._parse_security_group()
        self._conn = None

        # `*_instance_type` config items should be consumed here,
        # not in any sub-resource
        for key, value in list(extra_args.items()):
            if key.endswith('_instance_type'):
                self[key] = value
                extra_args.pop(key)

        # `self.subresource_args` is used to create subresources
        self.subresource_args = extra_args
        self.subresource_args['type'] = self.subresource_type
        self.subresource_args['architecture'] = self['architecture']
        self.subresource_args['max_cores'] = self['max_cores']
        self.subresource_args['max_cores_per_job'] = self['max_cores_per_job']
        self.subresource_args['max_memory_per_core'] = \
            self['max_memory_per_core']
        self.subresource_args['max_walltime'] = self['max_walltime']
        # SSH-specific configuration
        self.subresource_args['transport'] = 'ssh'
        self.subresource_args['auth'] = vm_auth
        self.subresource_args['ssh_timeout'] = 7  # FIXME: hard-coded!
        self.subresource_args['ignore_ssh_host_keys'] = True
        self.subresource_args['keyfile'] = self.public_key
        if self.subresource_args['keyfile'].endswith('.pub'):
            keyfile = self.subresource_args['keyfile'][:-len('.pub')]
            self.subresource_args['keyfile'] = keyfile
        # ShellcmdLrms by default trusts the configuration, instead of
        # checking the real amount of memory and number of cpus, but
        # we need the real values instead.
        if self.subresource_type == gc3libs.defaults.SHELLCMD_LRMS:
            self.subresource_args['override'] = 'True'

        if image_id is None:
            raise ConfigurationError(
                "No `image_id` specified in the configuration file.")

        # "Connect" to the cloud (connection is actually performed
        # only when needed by the `Client` class.
        self.compute_client, self.image_client, self.network_client = self._new_clients()

        # Set up the VMPool persistent class. This has been delayed
        # until here because otherwise self._conn is None
        pooldir = os.path.join(
            os.path.expandvars(OpenStackLrms.RESOURCE_DIR),
            'vmpool', self.name)
        self._vmpool = OpenStackVMPool(pooldir, self.compute_client)

    def _new_clients(self):
        self.__connected = False
        for kind in ['compute', 'image', 'network']:
            yield os_client_config.make_client(
                kind,
                auth_url=self.os_auth_url,
                username=self.os_username,
                password=self.os_password,
                project_name=self.os_tenant_name,
                region_name=self.os_region_name,
            )

    def _connect(self):
        if not self.__connected:
            self.compute_client.authenticate()
            # Fill the flavors and update the resource.
            self._flavors = self.compute_client.flavors.list()
            flavor = max(self._flavors, key=self._flavor_ordering_key)
            # FIXME: if the configured values are lower, should they be honored instead?
            self['max_cores'] = self['max_cores_per_job'] = flavor.vcpus
            self['max_memory_per_core'] = flavor.ram * MiB
            gc3libs.log.info(
                "Largest VM flavor available on the cloud: %s (%d vCPUs, %s RAM)",
                flavor.name, self['max_cores'], self['max_memory_per_core'])
        self.__connected = True

    def _create_instance(self, image_id, name='gc3pie-instance',
                         instance_type=None, user_data=None):
        """
        Create an instance using the image `image_id` and instance
        type `instance_type`. If no `instance_type` is defined, use
        the default.

        This method will also setup the keypair and the security
        groups, if needed.
        """

        args = {}
        if user_data:
            args['userdata'] = user_data

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
        instance_type = instance_type or self.instance_type
        # Setup security groups
        if 'security_group_name' in self:
            self._setup_security_groups()
            args['security_groups'] = [self.security_group_name]

        # FIXME: we should add check/creation of proper security
        # groups

        nics = None
        if hasattr(self,'network_ids') and self.network_ids:
            nics = [{'net-id': netid.strip(), 'v4-fixed-ip': ''}
                  for netid in self.network_ids.split(',')]
            gc3libs.log.debug("Specifying networks for vm %s: %s",
                      name, ', '.join([nic['net-id'] for nic in nics]))
        args['nics'] = nics

        gc3libs.log.debug("Create new VM using image id `%s`", image_id)
        try:
            vm = self.compute_client.servers.create(name, image_id, instance_type,
                                            key_name=self.keypair_name, **args)
        except Exception as err:
            # scrape actual error kind and message out of the
            # exception; we do this mostly for sensible logging, but
            # could be an actual improvement to Boto to provide
            # different exception classes based on the <Code>
            # element...
            # XXX: is there a more robust way of doing this?
            # fall back to normal reporting...
            raise UnrecoverableError("Error starting instance: %s" % err)

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
            self.compute_client.keypairs.create(self.keypair_name, key_material)
            keypair = self.compute_client.keypairs.get(self.keypair_name)
            gc3libs.log.info(
                "Successfully imported key `%s` with fingerprint `%s`"
                " as keypair `%s`" % (self.public_key,
                                      keypair.fingerprint,
                                      self.keypair_name))
            return keypair
        except Exception as ex:
            fd.close()
            raise UnrecoverableError("Error importing keypair %s: %s"
                                     % (self.keypair_name, ex))

    def _get_subresource(self, vm):
        """
        Return the resource associated to the virtual machine `vm`.

        Updates the internal list of available resources if needed.
        """
        self._connect()
        if vm.id not in self.subresources:
            self.subresources[vm.id] = self._make_subresource(
                vm.id, vm.preferred_ip)
            # Update resource based on flavor specs
            try:
                flavor = self.compute_client.flavors.get(vm.flavor['id'])
                res = self.subresources[vm.id]
                res['max_memory_per_core'] = flavor.ram * MiB
                res['max_cores_per_job'] = flavor.vcpus
                res['max_cores'] = flavor.vcpus
            except Exception as ex:
                # Ignore any error here, as we can get information on
                # the subresources when we connect to it.
                gc3libs.log.info(
                    "Ignoring error in setting max_cores_per_job/max_memory"
                    " values for new subresource %s based on flavor: %s",
                    self.subresources[vm.id].name, ex)
        return self.subresources[vm.id]

    def _get_vm(self, vm_id):
        """
        Return the instance with id `vm_id`, raises an error if there
        is no such instance with that id.
        """
        self._connect()
        vm = self._vmpool.get_vm(vm_id)
        return vm

    @staticmethod
    def __str_fingerprint(pkey):
        """
        Print key fingerprint like SSH commands do.

        We need to convert the key fingerprint from Paramiko's
        internal representation to this colon-separated hex format
        because that's the way the fingerprint is returned from the
        OpenStack API.
        """
        return ':'.join((i.encode('hex') for i in pkey.get_fingerprint()))

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
        fingerprints_from_agent = [self.__str_fingerprint(k)
                                   for k in agent.get_keys()]
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
                " but `%s` was found instead. Continuing anyway.",
                self.public_key)

        pkey = None
        for format, reader in [
                ('DSS', paramiko.DSSKey.from_private_key_file),
                ('RSA', paramiko.RSAKey.from_private_key_file),
        ]:
            try:
                gc3libs.log.debug(
                    "Trying to load key file `%s` as SSH %s key...",
                    keyfile, format)
                pkey = reader(keyfile)
                gc3libs.log.info(
                    "Successfully loaded key file `%s` as SSH %s key.",
                    keyfile, format)
            # PasswordRequiredException < SSHException so we must
            # check this first
            except paramiko.PasswordRequiredException:
                gc3libs.log.warning(
                    "Key %s is encripted with a password, so we cannot check"
                    " if it matches the remote keypair (maybe you should start"
                    " `ssh-agent`?). Continuing without consistency check with"
                    " remote fingerprint.", keyfile)
                return False
            except paramiko.SSHException as ex:
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
                "Keypair `%s` exists but has different fingerprint than local"
                " one: local public key file `%s` has fingerprint `%s`,"
                " whereas OpenStack API reports fingerprint `%s`."
                " Aborting!" % (
                    self.public_key,
                    self.keypair_name,
                    localkey_fingerprint,
                    keypair.fingerprint,
                ),
                do_log=True)

    @cache_for(120)
    def _get_security_groups(self):
        self._connect()
        try:
            # python-novaclient < 8.0.0
            return self.compute_client.security_groups.list()
        except AttributeError:
            return self.network_client.list_security_groups()['security_groups']

    @cache_for(120)
    def _get_security_group(self, name):
        groups = self._get_security_groups()
        group = [grp for grp in groups if lookup(grp, 'name') == name]
        if not group:
            raise NotFound("Security group %s not found." % name)
        return group[0]

    def _make_subresource(self, id, remote_ip):
        """
        Create a resource associated to the instance with `remote_ip`
        ip using configuration file parameters.
        """
        gc3libs.log.debug(
            "Creating remote ShellcmdLrms resource for IP address %s",
            remote_ip)
        args = self.subresource_args.copy()
        args['frontend'] = remote_ip
        args['name'] = "%s@%s" % (id, self.name)
        return ShellcmdLrms(**args)

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
                'from_port': int(rulesplit[1]),
                'to_port': int(rulesplit[2]),
                'cidr_ip': rulesplit[3],
            })

    def _setup_security_groups(self):
        """
        Check the current configuration and set up the security group
        if it does not exist.
        """
        self._connect()
        if not self.security_group_name:
            gc3libs.log.error("Group name in `security_group_name`"
                              " configuration option cannot be empty!")
            return

        try:
            self._get_security_group(self.security_group_name)
        except NotFound:
            try:
                gc3libs.log.info("Creating security group %s",
                                 self.security_group_name)

                self.compute_client.security_groups.create(
                    self.security_group_name,
                    "GC3Pie_%s" % self.security_group_name)
            except Exception as ex:
                gc3libs.log.error("Error creating security group %s: %s",
                                  self.security_group_name, ex)
                raise UnrecoverableError(
                    "Error creating security group %s: %s"
                    % (self.security_group_name, ex))

            self._get_security_group(self.security_group_name)
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
        self._connect()
        try:
            # python-novaclient < 8.0.0
            return self.compute_client.images.list()
        except AttributeError:
            # ``glance_client.images.list()`` returns a generator, but callers
            # of `._get_available_images()` expect a Python list
            return list(self.image_client.images.list())

    @cache_for(120)
    def _get_available_flavors(self):
        self._connect()
        return self.compute_client.flavors.list()

    def _get_flavor(self, name):
        for flavor in self._flavors:
            # pick the first match by that name
            if flavor.name == name:
                return flavor
        # no flavor by the given name
        raise NotFound("Flavor `{name}` not found.".format(name=name))

    @cache_for(120)
    def _get_keypair(self, keypair_name):
        self._connect()
        return self.compute_client.keypairs.get(keypair_name)

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

    def get_instance_type_for_job(self, task):
        """
        Return the "best" instance_type (aka flavor) for the task.

        This method will always returns an instance_type that accomodates the
        task's requirements. Preference will be given, in order, to:

        1) ``<application>_instance_type configuration`` option
        2) ``instance_type configuration`` option
        3) the smallest flavor (ordering by cpus, ram and disk)
           that fits the application requirements.
        """
        req_cores = self._get_task_requirement(task, 'requested_cores', 1)
        req_memory = self._get_task_requirement(task, 'requested_memory', 0 * MiB)

        # If there is an option <application>_instance_type, try to use it
        for conf_option in [
                task.application_name + '_instance_type',
                'instance_type'
        ]:
            gc3libs.log.debug("Trying configuration option `%s` ...", conf_option)
            try:
                flavor_name = self[conf_option]
            except KeyError:
                # ignore and try next option
                continue
            if flavor_name is None:
                continue

            try:
                flavor = self._get_flavor(flavor_name)
            except NotFound:
                gc3libs.log.warning(
                    "Unable to use flavor `%s` for task `%s` as requested"
                    " by config option `%s` because it's not available"
                    " in the cloud backend. Fix configuration file?",
                    flavor_name, task, conf_option)
                continue

            if self._flavor_matches_requirements(flavor, req_cores, req_memory):
                gc3libs.log.info(
                    "Using flavor `%s` for task `%s`"
                    " as requested by config option `%s`",
                    flavor.name, task, conf_option)
                return flavor
            else:
                gc3libs.log.warning(
                    "Unable to use flavor `%s` for task `%s`"
                    " as requested by config option `%s`"
                    " because it doesn't fit application's requirements",
                    flavor_name, task, conf_option)
                continue

        # fall back to smallest flavor that fits
        valid_flavors = [
            flv for flv in self._flavors
            if self._flavor_matches_requirements(flv, req_cores, req_memory)
        ]
        flavor = min(valid_flavors, key=self._flavor_ordering_key)
        gc3libs.log.info(
            "Using VM flavor `%s` as the smallest flavor that fits requirements"
            " of task `%s`", flavor.name, task)
        return flavor

    @staticmethod
    def _get_task_requirement(task, reqname, default):
        """
        Return attribute `reqname` of `task`.

        If the attribute is not set or is ``None``, then return `default`
        instead.
        """
        req = getattr(task, reqname, None)
        if req is None:
            req = default
        return req

    def _flavor_matches_requirements(self, flavor, req_cores, req_memory):
        """
        Return whether the flavor matches the given requirements.
        """
        return (flavor.vcpus >= req_cores and
                (flavor.ram * MiB - self.vm_os_overhead) >= req_memory)

    @staticmethod
    def _flavor_ordering_key(flavor):
        """
        Return the value to use when sorting flavors.

        By default, flavors are sorted lexicographically by:
        - nr. of vCPUS (first),
        - amount of RAM,
        - size of disk (last).
        """
        return (flavor.vcpus, flavor.ram, flavor.disk)

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
        try:
            subresource = self._get_subresource(
                self._get_vm(app.execution._lrms_vm_id))
            return subresource.cancel_job(app)
        except InstanceNotFound:
            # ignore -- if this VM exists no more, we need not cancel any job
            pass
        except UnrecoverableError as err:
            gc3libs.log.error(
                "Changing state of task '%s' to UNKNOWN because of"
                " an OpenStack API error (%s: %s)",
                app.execution.lrms_jobid, err.__class__.__name__, err)
            app.execution.state = Run.State.UNKNOWN
            app.execution.history.append(
                "State changed to UNKNOWN because of"
                " an OpenStack API error (%s: %s)."
                % (err.__class__.__name__, err))
            raise

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
            except UnrecoverableError:
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
                self.subresources.pop(vm.id)
                continue
            elif vm.status == 'DELETED':
                gc3libs.log.info(
                    "VM `%s` in DELETE state. It has probably been terminated"
                    " from outside GC3Pie. Removing it from the list of VM.",
                    vm.id)
                self._vmpool.remove_vm(vm.id)
                self.subresources.pop(vm.id)
                continue
            elif vm.status in ['SHUTOFF', 'SUSPENDED',
                               'RESCUE', 'VERIFY_RESIZE']:
                # The VM has probably ben stopped or shut down from
                # outside GC3Pie.
                gc3libs.log.error(
                    "VM with id `%s` is in permanent state `%s`.",
                    vm.id, vm.status)
                continue

            # Get or create a resource associated to the vm
            subresource = self._get_subresource(vm)
            try:
                subresource.get_resource_status()
            except TransportError as ex:
                # TODO: get all the IPs and try with all of them to connect.
                # Start with preferred_ip if defined
                gc3libs.log.info(
                    "Ignoring error in updating resource '%s': %s."
                    " Trying other IPs.", subresource.name, ex)
                for ip in sum(list(vm.networks.values()), []):
                    if vm.preferred_ip == ip:
                        continue
                    vm.preferred_ip = ip
                    subresource.frontend = ip
                    gc3libs.log.info(
                        "Connection error. Trying with alternate IP address "
                        "%s", vm.preferred_ip)
                    try:
                        subresource.get_resource_status()
                        break
                    except Exception as ex:
                        gc3libs.log.info(
                            "Ignoring error in updating resource '%s': %s."
                            " The corresponding VM may not be ready yet.",
                            subresource.name, ex)
                # Unable to connect to the VM using any IP.  Ensure
                # this resource is considered "pending" as we couldn't
                # update its status
                subresource.updated = False
            except Exception as ex:
                # XXX: Actually, we should try to identify the kind of
                # error we are getting. For instance, if the
                # configuration options `username` is wrong, we will
                # create VMs but we will never be able to submit jobs
                # to them, thus causing an increasing number of
                # useless VMs created on the cloud.
                if gc3libs.error_ignored(
                        # context:
                        # - module
                        'openstack',
                        # - class
                        'OpenStackLrms',
                        # - method
                        'get_resource_status',
                        # - actual error class
                        ex.__class__.__name__,
                        # - additional keywords
                        'resource',
                        'status',
                        'update',
                        'vm',
                ):
                    gc3libs.log.info(
                        "Ignoring error while updating resource '%s'. "
                        "The corresponding VM may not be ready yet. Error: %s",
                        subresource.name, ex)
                else:
                    # propagate exception back to caller
                    raise
        self._vmpool.update()
        return self

    @same_docstring_as(LRMS.get_results)
    def get_results(self, app, download_dir, overwrite=False,
                    changed_only=True):
        try:
            subresource = self._get_subresource(
                self._get_vm(app.execution._lrms_vm_id))
        except InstanceNotFound:
            gc3libs.log.error(
                "Changing state of task '%s' to TERMINATED since OpenStack"
                " instance '%s' does not exist anymore.",
                app.execution.lrms_jobid, app.execution._lrms_vm_id)
            app.execution.state = Run.State.TERMINATED
            app.execution.signal = Run.Signals.RemoteError
            app.execution.history.append(
                "State changed to TERMINATED since OpenStack"
                " instance '%s' does not exist anymore."
                % (app.execution._lrms_vm_id,))
            raise UnrecoverableDataStagingError(
                "VM where job was running is no longer available")
        except UnrecoverableError as err:
            gc3libs.log.error(
                "Changing state of task '%s' to UNKNOWN because of"
                " an OpenStack API error (%s: %s)",
                app.execution.lrms_jobid, err.__class__.__name__, err)
            app.execution.state = Run.State.UNKNOWN
            app.execution.history.append(
                "State changed to UNKNOWN because of"
                " an OpenStack API error (%s: %s)."
                % (err.__class__.__name__, err))
            raise
        return subresource.get_results(
            app, download_dir, overwrite, changed_only)

    @same_docstring_as(LRMS.update_job_state)
    def update_job_state(self, app):
        self._connect()
        if app.execution._lrms_vm_id not in self.subresources:
            try:
                self.subresources[app.execution._lrms_vm_id] = self._get_subresource(
                    self._get_vm(app.execution._lrms_vm_id))
            except InstanceNotFound:
                gc3libs.log.error(
                    "Changing state of task '%s' to TERMINATED since OpenStack"
                    " instance '%s' does not exist anymore.",
                    app.execution.lrms_jobid, app.execution._lrms_vm_id)
                app.execution.state = Run.State.TERMINATED
                app.execution.signal = Run.Signals.RemoteError
                app.execution.history.append(
                    "State changed to TERMINATED since OpenStack"
                    " instance '%s' does not exist anymore."
                    % (app.execution._lrms_vm_id,))
                raise
            except UnrecoverableError as err:
                gc3libs.log.error(
                    "Changing state of task '%s' to UNKNOWN because of"
                    " an OpenStack API error (%s: %s)",
                    app.execution.lrms_jobid, err.__class__.__name__, err)
                app.execution.state = Run.State.UNKNOWN
                app.execution.history.append(
                    "State changed to UNKNOWN because of"
                    " an OpenStack API error (%s: %s)."
                    % (err.__class__.__name__, err))
                raise
        return self.subresources[app.execution._lrms_vm_id].update_job_state(app)

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
        #     https://github.com/uzh/gc3pie/issues/386
        self.get_resource_status()
        pending_vms = set(vm.id for vm in self._vmpool.get_all_vms()
                          if vm.status in PENDING_STATES)

        image_id = self.get_image_id_for_job(job)
        # Check if the image id is valid
        if image_id not in [img.id for img in self._get_available_images()]:
            raise ConfigurationError("Image ID %s not found in cloud "
                                     "%s" % (image_id, self.os_auth_url))

        instance_type = self.get_instance_type_for_job(job)
        if not instance_type:
            raise RuntimeError(
                "Unable to find a suitable instance type for "
                "application %s" % job)

        # First of all, try to submit to one of the subresources.
        for vm_id, subresource in list(self.subresources.items()):
            if not subresource.updated:
                # The VM is probably still booting, let's skip to the
                # next one and add it to the list of "pending" VMs.
                pending_vms.add(vm_id)
                continue
            try:
                # Check that the required image id and instance type
                # are correct
                vm = self._get_vm(vm_id)
                if vm.image['id'] != image_id:
                    continue
                subresource.submit_job(job)
                job.execution._lrms_vm_id = vm_id
                job.changed = True
                gc3libs.log.info(
                    "Job successfully submitted to remote resource %s.",
                    subresource.name)
                return job
            except (LRMSSubmitError, InstanceNotFound) as ex:
                if gc3libs.error_ignored(
                        # context:
                        # - module
                        'openstack',
                        # - class
                        'OpenStackLrms',
                        # - method
                        'submit_job',
                        # - actual error class
                        ex.__class__.__name__,
                        # - additional keywords
                        'submit',
                ):
                    gc3libs.log.debug(
                        "Ignoring error in submitting to resource '%s': %s",
                        subresource.name, ex)
                else:
                    # propagate error back to caller
                    raise

        # Couldn't submit to any resource.
        if not pending_vms:
            # No pending VM, and no resource available. Create a new VM
            if not self.vm_pool_max_size \
                    or len(self._vmpool) < self.vm_pool_max_size:
                user_data = self.get_user_data_for_job(job)
                vm = self._create_instance(
                    image_id,
                    name="GC3Pie_%s_%d" % (self.name, (len(self._vmpool) + 1)),
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
        raise ResourceNotReady(
            "Delaying submission until one of the VMs currently pending"
            " is ready. (Pending VM ids: %r)" % (pending_vms,))

    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        try:
            subresource = self._get_subresource(
                self._get_vm(app.execution._lrms_vm_id))
        except InstanceNotFound:
            gc3libs.log.error(
                "Changing state of task '%s' to TERMINATED since OpenStack"
                " instance '%s' does not exist anymore.",
                app.execution.lrms_jobid, app.execution._lrms_vm_id)
            app.execution.state = Run.State.TERMINATED
            app.execution.signal = Run.Signals.RemoteError
            app.execution.history.append(
                "State changed to TERMINATED since OpenStack"
                " instance '%s' does not exist anymore."
                % (app.execution._lrms_vm_id,))
            raise UnrecoverableDataStagingError(
                "VM where job was running is no longer available.")
        return subresource.peek(app, remote_filename, local_file, offset, size)

    def validate_data(self, data_file_list=None):
        """
        Supported protocols: file
        """
        for url in data_file_list:
            if url.scheme not in ['file', 'http', 'https', 'swift', 'swifts', 'swt', 'swts']:
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
        try:
            subresource = self._get_subresource(
                self._get_vm(app.execution._lrms_vm_id))
        except InstanceNotFound:
            # ignore -- if the instance is no more, there is
            # nothing we should free
            return
        subresource.free(app)

        # FIXME: current approach in terminating running instances:
        # if no more applications are currently running, turn the instance off
        # check with the associated resource
        subresource.get_resource_status()
        if not subresource.has_running_tasks():
            # turn VM off
            vm = self._get_vm(app.execution._lrms_vm_id)

            gc3libs.log.info("VM instance %s at %s is no longer needed."
                             " Terminating.", vm.id, vm.preferred_ip)
            del self.subresources[vm.id]
            vm.delete()
            del self._vmpool[vm.id]

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
        for vm_id, subresource in list(self.subresources.items()):
            if subresource.updated and not subresource.has_running_tasks():
                vm = self._get_vm(vm_id)
                gc3libs.log.warning(
                    "VM instance %s at %s is no longer needed. "
                    "You may need to terminate it manually.",
                    vm.id, vm.preferred_ip)
                self._vmpool.remove_vm(vm.id)
            subresource.close()

    def __getstate__(self):
        # Do not save `novaclient.client.Client` class as it is
        # not pickle-compliant
        state = self.__dict__.copy()
        del state['client']
        del state['__connected']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.compute_client, self.image_client, self.network_client = self._new_clients()
        self._connect()


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="openstack",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
