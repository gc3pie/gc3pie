#! /usr/bin/env python

"""
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

from builtins import str

import hashlib
import os
import re
import paramiko

# EC2 APIs
try:
    import boto
    import boto.ec2.regioninfo
    import boto.exception
except ImportError as err:
    from gc3libs.exceptions import ConfigurationError
    raise ConfigurationError(
        "The EC2 backend is used but the `boto` module"
        " cannot be used: {err}. Please, either install it with"
        "`pip install boto` and verify that it works"
        " by running `python -c 'import boto'`,"
        " then try again, or update your configuration file and"
        " disable any EC2-based resources."
        .format(err=err))

import Crypto.PublicKey.DSA
import Crypto.PublicKey.RSA

# GC3Pie imports
import gc3libs
import gc3libs.defaults
from gc3libs.exceptions import UnrecoverableError, \
    ConfigurationError, MaximumCapacityReached, ResourceNotReady, \
    UnrecoverableAuthError, TransportError
import gc3libs.url
from gc3libs import Run
from gc3libs.utils import same_docstring_as, insert_char_every_n_chars
from gc3libs.backends import LRMS
from gc3libs.backends.shellcmd import ShellcmdLrms
from gc3libs.backends.vmpool import VMPool, InstanceNotFound


## module metadata
__docformat__ = 'reStructuredText'


## module-level constants
available_subresource_types = [gc3libs.defaults.SHELLCMD_LRMS]

# example Boto error message:
# <Response><Errors><Error><Code>TooManyInstances</Code><Message>Quota exceeded for ram: Requested 8000, but already used 16000 of 16384 ram</Message></Error></Errors><RequestID>req-c219213b-88d2-42dc-a3ab-10ac80aa7df7</RequestID></Response>  # noqa
#
_BOTO_ERRMSG_RE = re.compile(r'<Code>(?P<code>[A-Za-z0-9]+)</Code>'
                             '<Message>(?P<message>.*)</Message>', re.X)


## code

class EC2VMPool(VMPool):

    """
    Specific implementation of VMPool
    """

    def _get_instance(self, vm_id):
        # contact EC2 API to get VM info
        try:
            reservations = self.conn.get_all_instances(instance_ids=[vm_id])
        except boto.exception.EC2ResponseError as err:
            # scrape actual error kind and message out of the
            # exception; we do this mostly for sensible logging, but
            # could be an actual improvement to Boto to provide
            # different exception classes based on the <Code>
            # element...
            # XXX: is there a more robust way of doing this?
            match = _BOTO_ERRMSG_RE.search(str(err))
            if match:
                raise UnrecoverableError(
                    "Error getting info on VM %s: EC2ResponseError/%s: %s"
                    % (vm_id, match.group('code'), match.group('message')),
                    do_log=True)
            else:
                # fall back to normal reporting...
                raise UnrecoverableError(
                    "Error getting VM %s: %s" % (vm_id, err),
                    do_log=True)
        if not reservations:
            raise InstanceNotFound(
                "No instance with id %s has been found." % vm_id)

        instances = dict((i.id, i) for i in reservations[0].instances
                         if reservations)
        if vm_id not in instances:
            raise UnrecoverableError(
                "No instance with id %s has been found." % vm_id)

        return instances[vm_id]


class EC2Lrms(LRMS):

    """
    EC2 resource.
    """
    RESOURCE_DIR = '$HOME/.gc3/ec2.d'

    def __init__(self, name,
                 # these parameters are inherited from the `LRMS` class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime,
                 # these are specific of the EC2Lrms class
                 ec2_region, keypair_name, public_key, vm_auth,
                 image_id=None, image_name=None, ec2_url=None,
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

        self.region = ec2_region

        # Mapping of job.execution._lrms_vm_id => LRMS
        self.subresources = {}

        auth = self._auth_fn()
        self.ec2_access_key = auth.ec2_access_key
        self.ec2_secret_key = auth.ec2_secret_key
        if ec2_url is None:
            ec2_url = os.getenv('EC2_URL')
        if ec2_url is None:
            raise gc3libs.exceptions.InvalidArgument(
                "Cannot connect to the EC2 API:"
                " No 'EC2_URL' environment variable defined,"
                " and no 'ec2_url' argument passed to the EC2 backend.")
        self.ec2_url = gc3libs.url.Url(ec2_url)

        # Keypair names can only contain alphanumeric chars!
        if re.match(r'.*\W.*', keypair_name):
            raise ConfigurationError(
                "Keypair name `%s` is invalid: keypair names can only contain "
                "alphanumeric chars: [a-zA-Z0-9_]" % keypair_name)
        self.keypair_name = keypair_name
        self.public_key = os.path.expanduser(
            os.path.expandvars(public_key.strip()))
        self.image_id = image_id
        self.image_name = image_name
        self.instance_type = instance_type
        self._instance_type_specs = {}
        self.user_data = user_data

        self._parse_security_group()
        self._conn = None

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

        args = {
            'aws_access_key_id': self.ec2_access_key,
            'aws_secret_access_key': self.ec2_secret_key,
        }

        if self.ec2_url:
            region = boto.ec2.regioninfo.RegionInfo(
                name=self.region,
                endpoint=self.ec2_url.hostname,
            )
            args['region'] = region
            args['port'] = self.ec2_url.port
            args['host'] = self.ec2_url.hostname
            args['path'] = self.ec2_url.path
            if self.ec2_url.scheme in ['http']:
                args['is_secure'] = False

        self._conn = boto.connect_ec2(**args)
        # Set up the VMPool persistent class. This has been delayed
        # until here because otherwise self._conn is None
        pooldir = os.path.join(os.path.expandvars(EC2Lrms.RESOURCE_DIR),
                               'vmpool', self.name)
        self._vmpool = EC2VMPool(pooldir, self._conn)

        try:
            all_images = self._conn.get_all_images()
        except boto.exception.EC2ResponseError as ex:
            if ex.status == 404:
                # NotFound, probaly the endpoint is wrong
                raise RuntimeError(
                    "Unable to contact the EC2 endpoint at `%s`. Please, "
                    "verify that the URL in the configuration file is "
                    "correct." % (self.ec2_url,))
            raise RuntimeError(
                "Unknown error while connecting to the EC2 endpoint `%s`: "
                "%s" % (self.ec2_url, ex))
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

    def _create_instance(self, image_id, instance_type=None, user_data=None):
        """
        Create an instance using the image `image_id` and instance
        type `instance_type`. If not `instance_type` is defined, use
        the default.

        This method will also setup the keypair and the security
        groups, if needed.
        """
        self._connect()

        args = {
            'key_name': self.keypair_name,
            'min_count': 1,
            'max_count': 1
        }
        if instance_type:
            args['instance_type'] = instance_type

        if user_data:
            args['user_data'] = user_data

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
            self._have_keypair(keypairs[self.keypair_name])

        # Setup security groups
        if 'security_group_name' in self:
            self._setup_security_groups()
            args['security_groups'] = [self.security_group_name]

        # FIXME: we should add check/creation of proper security
        # groups
        gc3libs.log.debug("Create new VM using image id `%s`", image_id)
        try:
            reservation = self._conn.run_instances(image_id, **args)
        except boto.exception.EC2ResponseError as err:
            # scrape actual error kind and message out of the
            # exception; we do this mostly for sensible logging, but
            # could be an actual improvement to Boto to provide
            # different exception classes based on the <Code>
            # element...
            # XXX: is there a more robust way of doing this?
            match = _BOTO_ERRMSG_RE.search(str(err))
            if match:
                raise UnrecoverableError(
                    "Error starting instance: EC2ResponseError/%s: %s"
                    % (match.group('code'), match.group('message')))
            else:
                # fall back to normal reporting...
                raise UnrecoverableError("Error starting instance: %s" % err)
        except Exception as ex:
            raise UnrecoverableError("Error starting instance: %s" % ex)
        vm = reservation.instances[0]
        self._vmpool.add_vm(vm)
        gc3libs.log.info(
            "VM with id `%s` has been created and is in %s state.",
            vm.id, vm.state)
        return vm

    def _get_subresource(self, vm):
        """
        Return the resource associated to the virtual machine `vm`.

        Updates the internal list of available resources if needed.

        """
        if vm.id not in self.subresources:
            self.subresources[vm.id] = self._make_subresource(
                vm.id, vm.preferred_ip)
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
    def __pubkey_ssh_fingerprint(privkey):
        """
        Compute SSH public key fingerprint like `ssh-keygen -l -f`.

        We need to convert the key fingerprint from Paramiko's
        internal representation to this colon-separated hex format
        because that's the way the fingerprint is returned from the
        EC2 API.
        """
        return ':'.join((i.encode('hex') for i in privkey.get_fingerprint()))

    @staticmethod
    def __pubkey_aws_fingerprint(privkeypath,
                                 reader=Crypto.PublicKey.RSA.importKey):
        """
        Compute SSH public key fingerprint like AWS EC2 does.

        Return a string representation of the key fingerprint in
        colon-separated hex format (just like OpenSSH commands print
        it to the terminal).

        Argument `privkeypath` is the filesystem path to a file
        containing the private key data.
        """
        with open(privkeypath, 'r') as keydata:
            pubkey = reader(keydata).publickey()
            return insert_char_every_n_chars(
                hashlib.md5(pubkey.exportKey('DER')).hexdigest(), ':', 2)

    def _have_keypair(self, ec2_key):
        """
        Check if the given SSH key is available locally.

        Try to locate the given SSH key first among the keys loaded
        into a running ``ssh-agent`` (if any), then in the key file
        given in the ``public_key`` configuration key.
        """
        # try with SSH agent first
        gc3libs.log.debug("Checking if keypair is registered in SSH agent...")
        agent = paramiko.Agent()
        fingerprints_from_agent = [
            self.__pubkey_ssh_fingerprint(privkey)
            for privkey in agent.get_keys()
        ]
        if ec2_key.fingerprint in fingerprints_from_agent:
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

        privkey = None
        local_fingerprints = []
        for format, privkey_reader, pubkey_reader in [
                ('DSS', paramiko.DSSKey.from_private_key_file,
                 Crypto.PublicKey.DSA.importKey),
                ('RSA', paramiko.RSAKey.from_private_key_file,
                 Crypto.PublicKey.RSA.importKey),
        ]:
            try:
                gc3libs.log.debug(
                    "Trying to load key file `%s` as SSH %s key...",
                    keyfile, format)
                privkey = privkey_reader(keyfile)
                gc3libs.log.info(
                    "Successfully loaded key file `%s` as SSH %s key.",
                    keyfile, format)
                # compute public key fingerprints, for comparing them
                # with the remote one
                localkey_fingerprints = [
                    # Usual SSH key fingerprint, computed like
                    # `ssh-keygen -l -f` This is used, e.g., by
                    # OpenStack's EC2 compatibility layer.
                    self.__pubkey_ssh_fingerprint(privkey),
                    # Amazon EC2 computes the key fingerprint in a
                    # different way, see http://blog.jbrowne.com/?p=23 and
                    # https://gist.github.com/jtriley/7270594 for details.
                    self.__pubkey_aws_fingerprint(keyfile, pubkey_reader),
                ]
            # PasswordRequiredException < SSHException so we must
            # check this first
            except paramiko.PasswordRequiredException:
                gc3libs.log.warning(
                    "Key %s is encripted with a password, so we cannot check "
                    " if it matches the remote keypair (maybe you should"
                    " start `ssh-agent`?). Continuing without consistency"
                    " check with remote fingerprint.", keyfile)
                return False
            except paramiko.SSHException as ex:
                gc3libs.log.debug(
                    "File `%s` is not a valid %s private key: %s",
                    keyfile, format, ex)
                # try with next format
                continue
        if privkey is None:
            raise ValueError("Public key `%s` is neither a valid"
                             " RSA key nor a DSS key" % self.public_key)

        # check key fingerprint
        if (local_fingerprints
                and ec2_key.fingerprint not in localkey_fingerprints):
            raise UnrecoverableAuthError(
                "Keypair `%s` exists but has different fingerprint than"
                " local one: local public key file `%s` has fingerprint(s)"
                " `%s`, whereas EC2 API reports fingerprint `%s`."
                " Aborting!" % (
                    self.public_key,
                    self.keypair_name,
                    '/'.join(localkey_fingerprints),
                    ec2_key.fingerprint,
                ),
                do_log=True)

    def _import_keypair(self):
        """
        Create a new keypair and import the public key defined in the
        configuration file.
        """
        with open(os.path.expanduser(self.public_key)) as fd:
            try:
                key_material = fd.read()
                imported_key = self._conn.import_key_pair(
                    self.keypair_name, key_material)
                gc3libs.log.info(
                    "Successfully imported key `%s`"
                    " with fingerprint `%s` as keypair `%s`",
                    imported_key.name, imported_key.fingerprint,
                    self.keypair_name)
            except Exception as ex:
                raise UnrecoverableError("Error importing keypair %s: %s"
                                         % (self.keypair_name, ex))

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
            except Exception as ex:
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
                except Exception as ex:
                    if gc3libs.error_ignored(
                            # context:
                            # - module
                            'ec2',
                            # - class
                            'EC2Lrms',
                            # - method
                            'setup_security_groups',
                            # - actual error class
                            ex.__class__.__name__,
                            # - additional keywords
                            'setup',
                            'security',
                            'network',
                            'cloud',
                    ):
                        gc3libs.log.info(
                            "Ignoring error adding rule %s"
                            " to security group %s: %s",
                            rule, self.security_group_name, ex)
                    else:
                        # propagate exception to caller
                        raise
        else:
            # Check if the security group has all the rules we want
            security_group = groups[self.security_group_name]
            current_rules = []
            for rule in security_group.rules:
                rule_dict = {
                    'ip_protocol': rule.ip_protocol,
                    'from_port': int(rule.from_port),
                    'to_port': int(rule.to_port),
                    'cidr_ip': str(rule.grants[0]),
                }
                current_rules.append(rule_dict)

            for new_rule in self.security_group_rules:
                if new_rule not in current_rules:
                    security_group.authorize(**new_rule)

    # Public methods

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
        resource = self._get_subresource(self._get_vm(app.execution._lrms_vm_id))
        return resource.cancel_job(app)

    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        self.updated = False
        # Since we create the resource *before* the VM is actually up
        # & running, it's possible that the `frontend` value of the
        # resources points to a non-existent hostname. Therefore, we
        # have to update them with valid public_ip, if they are
        # present.

        self._connect()
        # Update status of known VMs
        for vm_id in self._vmpool:
            try:
                vm = self._vmpool.get_vm(vm_id)
            except UnrecoverableError as ex:
                gc3libs.log.warning(
                    "Removing stale information on VM `%s`. It has probably"
                    " been deleted from outside GC3Pie.", vm_id)
                self._vmpool.remove_vm(vm_id)
                continue

            vm.update()
            if vm.state == 'pending':
                # If VM is still in pending state, skip creation of
                # the resource
                continue
            elif vm.state == 'error':
                # The VM is in error state: exit.
                gc3libs.log.error(
                    "VM with id `%s` is in ERROR state."
                    " Terminating it!", vm.id)
                vm.terminate()
                self._vmpool.remove_vm(vm.id)
            elif vm.state == 'terminated':
                gc3libs.log.info(
                    "VM `%s` in TERMINATED state. It has probably been"
                    " terminated from outside GC3Pie. Removing it from the"
                    " list of VM.", vm.id)
                self._vmpool.remove_vm(vm.id)
            elif vm.state in ['shutting-down', 'stopped']:
                # The VM has probably ben stopped or shut down from
                # outside GC3Pie.
                gc3libs.log.error(
                    "VM with id `%s` is in terminal state `%s`.",
                    vm.id, vm.state)

            # Get or create a resource associated to the vm
            resource = self._get_subresource(vm)
            try:
                resource.get_resource_status()
            except TransportError as ex:
                for ip in [vm.public_dns_name, vm.private_ip_address]:
                    if vm.preferred_ip == ip:
                        continue
                    vm.preferred_ip = ip
                    resource.frontend = ip
                    gc3libs.log.info(
                        "Connection error. Trying with secondary IP"
                        " address %s", vm.preferred_ip)
                    try:
                        resource.get_resource_status()
                        break
                    except Exception as ex:
                        # XXX: I'm exempting this from the GC3Pie
                        # `error_ignored()` policy, since this is a kind of
                        # "expected" error -- it *will* happen if the VM has
                        # not booted up yet or if we're hunting for the correct
                        # address.
                        gc3libs.log.debug(
                            "Ignoring error in updating resource %s: %s"
                            " The corresponding VM may not be ready yet.",
                            resource.name, ex)
            except Exception as ex:
                # FIXME: Actually, we should try to identify the kind of
                # error we are getting. For instance, if the
                # configuration options `username` is wrong, we will
                # create VMs but we will never be able to submit jobs
                # to them, thus causing an increasing number of
                # useless VMs created on the cloud.
                if gc3libs.error_ignored(
                        # context:
                        # - module
                        'ec2',
                        # - class
                        'EC2Lrms',
                        # - method
                        'get_resource_status',
                        # - actual error class
                        ex.__class__.__name__,
                        # - additional keywords
                        'vm',
                ):
                    gc3libs.log.info(
                        "Ignoring error in updating resource %s: %s"
                        "The corresponding VM may not be ready yet.",
                        resource.name, ex)
                else:
                    # propagate exception to caller
                    raise
            if resource.updated:
                # Update also the instance_type specs, if not
                # already updated
                if not self._instance_type_specs:
                    specs = self._instance_type_specs
                    specs['architecture'] = resource['architecture']
                    specs['max_cores'] = resource['max_cores']
                    specs['max_cores_per_job'] = resource['max_cores_per_job']
                    specs['max_memory_per_core'] = resource['total_memory']
                    self.update(specs)

        self._vmpool.update()
        return self

    @same_docstring_as(LRMS.get_results)
    def get_results(self, app, download_dir, overwrite=False,
                    changed_only=True):
        subresource = self._get_subresource(self._get_vm(app.execution._lrms_vm_id))
        return subresource.get_results(app, download_dir,
                                       overwrite=overwrite,
                                       changed_only=changed_only)

    @same_docstring_as(LRMS.update_job_state)
    def update_job_state(self, app):
        if app.execution._lrms_vm_id not in self.subresources:
            try:
                self.subresources[app.execution._lrms_vm_id] = self._get_subresource(
                    self._get_vm(app.execution._lrms_vm_id))
            except InstanceNotFound as ex:
                gc3libs.log.error(
                    "Changing state of task '%s' to TERMINATED since EC2 "
                    "instance '%s' does not exist anymore.",
                    app.execution.lrms_jobid, app.execution._lrms_vm_id)
                app.execution.state = Run.State.TERMINATED
                raise ex
            except UnrecoverableError as ex:
                gc3libs.log.error(
                    "Changing state of task '%s' to UNKNOWN because of "
                    "an EC2 error.", app.execution.lrms_jobid)
                app.execution.state = Run.State.UNKNOWN
                raise ex

        return self.subresources[app.execution._lrms_vm_id].update_job_state(app)

    def submit_job(self, job):
        """
        Submission on an EC2 resource will usually happen in multiple
        steps, since creating a VM and attaching a resource to it will
        take some time.

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
        self._connect()
        # Updating resource is needed to update the subresources. This
        # is not always done before the submit_job because of issue
        # nr.  386:
        #     https://github.com/uzh/gc3pie/issues/386
        self.get_resource_status()

        pending_vms = set(vm.id for vm in self._vmpool.get_all_vms()
                          if vm.state == 'pending')

        image_id = self.get_image_id_for_job(job)
        instance_type = self.get_instance_type_for_job(job)
        # Check that we can actually submit to a flavor like this
        # XXX: this check shouldn't be done by the Engine???
        if self._instance_type_specs:
            specs = self._instance_type_specs
            max_mem = specs['max_memory_per_core']
            max_cpus = specs['max_cores_per_job']
            if (job.requested_memory is not None and
                job.requested_memory > max_mem) \
                or (job.requested_cores is not None and
                    job.requested_cores > max_cpus):
                raise gc3libs.exceptions.LRMSSubmitError(
                    "EC2 flavor %s does not have enough memory/cpus "
                    "to run application %s" % (
                        self.instance_type, job.jobname))

        # First of all, try to submit to one of the subresources.
        for vm_id, resource in list(self.subresources.items()):
            if not resource.updated:
                # The VM is probably still booting, let's skip to the
                # next one and add it to the list of "pending" VMs.
                pending_vms.add(vm_id)
                continue
            try:
                # Check that the required image id and instance type
                # are correct
                vm = self._get_vm(vm_id)
                if (vm.image_id != image_id
                        or vm.instance_type != instance_type):
                    continue
                resource.submit_job(job)
                job.execution._lrms_vm_id = vm_id
                job.changed = True
                gc3libs.log.info(
                    "Job successfully submitted to remote resource %s.",
                    resource.name)
                return job
            except gc3libs.exceptions.LRMSSubmitError as ex:
                if gc3libs.error_ignored(
                        # context:
                        # - module
                        'ec2',
                        # - class
                        'EC2Lrms',
                        # - method
                        'submit_job',
                        # - actual error class
                        ex.__class__.__name__,
                        # - additional keywords
                        'submit',
                ):
                    gc3libs.log.debug(
                        "Ignoring error in submitting to resource %s: %s. ",
                        resource.name, ex)
                else:
                    # propagate exception to caller
                    raise

        # Couldn't submit to any resource.
        if not pending_vms:
            # No pending VM, and no resource available. Create a new VM
            if not self.vm_pool_max_size \
                    or len(self._vmpool) < self.vm_pool_max_size:
                user_data = self.get_user_data_for_job(job)
                vm = self._create_instance(image_id,
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
        resource = self._get_subresource(
            self._get_vm(app.execution._lrms_vm_id))
        return resource.peek(app, remote_filename, local_file, offset, size)

    def validate_data(self, data_file_list=None):
        """
        Supported protocols: file
        """
        for url in data_file_list:
            if url.scheme not in ['file']:
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
        resource = self._get_subresource(self._get_vm(app.execution._lrms_vm_id))
        resource.free(app)

        # FIXME: current approach in terminating running instances:
        # if no more applications are currently running, turn the instance off
        # check with the associated resource
        resource.get_resource_status()
        if not resource.has_running_tasks():
            # turn VM off
            vm = self._get_vm(app.execution._lrms_vm_id)
            gc3libs.log.info("VM instance %s at %s is no longer needed."
                             " Terminating.", vm.id, vm.public_dns_name)
            del self.subresources[vm.id]
            vm.terminate()
            del self._vmpool[vm.id]
            # self._session.save_all()

    @same_docstring_as(LRMS.close)
    def close(self):
        gc3libs.log.info("Closing connection to cloud '%s'...",
                         self.name)
        if self._conn is None and not self.enabled:
            # The resources was most probably disabled by command
            # line. We didn't update it before, so we don't care about
            # currently running VMs now.
            return
        # Update status of VMs and remote resources
        self.get_resource_status()
        for vm_id, resource in list(self.subresources.items()):
            if resource.updated and not resource.has_running_tasks():
                vm = self._get_vm(vm_id)
                gc3libs.log.warning(
                    "VM instance %s at %s is no longer needed. "
                    "You may need to terminate it manually.",
                    vm.id, vm.public_dns_name)
                del self._vmpool[vm.id]
            resource.close()
        # self._session.save_all()


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="ec2",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
