#! /usr/bin/env python

"""
Deal with GC3Pie configuration files.
"""

# Copyright (C) 2021                   Google LLC.
# Copyright (C) 2012-2016, 2018, 2019  University of Zurich. All rights reserved.
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

from future import standard_library
standard_library.install_aliases()
from builtins import zip
from builtins import str

# stdlib imports
try:
    from collections.abc import Mapping  # Python 3.3+
except ImportError:
    from collections import Mapping
from collections import defaultdict
import os
import re
import sys
if sys.version_info[0] == 2:
    from ConfigParser import SafeConfigParser
    from ConfigParser import Error as ConfigParserError
    def make_config_parser():
        return SafeConfigParser()
    # a `SafeConfigParser` cannot easily be cast to Python dict
    def _cp2dict(parser):
        result = {}
        for sectname in parser.sections():
            result[sectname] = dict(parser.items(sectname))
        result['DEFAULT'] = parser.defaults()
        return result
    def read_config_lines(parser, stream, source):
        return parser.readfp(stream, source)
else:
    # `SafeConfigParser` was deprecated in Py3 in favor of `ConfigParser`
    from configparser import ConfigParser
    from configparser import Error as ConfigParserError
    def make_config_parser():
        return ConfigParser(strict=False)
    # `readfp()` is deprecated since Py3.2
    def read_config_lines(parser, stream, source):
        return parser.read_file(stream, source)
    def _cp2dict(parser):
        return dict(parser)

# GC3Pie imports
import gc3libs
import gc3libs.defaults
from gc3libs.compat._inspect import getargspec
import gc3libs.authentication
import gc3libs.utils

from gc3libs.quantity import Memory, GB, MB, MiB, Duration, hours


# module metadata
__docformat__ = 'reStructuredText'


# auxiliary methods for `Configuration`
#
# these must be defined before `Configuration` is parsed, because they
# are referenced in the definition of `Configuration` itself
#

# map values for the `architecture=...` configuration item
# into internal constants
_architecture_value_map = {
    # 'x86-32', 'x86 32-bit', '32-bit x86' and variants thereof
    re.compile('x86[ _-]+32([ _-]*bits?)?', re.I): gc3libs.Run.Arch.X86_32,
    re.compile('32[ _-]*bits? +[ix]86', re.I): gc3libs.Run.Arch.X86_32,
    # accept also values printed by `uname -a` on 32-bit x86 archs
    re.compile('i[3456]86', re.I): gc3libs.Run.Arch.X86_32,
    # 'x86_64', 'x86 64-bit', '64-bit x86' and variants thereof
    re.compile('x86[ _-]+64([ _-]*bits?)?', re.I): gc3libs.Run.Arch.X86_64,
    re.compile('64[ _-]*bits? +[ix]86', re.I): gc3libs.Run.Arch.X86_64,
    # also accept commercial arch names
    re.compile('(amd[ -]*64|x64|emt64|intel[ -]*64)( *bits?)?', re.I):
    gc3libs.Run.Arch.X86_64,
    # finally, map "32-bit" and "64-bit" to i686 and x86_64
    re.compile('32[ _-]*bits?', re.I): gc3libs.Run.Arch.X86_32,
    re.compile('64[ _-]*bits?', re.I): gc3libs.Run.Arch.X86_64,
}

def _matching_architecture(value):
    """Return first matching entry from `_architecture_value_map`."""
    for matcher, arch in _architecture_value_map.items():
        if matcher.match(value):
            return arch
    raise ValueError("Unknown architecture '%s'." % value)

def _parse_architecture(arch_str):
    """Return set of values in `Run.Arch` corresponding to a comma-separated list of architecture strings."""
    archs = [_matching_architecture(value.strip()) for value in arch_str.split(',')]
    if len(archs) == 0:
        raise ValueError("Empty or invalid 'architecture' setting.")
    return set(archs)


def _legacy_parse_duration(duration_str):
    try:
        # old-style config: integral number of hours
        val = int(duration_str) * hours
        gc3libs.log.warning("'max_walltime' should always have a "
                            "valid unit format (e.g. '24 hours'). Using "
                            "default unit: hours")
        return val
    except ValueError:
        # apply `Duration` parsing rules; if this fails, users will
        # see the error message from the `Duration` parser.
        return Duration(duration_str)


def _legacy_parse_memory(memory_str, name, unit=GB, unit_name='GB'):
    try:
        # old-style config: integral number of "unit"
        val = int(memory_str) * unit
        gc3libs.log.warning(
            "'%s' should always have a valid unit format (e.g. '2 GB')."
            " Using default unit: %s", name, unit_name)
        return val
    except ValueError:
        # apply usual quantity parsing rules; if this fails, users
        # will see the error message from the `Memory`/`Quantity` parser.
        return Memory(memory_str)


def _legacy_parse_os_overhead(os_overhead_str):
    try:
        # old-style config: integral number of MiBs
        val = int(os_overhead_str) * MiB
        gc3libs.log.warning("'vm_os_overhead' should always have a "
                            "valid unit format (e.g. '512 MiB'). Using "
                            "default unit: MiB")
        return val
    except ValueError:
        # apply usual quantity parsing rules; if this fails, users
        # will see the error message from the `Memory`/`Quantity` parser.
        return Memory(os_overhead_str)

# the main class of this module


class Configuration(gc3libs.utils.Struct):

    """
    In-memory representation of the GC3Pie configuration.

    This class provides facilities for:

    * parsing configuration files (methods `load`:meth: and
      `merge_file`:meth:);
    * parsing a configuration from a python dictionary
      (method `construct_from_cfg_dict`:meth:);
    * validating the loaded values;
    * instanciating the internal GC3Pie objects resulting from the
      configuration (methods `make_auth`:meth: and
      `make_resource`:meth:).

    The constructor takes a list of files to load (`locations`), a python
    dictionary of sections with key value pairs (`cfg_dict`), and a list of
    *key=value* pairs to provide defaults for the configuration. All three
    arguments are optional and can be omitted, resulting in a configuration
    containing only GC3Pie default values. If `locations` is not empty but
    there are no config files at those locations, the constructor will raise
    a `NoAccessibleConfigurationFile` exception if `cfg_dict` is None.

    Example 1: initialization from config file::

      >>> import os
      >>> example_cfgfile = os.path.join(
      ...    os.path.dirname(__file__), 'etc/gc3pie.conf.example')
      >>> cfg = Configuration(example_cfgfile)
      >>> cfg.debug
      '0'

    Example 2: initialization from a Python dictionary::

      >>> d = dict()
      >>> d["DEFAULT"] = {"debug": 0}
      >>> d["auth/ssh_bob"] = {
      ... "type": "ssh", "username": "your_ssh_user_name_on_computer_bob"}
      >>> cfg = Configuration(cfg_dict=d)
      >>> cfg.debug
      0
      >>> cfg.auths["ssh_bob"]["type"] == 'ssh'
      True

    Example 3: initialization from key=value list::

      >>> cfg = Configuration(auto_enable_auth=False, foo=1, bar='baz')
      >>> cfg.auto_enable_auth
      False
      >>> cfg.foo == 1
      True
      >>> cfg.bar == 'baz'
      True

    When all three arguments are supplied, configuration options are taken
    in the following order of precedence:
    * config file [highest priority],
    * Python dictionary [middle priority],
    * key=value list [lowest priority]

      >>> # config file > Python dictionary
      ... d = {"DEFAULTS": {"debug": 1}}
      >>> cfg = Configuration(example_cfgfile, config_dict=d)
      >>> cfg.debug == '0'
      True
      >>>
      >>> # config file > key=value list
      ... cfg = Configuration(example_cfgfile, debug=1)
      >>> cfg.debug == '0'
      True
      >>>
      >>> # Python dictionary > key=value list
      ... cfg = Configuration(config_dict=d, debug=0)
      >>> cfg.debug == '0'
      False

    Example 4: default initialization::

      >>> cfg = Configuration()
      >>> cfg.auto_enable_auth
      True

    """

    def __init__(self, *locations, **extra_args):
        self._auth_factory = None

        # these fields are required
        self.resources = defaultdict(gc3libs.utils.Struct)
        self.auths = defaultdict(gc3libs.utils.Struct)

        # ensure `auth = none` is always available
        self.auths['none'] = gc3libs.utils.Struct(type='none')

        # use keyword arguments to set defaults
        self.auto_enable_auth = extra_args.pop('auto_enable_auth', True)
        self.update(extra_args)

        # save the list of (valid) config files
        self.cfgfiles = []

        cfg_dict = extra_args.pop('cfg_dict', None)
        if cfg_dict:
            self.construct_from_cfg_dict(cfg_dict)

        # load configuration files if any
        if locations:
            try:
                self.load(*locations)
            # If we successfully loaded parameters from a cfg_dict,
            # don't raise an exception if we couldn't find any configuration files.
            except gc3libs.exceptions.NoAccessibleConfigurationFile as ex:
                if cfg_dict is None:
                    raise ex

        # actual resource constructor classes
        self._resource_constructors_cache = {}

    def construct_from_cfg_dict(self, cfg_dict, filename=None):
        """
        Create a Configuration object from the settings defined in `cfg_dict`.

        Parameter `cfg_dict` may either be a Python dictionary, having the
        same general format as a configuration file, or a `ConfigParser`
        instance into which an INI-format configuration file has been read.
        See below for an example of a configuration file converted to a
        dictionary.

        :param dict cfg_dict: The Python dictionary to load settings from.

        :param string filename: Optional. If this dictionary was constructed from
        a config file, `filename` is the name of the config file.

        Example: A Configuration File::

            [auth/ssh]
            type = ssh
            username = gc3pie

            [resource/test]
            type = shellcmd
            auth = ssh
            transport = local
            max_memory_per_core = 2
            max_walltime = 8
            max_cores = 2
            architecture = x86_64
            override = False

            [DEFAULT]
            max_cores_per_job = 2

        Example: Equivalent Dictionary::

            >>> cfg_dict = {
            ...         'auth/ssh': {
            ...             'type': 'ssh',
            ...             'username': 'gc3pie'
            ...         },
            ...         'resource/test': {
            ...             'type': 'shellcmd',
            ...             'auth': 'ssh',
            ...             'transport': 'local',
            ...             'max_memory_per_core': '2',
            ...             'max_walltime': '8',
            ...             'max_cores': '2',
            ...             'architecture': 'x86_64',
            ...             'override': 'False'
            ...         },
            ...         'DEFAULT': {
            ...             'max_cores_per_job': '2'
            ...         }
            ...     }
            >>>
        """
        # if `cfg_dict` is a non-iterable ConfigParser (which may happen on Py
        # 2.7), then convert it to a Python `dict` instance
        if not isinstance(cfg_dict, Mapping):
            cfg_dict = _cp2dict(cfg_dict)

        defaults, resources, auths = self._split(cfg_dict, filename)
        for name, values in resources.items():
            self.resources[name].update(values)
        for name, values in auths.items():
            self.auths[name].update(values)
        for name, value in defaults.items():
            if not name.startswith('_'):
                self[name] = value

    def load(self, *locations):
        """
        Merge settings from configuration files into this `Configuration`
        instance.

        Environment variables and `~` references are expanded in the
        location file names.

        If any of the specified files does not exist or cannot be read
        (for whatever reason), a message is logged but the error is
        ignored.  However, a `NoConfigurationFile` exception is raised
        if *none* of the specified locations could be read.

        :raise gc3libs.exceptions.NoConfigurationFile:
            if none of the specified files could be read.
        """
        files_successfully_read = 0
        files_successfully_parsed = 0

        for filename in locations:
            filename = os.path.expandvars(os.path.expanduser(filename))
            if os.path.exists(filename):
                if not os.access(filename, os.R_OK):
                    gc3libs.log.debug(
                        "Configuration.load(): File '%s' cannot be read,"
                        " ignoring.", filename)
                    continue  # with next `filename`
            else:
                gc3libs.log.debug(
                    "Configuration.load(): File '%s' does not exist,"
                    " ignoring.", filename)
                continue  # with next `filename`

            filename = os.path.abspath(filename)
            self.cfgfiles.append(filename)
            # since file passed the `R_OK` access test, we know it
            # *can* be read before actually doing it
            files_successfully_read += 1
            try:
                self.merge_file(filename)
                files_successfully_parsed += 1
            except gc3libs.exceptions.ConfigurationError:
                continue  # with next file

        if files_successfully_read == 0:
            raise gc3libs.exceptions.NoAccessibleConfigurationFile(
                "Could not read any configuration file; tried location '%s'."
                % "', '".join(locations))
        if files_successfully_parsed == 0:
            raise gc3libs.exceptions.NoValidConfigurationFile(
                "Could not parse any configuration file;"
                " tried location(s) '%s' but they all had errors."
                " (Which see in previous log messages.)"
                % "', '".join(locations))

    def merge_file(self, filename):
        """
        Read configuration files and merge the settings into this
        `Configuration` object.

        Contrary to `load`:meth: (which see), the file name is taken
        literally and an error is raised if the file cannot be read
        for whatever reason.

        Any parameter which is set in the configuration files'
        ``[DEFAULT]`` section, and whose name does not start with
        underscore (``_``) defines an attribute in the current
        `Configuration`.

        .. warning::

          No type conversion is performed on values set this way - so
          they all end up being strings!

        :raise gc3libs.exceptions.ConfigurationError: if the
            configuration file does not exist, cannot be read, is
            corrupt or has wrong format.
        """
        gc3libs.log.debug(
            "Configuration.merge_file(): Reading file '%s' ...",
            filename)
        with open(filename, 'r') as stream:
            parser = self._parse(stream, filename)
        self.construct_from_cfg_dict(parser, filename)

    def _parse(self, stream, filename=None):
        """
        Populates a `ConfigParser` or `SafeConfigParser` object
        with the content of `stream`. `stream` should be in a valid
        configuration file format.

        :raise gc3libs.exceptions.ConfigurationError if `stream`
            cannot be read.

        :rtype: a `ConfigParser` or `SafeConfigParser` object
        """
        parser = make_config_parser()
        try:
            read_config_lines(parser, stream, filename)
        except ConfigParserError as err:
            if filename is None:
                try:
                    filename = stream.name
                except AttributeError:
                    filename = repr(stream)
            raise gc3libs.exceptions.ConfigurationError(
                "Configuration file `%s` is unreadable or malformed: %s: %s"
                % (filename, err.__class__.__name__, err))

        return parser

    def _split(self, cfg_dict, filename=None):
        """
        Iterate through `cfg_dict` and return a `(defaults, resources, auths)`
        triple.

        The members of the result triple are as follows:

        * `defaults`: a dictionary containing keys found in the
          ``[DEFAULTS]`` section of the configuration file (if any);

        * `resources`: a dictionary mapping resource names into a
          dictionary of key/value attributes contained in the
          configuration file under the ``[resource/name]`` heading;

        * `auths`: same for the ``[auth/name]`` sections.

        In addition, key renaming (for compatibility with previous
        versions) and type conversion is performed here, so that the
        returned dictionaries conform to a specified schema.  Type
        conversion are performed according to the value of the
        `_convert` attribute on this object; any attribute not
        mentioned in that table will have type ``str`` (i.e., it is
        left unchanged).
        """
        defaults = dict()
        resources = defaultdict(dict)
        auths = defaultdict(dict)

        # update `defaults` with the contents of the `[DEFAULTS]` section
        if 'DEFAULT' in cfg_dict:
            defaults.update(cfg_dict['DEFAULT'])

        for sectname in cfg_dict:
            if sectname.startswith('auth/'):
                # handle auth section
                name = sectname.split('/', 1)[1]
                # Make sure defaults are merged in
                config_items = defaults.copy()
                config_items.update(dict(cfg_dict[sectname].items()))
                gc3libs.log.debug(
                    "Config._split():"
                    " Read configuration stanza for auth '%s'",
                    name)

                # minimal sanity check
                for key in self._auth_required_keys:
                    if key not in config_items:
                        raise gc3libs.exceptions.ConfigurationError(
                            "Missing mandatory configuration key `{key}`"
                            " in section [{sectname}]"
                            " of the configuration file `{filename}`."
                            " This configuration file will be ignored."
                            .format(
                                key=key,
                                sectname=sectname,
                                filename=filename
                            ))

                auths[name].update(config_items)
                auths[name]['name'] = name

            elif sectname.startswith('resource/'):
                # handle resource section
                name = sectname.split('/', 1)[1]
                gc3libs.log.debug(
                    "Config._split():"
                    " Read configuration stanza for resource '%s'." %
                    name)

                # Make sure defaults are merged in
                config_items = defaults.copy()
                config_items.update(dict(cfg_dict[sectname].items()))

                self._perform_key_renames(
                    config_items, self._renamed_keys, filename)
                self._perform_value_updates(
                    config_items, self._updated_values, filename)
                self._perform_filename_conversion(
                    config_items, self._path_key_regexp, filename)
                try:
                    self._perform_type_conversions(
                        config_items, self._convert, filename)
                except Exception as err:
                    raise gc3libs.exceptions.ConfigurationError(
                        "Incorrect entry for resource '%s' in configuration"
                        " file '%s': %s"
                        % (name, filename, str(err)))

                # minimal sanity checks
                for key in self._resource_required_keys:
                    if key not in config_items:
                        raise gc3libs.exceptions.ConfigurationError(
                            "Missing mandatory configuration key `{key}`"
                            " in section [{sectname}]"
                            " of the configuration file `{filename}`."
                            " This configuration file will be ignored."
                            .format(
                                key=key,
                                sectname=sectname,
                                filename=filename
                            ))

                for key in self._resource_keys_value_required:
                    if key in config_items and not config_items[key]:
                        raise gc3libs.exceptions.ConfigurationError(
                            "Found empty configuration key `{key}`"
                            " in section [{sectname}]"
                            .format(key=key, sectname=sectname))

                resources[name].update(config_items)
                resources[name]['name'] = name
                if __debug__:
                    gc3libs.log.debug(
                        "Config._split(): Resource '%s' defined by: %s.", name,
                        ', '.join([
                            ("%s=%r" %
                             (k, v)) for k, v in sorted(
                                 resources[name].items())]))

            # Don't warn about default section
            elif sectname != "DEFAULT":
                # Unhandled sectname
                gc3libs.log.warning(
                    "Config._split(): unknown configuration section '%s'"
                    " -- ignoring!",
                    sectname)

        return (defaults, resources, auths)

    # config keys common to every kind of `[auth/*]` section;
    # if any of these is missing, the section is clearly invalid
    _auth_required_keys = (
        'type',
    )

    # config keys common to every kind of `[resource/*]` section;
    # if any of these is missing, the section is clearly invalid
    _resource_required_keys = (
        'architecture',
        'max_cores',
        'max_cores_per_job',
        'max_memory_per_core',
        'max_walltime',
        'type',
    )

    # config keys that are not allowed to have empty values
    _resource_keys_value_required = (
        'keypair_name',
    )

    _renamed_keys = {
        # old key name           new key name
        # ===================    ===================
        'ncores'               : 'max_cores',
        'sge_accounting_delay' : 'accounting_delay',
    }

    @staticmethod
    def _perform_key_renames(config_items, renames, filename):
        for oldkey, newkey in renames.items():
            if oldkey in config_items:
                gc3libs.log.warning(
                    "Configuration item '%s' was renamed to '%s',"
                    " please change occurrences of '%s' to '%s'"
                    " in configuration file '%s'.",
                    oldkey, newkey, oldkey, newkey, filename)
                if newkey in config_items:
                    # drop
                    gc3libs.log.error(
                        "Both old-style configuration item '%s' and new-style"
                        " '%s' detected in file '%s': ignoring old-style item"
                        " '%s=%s'.",
                        oldkey,
                        newkey,
                        filename,
                        config_items[oldkey])
                else:
                    config_items[newkey] = config_items[oldkey]
                del config_items[oldkey]

    _updated_values = {
        # key name  old value             new value
        # ========  ===================   ==================
        'type': {
                    'arc'               : gc3libs.defaults.ARC0_LRMS,
                    'ssh'               : gc3libs.defaults.SGE_LRMS,
                    'subprocess'        : gc3libs.defaults.SHELLCMD_LRMS,
        },
    }

    @staticmethod
    def _perform_value_updates(config_items, renames, filename):
        for key, changed in renames.items():
            if key in config_items:
                value = config_items[key]
                if value in changed:
                    gc3libs.log.warning(
                        "Configuration value '%s' was renamed to '%s',"
                        " please change occurrences of '%s=%s' to '%s=%s'"
                        " in configuration file '%s'.",
                        value, changed[value],
                        key, value, key, changed[value],
                        filename)
                    config_items[key] = changed[value]

    _path_key_regexp = re.compile(r'^(\w+_)?(prologue|epilogue)$')

    @staticmethod
    def _perform_filename_conversion(config_items, path_regexp, filename):
        for key, value in config_items.items():
            if path_regexp.match(key):
                basedir = (os.path.dirname(value)
                           if os.path.isfile(value)
                           else os.path.dirname(filename))
                config_items[key] = os.path.join(basedir, value)

    # type transforms for well-known configuration keys
    _convert = {
        # item name             converter
        # ===================   ==================================
        'enabled'             : gc3libs.utils.string_to_boolean,
        'accounting_delay'    : int,
        'architecture'        : _parse_architecture,
        'max_cores'           : int,
        'max_cores_per_job'   : int,
        'max_memory_per_core' : (lambda val: _legacy_parse_memory(val, 'max_memory_per_core')),
        'max_walltime'        : _legacy_parse_duration,
        'override'            : gc3libs.utils.string_to_boolean,
        'port'                : int,
        'vm_os_overhead'      : _legacy_parse_os_overhead,
        'large_file_threshold': (lambda val: _legacy_parse_memory(val, 'large_file_threshold', MB, 'MB')),
        'large_file_chunk_size':(lambda val: _legacy_parse_memory(val, 'large_file_chunk_size', MB, 'MB')),
        # LSF-specific
        'lsf_continuation_line_prefix_length': int,
    }

    @staticmethod
    def _perform_type_conversions(config_items, converters, filename):
        for key, converter in converters.items():
            if key in config_items:
                try:
                    config_items[key] = converter(config_items[key])
                except Exception as err:
                    raise gc3libs.exceptions.ConfigurationError(
                        "Error parsing configuration item '%s': %s: %s"
                        % (key, err.__class__.__name__, err))

    @property
    def auth_factory(self):
        """
        The instance of `gc3libs.authentication.Auth`:class: used to
        manage auth access for the resources.

        This is a *read-only* attribute, created upon first access
        with the values set in `self.auths` and `self.auto_enabled`.
        """
        if self._auth_factory is None:
            try:
                self._auth_factory = gc3libs.authentication.Auth(
                    self.auths, self.auto_enable_auth)
            except Exception as err:
                gc3libs.log.critical(
                    "Failed initializing Auth module: %s: %s",
                    err.__class__.__name__, str(err))
                raise
        return self._auth_factory

    def make_auth(self, name):
        """
        Return factory for auth credentials configured in section
        ``[auth/name]``.
        """
        # use `lambda` for delayed evaluation
        return (lambda **extra_args: self.auth_factory.get(name, **extra_args))

    _removed_resource_types = (
        gc3libs.defaults.ARC0_LRMS,
        gc3libs.defaults.ARC1_LRMS,
    )

    # map resource type name (e.g., 'sge' or 'openstack') to module
    # name + class/function within that module
    TYPE_CONSTRUCTOR_MAP = {
        gc3libs.defaults.EC2_LRMS: ("gc3libs.backends.ec2",  "EC2Lrms"),
        gc3libs.defaults.LSF_LRMS: ("gc3libs.backends.lsf",  "LsfLrms"),
        gc3libs.defaults.PBS_LRMS: ("gc3libs.backends.pbs",  "PbsLrms"),
        gc3libs.defaults.OPENSTACK_LRMS:
                                   ("gc3libs.backends.openstack", "OpenStackLrms"),
        gc3libs.defaults.SGE_LRMS: ("gc3libs.backends.sge",  "SgeLrms"),
        gc3libs.defaults.SHELLCMD_LRMS:
                                   ("gc3libs.backends.shellcmd", "ShellcmdLrms"),
        gc3libs.defaults.SLURM_LRMS:
                                   ("gc3libs.backends.slurm", "SlurmLrms"),
    }

    def _get_resource_constructor(self, resource_type):
        """
        Return the callable to be used to instanciate resource of the
        given type name.
        """
        if resource_type not in self._resource_constructors_cache:
            if resource_type not in self.TYPE_CONSTRUCTOR_MAP:
                raise gc3libs.exceptions.ConfigurationError(
                    "Unknown resource type '%s'" % resource_type)
            else:
                modname, clsname = self.TYPE_CONSTRUCTOR_MAP[resource_type]
                try:
                    mod = __import__(modname,
                                     globals(), locals(),
                                     [clsname], 0)
                    cls = getattr(mod, clsname)
                except (ImportError, AttributeError) as err:
                    raise gc3libs.exceptions.Error(
                        ("Could not instanciate"
                         " resource type '{type}': {errcls}: {errmsg}"
                         .format(
                             type=resource_type,
                             errcls=err.__class__.__name__,
                             errmsg=err)),
                        do_log=True)
                self._resource_constructors_cache[resource_type] = cls
                gc3libs.log.debug(
                    "Using class %r from module %r"
                    " to instanciate resources of type %s",
                    cls, mod, resource_type)
                return cls
        return self._resource_constructors_cache[resource_type]

    def make_resources(self, ignore_errors=True):
        """
        Make backend objects corresponding to the configured resources.

        Return a dictionary, mapping the resource name (string) into
        the corresponding backend object.

        By default, errors in constructing backends (e.g., due to a
        bad configuration) are silently ignored: the offending
        configuration is just dropped.  This can be changed by setting
        the optional argument `ignore_errors` to `False`: in this
        case, an exception is raised whenever we fail to construct a
        backend.
        """
        resources = {}
        for name, resdict in self.resources.items():
            try:
                backend = self._make_resource(resdict)
                if backend is None:  # resource is disabled
                    continue
                assert name == backend.name
            except Exception as err:
                # Print the backtrace only if loglevel is DEBUG or
                # more.
                exc_info = gc3libs.log.level <= gc3libs.logging.DEBUG
                gc3libs.log.warning(
                    "Failed creating backend for resource '%s' of type '%s':"
                    " %s: %s",
                    resdict.get(
                        'name',
                        '(unknown name)'),
                    resdict.get(
                        'type',
                        '(unknown type)'),
                    err.__class__.__name__,
                    str(err),
                    exc_info=exc_info)
                if ignore_errors:
                    continue
                else:
                    raise
            resources[name] = backend
        return resources

    def _make_resource(self, resdict):
        """
        Return a backend initialized from the key/value pairs in `resdict`.
        """
        # name' should have been defined by the caller, so if it's
        # missing it's an internal coherency error
        assert 'name' in resdict, (
            "Invalid resource definition '%s': missing required key 'name'."
            % resdict)

        # default values
        resdict.setdefault('enabled', True)
        if not resdict['enabled']:
            gc3libs.log.info(
                "Dropping computational resource '%s'"
                " because of 'enabled=False' setting"
                " in configuration file.",
                resdict['name'])
            return None

        # minimal sanity check
        for key in self._resource_required_keys:
            if key not in resdict:
                raise gc3libs.exceptions.ConfigurationError(
                    "Missing required parameter '{key}'"
                    " in definition of resource '{name}'."
                    .format(key=key, name=resdict['name']))
        for key in self._resource_keys_value_required:
            if key in resdict and not resdict[key]:
                raise gc3libs.exceptions.ConfigurationError(
                    "Found empty configuration key `{key}`"
                    " in definition of resource '{name}'."
                    .format(key=key, name=resdict['name']))

        if resdict['type'] in self._removed_resource_types:
            resdict['enabled'] = False
            gc3libs.log.warning(
                "Dropping computational resource '{name}':"
                " resource type '{type}' is no longer supported."
                " Please update your configuration file."
                .format(**resdict))
            return None

        if __debug__:
            gc3libs.log.debug(
                "Creating resource '%s' defined by: %s.",
                resdict['name'], ', '.join([
                    ("%s=%r" % (k, v)) for k, v in sorted(resdict.items())
                ]))

        for auth_param in 'auth', 'vm_auth':
            if auth_param in resdict:
                resdict[auth_param] = self.make_auth(resdict[auth_param])

        try:
            # valid strings can be, e.g., `shellcmd+ssh` or `sge`
            resource_type = resdict['type'].split('+')[0]
            cls = self._get_resource_constructor(resource_type)
            # check that required parameters are given, and try to
            # give a sensible error message if not; if we do not
            # do this, users see a traceback like this::
            #
            #   gc3.gc3libs: ERROR: Could not create resource \
            #       'schroedinger-via-ssh': __init__() takes at least 10 \
            #       non-keyword arguments (9 given). Configuration file \
            #       problem?
            #
            # which gives no clue about what to correct!
            args, varargs, keywords, defaults = getargspec(cls.__init__)
            if defaults is not None:
                # `defaults` is a list of default values for the last N args
                defaulted = dict((argname, value)
                                 for argname, value in zip(reversed(args),
                                                           reversed(defaults)))
            else:
                # no default values at all
                defaulted = {}
            for argname in args[1:]:  # skip `self`
                if argname not in resdict and argname not in defaulted:
                    raise gc3libs.exceptions.ConfigurationError(
                        "Missing required configuration parameter '%s'"
                        " for resource '%s'" % (argname, resdict['name']))

            # finally, try to construct backend class...
            return cls(**dict(resdict))

        except Exception as err:
            gc3libs.log.error(
                "Could not create resource '%s': %s. Configuration file"
                " problem?" % (resdict['name'], str(err)))
            raise


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="config",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
