#! /usr/bin/env python
#
"""
Deal with GC3Pie configuration files.
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


# stdlib imports
import ConfigParser
import os
import re
import sys

# GC3Pie imports
import gc3libs
import gc3libs.authentication
from gc3libs.compat.collections import defaultdict
import gc3libs.utils


## auxiliary methods for `Configuration`
#
# these must be defined before `Configuration` is parsed, because they
# are referenced in the definition of `Configuration` itself
#

# map values for the `architecture=...` configuration item
# into internal constants
_architecture_value_map = {
    # 'x86-32', 'x86 32-bit', '32-bit x86' and variants thereof
    re.compile('x86[ _-]+32([ _-]*bits?)?', re.I): gc3libs.Run.Arch.X86_32,
    re.compile('32[ _-]*bits? +[ix]86', re.I):     gc3libs.Run.Arch.X86_32,
    # accept also values printed by `uname -a` on 32-bit x86 archs
    re.compile('i[3456]86', re.I):                 gc3libs.Run.Arch.X86_32,
    # 'x86_64', 'x86 64-bit', '64-bit x86' and variants thereof
    re.compile('x86[ _-]+64([ _-]*bits?)?', re.I): gc3libs.Run.Arch.X86_64,
    re.compile('64[ _-]*bits? +[ix]86', re.I):     gc3libs.Run.Arch.X86_64,
    # also accept commercial arch names
    re.compile('(amd[ -]*64|x64|emt64|intel[ -]*64)( *bits?)?', re.I): \
                                                   gc3libs.Run.Arch.X86_64,
    # finally, map "32-bit" and "64-bit" to i686 and x86_64
    re.compile('32[ _-]*bits?', re.I):             gc3libs.Run.Arch.X86_32,
    re.compile('64[ _-]*bits?', re.I):             gc3libs.Run.Arch.X86_64,
    }

def _parse_architecture(arch_str):
    def matching_architecture(value):
        for matcher, arch in _architecture_value_map.iteritems():
            if matcher.match(value):
                return arch
        raise ValueError("Unknown architecture '%s'." % value)
    archs = [ matching_architecture(value.strip())
              for value in arch_str.split(',') ]
    if len(archs) == 0:
        raise ValueError("Empty or invalid 'architecture' setting.")
    return set(archs)


## the main class of this module

class Configuration(gc3libs.utils.Struct):
    """
    In-memory representation of the GC3Pie configuration.

    This class provides facilities for:

    * parsing configuration files (methods `load`:meth: and
      `merge_file`:meth:);
    * validating the loaded values;
    * instanciating the internal GC3Pie objects resulting from the
      configuration (methods `make_auths`:meth: and
      `make_resource`:meth:).

    The constructor takes a list of files to load (`locations`) and a
    list of key=value pairs to provide defaults for the configuration.
    Both lists are optional and can be omitted, resulting in a
    configuration containing only GC3Pie default values.

    Example 1: initialization from config file::

      >>> import os
      >>> example_cfgfile = os.path.join(os.path.dirname(__file__), 'etc/gc3pie.conf.example')
      >>> cfg = Configuration(example_cfgfile)
      >>> cfg.debug
      '0'

    Example 2: initialization from key=value list::

      >>> cfg = Configuration(auto_enable_auth=False, foo=1, bar='baz')
      >>> cfg.auto_enable_auth
      False
      >>> cfg.foo
      1
      >>> cfg.bar
      'baz'

    When both a configuration file *and* a key=value list is present,
    values in the configuration files override those in the key=value
    list::

      >>> cfg = Configuration(example_cfgfile, debug=1)
      >>> cfg.debug
      '0'

    Example 3: default initialization::

      >>> cfg = Configuration()
      >>> cfg.auto_enable_auth
      True

    """

    def __init__(self, *locations, **kw):
        # these fields are required
        self.resources = defaultdict(gc3libs.utils.Struct)
        self.auths = defaultdict(gc3libs.utils.Struct)

        # use keyword arguments to set defaults
        self.auto_enable_auth = kw.pop('auto_enable_auth', True)
        self.update(kw)

        # load configuration files if any
        if len(locations) > 0:
            self.load(*locations)


    def load(self, *locations):
        """
        Merge settings from configuration files into this `Configuration` instance.

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

        for filename in locations:
            filename = os.path.expandvars(filename)
            if os.path.exists(filename):
                if not os.access(filename, os.R_OK):
                    gc3libs.log.debug("Configuration.load(): File '%s' cannot be read, ignoring." % filename)
                    continue # with next `filename`
            else:
                gc3libs.log.debug("Configuration.load(): File '%s' does not exist, ignoring." % filename)
                continue # with next `filename`

            try:
                self.merge_file(filename)
                files_successfully_read += 1
            except gc3libs.exceptions.ConfigurationError:
                continue # with next file

        if files_successfully_read == 0:
            raise gc3libs.exceptions.NoConfigurationFile(
                "Could not read any configuration file; tried location '%s'."
                % str.join("', '", locations))


    def merge_file(self, filename):
        """
        Read configuration files and merge the settings into this `Configuration` object.

        Contrary to `load`:meth: (which see), the file name is
        taken literally and an error is raised if it cannot be read
        for whatever reason.

        Any parameter which is set in the configuration files
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
        gc3libs.log.debug("Configuration.load(): Reading file '%s' ..." % filename)
        stream = open(filename, 'r')
        (defaults, resources, auths) = self._parse(stream, filename)
        stream.close()
        for name, values in resources.iteritems():
            self.resources[name].update(values)
        for name, values in auths.iteritems():
            self.auths[name].update(values)
        for name, value in defaults.iteritems():
            if not name.startswith('_'):
                self[name] = value


    def _parse(self, stream, filename=None):
        """
        Read configuration file and return a `(defaults, resources, auths)` triple.

        The members of the result triple are as follows:

        * `defaults`: a dictionary containing keys found in the
          ``[DEFAULTS]`` section of the configuration file (if any);

        * `resources`: a dictionary mapping resource names into a
          dictionary of key/value attributes contained in the
          configuration file under the ``[resource/name]`` heading;

        * `auths`: same for the ``[auth/name]`` sections.

        In addition, key renaming (for compatibility with previous versions) and
        type conversion is performed here, so that the returned dictionaries
        conform to a specified schema:

          ===================  ==============
          Attribute name       Type
          ===================  ==============
          architecture         set of strings
          max_cores_per_job    int
          max_memory_per_core  int
          max_walltime         int
          max_cores            int
          ===================  ==============

        Any attribute not mentioned in the above table will have type
        ``str`` (i.e., it is left unchanged).

        """
        defaults = dict()
        resources = defaultdict(dict)
        auths = defaultdict(dict)

        parser = ConfigParser.SafeConfigParser()
        try:
            parser.readfp(stream, filename)
        except ConfigParser.Error, err:
            if filename is None:
                if hasattr(stream, 'name'):
                    filename = stream.name
                else:
                    filename = repr(stream)
            raise gc3libs.exceptions.ConfigurationError(
                "Configuration file '%s' is unreadable or malformed: %s: %s"
                % (filename, err.__class__.__name__, str(err)))

        # update `defaults` with the contents of the `[DEFAULTS]` section
        defaults.update(parser.defaults())

        for sectname in parser.sections():
            if sectname.startswith('auth/'):
                # handle auth section
                name = sectname.split('/', 1)[1]
                gc3libs.log.debug("Config._parse():"
                                  " Read configuration stanza for auth '%s'." % name)
                config_items = dict(parser.items(sectname))
                auths[name].update(config_items)
                auths[name]['name'] = name
                if __debug__:
                    gc3libs.log.debug(
                        "Config._parse(): Auth '%s' defined by: %s.",
                        name, str.join(', ', [
                            ("%s=%r" % (k,v)) for k,v in auths[name].iteritems()
                            ]))

            elif  sectname.startswith('resource/'):
                # handle resource section
                name = sectname.split('/', 1)[1]
                gc3libs.log.debug("Config._parse():"
                                  " Read configuration stanza for resource '%s'." % name)

                config_items = dict(parser.items(sectname))
                self._perform_key_renames(config_items,      self._renamed_keys,   filename)
                self._perform_renamed_values(config_items,   self._renamed_values, filename)
                try:
                    self._perform_type_conversions(config_items, self._convert,        filename)
                except Exception, err:
                    raise gc3libs.exceptions.ConfigurationError(
                        "Incorrect entry for resource '%s' in configuration file '%s': %s"
                        % (name, filename, str(err)))

                if 'type' in config_items:
                    # validity checks
                    if config_items['type'] not in [
                        gc3libs.Default.ARC0_LRMS,
                        gc3libs.Default.ARC1_LRMS,
                        gc3libs.Default.SGE_LRMS,
                        gc3libs.Default.PBS_LRMS,
                        gc3libs.Default.LSF_LRMS,
                        gc3libs.Default.SHELLCMD_LRMS,
                        ]:
                        raise gc3libs.exceptions.ConfigurationError(
                            "Configuration error in file '%s':"
                            " '%s' is no valid resource type.",
                            filename, config_items['type'])

                resources[name].update(config_items)
                resources[name]['name'] = name
                if __debug__:
                    gc3libs.log.debug(
                        "Config._parse(): Resource '%s' defined by: %s.",
                        name, str.join(', ', [
                            ("%s=%r" % (k,v)) for k,v in resources[name].iteritems()
                            ]))

            else:
                # Unhandled sectname
                gc3libs.log.warning(
                    "Config._parse(): unknown configuration section '%s' -- ignoring!",
                    sectname)

        return (defaults, resources, auths)

    _renamed_keys = {
        # old key name         new key name
        # ===================  ===================
        'ncores':              'max_cores',
        }

    @staticmethod
    def _perform_key_renames(config_items, renames, filename):
        for oldkey, newkey in renames.iteritems():
            if oldkey in config_items:
                gc3libs.log.warning(
                    "Configuration item '%s' was renamed to '%s',"
                    " please change occurrences of '%s' to '%s'"
                    " in configuration file '%s'.",
                    oldkey, newkey, oldkey, newkey, filename)
                if newkey in config_items:
                    # drop
                    gc3libs.log.error(
                        "Both old-style configuration item '%s' and new-style '%s'"
                        " detected in file '%s': ignoring old-style item '%s=%s'.",
                        oldkey, newkey, filename, config_items[oldkey])
                else:
                    config_items[newkey] = config_items[oldkey]
                del config_items[oldkey]

    _renamed_values = {
        # key name             old value            new value
        # ===================  ===================  ==================
        'type': {
            # old value     new value
            # ============  ==================
            'arc':          gc3libs.Default.ARC0_LRMS,
            'ssh':          gc3libs.Default.SGE_LRMS,
            'subprocess':   gc3libs.Default.SHELLCMD_LRMS,
            },
        }

    @staticmethod
    def _perform_renamed_values(config_items, renames, filename):
        for key, changed in renames.iteritems():
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

    # type transforms for well-known configuration keys
    _convert = {
        # item name            converter
        # ===================  ==================================
        'enabled':             gc3libs.utils.string_to_boolean,
        'architecture':        _parse_architecture,
        'max_cores':           int,
        'max_cores_per_job':   int,
        'max_memory_per_core': int,
        'max_walltime':        int,
        }

    @staticmethod
    def _perform_type_conversions(config_items, converters, filename):
        for key, converter in converters.iteritems():
            if key in config_items:
                try:
                    config_items[key] = converter(config_items[key])
                except Exception, err:
                    raise gc3libs.exceptions.ConfigurationError(
                        "Error parsing configuration item '%s': %s: %s"
                        % (key, err.__class__.__name__, str(err)))


    def make_auths(self):
        try:
            return gc3libs.authentication.Auth(self.auths, self.auto_enable_auth)
        except Exception, ex:
            gc3libs.log.critical('Failed initializing Auth module: %s: %s',
                                 ex.__class__.__name__, str(ex))
            raise


    def make_resources(self):
        resources = { }
        for name, resdict in self.resources.iteritems():
            resobj = self._make_resource(resdict)
            assert name == resobj.name
            if not resobj.enabled:
                gc3libs.log.info(
                    "Dropping computational resource '%s'"
                    " because of 'enabled=False' setting"
                    " in configuration file.",
                    name)
                continue
            resources[name] = resobj
        return resources


    @staticmethod
    def _make_resource(resdict):
        """
        Return a `Resource` object initialized from the key/value pairs in `resdict`.
        """
        if __debug__:
            gc3libs.log.debug(
                "Creating resource '%s' defined by: %s.",
                resdict['name'], str.join(', ', [
                    ("%s=%r" % (k,v)) for k,v in resdict.iteritems()
                    ]))

        try:
            resobj = gc3libs.Resource.Resource(**resdict)
        except Exception, err:
            raise gc3libs.exceptions.ConfigurationError(
                "Could not create resource '%s': %s."
                " Please check configuration file(s)."
                % (resdict['name'], str(err)))

        if 'enabled' not in resobj:
            # resources are enabled by default
            resobj.enabled = True

        # XXX: should be done by the backend constructor!?
        if 'architecture' not in resobj:
            raise gc3libs.exceptions.ConfigurationError(
                "No architecture specified for resource '%s'" % resobj.name)

        # XXX: should be done by the backend constructor!?
        if (resobj.type in [gc3libs.Default.ARC0_LRMS, gc3libs.Default.ARC1_LRMS]
            and 'frontend' not in resobj):
            # extract frontend information from arc_ldap entry
            try:
                resource_url = gc3libs.url.Url(resobj.arc_ldap)
                resobj.frontend = resource_url.hostname
            except Exception, err:
                raise gc3libs.exceptions.ConfigurationError(
                    "Configuration error: resource '%s' has no valid 'arc_ldap' setting: %s: %s"
                    % (name, err.__class__.__name__, err.message))

        gc3libs.log.debug(
            "Created resource '%s' of type %s.",
            resobj.name, resobj.type)
        return resobj



## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="config",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
