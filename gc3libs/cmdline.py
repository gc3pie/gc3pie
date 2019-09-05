#! /usr/bin/env python

"""
Base classes for GC3Libs-based scripts.

Classes implemented in this file provide common and recurring
functionality for GC3Libs command-line utilities and scripts.  User
applications should implement their specific behavior by subclassing
and overriding a few customization methods.

The following public classes are exported from this module:

:class:`SessionBasedScript`
  Base class for the ``grosetta``/``ggamess``/``gcodeml`` scripts.
  Implements a long-running script to submit and manage a large number
  of tasks grouped into a "session".

`SessionBasedDaemon`:class:
  Base class for GC3Pie servers. Implements a long-running daemon with
  XML-RPC interface and support for "inboxes" (which can add or remove
  tasks based on external events).

`DaemonClient`:class:
  Command-line client for interacting with instances of a
  `SessionBasedDaemon`:class: via XML-RPC.
"""

#   cmdline.py -- Base classes for GC3Libs-based scripts
#
#   Copyright (C) 2010-2019  University of Zurich. All rights reserved.
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import (absolute_import, division, print_function, unicode_literals)

from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import range
from builtins import object

# stdlib modules
import atexit
import fnmatch
import json
import logging
import logging.handlers
from logging.handlers import SysLogHandler
import math
import multiprocessing.dummy as mp
import os
import os.path
import signal
import stat
import sys
import time
import threading
try:
    from io import StringIO
except ImportError:
    from io import StringIO
from collections import defaultdict
try:
    # Python 2
    from SimpleXMLRPCServer import SimpleXMLRPCServer
    import xmlrpc.client
    def ServerProxy(url):
        # **NOTE:** This has to be the built-in `bytes` type; when
        # using `future`'s `newstr` or `newbytes` objects, the
        # `ServerProxy` becomes unusable, as *every* method call
        # raises an exception `AttributeError: encode method has
        # been disabled in newbytes`
        return xmlrpc.client.ServerProxy(bytes(url))
except ImportError:
    # Python 3
    from xmlrpc.server import SimpleXMLRPCServer
    import xmlrpc.client
    from xmlrpc.client import ServerProxy
from warnings import warn


# 3rd party modules
import cli  # pyCLI
import cli.app
import cli._ext.argparse as argparse
import daemon
import lockfile
from lockfile.pidlockfile import PIDLockFile
from prettytable import PrettyTable
import yaml


# interface to GC3Pie
import gc3libs
import gc3libs.defaults
import gc3libs.config
import gc3libs.core
import gc3libs.exceptions
from gc3libs.exceptions import InvalidUsage
import gc3libs.persistence
from gc3libs.utils import (
    basename_sans,
    deploy_configuration_file,
    prettyprint,
    read_contents,
    remove as rm_f,
    same_docstring_as,
    check_file_access,
    write_contents,
)
import gc3libs.url
from gc3libs.url import Url
from gc3libs.quantity import Memory, GB, Duration, hours
from gc3libs.session import Session, TemporarySession
from gc3libs.poller import make_poller


## file metadata
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


##
# types for command-line parsing; see
# http://docs.python.org/dev/library/argparse.html#type
##

def nonnegative_int(num):
    """
    Raise `ArgumentTypeError` if `num` is a negative integer (<0), and
    return `int(num)` otherwise. `num` can be any object which can be
    converted to an int.

    >>> nonnegative_int('1')
    1
    >>> nonnegative_int(1)
    1
    >>> try:
    ...   nonnegative_int('-1')
    ... except argparse.ArgumentTypeError as err:
    ...   print(err)
    '-1' is not a non-negative integer number.
    >>> try:
    ...   nonnegative_int(-1)
    ... except argparse.ArgumentTypeError as err:
    ...   print(err)
    '-1' is not a non-negative integer number.

    Please note that `0` and `'-0'` are ok:

    >>> nonnegative_int(0)
    0
    >>> nonnegative_int(-0)
    0
    >>> nonnegative_int('0')
    0
    >>> nonnegative_int('-0')
    0

    Floats are ok too:

    >>> nonnegative_int(3.14)
    3
    >>> nonnegative_int(0.1)
    0

    >>> try:
    ...   nonnegative_int('ThisWillRaiseAnException')
    ... except argparse.ArgumentTypeError as err:
    ...   print(err) # doctest:+ELLIPSIS
    'ThisWillRaiseAnException' is not a non-negative ...
    """
    try:
        value = int(num)
        if value < 0:
            raise argparse.ArgumentTypeError(
                "'%s' is not a non-negative integer number." % (num,))
        return value
    except ValueError:
        raise argparse.ArgumentTypeError(
            "'%s' is not a non-negative integer number." % (num,))


def positive_int(num):
    """
    Raise `ArgumentTypeError` if `num` is not a *strictly* positive
    integer (>0) and return `int(num)` otherwise. `num` can be any
    object which can be converted to an int.

    >>> positive_int('1')
    1
    >>> positive_int(1)
    1
    >>> try:
    ...   positive_int('-1')
    ... except argparse.ArgumentTypeError as err:
    ...   print(err) # doctest:+ELLIPSIS
    '-1' is not a positive integer number.
    >>> try:
    ...   positive_int(-1)
    ... except argparse.ArgumentTypeError as err:
    ...   print(err) # doctest:+ELLIPSIS
    '-1' is not a positive integer number.
    >>> try:
    ...   positive_int(0)
    ... except argparse.ArgumentTypeError as err:
    ...   print(err) # doctest:+ELLIPSIS
    '0' is not a positive integer number.

    Floats are ok too:

    >>> positive_int(3.14)
    3

    but please take care that float *greater* than 0 but still less
    than 1 will fail:

    >>> try:
    ...    positive_int(0.1)
    ... except argparse.ArgumentTypeError as err:
    ...   print(err) # doctest:+ELLIPSIS
    '0.1' is not a positive integer number.

    Also note that `0` is *not* OK:

    >>> try:
    ...   positive_int(-0)
    ... except argparse.ArgumentTypeError as err:
    ...   print(err) # doctest:+ELLIPSIS
    '0' is not a positive integer number.
    >>> try:
    ...   positive_int('0')
    ... except argparse.ArgumentTypeError as err:
    ...   print(err) # doctest:+ELLIPSIS
    '0' is not a positive integer number.
    >>> try:
    ...   positive_int('-0')
    ... except argparse.ArgumentTypeError as err:
    ...   print(err) # doctest:+ELLIPSIS
    '-0' is not a positive integer number.

    Any string which does cannot be converted to an integer will fail:

    >>> try:
    ...   positive_int('ThisWillRaiseAnException')
    ... except argparse.ArgumentTypeError as err:
    ...   print(err) # doctest:+ELLIPSIS
    'ThisWillRaiseAnException' is not a positive integer ...
    """
    try:
        value = int(num)
        if value <= 0:
            raise argparse.ArgumentTypeError(
                "'%s' is not a positive integer number." % (num,))
        return value
    except ValueError:
        raise argparse.ArgumentTypeError(
            "'%s' is not a positive integer number." % (num,))


def existing_file(path):
    check_file_access(path, os.F_OK | os.R_OK,
              argparse.ArgumentTypeError)
    return path


def executable_file(path):
    check_file_access(path, os.F_OK | os.R_OK | os.X_OK,
              argparse.ArgumentTypeError)
    return path


def existing_directory(path):
    check_file_access(path, os.F_OK | os.R_OK | os.X_OK,
              argparse.ArgumentTypeError, isdir=True)
    return path


def valid_directory(path):
    if os.path.exists(path) and not os.path.isdir(path):
        raise argparse.ArgumentTypeError(
            "path '%s' already exists but is not a directory."
            % (path,))
    return path


##
# script classes
##

def make_logger(verbosity, name=None, threshold=0, progname=None):
    loglevel = max(1, logging.WARNING - 10*max(0, verbosity - threshold))
    return gc3libs.configure_logger(loglevel, progname or name or "gc3pie")


class _Script(cli.app.CommandLineApp):

    """
    Base class for GC3Libs scripts.

    By default, only the standard options ``-h``/``--help`` and
    ``-V``/``--version`` are considered; to add more, override
    `setup_options`:meth:

    There is no defaults for positional arguments, you *must* override
    `setup_args`:meth: in derived classes.
    """

    ##
    # CUSTOMIZATION METHODS
    ##
    # The following are meant to be freely customized in derived scripts.
    ##

    def setup_args(self):
        """
        Set up command-line argument parsing.

        The default command line parsing considers every argument as
        an (input) path name; processing of the given path names is
        done in `parse_args`:meth:
        """
        raise NotImplementedError(
            "Abstract method `_Script.setup_args()` called,"
            " which should have been implemented in a derived class")

    def setup_options(self):
        """
        Override this method to add command-line options.
        """
        pass

    def parse_args(self):
        """
        Do any parsing of the command-line arguments before the main
        loop starts.  This is the place to check validity of the
        parameters passed as command-line arguments, and to perform
        setup of shared data structures and default values.

        The default implementation does nothing; you are free to
        override this method in derived classes.
        """
        pass

    def terminate(self):
        """
        Called to stop the script from running.

        By default this does nothing; override in derived classes.
        """
        pass

    ##
    # pyCLI INTERFACE METHODS
    ##
    # The following methods adapt the behavior of the
    # `SessionBasedScript` class to the interface expected by pyCLI
    # applications.  Think twice before overriding them, and read
    # the pyCLI docs before :-)
    ##
    def __init__(self, **extra_args):
        """
        Perform initialization and set the version, help and usage
        strings.

        The help text to be printed when the script is invoked with the
        ``-h``/``--help`` option will be taken from (in order of preference):
          * the keyword argument `description`;
          * the attribute `self.description`.
        If neither is provided, an `AssertionError` is raised.

        The text to output when the the script is invoked with the
        ``-V``/``--version`` options is taken from (in order of
        preference):
          * the keyword argument `version`;
          * the attribute `self.version`.
        If none of these is provided, an `AssertionError` is raised.

        The `usage` keyword argument (if provided) will be used to
        provide the program help text; if not provided, one will be
        generated based on the options and positional arguments
        defined in the code.

        Any additional keyword argument will be used to set a
        corresponding instance attribute on this Python object.

        """
        # use keyword arguments to set additional instance attrs
        for k, v in list(extra_args.items()):
            if k not in ['name', 'description']:
                setattr(self, k, v)
        # init and setup pyCLI classes
        if 'version' not in extra_args:
            try:
                extra_args['version'] = self.version
            except AttributeError:
                raise AssertionError("Missing required parameter 'version'.")
        if 'description' not in extra_args:
            if self.__doc__ is not None:
                extra_args['description'] = self.__doc__
            else:
                raise AssertionError(
                    "Missing required parameter 'description'.")

        # init superclass
        extra_args.setdefault(
            'name',
            os.path.splitext(
                os.path.basename(
                    sys.argv[0]))[0])
        extra_args.setdefault('reraise', Exception)
        super(_Script, self).__init__(**extra_args)

        # provide some defaults
        self.verbose_logging_threshold = 0

    @staticmethod
    def argparser_factory(*args, **kwargs):
        """
        Allow orverriding command-line options in subclasses.
        """
        kwargs.setdefault('conflict_handler', 'resolve')
        kwargs.setdefault('formatter_class',
                          cli._ext.argparse.RawDescriptionHelpFormatter)
        return cli.app.CommandLineApp.argparser_factory(*args, **kwargs)

    @property
    def description(self):
        """A string describing the application.

        Unless specified when the :class:`Application` was created, this
        property will examine the :attr:`main` callable and use its
        docstring (:attr:`__doc__` attribute).
        """
        if self._description is not None:
            return self._description
        else:
            return getattr(self.main, "__doc__", '')

    def setup(self):
        """
        Setup standard command-line parsing.

        GC3Utils scripts should probably override `setup_args`:meth:
        and `setup_options`:meth: to modify command-line parsing.
        """
        # setup of base classes
        super(_Script, self).setup()

        self.add_param(
            "-v",
            "--verbose",
            action="count",
            dest="verbose",
            default=0,
            help="Print more detailed information about the program's"
            " activity. Increase verbosity each time this option is"
            " encountered on the command line.")

        self.add_param("--config-files",
                       action="store",
                       default=','.join(gc3libs.defaults.CONFIG_FILE_LOCATIONS),
                       help="Comma separated list of configuration files",
                       )

        # finish setup
        self.setup_options()
        self.setup_args()


    def pre_run(self):
        """
        Perform parsing of standard command-line options and call into
        `parse_args()` to do non-optional argument processing.

        Also sets up the ``gc3.gc3utils`` logger; it is controlled by
        the ``-v``/``--verbose`` command-line option.  Up to
        `self.verbose_logging_threshold` occurrences of ``-v`` are
        ignored, after which they start to lower the level of messages
        sent to standard error output.
        """
        # parse command-line
        super(_Script, self).pre_run()

        # setup GC3Libs logging
        self.log = make_logger(self.params.verbose,
                               name=self.name,
                               threshold=self.verbose_logging_threshold)
        self.log.info("Starting %s at %s; invoked as '%s'",
                      self.name, time.asctime(), ' '.join(sys.argv))

        # Read config file(s) from command line
        self.params.config_files = self.params.config_files.split(',')
        # interface to the GC3Libs main functionality
        self.config = self._make_config(self.params.config_files)
        try:
            self._core = gc3libs.core.Core(self.config)
        except gc3libs.exceptions.NoResources:
            # translate internal error `NoResources` to a
            # user-readable message.
            raise gc3libs.exceptions.FatalError(
                "No computational resources defined."
                " Please edit the configuration file(s): '%s'."
                % ("', '".join(self.params.config_files)))

        # call hook methods from derived classes
        self.parse_args()

    def run(self):
        """
        Execute `cli.app.Application.run`:meth: if any exception is
        raised, catch it, output an error message and then exit with
        an appropriate error code.
        """
        try:
            return super(_Script, self).run()
        except gc3libs.exceptions.InvalidUsage:
            # Fatal errors do their own printing,
            # we only add a short usage message
            sys.stderr.write(
                "Type '%s --help' to get usage help.\n" % self.name)
            return os.EX_USAGE  # see: /usr/include/sysexits.h
        except KeyboardInterrupt:
            sys.stderr.write(
                "%s: Exiting upon user request (Ctrl+C)\n" % self.name)
            self.terminate()
            return 13
        except SystemExit:
            #  sys.exit() has been called in `post_run()`.
            raise
        # the following exception handlers put their error message
        # into `msg` and the exit code into `rc`; the closing stanza
        # tries to log the message and only outputs it to stderr if
        # this fails
        except lockfile.Error as ex:
            msg = ("Error manipulating the lock file (%s: %s)."
                   " This likely points to a filesystem error"
                   " or a stale process holding the lock."
                   " If you cannot get this command to run after"
                   " a system reboot, please write to gc3pie@googlegroups.com"
                   " including any output you got by running '%s -vvvv %s'."
                   " (You need to be subscribed to post to the mailing list)")
            if len(sys.argv) > 0:
                msg %= (ex.__class__.__name__, ex,
                        self.name, ' '.join(sys.argv[1:]))
            else:
                msg %= (ex.__class__.__name__, ex, self.name, '')
            rc = os.EX_UNAVAILABLE  # see: /usr/include/sysexits.h
        except AssertionError as ex:
            msg = ("BUG: %s\n"
                   "Please send an email to gc3pie@googlegroups.com"
                   " including any output you got by running '%s -vvvv %s'."
                   " (You need to be subscribed to post to the mailing list)"
                   " Thanks for your cooperation!")
            if len(sys.argv) > 0:
                msg %= (ex, self.name, ' '.join(sys.argv[1:]))
            else:
                msg %= (ex, self.name, '')
            rc = os.EX_SOFTWARE  # see: /usr/include/sysexits.h
        except cli.app.Abort as ex:
            msg = "%s: %s" % (ex.__class__.__name__, ex)
            rc = ex.status
        except EnvironmentError as ex:
            msg = "%s: %s" % (ex.__class__.__name__, ex)
            rc = os.EX_IOERR  # see: /usr/include/sysexits.h
        except Exception as ex:
            if 'GC3PIE_NO_CATCH_ERRORS' in os.environ:
                # propagate generic exceptions for debugging purposes
                raise
            else:
                # generic error exit
                msg = "%s: %s" % (ex.__class__.__name__, ex)
                rc = 1
        # output error message and -maybe- backtrace...
        try:
            self.log.critical(
                msg,
                exc_info=(self.params.verbose > self.verbose_logging_threshold + 2))
        except:
            # no logging setup, output to stderr
            sys.stderr.write("%s: FATAL ERROR: %s\n" % (self.name, msg))
            # be careful here as `self.params` might not have been properly
            # constructed yet
            if ('verbose' in self.params
                and self.params.verbose > self.verbose_logging_threshold + 2):
                sys.excepthook(* sys.exc_info())
        # ...and exit
        return rc

    ##
    # INTERNAL METHODS
    ##
    # The following methods are for internal use; they can be
    # overridden and customized in derived classes, although there
    # should be no need to do so.
    ##

    def _make_config(
            self,
            config_file_locations=gc3libs.defaults.CONFIG_FILE_LOCATIONS,
            **extra_args):
        """
        Return a `gc3libs.config.Configuration`:class: instance configured
        by parsing the configuration file(s) located at
        `config_file_locations`.  Order of configuration files
        matters: files read last overwrite settings from
        previously-read ones; list the most specific configuration
        files last.

        Any additional keyword arguments are passed unchanged to the
        `gc3libs.config.Configuration`:class: constructor.  In
        particular, the `auto_enable_auth` parameter for the
        `Configuration` constructor is `True` if not set differently
        here as a keyword argument.

        """
        # ensure a configuration file exists in the most specific location
        for location in reversed(config_file_locations):
            if (os.access(os.path.dirname(location),
                          os.W_OK | os.X_OK) and not
                    deploy_configuration_file(
                        location, "gc3pie.conf.example")):
                # warn user
                self.log.warning(
                    "No configuration file '%s' was found;"
                    " a sample one has been copied in that location;"
                    " please edit it and define resources." % location)
        # set defaults
        extra_args.setdefault('auto_enable_auth', True)
        try:
            return gc3libs.config.Configuration(
                *config_file_locations, **extra_args)
        except:
            self.log.error("Failed loading config file(s) from '%s'",
                           "', '".join(config_file_locations))
            raise

    def _select_resources(self, *resource_names):
        """
        Restrict resources to those listed in `resource_names`.
        Argument `resource_names` is a string listing all names of
        allowed resources (comma-separated), or a list of names of the
        resources to keep active.
        """
        patterns = []
        for item in resource_names:
            patterns.extend(name for name in item.split(','))

        def keep_resource_if_matches(resource):
            """
            Return `True` iff `resource`'s `name` attribute matches
            one of the glob patterns in `patterns`.
            """
            for pattern in patterns:
                if fnmatch.fnmatch(resource.name, pattern):
                    return True
            return False
        kept = self._core.select_resource(keep_resource_if_matches)
        if kept == 0:
            raise gc3libs.exceptions.NoResources(
                "No resources match the names '%s'"
                % ','.join(resource_names))


class _SessionBasedCommand(_Script):
    """
    Base class for Session Based scripts (interactive or daemons)
    """
    ##
    # CUSTOMIZATION METHODS
    ##
    # The following are meant to be freely customized in derived scripts.
    ##

    def make_directory_path(self, pathspec, jobname):
        """
        Return a path to a directory, suitable for storing the output
        of a job (named after `jobname`).  It is not required that the
        returned path points to an existing directory.

        This is called by the default `process_args`:meth: using
        `self.params.output` (i.e., the argument to the
        ``-o``/``--output`` option) as `pathspec`, and `jobname` and
        `args` exactly as returned by `new_tasks`:meth:

        The default implementation substitutes the following strings
        within `pathspec`:

          * ``SESSION`` is replaced with the name of the current session
            (as specified by the ``-s``/``--session`` command-line option)
            with a suffix ``.out`` appended;
          * ``NAME`` is replaced with `jobname`;
          * ``DATE`` is replaced with the current date, in *YYYY-MM-DD* format;
          * ``TIME`` is replaced with the current time, in *HH:MM* format.

        """
        return (pathspec
                .replace('SESSION', self.params.session + '.out')
                .replace('NAME', jobname)
                .replace('DATE', time.strftime('%Y-%m-%d'))
                .replace('TIME', time.strftime('%H:%M')))

    def make_task_controller(self):
        """
        Return a 'Controller' object to be used for progressing tasks
        and getting statistics.  In detail, a good 'Controller' object
        has to implement `progress` and `stats` methods with the same
        interface as `gc3libs.core.Engine`.

        By the time this method is called (from `_main`:meth:), the
        following instance attributes are already defined:

        * `self._core`: a `gc3libs.core.Core` instance;
        * `self.session`: the `gc3libs.session.Session` instance
          that should be used to save/load jobs

        In addition, any other attribute created during initialization
        and command-line parsing is of course available.
        """
        return gc3libs.core.Engine(
            self._core,
            self.session,
            self.session.store,
            max_submitted=self.params.max_running,
            max_in_flight=self.params.max_running)

    def add(self, task):
        """
        Add a task to the session (and the controller)
        """
        self._controller.add(task)
        self.session.add(task, flush=False)

    def before_main_loop(self):
        """
        Hook executed before entering the scripts' main loop.

        This is the last chance to alter the script state as it will
        be seen by the main loop.

        Override in subclasses to plug any behavior here; the default
        implementation does nothing.
        """
        pass

    def every_main_loop(self):
        """
        Hook executed during each round of the main loop.

        This is called from within the main loop.

        Override in subclasses to plug any behavior here; the default
        implementation does nothing.
        """
        pass

    def after_main_loop(self):
        """
        Hook executed after exit from the main loop.

        This is called after the main loop has exited (for whatever
        reason), but *before* the session is finally saved and other
        connections are finalized.

        Override in subclasses to plug any behavior here; the default
        implementation does nothing.
        """
        pass

    def new_tasks(self, extra):
        """
        Iterate over :py:class:`Task` instances to be added to the session.

        Called before starting the main loop but only when creating a
        new session -- if the session exists already, then the call to
        `new_tasks` is skipped.

        :param extra: by default: `self.extra`
        :return: List (any iterable will do) of `Task` instances

        .. note::

          This method *needs* to be overridden in derived classes; the
          default implementation returns an empty task list, which
          will keep sessions empty and thus make every session-based
          command exit with "nothing to do".
        """
        return []


    ##
    # pyCLI INTERFACE METHODS
    ##
    # The following methods adapt the behavior of the
    # `SessionBasedScript` class to the interface expected by pyCLI
    # applications.  Think twice before overriding them, and read
    # the pyCLI docs before :-)
    ##

    def __init__(self, **extra_args):
        """
        Perform initialization and set the version, help and usage
        strings.

        The help text to be printed when the script is invoked with the
        ``-h``/``--help`` option will be taken from (in order of preference):
          * the keyword argument `description`
          * the attribute `self.description`
        If neither is provided, an `AssertionError` is raised.

        The text to output when the the script is invoked with the
        ``-V``/``--version`` options is taken from (in order of
        preference):
          * the keyword argument `version`
          * the attribute `self.version`
        If none of these is provided, an `AssertionError` is raised.

        The `usage` keyword argument (if provided) will be used to
        provide the program help text; if not provided, one will be
        generated based on the options and positional arguments
        defined in the code.

        Any additional keyword argument will be used to set a
        corresponding instance attribute on this Python object.
        """
        self.session = None
        # by default, print stats of all kind of jobs
        self.stats_only_for = None
        self.extra = {}  # extra arguments passed to `parse_args`
        # init base classes
        _Script.__init__(
            self,
            main=self._main,
            **extra_args
        )

    def process_args(self):
        """
        Process command-line positional arguments and set up the
        session accordingly.  In particular, new jobs should be added
        to the session during the execution of this method: additions
        are not contemplated elsewhere.

        This method is called by the standard `_main`:meth: after
        loading or creating a session into `self.session`.  New jobs
        should be appended to `self.session` and it is also permitted to
        remove existing ones.

        The default implementation calls `new_tasks`:meth: and adds to
        the session all jobs whose name does not clash with the
        jobname of an already existing task.

        See also: `new_tasks`:meth:
        """
        # default creation arguments
        self.extra.setdefault('requested_cores', self.params.ncores)
        self.extra.setdefault('requested_memory',
                              self.params.ncores * self.params.memory_per_core)
        self.extra.setdefault('requested_walltime', self.params.walltime)
        # XXX: assumes `make_directory_path` substitutes ``NAME`` with
        # `jobname`; keep in sync!
        self.extra.setdefault(
            'output_dir',
            self.make_directory_path(self.params.output, 'NAME'))
        # build job list
        new_jobs = list(self.new_tasks(self.extra.copy()))
        if new_jobs:
            self._add_new_tasks(new_jobs)

    def _add_new_tasks(self, new_jobs):
        # pre-allocate Job IDs
        if len(new_jobs) > 0:
            # XXX: can't we just make `reserve` part of the `IdFactory`
            # contract?
            try:
                self.session.store.idfactory.reserve(len(new_jobs))
            except AttributeError:
                # no `idfactory`, ignore
                pass

        # add new jobs to the session
        existing_job_names = self.session.list_names()
        for n, item in enumerate(new_jobs):
            if isinstance(item, tuple):
                # create a new `Task` object
                try:
                    warn(
                        "Using old-style tasks initializer;"
                        " please update the code in function `new_tasks`!",
                        DeprecationWarning)
                    task = self.__make_task_from_old_style_args(item)
                except Exception as err:
                    self.log.error("Could not create task '%s': %s.",
                                   jobname, err, exc_info=__debug__)
                    continue
                    # XXX: should we raise an exception here?
                    # raise AssertionError(
                    #        "Could not create job '%s': %s: %s"
                    #        % (jobname, ex.__class__.__name__, str(ex)))

            elif isinstance(item, gc3libs.Task):
                task = item
                if 'jobname' not in task:
                    task.jobname = (
                        "%s-N%d" % (task.__class__.__name__, n + 1))

            else:
                raise gc3libs.exceptions.InternalError(
                    "SessionBasedScript.process_args got %r %s,"
                    " but was expecting a gc3libs.Task instance"
                    % (item, type(item)))

            # silently ignore duplicates
            if task.jobname in existing_job_names:
                continue

            # patch output_dir if it's not changed from the default,
            # or if it's not defined (e.g., TaskCollection)
            if ('output_dir' not in task
                or task.output_dir == self.extra['output_dir']):
                # user did not change the `output_dir` default, expand it now
                self.__fix_output_dir(task, task.jobname)

            # all done, append to session
            self.session.add(task, flush=False)
            self.log.debug("Added task '%s' to session.", task.jobname)

    def __make_task_from_old_style_args(self, item):
        """
        Build Task from a ``(jobname, classname, args, kwargs)`` tuple.

        .. note::

          This function is provided for compatibility with very old
          script only.  You should return a `Task`:class: instance
          from the `new_tasks`:meth: instead.
        """
        jobname, cls, args, kwargs = item
        kwargs.setdefault('jobname', jobname)
        kwargs.setdefault(
            'output_dir',
            self.make_directory_path(self.params.output, jobname))
        kwargs.setdefault(
            'requested_cores', self.extra['requested_cores'])
        kwargs.setdefault(
            'requested_memory', self.extra['requested_memory'])
        kwargs.setdefault(
            'requested_walltime', self.extra['requested_walltime'])
        return cls(*args, **kwargs)

    def __fix_output_dir(self, task, name):
        """Substitute the NAME string in output paths."""
        if task.would_output:
            task.output_dir = task.output_dir.replace('NAME', name)
        try:
            for subtask in task.tasks:
                self._fix_output_dir(subtask, name)
        except AttributeError:
            # no subtasks
            pass
        try:
            # RetryableTask
            self._fix_output_dir(task.task, name)
        except AttributeError:
            # not a RetryableTask
            pass


    def setup(self):
        """
        Setup standard command-line parsing.

        GC3Libs scripts should probably override `setup_args`:meth:
        to modify command-line parsing.
        """
        # setup of base classes
        _Script.setup(self)

        #
        # add own "standard options"
        #
        # 1. job requirements
        self.add_param(
            "-c", "--cpu-cores", dest="ncores",
            type=positive_int, default=1,  # 1 core
            metavar="NUM",
            help="Set the number of CPU cores required for each job"
            " (default: %(default)s). NUM must be a whole number."
        )
        self.add_param(
            "-m", "--memory-per-core", dest="memory_per_core",
            type=Memory, default=2 * GB,  # 2 GB
            metavar="GIGABYTES",
            help="Set the amount of memory required per execution core;"
            " default: %(default)s. Specify this as an integral number"
            " followed by a unit, e.g., '512MB' or '4GB'.")
        self.add_param(
            "-r",
            "--resource",
            action="store",
            dest="resource_name",
            metavar="NAME",
            default=None,
            help="Submit jobs to a specific computational resources."
            " NAME is a resource name or comma-separated list of such names."
            " Use the command `gservers` to list available resources.")
        self.add_param(
            "-w",
            "--wall-clock-time",
            dest="wctime",
            default='8 hours',
            metavar="DURATION",
            help="Set the time limit for each job; default is %(default)s."
            " Jobs exceeding this limit will be stopped and considered as"
            " 'failed'. The duration can be expressed as a whole number"
            " followed by a time unit, e.g., '3600 s', '60 minutes',"
            " '8 hours', or a combination thereof, e.g., '2hours 30minutes'.")

        # 2. session control
        self.add_param(
            "-s",
            "--session",
            dest="session",
            default=os.path.join(
                os.getcwd(),
                self.name),
            metavar="PATH",
            help="Store the session information in the directory at PATH."
            " (Default: '%(default)s'). ")
        self.add_param("-u", "--store-url", metavar="URL",
                       action="store", default=None,
                       help="URL of the persistent store to use.")
        self.add_param(
            "-N",
            "--new-session",
            dest="new_session",
            action="store_true",
            default=False,
            help="Discard any information saved in the session directory"
            " (see '--session' option) and start a new session afresh."
            " Any information about previous tasks is lost.")

        # 3. script execution control
        self.add_param(
            "-C",
            "--continuous",
            "--watch",
            type=positive_int,
            dest="wait",
            default=0,
            metavar="NUM",
            help="Keep running, monitoring jobs and possibly submitting"
            " new ones or fetching results every NUM seconds. Exit when"
            " all tasks are finished.")
        self.add_param("-J", "--max-running",
                       type=positive_int, dest="max_running", default=50,
                       metavar="NUM",
                       help="Set the maximum NUMber of jobs"
                         " in SUBMITTED or RUNNING state."
                         " (Default: %(default)s)"
                       )
        self.add_param(
            "-o",
            "--output",
            dest="output",
            type=valid_directory,
            default=os.path.join(os.getcwd(), 'NAME'),
            metavar='DIRECTORY',
            help="Output files from all tasks will be collected in the"
            " specified DIRECTORY path; by default, output files are placed"
            " in the same directory where the corresponding input file"
            " resides.  If the destination directory does not exist, it is"
            " created.  The following strings will be substituted into"
            " DIRECTORY, to specify an output location that varies with each"
            " submitted job: the string 'NAME' is replaced by the job name;"
            " 'DATE' is replaced by the submission date in ISO format"
            " (YYYY-MM-DD); 'TIME' is replaced by the submission time"
            " formatted as HH:MM.  'SESSION' is replaced by the path to the"
            " session directory, with a '.out' suffix appended.")


    def pre_run(self):
        """
        Perform parsing of standard command-line options and call into
        `parse_args()` to do non-optional argument processing.
        """
        # call base classes first (note: calls `parse_args()`)
        _Script.pre_run(self)

        # consistency checks
        self.params.walltime = Duration(self.params.wctime)

        # determine the session file name (and possibly create an empty index)
        try:
            self.session_uri = gc3libs.url.Url(self.params.session)
        except Exception as err:
            raise gc3libs.exceptions.InvalidArgument(
                "Cannot parse session URL `{0}`: {1}"
                .format(self.params.session, err))
        if self.params.store_url == 'sqlite':
            self.params.store_url = (
                "sqlite:///%s/jobs.db" % self.session_uri.path)
        elif self.params.store_url == 'file':
            self.params.store_url = ("file:///%s/jobs" % self.session_uri.path)
        try:
            self.session = self._make_session(
                self.session_uri, self.params.store_url)
        except gc3libs.exceptions.InvalidArgument as err:
            raise RuntimeError(
                "Cannot load session `{0}`: {1}"
                .format(self.session_uri, err))

        # keep a copy of the credentials in the session dir, if needed
        self.config.auth_factory.add_params(
            private_copy_directory=self.session.path)

        # we need to make sure that each job downloads results in a new one.
        # The easiest way to do so is to append 'NAME' to the `output_dir`
        # (if it's not already there).
        if (self.params.output and 'NAME' not in self.params.output
                and 'ITER' not in self.params.output):
            self.params.output = os.path.join(self.params.output, 'NAME')


    ##
    # INTERNAL METHODS
    ##
    # The following methods are for internal use; they can be
    # overridden and customized in derived classes.
    ##

    def _make_session(self, session_uri, store_url):
        """
        Return a `gc3libs.session.Session` instance for use in this script.

        Override in subclasses to provide a specialized session.  For
        instance, if you need to add extra fields to a SQL/DB store,
        this is the place to do it.

        The arguments are exactly as in the `gc3libs.session.Session`
        constructor (which see), but this method is free to modify the
        passed parameters or add new ones, as long as the returned
        object implements the `Session` interface.
        """
        if session_uri.scheme == 'file':
            return Session(session_uri.path, create=True, store_or_url=store_url)
        else:
            if store_url is not None:
                raise gc3libs.exceptions.InvalidValue(
                    "When the session is not stored on a filesystem,"
                    " using a separate task storage is not possible."
                )
            return TemporarySession(session_uri)

    def _main(self, *args):
        """
        Implementation of the main logic in the `SessionBasedScript`.

        This is a template method, that you should not override in derived
        classes: rather use the provided customization hooks:
        :meth:`process_args`, :meth:`parse_args`, :meth:`setup_args`.
        """

        # zero out the session index if `-N` was given
        if self.params.new_session:
            self.__abort_old_session()

        # update session based on command-line args
        if len(self.session) == 0:
            self.process_args()
        else:
            self.log.warning(
                "Session already exists,"
                " some command-line arguments might be ignored.")

        # save the session list immediately, so newly added jobs will
        # be in it if the script is stopped here
        self.session.save_all()

        # obey the ``-r`` command-line option
        if self.params.resource_name:
            self._select_resources(self.params.resource_name)
            self.log.info(
                "Retained only resources: %s"
                " (restricted by command-line option '-r %s')",
                ','.join([
                    r['name'] for r in self._core.get_resources()
                    if r.enabled
                ]),
                self.params.resource_name)

        # create an `Engine` instance to manage the job list
        self._controller = self.make_task_controller()

        if self.stats_only_for is not None:
            self._controller.init_counts_for(self.stats_only_for)

        # the main loop, at long last!
        self.before_main_loop()
        rc = 13  # Keep in sync with `_Script.run()` method
        try:
            # do a first round of submit/update/retrieve...
            rc = self._main_loop()
            if self.params.wait > 0:
                self.log.info("sleeping for %d seconds..." % self.params.wait)
                while not self._main_loop_done(rc):
                    self._sleep(self.params.wait)
                    # ...and now repeat the submit/update/retrieve
                    rc = self._main_loop()
        except KeyboardInterrupt:  # gracefully intercept Ctrl+C
            sys.stderr.write(
                "%s: Exiting upon user request (Ctrl+C)\n" % self.name)
            self.terminate()
            pass
        self.after_main_loop()

        if rc in [0, 2]:
            # Set the end timestamp in the session directory
            self.session.set_end_timestamp()

        # save the session again before exiting, so the file reflects
        # jobs' statuses
        self.session.save_all()

        # XXX: shall we call the termination on the controller here ?
        # or rather as a post_run method in the SessionBasedScript ?
        self._controller.close()

        return rc


    def _main_loop(self):
        """
        The main loop of the application.  It is in a separate
        function so that we can call it just once or properly loop
        around it, as directed by the `self.params.wait` option.

        .. warning::

          Overriding this method can disrupt the whole functionality of
          the script, so be careful.

        Invocation of this method should return a numeric exitcode,
        that will be used as the scripts' exitcode.  See
        `_main_loop_exitcode` for an explanation.
        """
        # hook methods
        self._main_loop_before_tasks_progress()
        self.every_main_loop()
        # advance all jobs
        self._controller.progress()
        # compute exitcode based on the running status of jobs
        stats = self._main_loop_after_tasks_progress()
        if stats is None:
            stats = self._controller.counts()
        return self._main_loop_exitcode(stats)


    def _main_loop_exitcode(self, stats):
        """
        Compute the exit code for the `_main` function.
        (And, hence, for the whole session-based command.)

        The exitcode is a bitfield; only the 4 least-significant bits
        are used, with the following meaning:

           ===  ============================================================
           Bit  Meaning
           ===  ============================================================
             0  Set if a fatal error occurred: the script could not complete
             1  Set if there are jobs in `FAILED` state
             2  Set if there are jobs in `RUNNING` or `SUBMITTED` state
             3  Set if there are jobs in `NEW` state
           ===  ============================================================

        Override this method if you need to alter the termination
        condition for a `SessionBasedScript`.
        """
        rc = 0
        if stats['failed'] > 0:
            rc |= 2
        if stats[gc3libs.Run.State.RUNNING] > 0 \
                or stats[gc3libs.Run.State.SUBMITTED] > 0 \
                or stats[gc3libs.Run.State.UNKNOWN]:
            rc |= 4
        if stats[gc3libs.Run.State.NEW] > 0:
            rc |= 8
        return rc


    def _main_loop_before_tasks_progress(self):
        """
        Code that runs in the main loop before `.progress()` is invoked.

        Override in subclasses to plug any behavior here; the default
        implementation does nothing.
       """
        pass


    def _main_loop_after_tasks_progress(self):
        """
        Code that runs in the main loop after `.progress()` is invoked.

        Return either ``None`` or a dictionary of the same form that
        `Engine.counts()`:meth: would return.  In the latter case, the
        return value of this method is used *in stead* of the task
        statistics returned by ``self._controller.counts()``.

        Override in subclasses to plug any behavior here; the default
        implementation does nothing.
        """
        pass


    def __abort_old_session(self):
        old_task_ids = self.session.list_ids()
        if old_task_ids:
            self.log.warning(
                "Abort of existing session requested:"
                " will attempt to kill existing tasks."
                " This may generate a few spurious error messages"
                " if the tasks are too old and have already been"
                " cleaned up by the system.")
            for task_id in old_task_ids:
                # `id` is by contruction already in session, so no
                # need to additionally run `session.add()` here
                task = self.session.load(task_id, add=False)
                task.attach(self._core)
                try:
                    task.kill()
                except Exception as err:
                    self.log.info(
                        "Got this error while killing old task '%s',"
                        " ignore it: %s: %s",
                        task,
                        err.__class__.__name__,
                        str(err))
                try:
                    task.free()
                except Exception as err:
                    self.log.info(
                        "Got this error while cleaning up old task '%s',"
                        " ignore it: %s: %s",
                        task,
                        err.__class__.__name__,
                        str(err))
                task.detach()
                self.session.remove(task_id)
                self.log.debug("Removed task '%s' from session.", task)
            self.log.info(
                "Done cleaning up old session tasks, starting with new one"
                " afresh...")

    def _sleep(self, lapse):
        """
        Pause execution for `lapse` seconds.

        The default implementation just calls ``time.sleep(lapse)``.

        This is provided as an overrideable method so that one can use
        a different system call to comply with other threading models
        (e.g., gevent).
        """
        # Python scripts become unresponsive during
        # `time.sleep()`, so we just do the wait in small
        # steps, to allow the interpreter to process
        # interrupts in the breaks.  Ugly, but works...
        for x in range(self.params.wait):
            time.sleep(1)



##
#
# Foreground and interactive scripts
#
##

class SessionBasedScript(_SessionBasedCommand):
    """
    Base class for ``grosetta``/``ggamess``/``gcodeml`` and like scripts.
    Implements a long-running script to submit and manage a large number
    of jobs grouped into a "session".

    The generic scripts implements a command-line like the following::

      PROG [options] INPUT [INPUT ...]

    First, the script builds a list of input files by recursively
    scanning each of the given INPUT arguments for files matching the
    `self.input_file_pattern` glob string (you can set it via a
    keyword argument to the ctor).  To perform a different treatment
    of the command-line arguments, override the
    :py:meth:`process_args()` method.

    Then, new jobs are added to the session, based on the results of
    the `process_args()` method above.  For each tuple of items
    returned by `process_args()`, an instance of class
    `self.application` (which you can set by a keyword argument to the
    ctor) is created, passing it the tuple as init args, and added to
    the session.

    The script finally proceeds to updating the status of all jobs in
    the session, submitting new ones and retrieving output as needed.
    When all jobs are done, the method :py:meth:`done()` is called,
    and its return value is used as the script's exit code.

    The script's exitcode tracks job status, in the following way.
    The exitcode is a bitfield; only the 4 least-significant bits
    are used, with the following meaning:

       ===  ============================================================
       Bit  Meaning
       ===  ============================================================
         0  Set if a fatal error occurred: the script could not complete
         1  Set if there are jobs in `FAILED` state
         2  Set if there are jobs in `RUNNING` or `SUBMITTED` state
         3  Set if there are jobs in `NEW` state
       ===  ============================================================

    This boils down to the following rules:
       * exitcode == 0: all jobs terminated successfully, no further action
       * exitcode == 1: an error interrupted script execution
       * exitcode == 2: all jobs terminated, not all of them successfully
       * exitcode > 3: run the script again to progress jobs

    """

    @same_docstring_as(_SessionBasedCommand.__init__)
    def __init__(self, **extra_args):
        # these are parameters used in the stock `new_tasks()`
        self.instances_per_file = 1
        self.instances_per_job = 1
        # catch omission of mandatory `application` ctor params (see above)
        # use bogus values that should point ppl to the right place
        self.input_filename_pattern = (
            'PLEASE SET `input_filename_pattern`'
            'IN `SessionBasedScript` CONSTRUCTOR')
        self.application = self.__unset_application_cls
        # init base class(es)
        super(SessionBasedScript, self).__init__(**extra_args)


    # safeguard against programming errors: if the `application` ctor
    # parameter has not been given to the constructor, the following
    # method raises a fatal error (this function simulates a class ctor)
    def __unset_application_cls(*args, **kwargs):
        """
        Raise an error if users did not set `application` in
        `SessionBasedScript` initialization.
        """
        raise gc3libs.exceptions.InvalidArgument(
            "PLEASE SET `application` in `SessionBasedScript` CONSTRUCTOR")


    ##
    # CUSTOMIZATION METHODS
    ##
    # The following are meant to be freely customized in derived scripts.
    ##

    def setup_args(self):
        """
        Set up command-line argument parsing.

        The default command line parsing considers every argument as
        an (input) path name; processing of the given path names is
        done in `parse_args`:meth:
        """
        self.add_param('args', nargs='*', metavar='INPUT',
                       help="Path to input file or directory."
                       " Directories are recursively scanned for input files"
                       " matching the glob pattern '%s'"
                       % self.input_filename_pattern)

    def pre_run(self):
        super(SessionBasedScript, self).pre_run()
        # since it may take quite some time before jobs are created
        # and the first report is displayed, print a startup banner so
        # that users get some kind of feedback ...
        print("Starting %s;"
              " use the '-v' command-line option to get"
              " a more verbose report of activity."
              % (self.name,))
        # parse the `states` list
        self.params.states = self.params.states.split(',')

    def new_tasks(self, extra):
        """
        Iterate over jobs that should be added to the current session.
        Each item yielded must be a valid `Task`:class: instance.

        This method is called by the default `process_args`:meth:, passing
        `self.extra` as the `extra` parameter.

        The default implementation of this method scans the arguments
        on the command-line for files matching the glob pattern
        `self.input_filename_pattern`, and for each matching file returns
        a job name formed by the base name of the file (sans
        extension), the class given by `self.application`, and the
        full path to the input file as sole argument.

        If `self.instances_per_file` and `self.instances_per_job` are
        set to a value other than 1, for each matching file N jobs are
        generated, where N is the quotient of
        `self.instances_per_file` by `self.instances_per_job`.

        See also: `process_args`:meth:
        """
        inputs = self._search_for_input_files(self.params.args)

        for path in inputs:
            if self.instances_per_file > 1:
                for seqno in range(1,
                                   1 + self.instances_per_file,
                                   self.instances_per_job):
                    if self.instances_per_job > 1:
                        yield (
                            "%s.%d--%s" % (
                                basename_sans(path),
                                seqno,
                                min(seqno + self.instances_per_job - 1,
                                    self.instances_per_file)),
                            self.application, [path], extra.copy())
                    else:
                        yield ("%s.%d" % (basename_sans(path),
                                          seqno),
                               self.application, [path], extra.copy())
            else:
                yield (basename_sans(path),
                       self.application, [path], extra.copy())

    def print_summary_table(self, output, stats):
        """
        Print a text summary of the session status to `output`.
        This is used to provide the "normal" output of the
        script; when the ``-l`` option is given, the output
        of the `print_tasks_table` function is appended.

        Override this in subclasses to customize the report that you
        provide to users.  By default, this prints a table with the
        count of tasks for each possible state.

        The `output` argument is a file-like object, only the `write`
        method of which is used.  The `stats` argument is a
        dictionary, mapping each possible `Run.State` to the count of
        tasks in that state; see `Engine.counts` for a detailed
        description.
        """
        table = PrettyTable(['state', 'n', 'n%'])
        table.align = 'r'
        table.align['n%'] = 'c'
        table.border = False
        table.header = False
        total = stats['total']
        # ensure we display enough decimal digits in percentages when
        # running a large number of jobs; see Issue 308 for a more
        # detailed descrition of the problem
        if total > 0:
            precision = max(1, math.log10(total) - 1)
            fmt = '(%%.%df%%%%)' % precision
            for state in sorted(stats.keys()):
                table.add_row([
                    state,
                    "%d/%d" % (stats[state], total),
                    fmt % (100.00 * stats[state] / total)
                ])
        output.write(str(table))
        output.write("\n")

    def print_tasks_table(
            self, output=sys.stdout, states=gc3libs.Run.State, only=object):
        """
        Output a text table to stream `output`, giving details about
        tasks in the given states.

        Optional second argument `states` restricts the listing to
        tasks that are in one of the specified states.  By default, all
        task states are allowed.  The `states` argument should be a
        list or a set of `Run.State` values.

        Optional third argument `only` further restricts the listing
        to tasks that are instances of a subclass of `only`.  By
        default, there is no restriction and all tasks are listed. The
        `only` argument can be a Python class or a tuple -- anything
        infact, that you can pass as second argument to the
        `isinstance` operator.

        :param output: An output stream (file-like object)
        :param states: List of states (`Run.State` items) to consider.
        :param   only: Root class (or tuple of root classes) of tasks to
                       consider.
        """
        table = PrettyTable(['JobID', 'Job name', 'State', 'Info'])
        table.align = 'l'
        for task in self.session:
            if isinstance(task, only) and task.execution.in_state(*states):
                table.add_row([task.persistent_id, task.jobname,
                               task.execution.state, task.execution.info])

        # XXX: uses prettytable's internal implementation detail
        if len(table._rows) > 0:
            output.write(str(table))
            output.write("\n")

    ##
    # pyCLI INTERFACE METHODS
    ##
    # The following methods adapt the behavior of the
    # `SessionBasedScript` class to the interface expected by pyCLI
    # applications.  Think twice before overriding them, and read
    # the pyCLI docs before :-)
    ##

    def setup(self):
        """
        Setup standard command-line parsing.

        GC3Libs scripts should probably override `setup_args`:meth:
        to modify command-line parsing.
        """
        # setup of base classes
        super(SessionBasedScript, self).setup()
        # add own "standard options"
        self.add_param(
            "-l",
            "--state",
            action="store",
            nargs='?',
            dest="states",
            default='',
            const=','.join(gc3libs.Run.State),
            help="Print a table of jobs including their status."
            " Optionally, restrict output to jobs with a particular STATE or"
            " STATES (comma-separated list).  The pseudo-states `ok` and"
            " `failed` are also allowed for selecting jobs in TERMINATED"
            " state with exitcode 0 or nonzero, resp.")
        return

    ##
    # INTERNAL METHODS
    ##
    # The following methods are for internal use; they can be
    # overridden and customized in derived classes, although there
    # should be no need to do so.
    ##

    def _main_loop_after_tasks_progress(self):
        """
        Print statistics about managed tasks to STDOUT.

        See `_SessionBasedCommand._main_loop_after_tasks_progress`:meth:
        for a description of what this method can generally do.
        """
        stats = self._controller.counts()
        # print results to user
        print ("Status of jobs in the '%s' session: (at %s)"
               % (self.session.name, time.strftime('%X, %x')))
        total = stats['total']
        if total > 0:
            if self.stats_only_for is not None:
                self.print_summary_table(sys.stdout,
                                         self._controller.counts(
                                             self.stats_only_for))
            else:
                self.print_summary_table(sys.stdout, stats)
            # details table, as per ``-l`` option
            if self.params.states:
                self.print_tasks_table(sys.stdout, self.params.states)
        else:
            if self.params.session is not None:
                print ("  There are no tasks in session '%s'."
                       % self.session.name)
            else:
                print ("  No tasks in this session.")
        return stats


    def _main_loop_done(self, rc):
        """
        Returns ``True`` if the main loop has completed and we don't want
        to continue the processing. Returns ``False`` otherwise.
        """
        if rc > 3:
            return False
        else:
            return True


    def _search_for_input_files(self, paths, pattern=None):
        """
        Recursively scan each location in list `paths` for files
        matching a glob pattern, and return the set of path names to
        such files.

        By default, the value of `self.input_filename_pattern` is used
        as the glob pattern to match file names against, but this can
        be overridden by specifying an explicit argument `pattern`.
        """
        inputs = set()
        ext = None
        if pattern is None:
            pattern = self.input_filename_pattern
            # special case for '*.ext' patterns
            ext = None
            if pattern.startswith('*.'):
                ext = pattern[1:]
                # re-check for more wildcard characters
                if '*' in ext or '?' in ext or '[' in ext:
                    ext = None

        def matches(name):
            return (fnmatch.fnmatch(os.path.basename(name), pattern)
                    or fnmatch.fnmatch(name, pattern))
        for path in paths:
            self.log.debug("Now processing input path '%s' ..." % path)
            if os.path.isdir(path):
                # recursively scan for input files
                for dirpath, dirnames, filenames in os.walk(path):
                    for filename in filenames:
                        if matches(filename):
                            pathname = os.path.join(dirpath, filename)
                            self.log.debug("Path '%s' matches pattern '%s',"
                                           " adding it to input list"
                                           % (pathname, pattern))
                            inputs.add(pathname)
            elif matches(path) and os.path.exists(path):
                self.log.debug("Path '%s' matches pattern '%s',"
                               " adding it to input list" % (path, pattern))
                inputs.add(path)
            elif ext is not None \
                    and not path.endswith(ext) \
                    and os.path.exists(path + ext):
                self.log.debug("Path '%s' matched extension '%s',"
                               " adding to input list"
                               % (path + ext, ext))
                inputs.add(os.path.realpath(path + ext))
            else:
                self.log.error(
                    "Cannot access input path '%s' - ignoring it.",
                    path)

        return inputs



##
#
# Background/daemon scripts
#
##

## client class

class DaemonClient(_Script):
    """
    Send XML-RPC requests to a running `SessionBasedDaemon`.

    The generic command line looks like the following:

      PROG client SERVER CMD [ARG [ARG ...]]

    The SERVER string is the URL where the XML-RPC server can be
    contacted.  A pair `hostname:port` is accepted as abbreviation for
    `http://hostname:port/` and a simple `:port` string is a valid
    alias for `http://localhost:port/`.  Alternatively, the SERVER
    argument can be the path to the ``daemon.url`` path where a
    running server writes its contact information.

    COMMAND is an XML-RPC command name; valid commands depend on the
    server and can be listed by using `help` as the COMMAND string
    (with no further arguments).  The remaining ARGs (if any) depend
    on COMMAND.
    """

    # the generic client code is also useful as-is, without additional
    # customization, so give it the required `version` attribute
    version='1.0'

    def setup_args(self):
        """
        Override this method to replace standard command-line arguments.
        """
        self.add_param('server', metavar='SERVER',
                       help=("Path to the file containing hostname and port of the"
                             " XML-RPC enpdoint of the daemon"))
        self.add_param('cmd', metavar='COMMAND',
                       help=("XML-RPC command to run. When COMMAND is `help`"
                             " a list of available commands is printed."))
        self.add_param('args', nargs='*', metavar='ARGS',
                       help=("Optional arguments of COMMAND."))


    def setup_options(self):
        """
        Override this method to add command-line options.
        """
        pass


    def pre_run(self):
        # skip `_Script.pre_run()` in chain call to avoid adding
        # `--config-files` etc which are not relevant here
        cli.app.CommandLineApp.pre_run(self)

        # setup GC3Libs logging
        self.log = make_logger(self.params.verbose, self.name,
                               threshold=self.verbose_logging_threshold)

        # call hook methods from derived classes
        self.parse_args()


    def main(self):
        server = self._connect_to_server(self.params.server)
        if server is None:
            return os.EX_NOHOST
        return self._run_command(server, self.params.cmd, *self.params.args)

    def _connect_to_server(self, server_url):
        url = self._parse_connect_string(server_url)
        try:
            # **NOTE:** This has to be the built-in `bytes` type; when
            # using `future`'s `newstr` or `newbytes` objects, the
            # `ServerProxy` becomes unusable, as *every* method call
            # raises an exception `AttributeError: encode method has
            # been disabled in newbytes`
            return ServerProxy(str(url))
        except Exception as err:
            self.log.error("Cannot connect to server `%s`: %s", url, err)
            return None

    def _parse_connect_string(self, arg):
        if arg.count(':') == 1:
            if '://' in arg:
                # nothing to do
                pass
            elif arg.startswith(':'):
                # accept `:port` as alias for `localhost:port`
                arg = ('http://localhost' + arg)
            else:
                # accept `host:port` as abbrev for `http://host:port`
                arg = ('http://' + arg)
        url = Url(arg)
        if url.scheme == 'file':
            path = url.path
            info = os.stat(path)
            if not stat.S_ISSOCK(info.st_mode):
                # not a socket, so read actual URL from file
                if stat.S_ISDIR(info.st_mode):
                    path = os.path.join(path, 'daemon.url')
                url = Url(read_contents(path))
        return url

    def _run_command(self, server, cmd, *args):
        try:
            func = getattr(server, cmd)
        except AttributeError:
            self.log.error(
                "Server exports no command named `%s`."
                " Use the `help` command to list all available methods.",
                cmd)
            return os.EX_UNAVAILABLE
        try:
            print(func(*args))
            return os.EX_OK
        except xmlrpc.client.Fault as err:
            self.log.error(
                "Error running command `%s`: %s",
                cmd, err.faultString)
            return os.EX_SOFTWARE


## daemon/server class

class SessionBasedDaemon(_SessionBasedCommand):
    """
    Base class for GC3Pie daemons. Implements a long-running script
    that can daemonize, provides an XML-RPC interface to interact with
    the current workflow and implement the concept of "inbox" to
    trigger the creation of new jobs as soon as a new file is created
    on a folder, or is available on an HTTP(S) or SWIFT endpoint.

    The generic script implements a command line like the following::

      PROG [server options] INBOX [INBOX ...]
    """

    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 0

    class Server(SimpleXMLRPCServer):
        """

        """

        PORTFILE_NAME = 'daemon.url'

        # FIXME: pick an unassigned port nr from https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.csv ?
        def __init__(self, parent, commands=None,
                     addr='localhost', port=0, portfile=None):
            self.parent = parent
            self.log = self.parent.log

            # Start XMLRPC server
            SimpleXMLRPCServer.__init__(self, (addr, port))
            self.addr, self.port = self.socket.getsockname()
            self.log.info("XML-RPC daemon running on %s:%s", self.addr, self.port)

            # save listening URL
            self.portfile = (
                portfile
                or os.path.join(self.parent.session.path,
                                self.PORTFILE_NAME))
            # assume we are doing this after PID file check,
            # so if a "port file" already exists, it is stale
            write_contents(
                self.portfile, ('http://{addr}:{port}/'
                                .format(addr=self.addr, port=self.port)))
            # ensure portfile is removed upon exit
            atexit.register(rm_f, self.portfile)

            # Register XMLRPC methods
            self.register_function(self.hello, "hello")
            self.register_function(self.parent.help, "help")
            self.register_introspection_functions()  # needed by `help`
            self.register_function(self.parent.shutdown, "quit")
            self.register_instance(commands or self.parent.commands,
                                   allow_dotted_names=False)

        def start(self):
            """
            Start serving requests.

            Calls into this method never return,
            so it should be run in a separate thread.
            """
            return self.serve_forever()

        def stop(self):
            """
            Shut down the XML-RPC server and remove the URL file.
            """
            try:
                self.shutdown()
            except Exception as err:
                # self.stop() could be called twice, let's assume it's not
                # an issue but log the event anyway
                self.log.warning(
                    "Ignoring exception caught while shutting down: %s", err)
            try:
                if os.path.exists(self.portfile):
                    os.remove(self.portfile)
            except Exception as err:
                self.log.warning(
                    "Cannot remove file `%s`: %s", self.portfile, err)

        def hello(self):
            """
            Print server URL.

            Probably only useful for checking if the server is up and
            responsive.
            """
            return ("HELLO from http://{addr}:{port}/"
                    .format(addr=self.addr, port=self.port))


    class Commands(object):
        """
        User-visible XML-RPC methods.

        Subclass this to override default methods or add new ones.

        .. note::

          Every public *attribute* of this class is exposed by the
          server; make sure that anithing which is not a public method
          is prefixed with ``_``.
        """
        def __init__(self, parent):
            self._parent = parent


        def kill(self, jobid=None):
            """
            Usage: kill JOBID

            Abort execution of a task and set it to TERMINATED state.
            """
            if not jobid:
                return "Usage: kill JOBID"
            gc3libs.log.info("Daemon requested to kill job %s", jobid)

            try:
                task = self._parent._controller.find_task_by_id(jobid)
            except KeyError:
                try:
                    task = self._parent.session.load(jobid, add=False)
                except Exception as err:
                    return (
                        "ERROR: Could not load task `%s` from session: %s"
                        % (jobid, err))
            try:
                task.attach(self._parent._controller)
                task.kill()
                task.detach()
                self._parent.session.save(task)
                return ("Task `%s` successfully killed" % jobid)
            except Exception as err:
                return ("ERROR: could not kill task `%s`: %s" % (jobid, err))


        def list(self, *opts):
            """
            Usage: list [daemon|session] [json|text|yaml]

            List IDs of tasks managed by this daemon.
            If the word ``session`` is present on the command-line,
            then tasks stored in the session are printed instead
            (which may be a superset of the tasks managed by the
            engine).

            One of the words ``json``, ``yaml``, or ``text``
            (simple list of IDs, one per line) can be used to
            choose the output format, with ``text`` being the default.
            """

            if 'session' in opts:
                tasks = iter(self._parent.session.tasks)
            else:
                # default is `daemon`
                tasks = self._parent._controller.iter_tasks()

            task_ids = [str(task.persistent_id) for task in tasks]

            if 'json' in opts:
                return json.dumps(task_ids)
            elif 'yaml' in opts:
                return yaml.dump(task_ids)
            else:
                return '\n'.join(task_ids)


        def list_details(self, *opts):
            """
            Usage: list_details [daemon|session] [json|text|yaml]

            Give information about tasks managed by this daemon;
            for each task, the following information are printed:

            * task name
            * execution state (e.g., ``NEW``, ``RUNNING``, etc.)
            * process exit code (only meaningful if state is ``TERMINATED``)
            * last line in the execution log

            If the word ``session`` is present on the command-line,
            then tasks stored in the session are printed instead
            (which may be a superset of the tasks managed by the
            engine).

            One of the words ``json``, ``yaml``, or ``text``
            (human-readable plain text table) can be used to choose
            the output format, with ``text`` being the default.
            """
            if 'session' in opts:
                tasks = iter(self._parent.session.tasks)
            else:
                # default is `daemon`
                tasks = self._parent._controller.iter_tasks()

            rows = []
            for task in tasks:
                rows.extend(self._make_rows(task))

            if 'json' in opts:
                return json.dumps(rows)
            elif 'yaml' in opts:
                return yaml.dump(rows)
            else:
                # default is plain text table
                table = PrettyTable()
                table.border = True
                table.align = 'l'
                table.field_names = [
                    "Job ID",
                    "Name",
                    "State",
                    "Exit code",
                    "Last logged event"
                ]
                for row in rows:
                    table.add_row([
                        row['id'],
                        row['jobname'],
                        row['state'],
                        row['rc'],
                        row['log']
                    ])
                return table.get_string()

        def _make_rows(self, task, indent='  ', recursive=True):
            """
            Helper method for ``list_details``.

            List details for task and each of its subtasks (if
            ``recursive`` is ``True``).  The detailed info is a
            dictionary with the following keys:

            * ``id``: Task's job ID,
            * ``state``: Task execution state,
            * ``rc``: Task execution exit code (if available),
            * ``log``: Last logged line in Task history.
            """
            row = {
                'id':    indent + str(task.persistent_id),
                'state': task.execution.state,
                'rc':    task.execution.returncode,
                'log':   task.execution.info,
            }
            try:
                row['jobname'] = task.jobname
            except AttributeError:
                row['jobname'] = ''

            rows = [row]
            if recursive and 'tasks' in task:
                indent = indent + '  '
                for task in task.tasks:
                    rows.extend(self._make_rows(task, indent, recursive))

            return rows


        def manage(self, jobid=None):
            """
            Usage: manage JOBID

            Tell daemon to start actively managing a task.
            """
            if not jobid:
                return "Usage: manage JOBID"
            gc3libs.log.info("Daemon requested to manage job %s", jobid)

            # we should not add duplicates into the Engine,
            # so check first that the task is not already there
            try:
                self._parent._controller.find_task_by_id(jobid)
                return ("Task `%s` is already managed by the daemon.")
            except KeyError:
                pass

            try:
                task = self._parent.session.load(jobid, add=True)
            except Exception as err:
                return ("ERROR: Could not load task `%s`: %s" % (jobid, err))

            try:
                self._parent._controller.add(task)
                return ("Task `%s` successfully add to daemon's Engine." % jobid)
            except Exception as ex:
                return ("ERROR: Could not add task `%s` to daemon: %s" % (jobid, ex))


        def remove(self, jobid=None):
            """
            Usage: remove JOBID

            Unmanage a task and remove it from the session.

            WARNING: All traces of the task are removed and it will
            not be possible to load or manage it again.
            """
            if not jobid:
                return "Usage: remove JOBID"
            gc3libs.log.info("Daemon requested to remove job %s", jobid)

            try:
                task = self._parent._controller.find_task_by_id(jobid)
                managed = True
            except KeyError:
                try:
                    task = self._parent.session.load(jobid, add=False)
                    managed = False
                except Exception as err:
                    return (
                        "ERROR: Could not load task `%s` from session: %s"
                        % (jobid, err))

            if task.execution.state != gc3libs.Run.State.TERMINATED:
                return (
                    "ERROR: can only remove tasks in TERMINATED state;"
                    " current state is: %s"
                    % task.execution.state)

            try:
                if managed:
                    self._parent._controller.remove(task)
                self._parent.session.remove(jobid)
                return "Job %s successfully removed" % jobid
            except Exception as ex:
                return ("ERROR: could not remove task `%s`: %s" % (jobid, ex))


        def redo(self, jobid=None, from_stage=None):
            """
            Usage: redo JOBID [STAGE]

            Resubmit the task identified by JOBID.  If task is a
            `SequentialTaskCollection`, then resubmit it from the
            given stage (identified by its integer index in the
            collection; by default, sequential task collections resume
            from the very first task).

            Only tasks in TERMINATED state can be resubmitted;
            if necessary kill the task first.
            """
            if jobid is None:
                return "Usage: redo JOBID [STAGE]"
            if from_stage is None:
                args = []
                gc3libs.log.info("Daemon requested to redo job %s", jobid)
            else:
                try:
                    args = [int(from_stage)]
                except (TypeError, ValueError) as err:
                    return (
                        "ERROR: STAGE argument must be a non-negative integer,"
                        " got {0!r} instead.".format(from_stage))
                gc3libs.log.info(
                    "Daemon requested to redo job %s from stage %s",
                    jobid, from_stage)

            # ensure we re-read task from the DB to pick up updates
            try:
                task = self._parent._controller.find_task_by_id(jobid)
                gc3libs.log.debug(
                    "Daemon unloading job %s (will re-load soon)", jobid)
                self._parent._controller.remove(task)
            except KeyError:
                pass

            try:
                task = self._parent.session.load(jobid, add=True)
            except Exception as err:
                return (
                    "ERROR: Could not load task `%s` from session: %s"
                    % (jobid, err))

            try:
                self._parent._controller.redo(task, *args)
                self._parent.session.save(task)
                return ("Task `%s` successfully resubmitted" % jobid)
            except Exception as err:
                return ("ERROR: could not resubmit task `%s`: %s" % (jobid, err))


        def show(self, jobid=None, *attrs):
            """
            Usage: show JOBID [attributes]

            Same output as ``ginfo -v JOBID [-p attributes]``
            """
            if not jobid:
                return "Usage: show <jobid> [attributes]"

            try:
                task = self._parent._controller.find_task_by_id(jobid)
            except KeyError:
                try:
                    task = self._parent.session.load(jobid, add=False)
                except Exception as err:
                    return (
                        "ERROR: Could not load task `%s` from session: %s"
                        % (jobid, err))

            out = StringIO()
            if not attrs:
                attrs = None
            prettyprint(task, indent=2, output=out, only_keys=attrs)
            return out.getvalue()


        def stats(self, *opts):
            """
            Usage: stats [json|text|yaml]

            Print how many jobs are in any given state.

            One of the words ``json``, ``yaml``, or ``text``
            (human-readable plain text table) can be used to choose
            the output format, with ``text`` being the default.
            """

            stats = dict(self._parent._controller.counts())

            if 'json' in opts:
                return json.dumps(stats)
            elif 'yaml' in opts:
                return yaml.dump(stats)
            else:
                # default is plain text table
                table = PrettyTable()
                table.border = True
                table.align = 'l'
                table.field_names = ["State", "Count"]
                for state, count in sorted(stats.items()):
                    table.add_row([state, count])
                return table.get_string()


        def unmanage(self, jobid=None):
            """
            Usage: unmanage JOBID

            Tell daemon to stop actively managing a task.

            The task will keep its state until the daemon is told to
            manage it again.  In particular, tasks that are in
            ``RUNNING`` state keep running and may complete even while
            unmanaged.
            """
            if not jobid:
                return "Usage: unmanage JOBID"
            gc3libs.log.info("Daemon requested to unmanage job %s", jobid)

            try:
                task = self._parent._controller.find_task_by_id(jobid)
            except KeyError:
                return ("ERROR: Task `%s` not currently managed by daemon" % jobid)

            try:
                self._parent._controller.remove(task)
                return "Task `%s` successfully forgotten" % jobid
            except Exception as ex:
                return ("ERROR: could not forget task `%s`: %s" % (jobid, ex))


    #
    # Basic commands exposed via XML-RPC
    #

    def help(self, cmd=None):
        """
        Show available commands, or get information about a specific
        command.
        """
        if cmd:
            return self.server.system_methodHelp(cmd)
        else:
            return ("""
The following daemon commands are available:

  {cmds}

Run `help CMD` to get help on command CMD.
            """.format(cmds=("\n  ".join(sorted(self.server.system_listMethods())))))

    def shutdown(self):
        """Terminate daemon."""

        # run this in a separate thread so the server can reply to the requestor
        pid = os.getpid()
        def killme():
            self.log.info("Shutting down as requested by `quit` command ...")

            # stop main loop
            self.running = False

            # wait 1s so that the client connection is not hung up
            # abruptly and we have time to give feedback
            self._sleep(1)
            self.server.stop()

            # Send kill signal to current process if not terminated
            # within 10s
            self._sleep(9)
            self.log.warning(
                "Daemon still alive after 10s;"
                " sending SIGTERM to process %s ...", pid)
            os.kill(pid, signal.SIGTERM)

            # If this is not working, try a more aggressive approach:
            # SIGINT is interpreted by the Python interpreter as
            # `KeyboardInterrupt`. It will also call
            # `self.parent.cleanup()`, again.
            self._sleep(10)
            self.log.warning(
                "Daemon still alive after 10s;"
                " sending SIGINT to process %s ...", pid)
            os.kill(pid, signal.SIGINT)

            # We whould never reach this point, but Murphy's law...
            self._sleep(10)
            # Revert back to SIGKILL. This will leave the pidfile
            # hanging around.
            self.log.warning("Still alive: forcing death by SIGKILL.")
            os.kill(pid, signal.SIGKILL)

            self.log.error(
                "Unable to kill process %d; giving up."
                " Perhaps termination signals cannot be delivered"
                " due to the process being in 'uninterruptible sleep'"
                " (D) state?", pid)
            return os.EX_OSERR
        thread = mp.Process(target=killme)
        thread.daemon = True
        thread.start()
        return ("Terminating process %d in 10s" % pid)



    #
    # Internal mechanisms
    #

    def terminate(self, exc_type=None, exc_value=None, tb=None):
        self.running = False
        self.log.debug("Waiting for communication thread to terminate ...")
        try:
            self.server.stop()
            self.server_process.terminate()
            self.server_process.join(1)
        except AttributeError:
            # If the script is interrupted/killed during command line
            # parsing the `self.server` daemon is not present and we get
            # an AttributeError we can safely ignore.
            pass

    def setup(self):
        super(SessionBasedDaemon, self).setup()

        # change default for the `-C`, `--session` and `--output` options
        self.actions['wait'].default = 30
        self.actions['wait'].help = (
            'Check the status of tasks every NUM'
            ' seconds. Default: %(default)s')

        # Default session dir and output dir are computed from
        # --working-directory.  Set None here so that we can update it
        # in pre_run()
        self.actions['session'].default = None
        self.actions['output'].default = None

    def setup_options(self):
        self.add_param('-F', '--foreground',
                       action='store_true', default=False,
                       help=("Do not daemonize "
                             "and keep running as a foreground process"
                             " in the starting shell."
                             " Mostly useful for debugging."
                             " Off by default."))

        self.add_param('--working-dir',
                       metavar='PATH', default=os.getcwd(),
                       help=("Store session information and output files"
                             " in the directory pointed to by PATH."
                             " Ignored when runing in foreground."
                             " Default: %(default)s"))

        self.add_param('--listen', default="localhost:0", metavar="IP_ADDR",
                       help=("IP address or hostname"
                             " where the XML-RPC server should listen to."
                             " Optionally a port number can be specified"
                             " by separating it with a colon ':' character;"
                             " a port number of '0' means the actual port"
                             " will be dynamically allocated and only"
                             " known once the daemon is started."
                             " (It is written to the `daemon.url` file.)"
                             " Default: %(default)s"))

    def setup_args(self):
        self.add_param('inbox', nargs='+',
                       help=("'Inbox' directories:"
                             " whenever a new file is created"
                             " in one of these directories,"
                             " a callback is triggered to add new jobs"))

    def parse_args(self):
        super(SessionBasedDaemon, self).parse_args()

        self.params.working_dir = os.path.abspath(self.params.working_dir)

        # Default session dir is inside the working directory
        if not self.params.session:
            self.params.session = os.path.join(
                self.params.working_dir, 'session')

        # Convert inbox to Url objects
        self.params.inbox = [Url(inbox) for inbox in self.params.inbox]

        # Default output directory is the working directory.
        if not self.params.output:
            self.params.output = self.params.working_dir

        # parse the host:port listen string
        if ':' in self.params.listen:
            # use `.rsplit()` to be IPv6-safe
            addr, port = self.params.listen.rsplit(':', 1)
            # handle case `:port`
            if not addr:
                addr = 'localhost'
            # be forgiving of `host:` ...
            if not port:
                port = self.DEFAULT_PORT
            try:
                port = int(port)
            except ValueError as err:
                raise InvalidUsage(
                    "Port number must be an integer between 0 and 65535."
                    " Got `{port}` instead.".format(port=port))
        else:
            # only host given, no port
            addr = self.params.listen
            port = self.DEFAULT_PORT
        self.listen_addr = addr
        self.listen_port = port


    def _main(self):
        # make PID file (in the session dir, so no two
        # instances of this daemon can be concurrently
        # running)
        lockfile_path = os.path.join(
            self.session.path, self.name + '.pid')
        lockfile = PIDLockFile(lockfile_path)
        if lockfile.is_locked():
            raise gc3libs.exceptions.FatalError(
                "PID file `{0}` is already present."
                " Ensure no other daemon is running,"
                " then delete file and re-run this command."
                .format(lockfile_path))

        if self.params.foreground:
            context = lockfile
            self.log.warning(
                "Keep running in foreground"
                " as requested with `-F`/`--foreground` option ...")
        else:
            self.session.store.pre_fork()
            # redirect all output
            logfile = open(os.path.join(self.params.working_dir, 'daemon.log'), 'w')
            os.dup2(logfile.fileno(), 1)
            os.dup2(logfile.fileno(), 2)
            # use PEP 3134 context manager for daemonizing
            context = daemon.DaemonContext(
                files_preserve=list(set([1,2]).union(self.__get_logging_fds())),
                pidfile=lockfile,  # DaemonContext wraps the PID file ctx
                signal_map={signal.SIGTERM: self.terminate},
                stderr=logfile,
                stdout=logfile,
                umask=0o002,
                working_directory=self.params.working_dir,
            )
            self.log.info("About to daemonize ...")

        with context:
            # ensure PID file is removed upon termination -- we need
            # to do this *after* forking to avoid the PID file being
            # prematurely removed while the daemon is still
            # preparing...
            atexit.register(rm_f, lockfile_path)
            # un-suspend session store functionality
            self.session.store.post_fork()
            self._start_inboxes()
            self._start_server()
            self.running = True
            return super(SessionBasedDaemon, self)._main()

    def _start_inboxes(self):
        """
        Populate `self.pollers` based on `self.params.inbox`.
        """
        self.pollers = [
            make_poller(inbox, recurse=True)
            for inbox in self.params.inbox
        ]

    def _start_server(self):
        """
        Start command server in a separate thread.
        """
        self.log.info(
            "Starting XML-RPC server on %s:%s ...",
            self.listen_addr, self.listen_port)
        try:
            self.server = self.Server(
                self, self.Commands(self),
                self.listen_addr, self.listen_port)
            self.server_process = mp.Process(
                target=self.server.start, name='server')
            self.server_process.daemon = True
            # catch SIGTERM and turn it into a `quit` command;
            # signal handlers can only be set from the main thread
            orig_sigterm_handler = signal.getsignal(signal.SIGTERM)
            if orig_sigterm_handler is None:
                orig_sigterm_handler = signal.SIG_DFL
            def sigterm(signo, frame):
                self.shutdown()
                # restore old SIGTERM handler in case
                # `self.shortdown()` needs to force kill this process
                signal.signal(signal.SIGTERM, orig_sigterm_handler)
            signal.signal(signal.SIGTERM, sigterm)
            # actually start the serving thread
            self.server_process.start()
        except Exception as err:
            self.log.error("Could not start server: %s", err)
            raise

    def __get_logging_fds(self):
        fds = set([])
        for handler in logging._handlerList:
            # dereference weakref
            try:
                handler = handler()
            except:
                pass
            self.log.debug("Inspecting logging handler %r", handler)
            # ignore handlers without underlying file or socket
            if isinstance(handler, (
                    logging.NullHandler,
                    logging.handlers.BufferingHandler,
                    logging.handlers.HTTPHandler,
                    logging.handlers.MemoryHandler,
                    logging.handlers.SMTPHandler
            )):
                continue
            # code below works for StreamHandler and FileHandler
            try:
                fds.add(handler.stream.fileno())
                continue  # skip logging below
            except AttributeError:
                pass
            # code below works for SysLogHandler and SocketHandler
            try:
                fds.add(handler.socket.fileno())
                continue  # skip logging below
            except AttributeError:
                pass
            self.log.debug(
                "Cannot extract file descriptor number from %r;"
                " logging to this handler might be dropped"
                " after daemonizing.", handler)
        self.log.debug("File descriptors used in logging: %r", fds)
        return fds


    def _main_loop_before_tasks_progress(self):
        """
        Poll inboxes for events and fire the corresponding handlers.
        """
        self.log.debug("In `SessionBasedDaemon._main_loop_before_tasks_progress()`")
        for inbox in self.pollers:
            events = inbox.get_new_events()
            for subject, what in events:
                self.log.debug(
                    "Got event %s on %s from Inbox %s",
                    what, subject, inbox)
                if what == 'created':
                    self.created(inbox, subject)
                elif what == 'modified':
                    self.modified(inbox, subject)
                elif what == 'deleted':
                    self.deleted(inbox, subject)


    def _main_loop_done(self, rc):
        """
        Tell the main loop to run until interrupted.
        """
        return not self.running


    #
    # react on Inbox events
    #

    def created(self, inbox, subject):
        """
        React to creation of `subject` in `inbox`.

        A typical scenario is this: a new file is created in a watched
        directory; this method could then react by creating a new task
        to process that file.

        This method should be overridden in derived classes, as the
        default implementation does nothing.
        """
        pass

    def modified(self, inbox, subject):
        """
        React to modification of `subject` in `inbox`.

        .. note::

          Not all Pollers are capable of generating ``modified``
          events reliably.  This method is provided for completeness,
          but likely only useful for filesystem-watching inboxes.

        This method should be overridden in derived classes, as the
        default implementation does nothing.
        """
        pass

    def deleted(self, inbox, subject):
        """
        React to removal of `subject` from `inbox`.

        This method should be overridden in derived classes, as the
        default implementation does nothing.
        """
        pass
