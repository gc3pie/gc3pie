#! /usr/bin/env python
#
#   cmdline.py -- Prototypes for GC3Libs-based scripts
#
#   Copyright (C) 2010-2018 University of Zurich
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
#
"""Prototype classes for GC3Libs-based scripts.

Classes implemented in this file provide common and recurring
functionality for GC3Libs command-line utilities and scripts.  User
applications should implement their specific behavior by subclassing
and overriding a few customization methods.

There are currently two public classes provided here:

:class:`GC3UtilsScript`
  Base class for all the GC3Utils commands. Implements a few methods
  useful for writing command-line scripts that operate on jobs by ID.

:class:`SessionBasedScript`
  Base class for the ``grosetta``/``ggamess``/``gcodeml`` scripts.
  Implements a long-running script to submit and manage a large number
  of jobs grouped into a "session".

:class:`SessionBasedDaemon`
  Base class for GC3Pie daemons. Implements a long-running daemon with
  XML-RPC interface and support for file/http/swift based inboxes

"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


# stdlib modules
import fnmatch
import logging
from logging.handlers import SysLogHandler
import math
import os
import os.path
import signal
import sys
from prettytable import PrettyTable
import time
import threading
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from collections import defaultdict
from prettytable import PrettyTable
import SimpleXMLRPCServer as sxmlrpc
import xmlrpclib

import json
import yaml

# 3rd party modules
import daemon
import cli  # pyCLI
import cli.app
import cli._ext.argparse as argparse


# interface to Gc3libs
import gc3libs
import gc3libs.config
from gc3libs.compat import lockfile
from gc3libs.compat.lockfile.pidlockfile import PIDLockFile
import gc3libs.core
import gc3libs.exceptions
import gc3libs.persistence
import gc3libs.utils
import gc3libs.url
from gc3libs.quantity import Memory, GB, Duration, hours
from gc3libs.session import Session
from gc3libs.poller import get_poller, get_mask_description, events as notify_events

# types for command-line parsing; see
# http://docs.python.org/dev/library/argparse.html#type

def nonnegative_int(num):
    """
    Raise `ArgumentTypeError` if `num` is a negative integer (<0), and
    return `int(num)` otherwise. `num` can be any object which can be
    converted to an int.

    >>> nonnegative_int('1')
    1
    >>> nonnegative_int(1)
    1
    >>> nonnegative_int('-1') # doctest:+ELLIPSIS
    Traceback (most recent call last):
        ...
    ArgumentTypeError: '-1' is not a non-negative integer number.
    >>> nonnegative_int(-1) # doctest:+ELLIPSIS
    Traceback (most recent call last):
        ...
    ArgumentTypeError: '-1' is not a non-negative integer number.

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

    >>> nonnegative_int('ThisWillRaiseAnException') # doctest:+ELLIPSIS
    Traceback (most recent call last):
        ...
    ArgumentTypeError: 'ThisWillRaiseAnException' is not a non-negative ...
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
    >>> positive_int('-1') # doctest:+ELLIPSIS
    Traceback (most recent call last):
    ...
    ArgumentTypeError: '-1' is not a positive integer number.
    >>> positive_int(-1) # doctest:+ELLIPSIS
    Traceback (most recent call last):
    ...
    ArgumentTypeError: '-1' is not a positive integer number.
    >>> positive_int(0) # doctest:+ELLIPSIS
    Traceback (most recent call last):
    ...
    ArgumentTypeError: '0' is not a positive integer number.

    Floats are ok too:

    >>> positive_int(3.14)
    3

    but please take care that float *greater* than 0 will fail:

    >>> positive_int(0.1)
    Traceback (most recent call last):
    ...
    ArgumentTypeError: '0.1' is not a positive integer number.

    Please note that `0` is NOT ok:

    >>> positive_int(-0) # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ArgumentTypeError: '0' is not a positive integer number.
    >>> positive_int('0') # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ArgumentTypeError: '0' is not a positive integer number.
    >>> positive_int('-0') # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ArgumentTypeError: '-0' is not a positive integer number.

    Any string which does cannot be converted to an integer will fail:

    >>> positive_int('ThisWillRaiseAnException') # doctest:+ELLIPSIS
    Traceback (most recent call last):
        ...
    ArgumentTypeError: 'ThisWillRaiseAnException' is not a positive integer ...

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
    gc3libs.utils.test_file(path,
                            os.F_OK | os.R_OK,
                            argparse.ArgumentTypeError)
    return path


def executable_file(path):
    gc3libs.utils.test_file(path,
                            os.F_OK | os.R_OK | os.X_OK,
                            argparse.ArgumentTypeError)
    return path


def existing_directory(path):
    gc3libs.utils.test_file(path,
                            os.F_OK | os.R_OK | os.X_OK,
                            argparse.ArgumentTypeError, isdir=True)
    return path


def valid_directory(path):
    if os.path.exists(path) and not os.path.isdir(path):
        raise argparse.ArgumentTypeError(
            "path '%s' already exists but is not a directory."
            % (path,))
    return path


# script classes

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

    def cleanup(self):
        """
        Method called when the script is interrupted
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
        for k, v in extra_args.items():
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

        # allow overriding command-line options in subclasses
        def argparser_factory(*args, **kwargs):
            kwargs.setdefault('conflict_handler', 'resolve')
            kwargs.setdefault('formatter_class',
                              cli._ext.argparse.RawDescriptionHelpFormatter)
            return cli.app.CommandLineApp.argparser_factory(*args, **kwargs)

        self.argparser_factory = argparser_factory
        # init superclass
        extra_args.setdefault(
            'name',
            os.path.splitext(
                os.path.basename(
                    sys.argv[0]))[0])
        extra_args.setdefault('reraise', Exception)
        cli.app.CommandLineApp.__init__(self, **extra_args)
        # provide some defaults
        self.verbose_logging_threshold = 0

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
        cli.app.CommandLineApp.setup(self)

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
                       default=str.join(
                           ',', gc3libs.Default.CONFIG_FILE_LOCATIONS),
                       help="Comma separated list of configuration files",
                       )
        return

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
        # finish setup
        self.setup_options()
        self.setup_args()

        # parse command-line
        cli.app.CommandLineApp.pre_run(self)

        # setup GC3Libs logging
        loglevel = max(1, logging.WARNING -
                       10 *
                       max(0, self.params.verbose -
                           self.verbose_logging_threshold))
        gc3libs.configure_logger(loglevel, "gc3utils")  # alternate: self.name
        # alternate: ('gc3.' + self.name)
        self.log = logging.getLogger('gc3.gc3utils')
        self.log.setLevel(loglevel)
        self.log.propagate = True
        self.log.info("Starting %s at %s; invoked as '%s'",
                      self.name, time.asctime(), str.join(' ', sys.argv))

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
                % (str.join("', '", self.params.config_files)))

        # call hook methods from derived classes
        self.parse_args()

    def run(self):
        """
        Execute `cli.app.Application.run`:meth: if any exception is
        raised, catch it, output an error message and then exit with
        an appropriate error code.
        """
        try:
            return cli.app.CommandLineApp.run(self)
        except gc3libs.exceptions.InvalidUsage as ex:
            # Fatal errors do their own printing, we only add a short usage
            # message
            sys.stderr.write(
                "Type '%s --help' to get usage help.\n" % self.name)
            return 64  # EX_USAGE in /usr/include/sysexits.h
        except KeyboardInterrupt:
            sys.stderr.write(
                "%s: Exiting upon user request (Ctrl+C)\n" % self.name)
            self.cleanup()
            return 13
        except SystemExit as ex:
            #  sys.exit() has been called in `post_run()`.
            raise
        # the following exception handlers put their error message
        # into `msg` and the exit code into `rc`; the closing stanza
        # tries to log the message and only outputs it to stderr if
        # this fails
        except lockfile.Error as ex:
            # exc_info = sys.exc_info()
            msg = ("Error manipulating the lock file (%s: %s)."
                   " This likely points to a filesystem error"
                   " or a stale process holding the lock."
                   " If you cannot get this command to run after"
                   " a system reboot, please write to gc3pie@googlegroups.com"
                   " including any output you got by running '%s -vvvv %s'."
                   " (You need to be subscribed to post to the mailing list)")
            if len(sys.argv) > 0:
                msg %= (ex.__class__.__name__, str(ex),
                        self.name, str.join(' ', sys.argv[1:]))
            else:
                msg %= (ex.__class__.__name__, str(ex), self.name, '')
            # rc = 1
        except AssertionError as ex:
            # exc_info = sys.exc_info()
            msg = ("BUG: %s\n"
                   "Please send an email to gc3pie@googlegroups.com"
                   " including any output you got by running '%s -vvvv %s'."
                   " (You need to be subscribed to post to the mailing list)"
                   " Thanks for your cooperation!")
            if len(sys.argv) > 0:
                msg %= (ex, self.name, str.join(' ', sys.argv[1:]))
            else:
                msg %= (ex, self.name, '')
            # rc = 1
        except cli.app.Abort as ex:
            msg = "%s: %s" % (ex.__class__.__name__, ex)
            # rc = ex.status
        except EnvironmentError as ex:
            msg = "%s: %s" % (ex.__class__.__name__, ex)
            # rc = os.EX_IOERR  # 74 (see: /usr/include/sysexits.h )
        except Exception as ex:
            if 'GC3PIE_NO_CATCH_ERRORS' in os.environ:
                # propagate generic exceptions for debugging purposes
                raise
            else:
                # generic error exit
                msg = "%s: %s" % (ex.__class__.__name__, ex)
                # rc = 1
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
            if ('verbose' in self.params and self.params.verbose >
                    self.verbose_logging_threshold + 2):
                sys.excepthook(* sys.exc_info())
        # ...and exit
        return 1

    ##
    # INTERNAL METHODS
    ##
    # The following methods are for internal use; they can be
    # overridden and customized in derived classes, although there
    # should be no need to do so.
    ##

    def _make_config(
            self,
            config_file_locations=gc3libs.Default.CONFIG_FILE_LOCATIONS,
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
                    gc3libs.utils.deploy_configuration_file(
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
                           str.join("', '", config_file_locations))
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
                % str.join(',', resource_names))


class GC3UtilsScript(_Script):

    """
    Base class for GC3Utils scripts.

    The default command line implemented is the following:

      script [options] JOBID [JOBID ...]

    By default, only the standard options ``-h``/``--help`` and
    ``-V``/``--version`` are considered; to add more, override
    `setup_options`:meth:
    To change default positional argument parsing, override
    `setup_args`:meth:

    """

    ##
    # CUSTOMIZATION METHODS
    ##
    # The following are meant to be freely customized in derived scripts.
    ##

    def setup_args(self):
        """
        Set up command-line argument parsing.

        The default command line parsing considers every argument as a
        job ID; actual processing of the IDs is done in
        `parse_args`:meth:
        """
        self.add_param(
            'args',
            nargs='*',
            metavar='JOBID',
            help="Job ID string identifying the jobs to operate upon.")

    def parse_args(self):
        if hasattr(self.params, 'args') and '-' in self.params.args:
            # Get input arguments *also* from standard input
            self.params.args.remove('-')
            self.params.args.extend(sys.stdin.read().split())

    ##
    # pyCLI INTERFACE METHODS
    ##
    # The following methods adapt the behavior of the
    # `SessionBasedScript` class to the interface expected by pyCLI
    # applications.  Think twice before overriding them, and read
    # the pyCLI docs before :-)
    ##

    def __init__(self, **extra_args):
        _Script.__init__(self, main=self.main, **extra_args)

    def setup(self):
        """
        Setup standard command-line parsing.

        GC3Utils scripts should probably override `setup_args`:meth:
        and `setup_options`:meth: to modify command-line parsing.
        """
        # setup of base classes (creates the argparse stuff)
        _Script.setup(self)
        # local additions
        self.add_param("-s",
                       "--session",
                       action="store",
                       required=True,
                       default=gc3libs.Default.JOBS_DIR,
                       help="Directory where job information will be stored.")

    def pre_run(self):
        """
        Perform parsing of standard command-line options and call into
        `parse_args()` to do non-optional argument processing.
        """
        # base class parses command-line
        _Script.pre_run(self)

    ##
    # INTERNAL METHODS
    ##
    # The following methods are for internal use; they can be
    # overridden and customized in derived classes, although there
    # should be no need to do so.
    ##

    def _get_jobs(self, job_ids, ignore_failures=True):
        """
        Iterate over jobs (gc3libs.Application objects) corresponding
        to the given Job IDs.

        If `ignore_failures` is `True` (default), errors retrieving a
        job from the persistence layer are ignored and the jobid is
        skipped, therefore the returned list can be shorter than the
        list of Job IDs given as argument.  If `ignore_failures` is
        `False`, then any errors result in the relevant exception being
        re-raised.
        """
        for jobid in job_ids:
            try:
                yield self.session.load(jobid)
            except Exception as ex:
                # Exempted from GC3Pie's `error_ignored()` policy as there
                # is explicit control via the `ignore_failures` parameter
                if ignore_failures:
                    gc3libs.log.error(
                        "Could not retrieve job '%s' (%s: %s). Ignoring.",
                        jobid, ex.__class__.__name__, ex,
                        exc_info=(self.params.verbose > 2))
                    continue
                else:
                    raise


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
        Method to add a task to the session (and the controller)
        """
        self.controller.add(task)
        self.session.add(task)

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

        FIXME: While on a SessionBasedScript this method is called
        *after* processing all the jobs, on a SessionBasedDaemon it is
        called *before*.

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

    ##
    # pyCLI INTERFACE METHODS
    ##
    # The following methods adapt the behavior of the
    # `SessionBasedScript` class to the interface expected by pyCLI
    # applications.  Think twice before overriding them, and read
    # the pyCLI docs before :-)
    ##

    # safeguard against programming errors: if the `application` ctor
    # parameter has not been given to the constructor, the following
    # method raises a fatal error (this function simulates a class ctor)
    def __unset_application_cls(*args, **kwargs):
        """Raise an error if users did not set `application` in
        `SessionBasedScript` initialization."""
        raise gc3libs.exceptions.InvalidArgument(
            "PLEASE SET `application` in `SessionBasedScript` CONSTRUCTOR")

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
        self.instances_per_file = 1
        self.instances_per_job = 1
        self.extra = {}  # extra extra_args arguments passed to `parse_args`
        # use bogus values that should point ppl to the right place
        self.input_filename_pattern = 'PLEASE SET `input_filename_pattern`'
        'IN `SessionBasedScript` CONSTRUCTOR'
        # catch omission of mandatory `application` ctor param (see above)
        self.application = _SessionBasedCommand.__unset_application_cls
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
            self.make_directory_path(
                self.params.output,
                'NAME'))
        # build job list
        new_jobs = list(self.new_tasks(self.extra.copy()))
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
        warning_on_old_style_given = False
        for n, item in enumerate(new_jobs):
            if isinstance(item, tuple):
                if not warning_on_old_style_given:
                    self.log.warning(
                        "Using old-style new tasks list; please update"
                        " the code!")
                    warning_on_old_style_given = True
                # build Task for (jobname, classname, args, kwargs)
                jobname, cls, args, kwargs = item
                if jobname in existing_job_names:
                    continue
                kwargs.setdefault('jobname', jobname)
                kwargs.setdefault(
                    'output_dir',
                    self.make_directory_path(
                        self.params.output,
                        jobname))
                kwargs.setdefault(
                    'requested_cores', self.extra['requested_cores'])
                kwargs.setdefault(
                    'requested_memory', self.extra['requested_memory'])
                kwargs.setdefault(
                    'requested_walltime', self.extra['requested_walltime'])
                # create a new `Task` object
                try:
                    task = cls(*args, **kwargs)
                except Exception as ex:
                    self.log.error("Could not create job '%s': %s."
                                   % (jobname, str(ex)), exc_info=__debug__)
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
                    "SessionBasedScript.process_args got %r (%s),"
                    " but was expecting a gc3libs.Task instance" %
                    (item, type(item)))

            # patch output_dir if it's not changed from the default,
            # or if it's not defined (e.g., TaskCollection)
            if 'output_dir' not in task or task.output_dir == self.extra[
                    'output_dir']:
                # user did not change the `output_dir` default, expand it now
                self._fix_output_dir(task, task.jobname)

            # all done, append to session
            self.session.add(task, flush=False)
            self.log.debug("Added task '%s' to session." % task.jobname)

    def _fix_output_dir(self, task, name):
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

    def setup_common_options(self, parser):
        # 1. job requirements
        parser.add_param(
            "-c", "--cpu-cores", dest="ncores",
            type=positive_int, default=1,  # 1 core
            metavar="NUM",
            help="Set the number of CPU cores required for each job"
            " (default: %(default)s). NUM must be a whole number."
        )
        parser.add_param(
            "-m", "--memory-per-core", dest="memory_per_core",
            type=Memory, default=2 * GB,  # 2 GB
            metavar="GIGABYTES",
            help="Set the amount of memory required per execution core;"
            " default: %(default)s. Specify this as an integral number"
            " followed by a unit, e.g., '512MB' or '4GB'.")
        parser.add_param(
            "-r",
            "--resource",
            action="store",
            dest="resource_name",
            metavar="NAME",
            default=None,
            help="Submit jobs to a specific computational resources."
            " NAME is a resource name or comma-separated list of such names."
            " Use the command `gservers` to list available resources.")
        parser.add_param(
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
        parser.add_param(
            "-s",
            "--session",
            dest="session",
            default=os.path.join(
                os.getcwd(),
                self.name),
            metavar="PATH",
            help="Store the session information in the directory at PATH."
            " (Default: '%(default)s'). If PATH is an existing directory, it"
            " will be used for storing job information, and an index file"
            " (with suffix '.csv') will be created in it.  Otherwise, the job"
            " information will be stored in a directory named after PATH with"
            " a suffix '.jobs' appended, and the index file"
            " will be named after PATH with a suffix '.csv' added.")
        parser.add_param("-u", "--store-url",
                       action="store",
                       metavar="URL",
                       help="URL of the persistent store to use.")
        parser.add_param(
            "-N",
            "--new-session",
            dest="new_session",
            action="store_true",
            default=False,
            help="Discard any information saved in the session directory (see"
            " '--session' option) and start a new session afresh.  Any"
            " information about previous jobs is lost.")

        # 3. script execution control
        parser.add_param(
            "-C",
            "--continuous",
            "--watch",
            type=positive_int,
            dest="wait",
            default=0,
            metavar="NUM",
            help="Keep running, monitoring jobs and possibly submitting"
            " new ones or fetching results every NUM seconds. Exit when"
            " all jobs are finished.")
        parser.add_param("-J", "--max-running",
                       type=positive_int, dest="max_running", default=50,
                       metavar="NUM",
                       help="Set the max NUMber of jobs (default: %(default)s)"
                       " in SUBMITTED or RUNNING state."
                       )
        parser.add_param(
            "-o",
            "--output",
            dest="output",
            type=valid_directory,
            default=os.path.join(
                os.getcwd(),
                'NAME'),
            metavar='DIRECTORY',
            help="Output files from all jobs will be collected in the"
            " specified DIRECTORY path; by default, output files are placed"
            " in the same directory where the corresponding input file"
            " resides.  If the destination directory does not exist, it is"
            " created.  The following strings will be substituted into"
            " DIRECTORY, to specify an output location that varies with each"
            " submitted job: the string 'NAME' is replaced by the job name;"
            " 'DATE' is replaced by the submission date in ISO format"
            " (YYYY-MM-DD); 'TIME' is replaced by the submission time"
            " formatted as HH:MM.  'SESSION' is replaced by the path to the"
            " session directory, with a '.out' appended.")
        return

    def setup(self):
        """
        Setup standard command-line parsing.

        GC3Libs scripts should probably override `setup_args`:meth:
        to modify command-line parsing.
        """
        # setup of base classes
        _Script.setup(self)

        # add own "standard options"
        self.setup_common_options(self)

    def _prerun_common_checks(self):
        # consistency checks
        try:
            # FIXME: backwards-compatibility, remove after 2.0 release
            self.params.walltime = Duration(int(self.params.wctime), hours)
        except ValueError:
            # cannot convert to `int`, use extended parsing
            self.params.walltime = Duration(self.params.wctime)

        # determine the session file name (and possibly create an empty index)
        self.session_uri = gc3libs.url.Url(self.params.session)
        if self.params.store_url == 'sqlite':
            self.params.store_url = (
                "sqlite:///%s/jobs.db" % self.session_uri.path)
        elif self.params.store_url == 'file':
            self.params.store_url = ("file:///%s/jobs" % self.session_uri.path)
        self.session = self._make_session(
            self.session_uri.path, self.params.store_url)

        # keep a copy of the credentials in the session dir
        self.config.auth_factory.add_params(
            private_copy_directory=self.session.path)

        # XXX: ARClib errors out if the download directory already exists, so
        # we need to make sure that each job downloads results in a new one.
        # The easiest way to do so is to append 'NAME' to the `output_dir`
        # (if it's not already there).
        if (self.params.output and 'NAME' not in self.params.output
                and 'ITER' not in self.params.output):
            self.params.output = os.path.join(self.params.output, 'NAME')

    def pre_run(self):
        """
        Perform parsing of standard command-line options and call into
        `parse_args()` to do non-optional argument processing.
        """
        # call base classes first (note: calls `parse_args()`)
        _Script.pre_run(self)

        self._prerun_common_checks()
        # since it may time quite some time before jobs are created
        # and the first report is displayed, print a startup banner so
        # that users get some kind of feedback ...
        print("Starting %s;"
              " use the '-v' command-line option to get"
              " a more verbose report of activity."
              % (self.name,))


    ##
    # INTERNAL METHODS
    ##
    # The following methods are for internal use; they can be
    # overridden and customized in derived classes, although there
    # should be no need to do so.
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
        return Session(session_uri, create=True, store_or_url=store_url)

    def _main(self, *args):
        """
        Implementation of the main logic in the `SessionBasedScript`.

        This is a template method, that you should not override in derived
        classes: rather use the provided customization hooks:
        :meth:`process_args`, :meth:`parse_args`, :meth:`setup_args`.
        """

        # zero out the session index if `-N` was given
        if self.params.new_session:
            old_jobids = self.session.list_ids()
            if old_jobids:
                self.log.warning(
                    "Abort of existing session requested:"
                    " will attempt to kill existing jobs."
                    " This may generate a few spurious error messages"
                    " if the jobs are too old and have already been"
                    " cleaned up by the system.")
                for jobid in old_jobids:
                    job = self.session.load(jobid)
                    job.attach(self._core)
                    try:
                        job.kill()
                    except Exception as err:
                        self.log.info(
                            "Got this error while killing old job '%s',"
                            " ignore it: %s: %s",
                            job,
                            err.__class__.__name__,
                            str(err))
                    try:
                        job.free()
                    except Exception as err:
                        self.log.info(
                            "Got this error while cleaning up old job '%s',"
                            " ignore it: %s: %s",
                            job,
                            err.__class__.__name__,
                            str(err))
                    job.detach()
                    self.session.remove(jobid)
                    self.log.debug("Removed job '%s' from session.", job)
                self.log.info(
                    "Done cleaning up old session jobs, starting with new one"
                    " afresh...")

        # update session based on command-line args
        if len(self.session) == 0:
            self.process_args()
        else:
            self.log.warning(
                "Session already exists, some command-line arguments"
                " might be ignored."
            )

        # save the session list immediately, so newly added jobs will
        # be in it if the script is stopped here
        self.session.save_all()

        # obey the ``-r`` command-line option
        if self.params.resource_name:
            self._select_resources(self.params.resource_name)
            self.log.info(
                "Retained only resources: %s (restricted by command-line"
                " option '-r %s')",
                str.join(
                    ",",
                    [
                        r['name'] for r in self._core.get_resources()
                        if r.enabled]),
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
                    # Python scripts become unresponsive during
                    # `time.sleep()`, so we just do the wait in small
                    # steps, to allow the interpreter to process
                    # interrupts in the breaks.  Ugly, but works...
                    for x in xrange(self.params.wait):
                        time.sleep(1)
                    # ...and now repeat the submit/update/retrieve
                    rc = self._main_loop()
        except KeyboardInterrupt:  # gracefully intercept Ctrl+C
            sys.stderr.write(
                "%s: Exiting upon user request (Ctrl+C)\n" % self.name)
            self.cleanup()
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


class _CommDaemon(object):
    portfile_name = 'daemon.port'

    def __init__(self, name, listenip, workingdir, parent):
        self.parent = parent
        self.log = self.parent.log

        # Start XMLRPC server
        self.server = sxmlrpc.SimpleXMLRPCServer((listenip, 0),
                                                 logRequests=False)
        self.ip, self.port = self.server.socket.getsockname()
        self.log.info("XMLRPC daemon running on %s,"
                      "port %d.", self.ip, self.port)
        self.portfile = os.path.join(workingdir, self.portfile_name)
        ### FIXME: we should check if the file exists already
        with open(self.portfile, 'w') as fd:
            self.log.debug("Writing current port (%d) I'm listening to"
                           " in file %s" % (self.port, self.portfile))
            fd.write("%s:%d\n" % (self.ip, self.port))

        # Register XMLRPC methods
        self.server.register_introspection_functions()

        self.server.register_function(self.help, "help")
        self.server.register_function(self.list_jobs, "list")
        self.server.register_function(self.show_job, "show")
        self.server.register_function(self.stat_jobs, "stat")
        self.server.register_function(self.kill_job, "kill")
        self.server.register_function(self.resubmit_job, "resubmit")
        self.server.register_function(self.remove_job, "remove")
        self.server.register_function(self.terminate, "terminate")
        self.server.register_function(self.json_list, "json_list")
        self.server.register_function(self.json_show_job, "json_show")

    def start(self):
        return self.server.serve_forever()

    def stop(self):
        try:
            self.server.shutdown()
            self.server.socket.close()
            os.remove(self.portfile)
        except Exception as ex:
            # self.stop() could be called twice, let's assume it's not
            # an issue but log the event anyway
            self.log.warning(
                "Ignoring exception caught while closing socket: %s", ex)

    def terminate(self):
        """Terminate daemon"""
        # Start a new thread so that we can reply
        def killme():
            self.log.info("Terminating as requested via IPC")
            # Wait 1s so that the client connection is not hang up and
            # we have time to give feedback
            time.sleep(1)
            # daemon cleanup will aslo call self.stop()
            self.parent.cleanup()

            # Send kill signal to current process
            os.kill(os.getpid(), signal.SIGTERM)

            # This should be enough, but just in case...
            self.log.warning(
                "Waiting 5s to see if my sucide attempt succeeded")
            time.sleep(5)

            # If this is not working, try mor aggressive approach.

            # SIGINT is interpreted by SessionBasedDaemon class. It
            # will also call self.parent.cleanup(), again.
            self.log.warning("Still alive: sending SIGINT signal to myself")
            os.kill(os.getpid(), signal.SIGINT)

            # We whould never reach this point, but Murphy's law...
            time.sleep(5)
            # Revert back to SIGKILL. This will leave the pidfile
            # hanging around.
            self.log.warning("Still alive: forcing SIGKILL signal.")
            os.kill(os.getpid(), signal.SIGKILL)

        t = threading.Thread(target=killme)
        t.start()
        return "Terminating %d in 1s" % os.getpid()

    ### user visible methods

    def help(self, cmd=None):
        """Show available commands, or get information about a specific
        command"""

        if not cmd:
            return self.server.system_listMethods()
        else:
            return self.server.system_methodHelp(cmd)

    @staticmethod
    def print_app_table(app, indent, recursive):
        """Returns a list of lists containing a summary table of the jobs"""
        rows = []
        try:
            jobname = app.jobname
        except AttributeError:
            jobname = ''

        rows.append([indent + str(app.persistent_id),
                     jobname,
                     app.execution.state,
                     app.execution.returncode,
                     app.execution.info])
        if recursive and 'tasks' in app:
            indent = " "*len(indent) + '  '
            for task in app.tasks:
                rows.extend(_CommDaemon.print_app_table(task, indent, recursive))
        return rows

    def list_jobs(self, opts=None):
        """usage: list [detail]

        List jobs"""

        if opts and 'details'.startswith(opts):
            rows = []
            for app in self.parent.session.tasks.values():
                rows.extend(self.print_app_table(app, '', True))
            table = PrettyTable(["JobID", "Job name", "State", "rc", "Info"])
            table.align = 'l'
            for row in rows:
                table.add_row(row)
            return str(table)
        elif opts and 'all'.startswith(opts):
            return str.join(' ', [i.persistent_id for i in self.parent.session.iter_workflow()])
        else:
            return str.join(' ', self.parent.session.list_ids())

    def json_list(self):
        """usage: json_show jobid

        List jobs"""

        jobids = [i.persistent_id for i in self.parent.session.iter_workflow()]
        jobs = []
        for jobid in jobids:
            app = self.parent.session.store.load(jobid)
            sapp = StringIO()
            gc3libs.utils.prettyprint(app, output=sapp)
            jobs.append(yaml.load(sapp.getvalue()))
        return json.dumps(jobs)

    def json_show_job(self, jobid=None, *attrs):
        """usage: json_show <jobid>

        Same output as `ginfo -v <jobid>
        """

        if not jobid:
            return "Usage: show <jobid>"

        if jobid not in self.parent.session.tasks:
            all_tasks = dict((i.persistent_id,i) for i in self.parent.session.iter_workflow() if hasattr(i, 'persistent_id'))
            if jobid not in all_tasks:
                return "Job %s not found in session" % jobid
            else:
                app = all_tasks[jobid]
        else:
            app = self.parent.session.tasks[jobid]
        sapp = StringIO()
        gc3libs.utils.prettyprint(app, output=sapp)
        return json.dumps(yaml.load(sapp.getvalue()))

    def show_job(self, jobid=None, *attrs):
        """usage: show <jobid> [attributes]

        Same output as `ginfo -v <jobid> [-p attributes]`
        """

        if not jobid:
            return "Usage: show <jobid> [attributes]"

        if jobid not in self.parent.session.tasks:
            all_tasks = dict((i.persistent_id,i) for i in self.parent.session.iter_workflow() if hasattr(i, 'persistent_id'))
            if jobid not in all_tasks:
                return "Job %s not found in session" % jobid
            else:
                app = all_tasks[jobid]
        else:
            app = self.parent.session.tasks[jobid]
        try:
            out = StringIO()
            if not attrs:
                attrs = None
            gc3libs.utils.prettyprint(app,
                                      indent=4,
                                      output=out,
                                      only_keys=attrs)
            return out.getvalue()
        except Exception as ex:
            return "Unable to find job %s" % jobid

    def kill_job(self, jobid=None):
        if not jobid:
            return "Usage: kill <jobid>"

        if jobid not in self.parent.session.tasks:
            all_tasks = dict((i.persistent_id,i) for i in self.parent.session.iter_workflow() if hasattr(i, 'persistent_id'))
            if jobid not in all_tasks:
                return "Job %s not found in session" % jobid
            else:
                app = all_tasks[jobid]
        else:
            app = self.parent.session.tasks[jobid]
        try:
            app.attach(self.parent._controller)
            app.kill()
            return "Job %s successfully killed" % jobid
        except Exception as ex:
            return "Error while killing job %s: %s" % (jobid, ex)

    def remove_job(self, jobid=None):
        if not jobid:
            return "Usage: remove <jobid>"

        if jobid not in self.parent.session.tasks:
            return "Job %s not found in session" % jobid
        app = self.parent.session.load(jobid)
        if app.execution.state != gc3libs.Run.State.TERMINATED:
            return "Error: you can only remove a terminated job. Current status is: %s" % app.execution.state
        try:
            self.parent._controller.remove(app)
            self.parent.session.remove(jobid)
            return "Job %s successfully removed" % jobid
        except Exception as ex:
            return "Error while removing job %s: %s" % (jobid, ex)

    def resubmit_job(self, jobid=None):
        if not jobid:
            return "Usage: resubmit <jobid>"
        if jobid not in self.parent.session.tasks:
            all_tasks = dict((i.persistent_id,i) for i in self.parent.session.iter_workflow() if hasattr(i, 'persistent_id'))
            if jobid not in all_tasks:
                return "Job %s not found in session" % jobid
            else:
                app = all_tasks[jobid]
        else:
            app = self.parent.session.tasks[jobid]

        try:
            self.parent._controller.redo(app)
        except Exception as ex:
            return "Error while resubmitting job %s: %s" % (jobid, ex)
        return "Successfully resubmitted job %s" % jobid

    def stat_jobs(self):
        """Print how many jobs are in any given state"""

        stats = self.parent._controller.stats()
        return str.join('\n', ["%s:%s" % x for x in stats.items()])


class SessionBasedDaemon(_SessionBasedCommand):
    """Base class for GC3Pie daemons. Implements a long-running script
    that can daemonize, provides an XML-RPC interface to interact with
    the current workflow and implement the concept of "inbox" to
    trigger the creation of new jobs as soon as a new file is created
    on a folder, or is available on an HTTP(s) or SWIFT endpoint.

    The generic script impelemnts a command line like the following::

      PROG [global options] server|client [specific options]

    When running as a `server` it will accepts the same options as a
    :py:class:`SessionBasedScript()` class, plus some extra options:

      PROG server [server options] [INBOX [INBOX]]

    Available options:

    `-F, --foreground`
      do not daemonize.

    `--syslog`
      send logs to syslog instead of writing it to a file

    `--working-dir`
      working directory of the daemon, where the session
      will be stored and some auxiliary files will be saved

    `--notify-state [EVENT,[EVENT]]`
      a comma separated list of events
      on the inbox we want to be notified of

    `--listen IP`
      IP or hostname we want to listen to. Default is localhost.

    `INBOX`
      path or url of one or more folder/url that will be watched for
      new events.

    When running as client instead::

      PROG client [-c FILE] ARGS

    `-c`
      file written by the server conatining the hostname and port the
      server is listening to. Also accepts a path to the directory
      (the value of `--working-dir` the server was started with) where
      the `daemon.port` file created by the server is stored.

    """

    def cleanup(self, signume=None, frame=None):
        self.log.debug("Waiting for communication thread to terminate")
        try:
            self.comm.stop()
            self.commthread._Thread__stop()
            self.commthread._Thread__delete()
            self.commthread.join(1)
        except AttributeError:
            # If the script is interrupted/killed during command line
            # parsing the `self.comm` daemon is not present and we get
            # an AttributeError we can safely ignore.
            pass

    def setup(self):
        _Script.setup(self)
        self.subparsers = self.argparser.add_subparsers(title="commands")
        self.parser_server = self.subparsers.add_parser('server', help="Run the main daemon script.")
        self.parser_client = self.subparsers.add_parser('client', help="Connect to a running daemon.")

        # Wrapper function around add_argument.
        def _add_param_server(*args, **kwargs):
            action = self.parser_server.add_argument(*args, **kwargs)
            self.actions[action.dest] = action
            return action
        self.parser_server.add_param = _add_param_server
        # This method assumes `self.add_param` exists
        self.setup_common_options(self.parser_server)

        # Also, when subclassing, add_param should add arguments to
        # the server parser by default.
        self.add_param = lambda *x, **kw: self.parser_server.add_param(*x, **kw)

        self.parser_server.set_defaults(func=self._main_server)
        self.parser_client.set_defaults(func=self._main_client)

        # change default for the `-C`, `--session` and `--output` options
        self.actions['wait'].default = 30
        self.actions['wait'].help = 'Check the status of the jobs every NUM'
        ' seconds. Default: %(default)s'

        # Default session dir and output dir are computed from
        # --working-directory.  Set None here so that we can update it
        # in pre_run()
        self.actions['session'].default = None
        self.actions['output'].default = None

    def setup_args(self):
        self.parser_server.add_param('-F', '--foreground',
                       action='store_true',
                       default=False,
                       help="Run in foreground. Default: %(default)s")

        self.parser_server.add_param('--syslog',
                       action='store_true',
                       default=False,
                       help="Send log messages (also) to syslog."
                       " Default: %(default)s")

        self.parser_server.add_param('--working-dir',
                       default=os.getcwd(),
                       help="Run in WORKING_DIR. Ignored if run in foreground."
                       " Default: %(default)s")

        self.parser_server.add_param('--notify-state',
                       nargs='?',
                       default='CLOSE_WRITE',
                       help="Comma separated list of notify events to watch."
                       " Default: %(default)s. Available events: " +
                       str.join(', ', [i[3:] for i in notify_events.keys()]))

        self.parser_server.add_param(
            '--listen',
            default="localhost",
            metavar="IP",
            help="IP or hostname where the XML-RPC thread should"
            " listen to. Default: %(default)s")

        self.parser_server.add_param('inbox', nargs='*',
                       help="`inbox` directories: whenever a new file is"
                       " created in one of these directories, a callback is"
                       " triggered to add new jobs")

        self.parser_client.add_argument('-c', '--connect',
                                        metavar="FILE",
                                        required=True,
                                        help="Path to the file containing hostname and port of the"
                                        " XML-RPC enpdoint of the daemon")
        self.parser_client.add_argument('cmd',
                                        metavar='COMMAND',
                                        help="XML-RPC command. Run `help` to know"
                                        " which commands are available.")
        self.parser_client.add_argument('args',
                                        nargs='*',
                                        metavar='ARGS',
                                        help="Optional arguments of CMD.")

    def pre_run(self):
        ### FIXME: Some code copied from _Script.pre_run()

        self.setup_options()
        self.setup_args()
        cli.app.CommandLineApp.pre_run(self)
        loglevel = max(1, logging.WARNING -
                       10 *
                       max(0, self.params.verbose -
                           self.verbose_logging_threshold))
        gc3libs.configure_logger(loglevel, "gc3.gc3utils")  # alternate: self.name
        logging.root.setLevel(loglevel)
        # alternate: ('gc3.' + self.name)
        self.log = logging.getLogger('gc3.gc3utils')
        self.log.setLevel(loglevel)
        self.log.propagate = True
        self.log.info("Starting %s at %s; invoked as '%s'",
                      self.name, time.asctime(), str.join(' ', sys.argv))

        # FIXME: we need to ignore the process_args method as the
        # `client` subparser has different options than the `server`
        # one, and the default is the `server` subparser.
        if self.params.func == self._main_client:
            # override `process_args` with a noop.
            self.process_args = lambda *x, **kw: None
            return

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
                % (str.join("', '", self.params.config_files)))

        self.params.working_dir = os.path.abspath(self.params.working_dir)

        # Default session dir is inside the working directory
        if not self.params.session:
            self.params.session = os.path.join(
                self.params.working_dir, self.name)

        # Convert inbox to Url objects
        self.params.inbox = [gc3libs.url.Url(i) for i in self.params.inbox]
        # Default output directory is the working directory.
        if not self.params.output:
            self.params.output = self.params.working_dir

        self._prerun_common_checks()
        self.parse_args()

        # Syntax check for notify events
        self.params.notify_state = self.params.notify_state.split(',')

        # Add IN_ as we use shorter names for the command line
        state_names = ['IN_' + i for i in self.params.notify_state]

        # Build the notify mask, for later use
        self.notify_event_mask = 0

        # Ensure all the supplied states are correct
        for istate in state_names:
            if istate not in notify_events:
                raise gc3libs.exceptions.InvalidUsage(
                    "Invalid notify state %s." % state)
            self.notify_event_mask |= notify_events[istate]

    def __setup_logging(self):
        # FIXME: Apparently, when running in foreground _and_ --syslog
        # is used, only a few logs are sent to syslog and then nothing
        # more.

        # We use a function to avoid repetitions: since when
        # demonizing all open file descriptors are called, we need to
        # configure logging from *within* the DaemonContext context

        # If --syslog, add a logging handler to send to local syslog
        if self.params.syslog:
            # Update the root logger, not just 'gc3utils'
            logging.root.addHandler(
                SysLogHandler(address="/dev/log",
                              facility=SysLogHandler.LOG_USER))
        elif not self.params.foreground:
            # The default behavior when run in daemon mode
            # is to log to a file in working directory called
            # <application>.log
            #
            # Also, update the root logger, not just 'gc3utils'
            logging.root.addHandler(
                logging.FileHandler(
                    os.path.join(self.params.working_dir,
                                 self.name + '.log')))
        # else: log to stdout, which is the default.

    def __setup_pollers(self):
        # Setup inotify on inbox directories
        self.pollers = []
        # We need to create an inotify file descriptor for each
        # directory, because events returned by
        # `inotifyx.get_events()` do not contains the full path to the
        # file.
        for inbox in self.params.inbox:
            self.pollers.append(get_poller(inbox, self.notify_event_mask, recurse=True))

    def __setup_comm(self, listen):
        # Communication thread must run on a different thread
        try:
            self.comm = _CommDaemon(
                self.name,
                self.params.listen,
                self.params.working_dir,
                self)
            def commthread():
                self.log.info("Starting XML-RPC server")
                self.comm.start()

            self.commthread = threading.Thread(target=commthread)
            self.commthread.start()
            self.log.info("Communication thread started")
        except Exception as ex:
            self.log.error(
                "Error initializinig Communication thread: %s" % ex)

    def _main_client(self):
        portfile = self.params.connect
        if os.path.isdir(portfile):
            gc3libs.log.debug("First argument of --client is a directory. Checking if file `%s` is present in it" % _CommDaemon.portfile_name)
            portfile = os.path.join(portfile, _CommDaemon.portfile_name)
            if not os.path.isfile(portfile):
                self.argparser.error("First argument of --client must be a file.")
            gc3libs.log.info("Using file `%s` as argument of --connect option" % portfile)
        with open(portfile, 'r') as fd:
            try:
                ip, port = fd.read().split(':')
                port = int(port)
            except Exception as ex:
                print("Error parsing file %s: %s" % (portfile, ex))
        server = xmlrpclib.ServerProxy('http://%s:%d' % (ip, port))
        func = getattr(server, self.params.cmd)
        try:
            print(func(*self.params.args))
        except xmlrpclib.Fault as ex:
            print("Error while running command `%s`: %s" % (self.params.cmd, ex.faultString))
            print("Use `help` command to list all available methods")

    def _main(self):
        return self.params.func()

    def _main_server(self):
        self.process_args()

        if self.params.foreground:
            # If --foreground, then behave like a SessionBasedScript with
            # the exception that the script will never end.
            self.__setup_logging()
            self.__setup_pollers()
            self.log.info("Running in foreground as requested")

            self.__setup_comm(self.params.listen)
            return super(SessionBasedDaemon, self)._main()
        else:
            lockfile = os.path.join(self.params.working_dir,
                                    self.name + '.pid')
            lock = PIDLockFile(lockfile)
            if lock.is_locked():
                raise gc3libs.exceptions.FatalError(
                    "PID File %s is already present. Ensure not other daemon"
                    " is running. Delete file to continue." % lockfile)
            context = daemon.DaemonContext(
                working_directory=self.params.working_dir,
                umask=0o002,
                pidfile=lock,
                stdout=open(
                    os.path.join(self.params.working_dir, 'stdout.txt'),
                    'w'),
                stderr=open(
                    os.path.join(self.params.working_dir, 'stderr.txt'),
                    'w'),
            )

            context.signal_map = {signal.SIGTERM: self.cleanup}
            self.log.info("About to daemonize")
            with context:
                self.__setup_logging()
                self.__setup_pollers()
                self.log.info("Daemonizing ...")
                self.__setup_comm(self.params.listen)
                return super(SessionBasedDaemon, self)._main()

    def _main_loop_done(self, rc):
        # Run until interrupted
        return False

    def _main_loop(self):
        """
        The main loop of the application.  It is in a separate
        function so that we can call it just once or properly loop
        around it, as directed by the `self.params.wait` option.

        .. note::

          Overriding this method can disrupt the whole functionality of
          the script, so be careful.

        Invocation of this method should return a numeric exitcode,
        that will be used as the scripts' exitcode.  See
        `_main_loop_exitcode` for an explanation.
        """
        # hook method: this is the method used to add new applications
        # to the session.
        self.every_main_loop()

        # Check if new files were created. 1s timeout
        for poller in self.pollers:
            events = poller.get_events()
            for url, mask in events:
                self.log.debug("Received notify event %s for %s",
                               get_mask_description(mask), url)

                new_jobs = self.new_tasks(self.extra.copy(),
                                          epath=url,
                                          emask=mask)
                self._add_new_tasks(list(new_jobs))
                for task in list(new_jobs):
                    self._controller.add(task)

        # advance all jobs
        self._controller.progress()

        # summary
        stats = self._controller.stats()
        # compute exitcode based on the running status of jobs
        self.session.save_all()
        return self._main_loop_exitcode(stats)

    def _main_loop_exitcode(self, stats):
        # FIXME: Do these exit statuses make sense?
        """
        Compute the exit code for the `_main` function.
        (And, hence, for the whole `SessionBasedScript`.)

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

    def new_tasks(self, extra, epath=None, emask=0):
        """
        This method is called every time the daemon is started with
        `epath=None` and `emask=0`. Similarly to
        :py:meth:`SessionBasedScript.new_tasks()` method it is
        supposed to return a list of :py:class:`Task()` instances to
        be added to the session.

        It is also called for each event in INBOX (for instance every
        time a file is created), depending on the value of
        `--notify-state` option. In this case the method will be
        called for each file with the file path and the mask of the
        event as arguments.

        :param extra: by default: `self.extra`

        :param epath: an instance of :py:class:`gc3libs.url.Url`
                      containing the path to the updated file or
                      directory :param

        :param emask: mask explaining the event type. This reflects
                      the inotify events, and are defined in
                      :py:const:`gc3libs.poller.events`

        """
        return []


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
                                gc3libs.utils.basename_sans(path),
                                seqno,
                                min(seqno + self.instances_per_job - 1,
                                    self.instances_per_file)),
                            self.application, [path], extra.copy())
                    else:
                        yield ("%s.%d" % (gc3libs.utils.basename_sans(path),
                                          seqno),
                               self.application, [path], extra.copy())
            else:
                yield (gc3libs.utils.basename_sans(path),
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
        tasks in that state; see `Engine.stats` for a detailed
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
        _SessionBasedCommand.setup(self)
        # add own "standard options"
        self.add_param(
            "-l",
            "--state",
            action="store",
            nargs='?',
            dest="states",
            default='',
            const=str.join(
                ',',
                gc3libs.Run.State),
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

    def _main_loop(self):
        """
        The main loop of the application.  It is in a separate
        function so that we can call it just once or properly loop
        around it, as directed by the `self.params.wait` option.

        .. note::

          Overriding this method can disrupt the whole functionality of
          the script, so be careful.

        Invocation of this method should return a numeric exitcode,
        that will be used as the scripts' exitcode.  See
        `_main_loop_exitcode` for an explanation.
        """
        # advance all jobs
        self._controller.progress()
        # hook method
        self.every_main_loop()
        # print results to user
        print ("Status of jobs in the '%s' session: (at %s)"
               % (self.session.name, time.strftime('%X, %x')))
        # summary
        stats = self._controller.stats()
        total = stats['total']
        if total > 0:
            if self.stats_only_for is not None:
                self.print_summary_table(sys.stdout,
                                         self._controller.stats(
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
        # compute exitcode based on the running status of jobs
        return self._main_loop_exitcode(stats)

    def _main_loop_exitcode(self, stats):
        # FIXME: Do these exit statuses make sense?
        """
        Compute the exit code for the `_main` function.
        (And, hence, for the whole `SessionBasedScript`.)

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

    def _main_loop_done(self, rc):
        """
        Returns True if the main loop is completed and we don't want to
        continue the processing. Returns False otherwise.
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
