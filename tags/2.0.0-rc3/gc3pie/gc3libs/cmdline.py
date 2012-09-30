#! /usr/bin/env python
#
#   cmdline.py -- Prototypes for GC3Libs-based scripts
#
#   Copyright (C) 2010, 2011, 2012 GC3, University of Zurich
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
"""
Prototype classes for GC3Libs-based scripts.

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

"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


## stdlib modules
import csv
import fnmatch
import lockfile
import logging
import math
import os
import os.path
import re
import sys
from texttable import Texttable
import time

## 3rd party modules
import cli  # pyCLI
import cli.app
import cli._ext.argparse as argparse

## interface to Gc3libs
import gc3libs
import gc3libs.config
import gc3libs.core
import gc3libs.exceptions
import gc3libs.persistence
import gc3libs.utils
import gc3libs.url
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.session import Session


## types for command-line parsing; see
## http://docs.python.org/dev/library/argparse.html#type

def nonnegative_int(num):
    """This function raise an ArgumentTypeError if `num` is a negative
    integer (<0), and returns int(num) otherwise. `num` can be any
    object which can be converted to an int.

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
    ArgumentTypeError: 'ThisWillRaiseAnException' is not a non-negative integer number.

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
    """This function raises an ArgumentTypeError if `num` is not
    a*strictly* positive integer (>0) and returns int(num)
    otherwise. `num` can be any object which can be converted to an
    int.

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
    ArgumentTypeError: 'ThisWillRaiseAnException' is not a positive integer number.

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


## script classes

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
    ## CUSTOMIZATION METHODS
    ##
    ## The following are meant to be freely customized in derived scripts.
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

    ##
    ## pyCLI INTERFACE METHODS
    ##
    ## The following methods adapt the behavior of the
    ## `SessionBasedScript` class to the interface expected by pyCLI
    ## applications.  Think twice before overriding them, and read
    ## the pyCLI docs before :-)
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
                raise AssertionError("Missing required parameter 'description'.")

        # allow overriding command-line options in subclasses
        def argparser_factory(*args, **kwargs):
            kwargs.setdefault('conflict_handler', 'resolve')
            kwargs.setdefault('formatter_class',
                              cli._ext.argparse.RawDescriptionHelpFormatter)
            return cli.app.CommandLineApp.argparser_factory(*args, **kwargs)

        self.argparser_factory = argparser_factory
        # init superclass
        extra_args.setdefault('name',
                      os.path.splitext(os.path.basename(sys.argv[0]))[0])
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
        ## setup of base classes
        cli.app.CommandLineApp.setup(self)

        self.add_param("-v", "--verbose",
                       action="count",
                       dest="verbose",
                       default=0,
                       help="Print more detailed information about the program's activity."
                       " Increase verbosity each time this option is encountered on the"
                       " command line."
                       )

        self.add_param("--config-files",
                       action="store",
                       default=str.join(',', gc3libs.Default.CONFIG_FILE_LOCATIONS),
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
        ## finish setup
        self.setup_options()
        self.setup_args()

        ## parse command-line
        cli.app.CommandLineApp.pre_run(self)

        ## setup GC3Libs logging
        loglevel = max(1, logging.WARNING - 10 * max(0, self.params.verbose - self.verbose_logging_threshold))
        gc3libs.configure_logger(loglevel, "gc3utils") # alternate: self.name
        self.log = logging.getLogger('gc3.gc3utils')  # alternate: ('gc3.' + self.name)
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
        except gc3libs.exceptions.InvalidUsage, ex:
            # Fatal errors do their own printing, we only add a short usage message
            sys.stderr.write("Type '%s --help' to get usage help.\n" % self.name)
            return 64  # EX_USAGE in /usr/include/sysexits.h
        except KeyboardInterrupt:
            sys.stderr.write("%s: Exiting upon user request (Ctrl+C)\n" % self.name)
            return 13
        except SystemExit, ex:
            return ex.code
        # the following exception handlers put their error message
        # into `msg` and the exit code into `rc`; the closing stanza
        # tries to log the message and only outputs it to stderr if
        # this fails
        except lockfile.Error, ex:
            exc_info = sys.exc_info()
            msg = ("Error manipulating the lock file (%s: %s)."
                   " This likely points to a filesystem error"
                   " or a stale process holding the lock."
                   " If you cannot get this command to run after"
                   " a system reboot, please write to gc3pie@googlegroups.com"
                   " including any output you got by running '%s -vvvv %s'.")
            if len(sys.argv) > 0:
                msg %= (ex.__class__.__name__, str(ex),
                        self.name, str.join(' ', sys.argv[1:]))
            else:
                msg %= (ex.__class__.__name__, str(ex), self.name, '')
            rc = 1
        except AssertionError, ex:
            exc_info = sys.exc_info()
            msg = ("BUG: %s\n"
                   "Please send an email to gc3pie@googlegroups.com"
                   " including any output you got by running '%s -vvvv %s'."
                   " Thanks for your cooperation!")
            if len(sys.argv) > 0:
                msg %= (str(ex), self.name, str.join(' ', sys.argv[1:]))
            else:
                msg %= (str(ex), self.name, '')
            rc = 1
        except Exception, ex:
            msg = "%s: %s" % (ex.__class__.__name__, str(ex))
            if isinstance(ex, cli.app.Abort):
                rc = (ex.status)
            elif isinstance(ex, EnvironmentError):
                rc = 74  # EX_IOERR in /usr/include/sysexits.h
            else:
                # generic error exit
                rc = 1
        # output error message and -maybe- backtrace...
        try:
            self.log.critical(msg,
                              exc_info=(self.params.verbose > self.verbose_logging_threshold + 2))
        except:
            # no logging setup, output to stderr
            sys.stderr.write("%s: FATAL ERROR: %s\n" % (self.name, msg))
            if self.params.verbose > self.verbose_logging_threshold + 2:
                sys.excepthook(* sys.exc_info())
        # ...and exit
        return 1

    ##
    ## INTERNAL METHODS
    ##
    ## The following methods are for internal use; they can be
    ## overridden and customized in derived classes, although there
    ## should be no need to do so.
    ##

    def _make_config(self,
                     config_file_locations=gc3libs.Default.CONFIG_FILE_LOCATIONS,
                     **extra_args):
        """
        Return a `gc3libs.config.Configuration`:class: instance configured by parsing
        the configuration file(s) located at `config_file_locations`.
        Order of configuration files matters: files read last
        overwrite settings from previously-read ones; list the most
        specific configuration files last.

        Any additional keyword arguments are passed unchanged to the
        `gc3libs.config.Configuration`:class: constructor.  In
        particular, the `auto_enable_auth` parameter for the
        `Configuration` constructor is `True` if not set differently
        here as a keyword argument.
        """
        # ensure a configuration file exists in the most specific location
        for location in reversed(config_file_locations):
            if os.access(os.path.dirname(location), os.W_OK | os.X_OK) \
                    and not gc3libs.utils.deploy_configuration_file(location, "gc3pie.conf.example"):
                # warn user
                self.log.warning(
                    "No configuration file '%s' was found;"
                    " a sample one has been copied in that location;"
                    " please edit it and define resources." % location)
        # set defaults
        extra_args.setdefault('auto_enable_auth', True)
        try:
            return gc3libs.config.Configuration(*config_file_locations, **extra_args)
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
    ## CUSTOMIZATION METHODS
    ##
    ## The following are meant to be freely customized in derived scripts.
    ##

    def setup_args(self):
        """
        Set up command-line argument parsing.

        The default command line parsing considers every argument as a
        job ID; actual processing of the IDs is done in
        `parse_args`:meth:
        """
        self.add_param('args',
                       nargs='*',
                       metavar='JOBID',
                       help="Job ID string identifying the jobs to operate upon.")

    def parse_args(self):
        if hasattr(self.params, 'args') and '-' in self.params.args:
            # Get input arguments *also* from standard input
            self.params.args.remove('-')
            self.params.args.extend(sys.stdin.read().split())

    ##
    ## pyCLI INTERFACE METHODS
    ##
    ## The following methods adapt the behavior of the
    ## `SessionBasedScript` class to the interface expected by pyCLI
    ## applications.  Think twice before overriding them, and read
    ## the pyCLI docs before :-)
    ##

    def __init__(self, **extra_args):
        _Script.__init__(self, main=self.main, **extra_args)

    def setup(self):
        """
        Setup standard command-line parsing.

        GC3Utils scripts should probably override `setup_args`:meth:
        and `setup_options`:meth: to modify command-line parsing.
        """
        ## setup of base classes (creates the argparse stuff)
        _Script.setup(self)
        ## local additions
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
        ## base class parses command-line
        _Script.pre_run(self)

    ##
    ## INTERNAL METHODS
    ##
    ## The following methods are for internal use; they can be
    ## overridden and customized in derived classes, although there
    ## should be no need to do so.
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
            except Exception, ex:
                if ignore_failures:
                    gc3libs.log.error("Could not retrieve job '%s' (%s: %s). Ignoring.",
                                      jobid, ex.__class__.__name__, str(ex),
                                      exc_info=(self.params.verbose > 2))
                    continue
                else:
                    raise


class SessionBasedScript(_Script):
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
    ## CUSTOMIZATION METHODS
    ##
    ## The following are meant to be freely customized in derived scripts.
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
        ## default creation arguments
        self.extra.setdefault('requested_cores', self.params.ncores)
        self.extra.setdefault('requested_memory',
                            self.params.ncores * self.params.memory_per_core)
        self.extra.setdefault('requested_walltime', self.params.walltime)
        # XXX: assumes `make_directory_path` substitutes ``NAME`` with `jobname`; keep in sync!
        self.extra.setdefault('output_dir',
                              self.make_directory_path(self.params.output, 'NAME'))

        ## build job list
        new_jobs = list(self.new_tasks(self.extra.copy()))
        # pre-allocate Job IDs
        if len(new_jobs) > 0:
            # XXX: can't we just make `reserve` part of the `IdFactory` contract?
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
                    self.log.warning("Using old-style new tasks list; please update the code!")
                    warning_on_old_style_given = True
                # build Task for (jobname, classname, args, kwargs)
                jobname, cls, args, kwargs = item
                if jobname in existing_job_names:
                    continue
                kwargs.setdefault('jobname', jobname)
                kwargs.setdefault('output_dir',
                                  self.make_directory_path(self.params.output, jobname))
                kwargs.setdefault('requested_cores',    self.extra['requested_cores'])
                kwargs.setdefault('requested_memory',   self.extra['requested_memory'])
                kwargs.setdefault('requested_walltime', self.extra['requested_walltime'])
                # create a new `Task` object
                try:
                    task = cls(*args, **kwargs)
                except Exception, ex:
                    self.log.error("Could not create job '%s': %s."
                                   % (jobname, str(ex)), exc_info=__debug__)
                    continue
                    # XXX: should we raise an exception here?
                    #raise AssertionError("Could not create job '%s': %s: %s"
                    #                     % (jobname, ex.__class__.__name__, str(ex)))

            elif isinstance(item, gc3libs.Task):
                task = item
                if 'jobname' not in task:
                    task.jobname = ("%s-N%d" % (task.__class__.__name__, n+1))

            else:
                raise InternalError(
                    "SessionBasedScript.process_args got %r (%s),"
                    " but was expecting a gc3libs.Task instance" % (item, type(item)))

            # patch output_dir if it's not changed from the default,
            # or if it's not defined (e.g., TaskCollection)
            if 'output_dir' not in task or task.output_dir == self.extra['output_dir']:
                # user did not change the `output_dir` default, expand it now
                self._fix_output_dir(task, task.jobname)

            # all done, append to session
            self.session.add(task, flush=False)
            self.log.debug("Added task '%s' to session." % task.jobname)

    def _fix_output_dir(self, task, name):
        """Substitute the NAME string in output paths."""
        task.output_dir = task.output_dir.replace('NAME', name)
        try:
            for subtask in task.tasks:
                self._fix_output_dir(subtask, name)
        except AttributeError:
            # no subtasks
            pass


    def new_tasks(self, extra):
        """
        Iterate over jobs that should be added to the current session.
        Each item yielded must have the form `(jobname, cls, args,
        kwargs)`, where:

        * `jobname` is a string uniquely identifying the job in the
          session; if a job with the same name already exists, this
          item will be ignored.

        * `cls` is a callable that returns an instance of
          `gc3libs.Application` when called as `cls(*args, **kwargs)`.

        * `args` is a tuple of arguments for calling `cls`.

        * `kwargs` is a dictionary used to provide keyword arguments
          when calling `cls`.

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
                        yield ("%s.%d--%s" % (gc3libs.utils.basename_sans(path),
                                              seqno,
                                              min(seqno + self.instances_per_job - 1,
                                                  self.instances_per_file)),
                               self.application, [path], extra.copy())
                    else:
                        yield ("%s.%d" % (gc3libs.utils.basename_sans(path), seqno),
                               self.application, [path], extra.copy())
            else:
                yield (gc3libs.utils.basename_sans(path),
                       self.application, [path], extra.copy())

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
        return gc3libs.core.Engine(self._core, self.session, self.session.store,
                                   max_submitted=self.params.max_running,
                                   max_in_flight=self.params.max_running)

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
        table = Texttable(0)  # max_width=0 => dynamically resize cells
        table.set_deco(0)     # no decorations
        table.set_cols_align(['r', 'r', 'c'])
        total = stats['total']
        # ensure we display enough decimal digits in percentages when
        # running a large number of jobs; see Issue 308 for a more
        # detailed descrition of the problem
        precision = max(1, math.log10(total) - 1)
        fmt = '(%%.%df%%%%)' % precision
        for state in sorted(stats.keys()):
            table.add_row([
                    state,
                    "%d/%d" % (stats[state], total),
                    fmt % (100.00 * stats[state] / total)
                    ])
        output.write(table.draw())
        output.write("\n")

    def print_tasks_table(self, output=sys.stdout, states=gc3libs.Run.State, only=object):
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
        :param   only: Root class (or tuple of root classes) of tasks to consider.
        """
        table = Texttable(0)  # max_width=0 => dynamically resize cells
        table.set_deco(Texttable.HEADER)  # also: .VLINES, .HLINES .BORDER
        table.header(['JobID', 'Job name', 'State', 'Info'])
        #table.set_cols_width([10, 20, 10, 35])
        table.set_cols_align(['l', 'l', 'l', 'l'])
        table.add_rows([
            (task.persistent_id, task.jobname,
             task.execution.state, task.execution.info)
            for task in self.session
            if isinstance(task, only) and task.execution.in_state(*states)],
                       header=False)
        # XXX: uses texttable's internal implementation detail
        if len(table._rows) > 0:
            output.write(table.draw())
            output.write("\n")

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

        This is called from within the main loop, after progressing
        all tasks.

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

    ##
    ## pyCLI INTERFACE METHODS
    ##
    ## The following methods adapt the behavior of the
    ## `SessionBasedScript` class to the interface expected by pyCLI
    ## applications.  Think twice before overriding them, and read
    ## the pyCLI docs before :-)
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
        self.stats_only_for = None  # by default, print stats of all kind of jobs
        self.instances_per_file = 1
        self.instances_per_job = 1
        self.extra = {}  # extra extra_args arguments passed to `parse_args`
        # use bogus values that should point ppl to the right place
        self.input_filename_pattern = 'PLEASE SET `input_filename_pattern` IN `SessionBasedScript` CONSTRUCTOR'
        # catch omission of mandatory `application` ctor param (see above)
        self.application = SessionBasedScript.__unset_application_cls
        ## init base classes
        _Script.__init__(
            self,
            main=self._main,
            **extra_args
            )

    def setup(self):
        """
        Setup standard command-line parsing.

        GC3Libs scripts should probably override `setup_args`:meth:
        to modify command-line parsing.
        """
        ## setup of base classes
        _Script.setup(self)

        ## add own "standard options"

        # 1. job requirements
        self.add_param("-c", "--cpu-cores", dest="ncores",
                       type=positive_int, default=1,  # 1 core
                       metavar="NUM",
                       help="Set the number of CPU cores required for each job (default: %(default)s)."
                       " NUM must be a whole number."
                       )
        self.add_param("-m", "--memory-per-core", dest="memory_per_core",
                       type=Memory, default=2*GB,  # 2 GB
                       metavar="GIGABYTES",
                       help="Set the amount of memory required per execution core; default: %(default)s."
                       " Specify this as an integral number followed by a unit, e.g.,"
                       " '512MB' or '4GB'.")
        self.add_param("-r", "--resource", action="store", dest="resource_name", metavar="NAME",
                       default=None,
                       help="Submit jobs to a specific computational resources."
                       " NAME is a resource name or comma-separated list of such names."
                       " Use the command `gservers` to list available resources.")
        self.add_param("-w", "--wall-clock-time", dest="wctime", default='8 hours',
                       metavar="DURATION",
                       help="Set the time limit for each job; default is %(default)s."
                       " Jobs exceeding this limit will be stopped and considered as 'failed'."
                       " The duration can be expressed as a whole number followed by a time unit,"
                       " e.g., '3600 s', '60 minutes', '8 hours', or a combination thereof,"
                       " e.g., '2hours 30minutes'."
                       )

        # 2. session control
        self.add_param("-s", "--session", dest="session",
                       default=os.path.join(os.getcwd(), self.name),
                       metavar="PATH",
                       help="Store the session information in the directory at PATH. (Default: '%(default)s')."
                       " If PATH is an existing directory, it will be used for storing job"
                       " information, and an index file (with suffix '.csv') will be created"
                       " in it.  Otherwise, the job information will be stored in a directory"
                       " named after PATH with a suffix '.jobs' appended, and the index file"
                       " will be named after PATH with a suffix '.csv' added."
                       )
        self.add_param("-u", "--store-url",
                       action="store",
                       metavar="URL",
                       help="URL of the persistent store to use.")
        self.add_param("-N", "--new-session", dest="new_session", action="store_true", default=False,
                       help="Discard any information saved in the session directory (see '--session' option)"
                       " and start a new session afresh.  Any information about previous jobs is lost.")

        # 3. script execution control
        self.add_param("-C", "--continuous",
                       type=positive_int, dest="wait", default=0,
                       metavar="NUM",
                       help="Keep running, monitoring jobs and possibly submitting new ones or"
                       " fetching results every NUM seconds. Exit when all jobs are finished."
                       )
        self.add_param("-J", "--max-running",
                       type=positive_int, dest="max_running", default=50,
                       metavar="NUM",
                       help="Set the max NUMber of jobs (default: %(default)s)"
                       " in SUBMITTED or RUNNING state."
                       )
        self.add_param("-o", "--output", dest="output",
                       type=valid_directory, default=os.path.join(os.getcwd(), 'NAME'),
                       metavar='DIRECTORY',
                       help="Output files from all jobs will be collected in the specified"
                       " DIRECTORY path; by default, output files are placed in the same"
                       " directory where the corresponding input file resides.  If the"
                       " destination directory does not exist, it is created."
                       " The following strings will be substituted into DIRECTORY,"
                       " to specify an output location that varies with each submitted job:"
                       " the string 'NAME' is replaced by the job name;"
                       " 'DATE' is replaced by the submission date in ISO format (YYYY-MM-DD);"
                       " 'TIME' is replaced by the submission time formatted as HH:MM."
                       " 'SESSION' is replaced by the path to the session directory, with a '.out' appended."
                       )
        self.add_param("-l", "--state", action="store", nargs='?',
                       dest="states", default='',
                       const=str.join(',', gc3libs.Run.State),
                       help="Print a table of jobs including their status."
                       " Optionally, restrict output to jobs with a particular STATE or STATES"
                       " (comma-separated list).  The pseudo-states `ok` and `failed`"
                       " are also allowed for selecting jobs in TERMINATED state with"
                       " exitcode 0 or nonzero, resp."
                       )
        return

    def pre_run(self):
        """
        Perform parsing of standard command-line options and call into
        `parse_args()` to do non-optional argument processing.
        """
        ## call base classes first (note: calls `parse_args()`)
        _Script.pre_run(self)
        # since it may time quite some time before jobs are created
        # and the first report is displayed, print a startup banner so
        # that users get some kind of feedback ...
        print("Starting %s;"
              " use the '-v' command-line option to get"
              " a more verbose report of activity."
              % (self.name,))

        ## consistency checks
        try:
            # FIXME: backwards-compatibility, remove after 2.0 release
            self.params.walltime = Duration(int(self.params.wctime), hours)
        except ValueError:
            # cannot convert to `int`, use extended parsing
            self.params.walltime = Duration(self.params.wctime)

        ## determine the session file name (and possibly create an empty index)
        self.session_uri = gc3libs.url.Url(self.params.session)
        if self.params.store_url == 'sqlite':
            self.params.store_url = ("sqlite:///%s/jobs.db" % self.session_uri.path)
        elif self.params.store_url == 'file':
            self.params.store_url = ("file:///%s/jobs" % self.session_uri.path)
        self.session = self._make_session(self.session_uri.path, self.params.store_url)

        ## keep a copy of the credentials in the session dir
        self.config.auth_factory.add_params(private_copy_directory=self.session.path)

        # XXX: ARClib errors out if the download directory already exists, so
        # we need to make sure that each job downloads results in a new one.
        # The easiest way to do so is to append 'NAME' to the `output_dir`
        # (if it's not already there).
        if (not 'NAME' in self.params.output
            and not 'ITER' in self.params.output):
            self.params.output = os.path.join(self.params.output, 'NAME')

        ## parse the `states` list
        self.params.states = self.params.states.split(',')

    ##
    ## INTERNAL METHODS
    ##
    ## The following methods are for internal use; they can be
    ## overridden and customized in derived classes, although there
    ## should be no need to do so.
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
        return Session(session_uri, store_url)

    def _main(self, *args):
        """
        Implementation of the main logic in the `SessionBasedScript`.

        This is a template method, that you should not override in derived
        classes: rather use the provided customization hooks:
        :meth:`process_args`, :meth:`parse_args`, :meth:`setup_args`.
        """

        ## zero out the session index if `-N` was given
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
                    except Exception, err:
                        self.log.info(
                            "Got this error while killing old job '%s', ignore it: %s: %s",
                            job, err.__class__.__name__, str(err))
                    try:
                        job.free()
                    except Exception, err:
                        self.log.info(
                            "Got this error while cleaning up old job '%s', ignore it: %s: %s",
                            job, err.__class__.__name__, str(err))
                    job.detach()
                    self.session.remove(jobid)
                    self.log.debug("Removed job '%s' from session.", job)
                self.log.info("Done cleaning up old session jobs, starting with new one afresh...")

        ## update session based on command-line args
        if len(self.session) == 0:
            self.process_args()
        elif self.params.args:
            self.log.warning(
                "Session already exists, ignoring command-line arguments: %s",
                str.join(' ', self.params.args))

        # save the session list immediately, so newly added jobs will
        # be in it if the script is stopped here
        self.session.save_all()

        # obey the ``-r`` command-line option
        if self.params.resource_name:
            self._select_resources(self.params.resource_name)
            self.log.info("Retained only resources: %s (restricted by command-line option '-r %s')",
                          str.join(",", [r['name'] for r in self._core.get_resources()]),
                          self.params.resource_name)

        ## create an `Engine` instance to manage the job list
        self._controller = self.make_task_controller()

        # ...now do a first round of submit/update/retrieve
        self.before_main_loop()
        rc = self._main_loop()
        if self.params.wait > 0:
            self.log.info("sleeping for %d seconds..." % self.params.wait)
            try:
                while rc > 3:
                    # Python scripts become unresponsive during
                    # `time.sleep()`, so we just do the wait in small
                    # steps, to allow the interpreter to process
                    # interrupts in the breaks.  Ugly, but works...
                    for x in xrange(self.params.wait):
                        time.sleep(1)
                    rc = self._main_loop()
            except KeyboardInterrupt:  # gracefully intercept Ctrl+C
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

        .. note::

          Overriding this method can disrupt the whole functionality of
          the script, so be careful.

        Invocation of this method should return a numeric exitcode,
        that will be used as the scripts' exitcode.  As stated in the
        `SessionBasedScript`, the exitcode is a bitfield; only the 4
        least-significant bits are used, with the following meaning:

           ===  ============================================================
           Bit  Meaning
           ===  ============================================================
             0  Set if a fatal error occurred: the script could not complete
             1  Set if there are jobs in `FAILED` state
             2  Set if there are jobs in `RUNNING` or `SUBMITTED` state
             3  Set if there are jobs in `NEW` state
           ===  ============================================================
        """
        # advance all jobs
        self._controller.progress()
        # hook method
        self.every_main_loop()
        # print results to user
        print ("Status of jobs in the '%s' session: (at %s)"
               % (os.path.basename(self.params.session),
                  time.strftime('%X, %x')))
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
                       % self.params.session)
            else:
                print ("  No tasks in this session.")
        # compute exitcode based on the running status of jobs
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
