#!/usr/bin/env python
#
"""
Implementation of the `core` command-line front-ends.
"""
# Copyright (C) 2009-2011 GC3, University of Zurich. All rights reserved.
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
__author__="Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>, Riccardo Murri <riccardo.murri@uzh.ch>"
__date__ = '$Date$'
__copyright__="Copyright (c) 2009-2011 Grid Computing Competence Center, University of Zurich"



import cli # pyCLI
import cli.app
import logging
import sys
import os
import tarfile
from texttable import Texttable
import time


from gc3libs import Application, Run
import gc3libs.application.gamess as gamess
import gc3libs.application.rosetta as rosetta
import gc3libs.Default as Default
from   gc3libs.Exceptions import *
import gc3libs.core as core
import gc3libs.persistence
import gc3libs.utils as utils

import gc3utils


## defaults
DEFAULT_CONFIG_FILE_LOCATIONS = [ 
    "/etc/gc3/gc3pie.conf", 
    os.path.join(gc3libs.Default.RCDIR, "gc3pie.conf") 
    ]


class _BaseCmd(cli.app.CommandLineApp):
    """
    Base class for GC3Utils scripts.  

    The default command line implemented is the following:

      script [options] JOBID [JOBID ...]

    By default, only the standard options ``-h``/``--help`` and
    ``-V``/``--version`` are considered; to add more, override
    `setup_options`:method:
    To change default positional argument parsing, override 
    `setup_args`:method:
    
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
        done in `parse_args`:method:
        """
        self.add_param('args', nargs='*', metavar='JOBID', 
                       help="Job ID string identifying the jobs to operate upon.")
                       
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

    def __init__(self, **kw):
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
        # use keyword arguments to set additional instance attrs
        for k,v in kw.items():
            if k not in ['name', 'description']:
                setattr(self, k, v)
        # init and setup pyCLI classes
        if not kw.has_key('version'):
            try:
                kw['version'] = self.version
            except AttributeError:
                # use package version instead
                kw['version'] = __version__
        if not kw.has_key('description'):
            if self.__doc__ is not None:
                kw['description'] = self.__doc__
            else:
                raise AssertionError("Missing required parameter 'description'.")
        cli.app.CommandLineApp.__init__(
            self,
            main=self.main,
            name=os.path.basename(sys.argv[0]),
            **kw
            )
        
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

        GC3Utils scripts should probably override `setup_args`:method: 
        and `setup_options`:method: to modify command-line parsing.
        """
        ## setup of base classes
        cli.app.CommandLineApp.setup(self)

        self.add_param("-s", "--session", action="store", default=Default.JOBS_DIR,
                       help="Directory where job information will be stored.")
        self.add_param("-v", "--verbose", action="count", dest="verbose", default=0,
                       help="Be more detailed in reporting program activity."
                       " Repeat to increase verbosity.")
        self.setup_options()
        self.setup_args()
        return


    def pre_run(self):
        """
        Perform parsing of standard command-line options and call into
        `parse_args()` to do non-optional argument processing.
        """
        ## parse command-line
        cli.app.CommandLineApp.pre_run(self)

        ## setup GC3Libs logging
        loglevel = max(1, logging.ERROR - 10 * self.params.verbose)
        gc3libs.configure_logger(loglevel, self.name)
        self.log = logging.getLogger('gc3.gc3utils') # alternate: ('gc3.' + self.name)
        self.log.setLevel(loglevel)
        self.log.propagate = True

        # interface to the GC3Libs main functionality
        self._core = self._get_core(DEFAULT_CONFIG_FILE_LOCATIONS)

        jobs_dir = self.params.session
        if jobs_dir != Default.JOBS_DIR:
            if (not os.path.isdir(jobs_dir)
                and not jobs_dir.endswith('.jobs')):
                jobs_dir = jobs_dir + '.jobs'
        self._store = gc3libs.persistence.FilesystemStore(jobs_dir, 
                                                          idfactory=gc3libs.persistence.JobIdFactory)


        # call hook methods from derived classes
        self.parse_args()


    def run(self):
        """
        Execute `cli.app.Application.run`:method: if any exception is
        raised, catch it, output an error message and then exit with
        an appropriate error code.
        """
        try:
            return cli.app.CommandLineApp.run(self)
        except KeyboardInterrupt:
            sys.stderr.write("%s: Exiting upon user request (Ctrl+C)\n" % self.name)
            return 13
        except SystemExit, ex:
            return ex.code
        except InvalidUsage, ex:
            # Fatal errors do their own printing, we only add a short usage message
            sys.stderr.write("Type '%s --help' to get usage help.\n" % self.name)
            return 1
        except AssertionError, ex:
            sys.stderr.write("%s: BUG: %s\n"
                             "Please send an email to gc3utils-dev@gc3.uzh.ch copying this\n"
                             "output and and attach file '~/.gc3/debug.log'.  Many thanks for\n"
                             "your cooperation.\n"
                             % (self.name, str(ex)))
            return 1
        except Exception, ex:
            self.log.critical("%s: %s" % (ex.__class__.__name__, str(ex)), 
                              exc_info=(self.params.verbose > 2))
            if isinstance(ex, cli.app.Abort):
                sys.exit(ex.status)
            elif isinstance(ex, EnvironmentError):
                sys.exit(74) # EX_IOERR in /usr/include/sysexits.h
            else:
                # generic error exit
                sys.exit(1)


    ##
    ## INTERNAL METHODS
    ##
    ## The following methods are for internal use; they can be
    ## overridden and customized in derived classes, although there
    ## should be no need to do so.
    ##

    def _get_core(self, config_file_locations, auto_enable_auth=True):
        """
        Return a `gc3libs.core.Core` instance configured by parsing
        the configuration file(s) located at `config_file_locations`.
        Order of configuration files matters: files read last overwrite
        settings from previously-read ones; list the most specific configuration
        files last.

        If `auto_enable_auth` is `True` (default), then `Core` will try to renew
        expired credentials; this requires interaction with the user and will
        certainly fail unless stdin & stdout are connected to a terminal.
        """
        # ensure a configuration file exists in the most specific location
        for location in reversed(config_file_locations):
            if os.access(os.path.dirname(location), os.W_OK|os.X_OK) \
                    and not gc3libs.utils.deploy_configuration_file(location, "gc3pie.conf.example"):
                # warn user
                self.log.warning("No configuration file '%s' was found;"
                                 " a sample one has been copied in that location;"
                                 " please edit it and define resources." % location)
        try:
            self.log.debug('Creating instance of Core ...')
            return gc3libs.core.Core(* gc3libs.core.import_config(config_file_locations, auto_enable_auth))
        except NoResources:
            raise FatalError("No computational resources defined.  Please edit the configuration file '%s'." 
                             % config_file_locations)
        except:
            self.log.debug("Failed loading config file from '%s'", 
                           str.join("', '", config_file_locations))
            raise


    def _get_jobs(self, job_ids, ignore_failures=True):
        """
        Return list of jobs (gc3libs.Application objects) corresponding to
        the given Job IDs.

        If `ignore_failures` is `True` (default), errors retrieving a
        job from the persistence layer are ignored and the jobid is
        skipped, therefore the returned list can be shorter than the
        list of Job IDs given as argument.  If `ignore_failures` is
        `False`, then any errors result in the relevant exception being
        re-raised.

        If `job_ids` is `None` (default), then load all jobs available
        in the persistent storage; if persistent storage does not
        implement the `list` operation, then an empty list is returned
        (when `ignore_failures` is `True`) or a `NotImplementedError`
        exception is raised (when `ignore_failures` is `False`).
        """
        if len(job_ids) == 0:
            try:
                job_ids = self._store.list()
            except NotImplementedError, ex:
                if ignore_failures:
                    return [ ]
                else:
                    raise
        apps = [ ]
        for jobid in job_ids:
            try:
                apps.append(self._store.load(jobid))
            except Exception, ex:
                if ignore_failures:
                    gc3libs.log.error("Could not retrieve job '%s' (%s: %s). Ignoring.", 
                                      jobid, ex.__class__.__name__, str(ex),
                                      exc_info=(self.params.verbose > 2))
                    continue
                else:
                    raise
        return apps


    def _select_resources(self, *resource_names):
        """
        Restrict resources to those listed in `resource_names`.
        Argument `resource_names` is a string listing all names of
        allowed resources (comma-separated), or a list of names of the
        resources to keep active.
        """
        resource_list = [ ]
        for item in resource_names:
            resource_list.extend(name for name in item.split(','))
        kept = self._core.select_resource(lambda r: r.name in resource_list)
        if kept == 0:
            raise NoResources("No resources match the names '%s'" 
                              % str.join(',', resource_list))


#====== Main ========


class cmd_gclean(_BaseCmd):
    """
    Permanently remove jobs from local and remote storage.

    In normal operation, only jobs that are in a terminal status can
    be removed; if you want to force ``gclean`` to remove a job that
    is not in any one of those states, add the ``-f`` option to the
    command line. 

    If a job description cannot be successfully read, the
    corresponding job will not be deleted; use the ``-f`` option to
    force removal of a job regardless.
    """

    def setup_options(self):
        self.add_param("-A", action="store_true", dest="all", default=False, 
                       help="Remove all stored jobs.")
        self.add_param("-f", "--force", action="store_true", dest="force", default=False, 
                       help="Remove job even when not in terminal state.")

    def main(self):
        if self.params.all and len(self.params.args) > 0:
            raise InvalidUsage("Option '-A' conflicts with list of job IDs to remove.")
    
        if self.params.all:
            args = self._store.list()
        else:
            args = self.params.args

        failed = 0
        for jobid in args:
            try:
                app = self._store.load(jobid)

                if app.execution.state != Run.State.NEW:
                    if app.execution.state != Run.State.TERMINATED:
                        if self.params.force:
                            self.log.warning("Job '%s' not in terminal state:"
                                             " attempting to kill before cleaning up.")
                            try:
                                self._core.kill(app)
                            except Exception, ex:
                                self.log.warning("Killing job '%s' failed (%s: %s);"
                                                 " continuing anyway, but errors might ensue.",
                                                 app, ex.__class__.__name__, str(ex))

                                app.execution.state = Run.State.TERMINATED
                        else:
                            failed = 1
                            self.log.error("Job '%s' not in terminal state: ignoring.", jobid)
                            continue

                    try:
                        self._core.free(app)
                    except Exception, ex:
                        if self.params.force:
                            pass
                        else:
                            failed = 1
                            self.log.warning("Freeing job '%s' failed (%s: %s);"
                                             " continuing anyway, but errors might ensue.",
                                             app, ex.__class__.__name__, str(ex))
                            continue

            except LoadError:
                if self.params.force:
                    pass
                else:
                    failed = 1
                    self.log.error("Could not load '%s': ignoring"
                                   " (use option '-f' to remove regardless).", jobid)
                    continue

            try:
                self._store.remove(jobid)
                self.log.info("Removed job '%s'", jobid)
            except:
                failed = 1
                self.log.error("Failed removing '%s' from persistency layer."
                                   " option '-f' harmless"% jobid)
                continue

        return failed


class cmd_ginfo(_BaseCmd):
    """
    Print detailed information about a job.

    A complete dump of all the information known about jobs listed on
    the command line is printed; this will only make sense if you know
    GC3Libs internals.
    """
    def main(self):
        apps = self._get_jobs(self.params.args)
        for app in apps:
            table = Texttable(0) # max_width=0 => dynamically resize cells
            table.set_deco(Texttable.HEADER | Texttable.BORDER) # also: .VLINES, .HLINES
            table.set_cols_align(['l', 'l'])
            table.header([str(app.persistent_id), ''])
            for key, value in sorted(app.execution.items()):
                if (self.params.verbose == 0 
                    and (key.startswith('_') 
                         or key == 'log' 
                         or str(value) in ['', '-1'])):
                    continue
                table.add_row((key, value))
            print(table.draw())
        return 0


class cmd_gsub(_BaseCmd):
    """
    Submit an application job.  Option arguments set computational job
    requirements.  Interpretation of positional arguments varies with
    the application being submitted; the application name is always
    the first non-option argument.

    Currently supported applications are:

      * ``gamess``: Each positional argument (after the application
        name) is the path to an input file; the first one is the
        GAMESS '.inp' file and is required.
        
      * ``rosetta``: The first positional argument is the name of the
        Rosetta application/protocol to run (e.g.,
        ``minirosetta.static`` or ``docking_protocol``); after that
        comes the path to the flags file; remaining positional
        arguments are paths to input files (at least one must be
        provided).  A list of output files may additionally be
        specified after the list of input files, separated from this
        by a ``:`` character.
    """

    def setup_args(self):
        self.add_param("application", nargs=1,
                       help="Application to run (either ``gamess`` or ``rosetta``)")
        self.add_param("args", nargs='+',
                       help="Input files for the application.")

    def setup_options(self):
        self.add_param("-r", "--resource", action="store", dest="resource_name", 
                       metavar="NAME", default=None, 
                       help='Select execution resource by name')
        self.add_param("-c", "--cores", action="store", dest="ncores", type=int,
                       metavar="NUM", default=1, 
                       help='Request running job on this number of CPU cores')
        self.add_param("-m", "--memory", action="store", dest="memory_per_core", type=int, 
                       metavar="NUM", default=1, 
                       help='Request at least this memory per core (GB) on execution site')
        self.add_param("-w", "--walltime", action="store", dest="walltime", type=int,
                       metavar="HOURS", default=1, 
                       help='Guaranteed minimal duration of job, in hours.')

    def main(self):
        # argparse seems to return a list, regardless of the value of `nargs`
        application_tag = self.params.application[0] 
        args = self.params.args
        if application_tag == 'gamess':
            if len(args) < 1:
                raise InvalidUsage("Wrong number of arguments:"
                                   " at least an '.inp' file argument should be specified"
                                   " for the 'gamess' application.")
            self.log.debug("Submitting GAMESS application with arguments: %s" % args)
            app = gamess.GamessApplication(
                 *args, # 1st arg is .INP file path, rest are (optional) additional inputs
                 **{
                    'requested_memory'  : self.params.memory_per_core,
                    'requested_cores'   : self.params.ncores,
                    'requested_walltime': self.params.walltime,
                    # for command-line submissions, `output_dir` is always
                    # overwritten by `gget`, so we just set a bogus value here
                    'output_dir':'/tmp',
                    }
                 )
        elif application_tag == 'rosetta':
            if len(args) < 3:
                raise InvalidUsage("Wrong number of arguments for the 'rosetta' application")
            if ':' in args:
                colon = args.index(':')
                inputs = args[2:colon]
                outputs = args[(colon+1):]
            else:
                inputs = args[2:]
                outputs = [ '*.pdb', '*.sc', '*.fasc' ]
            app = rosetta.RosettaApplication(
                application = args[0],
                flags_file= args[1],
                inputs = inputs,
                outputs = outputs,
                # computational requirements
                requested_memory = self.params.memory_per_core,
                requested_cores = self.params.ncores,
                requested_walltime = self.params.walltime,
                )
        else:
            raise InvalidUsage("Unknown application '%s'" % application_tag)

        if self.params.resource_name:
            self._select_resources(self.params.resource_name)
            self.log.info("Retained only resources: %s "
                          "(restricted by command-line option '-r %s')",
                          str.join(",", [res['name'] for res in self._core._resources]), 
                          self.params.resource_name)

        self._core.submit(app)
        self._store.save(app)

        print("Successfully submitted %s;"
              " use the 'gstat' command to monitor its progress." % app)
        return 0


class cmd_gresub(_BaseCmd):
    """
    Resubmit an already-submitted job with (possibly) different parameters.

    If you resubmit a job that is not in terminal state, the existing job
    is canceled before re-submission.
    """

    def setup_options(self):
        self.add_param("-r", "--resource", action="store", dest="resource_name", 
                       metavar="NAME", default=None, 
                       help='Select execution resource by name')
        self.add_param("-c", "--cores", action="store", dest="ncores", type=int,
                       metavar="NUM", default=1, 
                       help='Request running job on this number of CPU cores')
        self.add_param("-m", "--memory", action="store", dest="memory_per_core", type=int, 
                       metavar="NUM", default=1, 
                       help='Request at least this memory per core (GB) on execution site')
        self.add_param("-w", "--walltime", action="store", dest="walltime", type=int,
                       metavar="HOURS", default=1, 
                       help='Guaranteed minimal duration of job, in hours.')

    def main(self):
        if self.params.resource_name:
            self._select_resources(self.params.resource_name)
            self.log.info("Retained only resources: %s (restricted by command-line option '-r %s')",
                              str.join(",", [res['name'] for res in self._core._resources]), 
                              self.params.resource_name)

        failed = 0
        for jobid in self.params.args:
            app = self._store.load(jobid.strip())
            try:
                self._core.update_job_state(app) # update state
            except Exception, ex:
                # ignore errors, and proceed to resubmission anyway
                self.log.warning("Could not update state of %s: %s: %s", 
                                     jobid, ex.__class__.__name__, str(ex))
            # kill remote job
            try:
                app = self._core.kill(app)
            except Exception, ex:
                # ignore errors, but alert user...
                pass

            try:
                self._core.submit(app)
                print("Successfully re-submitted %s; use the 'gstat' command to monitor its progress." % app)
                self._store.replace(jobid, app)
            except Exception, ex:
                failed = 1
                self.log.error("Failed resubmission of job '%s': %s: %s", 
                                   jobid, ex.__class__.__name__, str(ex))
        return failed


class cmd_gstat(_BaseCmd):
    """
    Print job state.
    """

    def main(self):
        apps = self._get_jobs(self.params.args)

        self._core.update_job_state(*apps)

        # Print result
        if len(apps) == 0:
            print ("No jobs submitted.")
        else:
            table = Texttable(0) # max_width=0 => dynamically resize cells
            table.set_deco(Texttable.HEADER) # also: .VLINES, .HLINES .BORDER
            table.set_cols_align(['l', 'l', 'l'])
            table.header(["Job ID", "State", "Info"])
            def cmp_job_ids(a,b):
                return cmp(a.persistent_id, b.persistent_id)
            for app in sorted(apps, cmp=cmp_job_ids):
                table.add_row((app, app.execution.state, app.execution.info))
            print(table.draw())

        # save jobs back to disk
        for app in apps:
            self._store.replace(app.persistent_id, app)

        return 0


class cmd_gget(_BaseCmd):
    """
    Retrieve output files of a job.

    Output files can only be retrieved once a job has reached the 
    'RUNNING' state; this command will print an error message if
    no output files are available.

    Output files can be retrieved multiple times until a job reaches
    'TERMINATED' state: after that, the remote storage will be
    released once the output files have been fetched.
    """
    def setup_options(self):
        self.add_param("-d", "--download-dir", action="store", dest="download_dir", default=None,
                       help="Destination directory (job id will be appended to it); default is '.'")
        self.add_param("-f", "--overwrite", action="store_true", dest="overwrite", default=False, 
                       help="Overwrite files in destination directory")

    def main(self):
        failed = False
        for jobid in self.params.args:
            try:
                app = self._store.load(jobid)

                if app.execution.state == Run.State.NEW:
                    raise InvalidOperation("Job '%s' is not yet submitted. Output cannot be retrieved"
                                           % app.persistent_id)

                if app.final_output_retrieved:
                    raise InvalidOperation("Output of '%s' already downloaded to '%s'" 
                                           % (app.persistent_id, app.output_dir))

                if self.params.download_dir is None:
                    download_dir = os.path.join(os.getcwd(), app.persistent_id)
                else:
                    download_dir = self.params.download_dir

                self._core.fetch_output(app, download_dir, overwrite=self.params.overwrite)
                print("Job results successfully retrieved in '%s'" % app.output_dir)
                self._store.replace(app.persistent_id, app)

            except Exception, ex:
                print("Failed retrieving results of job '%s': %s"% (jobid, str(ex)))
                failed = True
                continue

        return failed


class cmd_gkill(_BaseCmd):
    """
    Cancel a submitted job.  Given a list of jobs, try to cancel each
    one of them; exit with code 0 if all jobs were cancelled
    successfully, and 1 if some job was not.
    
    The command will print an error message if a job cannot be
    canceled because it's in NEW or TERMINATED state, or if some other
    error occurred.
    """
    def main(self):
        failed = 0
        for jobid in self.params.args:
            try:
                app = self._store.load(jobid)

                self.log.debug("gkill: Job '%s' in state %s" % (jobid, app.execution.state))
                if app.execution.state == Run.State.NEW:
                    raise InvalidOperation("Job '%s' not submitted." % app)
                if app.execution.state == Run.State.TERMINATED:
                    raise InvalidOperation("Job '%s' is already in terminal state" % app)
                else:
                    self._core.kill(app)
                    self._store.replace(jobid, app)

                    # or shall we simply return an ack message ?
                    print("Sent request to cancel job '%s'."% jobid)

            except Exception, ex:
                print("Failed canceling job '%s': %s"% (jobid, str(ex)))
                failed = 1
                continue

        return failed


class cmd_gtail(_BaseCmd):
    """
    Display the last lines from a job's standard output or error stream.
    Optionally, keep running and displaying the last part of the file
    as more lines are written to the given stream.
    """
    def setup_args(self):
        self.add_param("jobid", nargs=1)
    
    def setup_options(self):
        self.add_param("-e", "--stderr", action="store_true", dest="stderr", default=False, help="show stderr of the job")
        self.add_param("-f", "--follow", action="store_true", dest="follow", default=False, help="output appended data as the file grows")
        self.add_param("-o", "--stdout", action="store_true", dest="stdout", default=True, help="show stdout of the job (default)")
        self.add_param("-n", "--lines", dest="num_lines", type=int, default=10, help="output  the  last N lines, instead of the last 10")

    def main(self):
        if len(self.params.jobid) != 1:
            raise InvalidUsage("This command takes only one argument: the Job ID.")
        jobid = self.params.jobid[0]

        if self.params.stderr:
            stream = 'stderr'
        else:
            stream = 'stdout'

        app = self._store.load(jobid)
        if app.execution.state == Run.State.UNKNOWN \
                or app.execution.state == Run.State.SUBMITTED \
                or app.execution.state == Run.State.NEW:
            raise RuntimeError('Job output not yet available')

        if self.params.follow:
            where = 0
            while True:
                file_handle = self._core.peek(app, stream)
                self.log.debug("Seeking position %d in stream %s" % (where, stream))
                file_handle.seek(where)
                for line in file_handle.readlines():
                    print line.strip()
                where = file_handle.tell()
                self.log.debug("Read up to position %d in stream %s" % (where, stream))
                file_handle.close()
                time.sleep(5)
        else:
            file_handle = self._core.peek(app, stream)
            for line in file_handle.readlines()[-(self.params.num_lines):]:
                print line.strip()
            file_handle.close()

        return 0


class cmd_gnotify(_BaseCmd):
    """
    Report a failed job to the GC3Libs developers.

    This command will not likely work on any machine other than
    the ones directly controlled by GC3 sysadmins, so just don't
    use it and send an email to gc3pie@googlegroups.com describing
    your problem instead.
    """
    def setup_options(self):
        self.add_param("-s", "--sender", action="store", dest="sender", default="default_username@gc3.uzh.ch", help="Set email's sender address")
        self.add_param("-r", "--receiver", action="store", dest="receiver", default="root@localhost", help="Set email's receiver  address")
        self.add_param("-m", "--subject", action="store", dest="subject", default="Job notification", help="Set email's subject")
        self.add_param("-t", "--text", action="store", dest="message", default="This is an automatic generated email.", help="Set email's body text")
        self.add_param("-i", "--include", action="store_true", dest="include_job_results", default=False, help="Include Job's results in notification package")

    def main(self):
        # this should be probably specified in the configuration file
        _tmp_folder = '/tmp'

        failed = 0
        for jobid in self.params.args:
            try:
                app = self._store.load(jobid)

                # create tgz with job information
                tar_filename = os.path.join(_tmp_folder,jobid + '.tgz')
                tar = tarfile.open(tar_filename, "w:gz")
                if self.params.include_job_results:
                    try:
                        for filename in os.listdir(app.output_dir):
                            tar.add(os.path.join(app.output_dir,filename))
                    except Exception, ex:
                        gc3libs.log.error("Could not add file '%s/%s' to tar file '%s': %s: %s", 
                                          app.output_dir, filename, tar_filename,
                                          ex.__class__.__name__, str(ex))
                # FIXME: this requires knowledge of how the persistence layer is saving jobs...
                tar.add(os.path.join(gc3libs.Default.JOBS_DIR, jobid))
                tar.close()

                # send notification email to gc3admin
                utils.send_mail(self.params.sender,
                                self.params.receiver,
                                self.params.subject,
                                self.params.message,
                                [tar_filename])

                # clean up tar.gz archive
                os.remove(tar_filename)

            except Exception, ex:
                self.log.error("Error generating %s report: %s" 
                               % (jobid, str(ex)))
                failed += 1

        return failed


class cmd_glist(_BaseCmd):
    """
    List status of computational resources.
    """

    def main(self):
        self._select_resources(* self.params.args)

        resources = self._core.get_all_updated_resources()
        for resource in resources:
            table = Texttable(0) # max_width=0 => dynamically resize cells
            table.set_deco(Texttable.HEADER | Texttable.BORDER) # also: .VLINES, .HLINES
            table.set_cols_align(['r', 'l'])
            table.header([resource.name, ""])

            table.add_row(("Frontend name", resource.frontend))
            if resource.type is Default.ARC_LRMS:
                resource_access_type = "arc"
            elif resource.type is Default.SGE_LRMS:
                resource_access_type = "ssh"
            table.add_row(("Resource access type", resource_access_type))
            if resource.has_key('auth'):
                table.add_row(("Authorization type", resource.auth))
            if resource.has_key('updated'):
                table.add_row(("User can access", resource.updated))
            if resource.has_key('ncores'):
                table.add_row(("Total number of cores", resource.ncores))
            if resource.has_key('queued'):
                table.add_row(("Queued jobs", resource.queued))
            if resource.has_key('user_run'):
                table.add_row(("Running jobs", resource.user_run))
            if resource.has_key('max_cores_per_job'):
                table.add_row(("Max cores per job", resource.max_cores_per_job))
            if resource.has_key('max_memory_per_core'):
                table.add_row(("Max memory per core (MB)", resource.max_memory_per_core))
            if resource.has_key('max_walltime'):
                table.add_row(("Max walltime per job (minutes)", resource.max_walltime))
            if resource.has_key('applications'):
                table.add_row(("Supported applications", resource.applications))
            print(table.draw())

