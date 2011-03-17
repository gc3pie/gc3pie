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
__version__ = '1.0rc4 (SVN $Revision$)'
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
import gc3libs.exceptions
import gc3libs.cmdline
import gc3libs.core as core
import gc3libs.persistence
import gc3libs.utils as utils

import gc3utils


class _BaseCmd(gc3libs.cmdline.GC3UtilsScript):
    """
    Set version string in `gc3libs.cmdline.GC3UtilsScript`:class: to
    this package's version.
    """
    def __init__(self, **kw):
        gc3libs.cmdline.GC3UtilsScript.__init__(self, version=__version__)


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
            raise gc3libs.exceptions.InvalidUsage("Option '-A' conflicts with list of job IDs to remove.")
    
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

            except gc3libs.exceptions.LoadError:
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

    verbose_logging_threshold = 2
    
    def setup_options(self):
        self.add_param("-p", "--print", action="store", dest="keys", 
                       metavar="LIST", default='', 
                       help="Only print job attributes whose name appears in"
                       " this comma-separated list.")

    def main(self):
        if len(self.params.args) == 0:
            # if no arguments, operate on all known jobs
            try:
                self.params.args = self._store.list()
            except NotImplementedError, ex:
                raise NotImplementedError("Job storage module does not allow listing all jobs."
                                          " Please specify the job IDs you wish to operate on.")

        try:
            width = int(os.environ['COLUMNS'])
        except:
            width = 0

        if len(self.params.keys) > 0:
            only_keys = self.params.keys.split(',')
        else:
            if self.params.verbose < 2:
                def names_not_starting_with_underscore(name):
                    return not name.startswith('_')
                only_keys = names_not_starting_with_underscore
            else:
                # print *all* keys if `-vv` is given
                only_keys = None

        def cmp_by_jobid(x,y):
            return cmp(x.persistent_id, y.persistent_id)
        for app in sorted(self._get_jobs(self.params.args), cmp=cmp_by_jobid):
            print(str(app.persistent_id))
            if self.params.verbose == 0:
                utils.prettyprint(app.execution, indent=4, width=width, only_keys=only_keys)
            else:
                # with `-v` and above, dump the whole `Application` object
                utils.prettyprint(app, indent=4, width=width, only_keys=only_keys)
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
                raise gc3libs.exceptions.InvalidUsage("Wrong number of arguments:"
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
                raise gc3libs.exceptions.InvalidUsage("Wrong number of arguments for the 'rosetta' application")
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
            raise gc3libs.exceptions.InvalidUsage("Unknown application '%s'" % application_tag)

        if self.params.resource_name:
            self._select_resources(self.params.resource_name)
            self.log.info("Retained only resources: %s "
                          "(restricted by command-line option '-r %s')",
                          str.join(",", [res['name'] for res in self._core._resources]), 
                          self.params.resource_name)

        self._core.submit(app)
        if app.execution.state is Run.State.SUBMITTED:
            self._store.save(app)
            print("Successfully submitted %s;"
                  " use the 'gstat' command to monitor its progress." % app)
            return 0
        else:
            self.log.error("Could not submit computational job."
                           " Please check log file or re-run with"
                           " higher verbosity ('-vvvv' option)")
            return 1


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
        if len(self.params.args) == 0:
            self.log.error("No job IDs given on command line: nothing to do."
                           " Type '%s --help' for usage help." 
                           # if we were called with an absolute path,
                           # presume the command has been found by the
                           # shell through PATH and just print the command name,
                           # otherwise print the exact path name.
                           % utils.ifelse(os.path.isabs(sys.argv[0]),
                                          os.path.basename(sys.argv[0]),
                                          sys.argv[0]))

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
                self._core.kill(app)
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
    verbose_logging_threshold = 1

    def setup_options(self):
        self.add_param("-l", "--state", action="store", dest="states", 
                       metavar="STATE", default=None, 
                       help="Only report about jobs in the given state."
                       " Multiple states are allowed: separate them with commas.")
        self.add_param("-n", "--no-update", action="store_false",
                       dest="update", default=True,
                       help="Do not update job statuses;"
                       " only print what's in the local database.")
        self.add_param("-p", "--print", action="store", dest="keys", 
                       metavar="LIST", default=None, 
                       help="Additionally print job attributes whose name appears in"
                       " this comma-separated list.")

    def main(self):
        if len(self.params.args) == 0:
            # if no arguments, operate on all known jobs
            try:
                self.params.args = self._store.list()
            except NotImplementedError, ex:
                raise NotImplementedError("Job storage module does not allow listing all jobs."
                                          " Please specify the job IDs you wish to operate on.")

        if len(self.params.args) == 0:
            print("No jobs submitted.")
            return 0

        # try to determine how many lines of output can we fit in a screen
        try:
            capacity = int(os.environ['LINES']) - 5
        except:
            capacity = 20

        # limit to specified job states?
        if self.params.states is not None:
            states = self.params.states.split(',')
        else:
            states = None

        # any additional values to print?
        if self.params.keys is not None:
            keys = self.params.keys.split(',')
        else:
            keys = [ ]
        
        # update states and compute statistics
        stats = utils.defaultdict(lambda: 0)
        tot = 0
        rows = [ ]
        for app in self._get_jobs(self.params.args):
            if self.params.update:
                self._core.update_job_state(app)
                self._store.replace(app.persistent_id, app)
            if states is None or app.execution.state in states:
                rows.append([app.persistent_id, app.execution.state, app.execution.info] +
                            [ app.execution.get(name, "N/A") for name in keys ])
            stats[app.execution.state] += 1
            tot += 1

        if len(rows) > capacity and self.params.verbose == 0:
            # only print table with statistics
            table = Texttable(0) # max_width=0 => dynamically resize cells
            table.set_deco(0) # also: .VLINES, .HLINES .BORDER
            table.set_cols_align(['r', 'c', 'r'])
            for state, num in sorted(stats.items()):
                if (states is None) or (str(state) in states):
                    table.add_row([
                        state,
                        "%d/%d" % (num, tot),
                        "%.2f%%" % (100.0 * num / tot)
                        ])
        else:
            # print table of job status
            table = Texttable(0) # max_width=0 => dynamically resize cells
            table.set_deco(Texttable.HEADER) # also: .VLINES, .HLINES .BORDER
            table.set_cols_align(['l'] * (3 + len(keys)))
            table.header(["Job ID", "State", "Info"] + keys)
            table.add_rows(sorted(rows), header=False)
        print(table.draw())
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
        if len(self.params.args) == 0:
            self.log.error("No job IDs given on command line: nothing to do."
                           " Type '%s --help' for usage help." 
                           # if we were called with an absolute path,
                           # presume the command has been found by the
                           # shell through PATH and just print the command name,
                           # otherwise print the exact path name.
                           % utils.ifelse(os.path.isabs(sys.argv[0]),
                                          os.path.basename(sys.argv[0]),
                                          sys.argv[0]))

        failed = False
        for jobid in self.params.args:
            try:
                app = self._store.load(jobid)

                if app.execution.state == Run.State.NEW:
                    raise gc3libs.exceptions.InvalidOperation("Job '%s' is not yet submitted. Output cannot be retrieved"
                                           % app.persistent_id)

                if app.final_output_retrieved:
                    raise gc3libs.exceptions.InvalidOperation("Output of '%s' already downloaded to '%s'" 
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
        if len(self.params.args) == 0:
            self.log.error("No job IDs given on command line: nothing to do."
                           " Type '%s --help' for usage help." 
                           # if we were called with an absolute path,
                           # presume the command has been found by the
                           # shell through PATH and just print the command name,
                           # otherwise print the exact path name.
                           % utils.ifelse(os.path.isabs(sys.argv[0]),
                                          os.path.basename(sys.argv[0]),
                                          sys.argv[0]))

        failed = 0
        for jobid in self.params.args:
            try:
                app = self._store.load(jobid)

                self.log.debug("gkill: Job '%s' in state %s" % (jobid, app.execution.state))
                if app.execution.state == Run.State.NEW:
                    raise gc3libs.exceptions.InvalidOperation("Job '%s' not submitted." % app)
                if app.execution.state == Run.State.TERMINATED:
                    raise gc3libs.exceptions.InvalidOperation("Job '%s' is already in terminal state" % app)
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
            raise gc3libs.exceptions.InvalidUsage("This command takes only one argument: the Job ID.")
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
        if len(self.params.args) > 0:
            self._select_resources(* self.params.args)

        resources = self._core.get_all_updated_resources()
        def cmp_by_name(x,y):
            return cmp(x.name, y.name)
        for resource in sorted(resources, cmp=cmp_by_name):
            table = Texttable(0) # max_width=0 => dynamically resize cells
            table.set_deco(Texttable.HEADER | Texttable.BORDER) # also: .VLINES, .HLINES
            table.set_cols_align(['r', 'l'])
            table.header([resource.name, ""])

            # not all resources support the same keys...
            def output_if_exists(name, print_name):
                if hasattr(resource, name):
                    table.add_row((print_name, getattr(resource, name)))
            output_if_exists('frontend', "Frontend host name")
            output_if_exists('type', "Access mode")
            output_if_exists('auth', "Authorization name")
            output_if_exists('updated', "Accessible?")
            output_if_exists('ncores', "Total number of cores")
            output_if_exists('queued', "Total queued jobs")
            output_if_exists('user_queued', "Own queued jobs")
            output_if_exists('user_run', "Own running jobs")
            #output_if_exists('free_slots', "Free job slots")
            output_if_exists('max_cores_per_job', "Max cores per job")
            output_if_exists('max_memory_per_core', "Max memory per core (MB)")
            output_if_exists('max_walltime', "Max walltime per job (minutes)")
            output_if_exists('applications', "Supported applications")
            print(table.draw())

