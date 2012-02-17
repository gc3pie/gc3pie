#!/usr/bin/env python
#
"""
Implementation of the `core` command-line front-ends.
"""
# Copyright (C) 2009-2012 GC3, University of Zurich. All rights reserved.
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
__version__ = 'development version (SVN $Revision$)'
__author__="Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>, Riccardo Murri <riccardo.murri@uzh.ch>"
__date__ = '$Date$'
__copyright__="Copyright (c) 2009-2011 Grid Computing Competence Center, University of Zurich"



import cli # pyCLI
import cli.app
import logging
import sys
import os
import posix
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
            if len(args) == 0:
                self.log.info("No jobs in session: nothing to do.")
        else:
            args = self.params.args
            if len(args) == 0:
                self.log.warning("No job IDs given on command line: nothing to do."
                                 " Type '%s --help' for usage help." 
                                 # if we were called with an absolute path,
                                 # presume the command has been found by the
                                 # shell through PATH and just print the command name,
                                 # otherwise print the exact path name.
                                 % utils.ifelse(os.path.isabs(sys.argv[0]),
                                                os.path.basename(sys.argv[0]),
                                                sys.argv[0]))

        failed = 0
        for jobid in args:
            try:
                app = self._store.load(jobid)
                app.attach(self._core)

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
                            failed += 1
                            self.log.error("Job '%s' not in terminal state: ignoring.", jobid)
                            continue

                    try:
                        self._core.free(app)
                    except Exception, ex:
                        if self.params.force:
                            pass
                        else:
                            failed += 1
                            self.log.warning("Freeing job '%s' failed (%s: %s);"
                                             " continuing anyway, but errors might ensue.",
                                             app, ex.__class__.__name__, str(ex))
                            continue

            except gc3libs.exceptions.LoadError:
                if self.params.force:
                    pass
                else:
                    failed += 1
                    self.log.error("Could not load '%s': ignoring"
                                   " (use option '-f' to remove regardless).", jobid)
                    continue

            try:
                self._store.remove(jobid)
                self.log.info("Removed job '%s'", jobid)
            except:
                failed += 1
                self.log.error("Failed removing '%s' from persistency layer."
                                   " option '-f' harmless"% jobid)
                continue

        # exit code is practically limited to 7 bits ...
        return utils.ifelse(failed < 127, failed, 126)


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
        self.add_param("-t", "--tabular", action="store_true", dest="tabular",
                       default=False,
                       help="Print attributes in table format."
                       " Must be used together with '--print'.")
        
    def main(self):
        if len(self.params.args) == 0:
            # if no arguments, operate on all known jobs
            try:
                self.params.args = self._store.list()
            except NotImplementedError, ex:
                raise NotImplementedError(
                    "Job storage module does not allow listing all jobs."
                    " Please specify the job IDs you wish to operate on.")

        if posix.isatty(sys.stdout.fileno()):
            # try to screen width
            try:
                width = int(os.environ['COLUMNS'])
            except:
                width = 0
        else:
            # presume output goes to a file, so no width restrictions
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

        if self.params.tabular:
            if len(self.params.keys) == 0:
                raise gc3libs.exceptions.InvalidUsage("Option '--tabular' only makes sense in conjuction with '--print'.")
            # print table of job status
            table = Texttable(0) # max_width=0 => dynamically resize cells
            table.set_deco(Texttable.HEADER) # also: .VLINES, .HLINES .BORDER
            table.set_cols_align(['l'] * (1 + len(only_keys)))
            table.header(["Job ID"] + only_keys)

        def cmp_by_jobid(x,y):
            return cmp(x.persistent_id, y.persistent_id)
        ok = 0
        for app in sorted(self._get_jobs(self.params.args), cmp=cmp_by_jobid):
            # since `_get_jobs` swallows any exception raised by
            # invalid job IDs or corrupted files, let us determine the
            # number of failures by counting the number of times we
            # actually run this loop and then subtract from the number
            # of times we *should* have run, i.e., the number of
            # arguments we were passed.
            ok += 1
            if self.params.tabular:
                row = [str(app)]
                for key in only_keys:
                    try:
                        row.append(gc3libs.utils.getattr_nested(app, key))
                    except AttributeError:
                        row.append("N/A")
                table.add_row(row)
            else:
                # usual YAML-like output
                print(str(app.persistent_id))
                if self.params.verbose == 0:
                    utils.prettyprint(app.execution, indent=4, width=width, only_keys=only_keys)
                else:
                    # with `-v` and above, dump the whole `Application` object
                    utils.prettyprint(app, indent=4, width=width, only_keys=only_keys)
        if self.params.tabular:
            print(table.draw())
        failed = len(self.params.args) - ok
        # exit code is practically limited to 7 bits ...
        return utils.ifelse(failed < 127, failed, 126)
        


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
            app.attach(self._core)
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
                # ignore errors (alert user?)
                pass

            try:
                self._core.submit(app)
                print("Successfully re-submitted %s; use the 'gstat' command to monitor its progress." % app)
                self._store.replace(jobid, app)
            except Exception, ex:
                failed += 1
                self.log.error("Failed resubmission of job '%s': %s: %s", 
                               jobid, ex.__class__.__name__, str(ex))

        # exit code is practically limited to 7 bits ...
        return utils.ifelse(failed < 127, failed, 126)


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
        self.add_param("-n", "--no-update", action="store_false", dest="update",
                       help="Do not update job statuses;"
                       " only print what's in the local database.")
        self.add_param("-u", "--update", action="store_true", dest="update", 
                       help="Update job statuses before printing results"
                       " (this is the default.)")
        self.add_param("-p", "--print", action="store", dest="keys", 
                       metavar="LIST", default=None, 
                       help="Additionally print job attributes whose name appears in"
                       " this comma-separated list.")

    def main(self):
        # by default, update job statuses
        if 'update' not in self.params:
            self.params.update = True
        assert self.params.update in [True, False]
        
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

        if posix.isatty(sys.stdout.fileno()):
            # try to determine how many lines of output can we fit in a screen
            try:
                capacity = int(os.environ['LINES']) - 5
            except:
                capacity = 20
        else:
            # output is dumped to a file, so no restrictions
            capacity = gc3libs.utils.PlusInfinity

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
                app.attach(self._core)
                self._core.update_job_state(app)
                self._store.replace(app.persistent_id, app)
            if states is None or app.execution.in_state(*states):
                rows.append([app.persistent_id, app.execution.state, app.execution.info] +
                            [ app.execution.get(name, "N/A") for name in keys ])
            stats[app.execution.state] += 1
            if app.execution.state == Run.State.TERMINATED:
                if app.execution.returncode == 0:
                    stats['ok'] += 1
                else:
                    stats['failed'] += 1
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

        # since `_get_jobs` swallows any exception raised by invalid
        # job IDs or corrupted files, let us determine the number of
        # failures by counting the number of times we actually run
        # this loop and then subtract from the number of times we
        # *should* have run, i.e., the number of arguments we were passed.
        failed = len(self.params.args) - tot
        # exit code is practically limited to 7 bits ...
        return utils.ifelse(failed < 127, failed, 126)


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

        failed = 0
        for jobid in self.params.args:
            try:
                app = self._store.load(jobid)
                app.attach(self._core)

                if app.execution.state == Run.State.NEW:
                    raise gc3libs.exceptions.InvalidOperation(
                        "Job '%s' is not yet submitted. Output cannot be retrieved"
                        % app.persistent_id)
                elif app.execution.state == Run.State.TERMINATED:
                    raise gc3libs.exceptions.InvalidOperation(
                        "Output of '%s' already downloaded to '%s'" 
                        % (app.persistent_id, app.output_dir))

                if self.params.download_dir is None:
                    download_dir = os.path.join(os.getcwd(), app.persistent_id)
                else:
                    download_dir = os.path.join(self.params.download_dir, app.persistent_id)

                self._core.fetch_output(app, download_dir, overwrite=self.params.overwrite)
                if app.execution.state == Run.State.TERMINATED:
                    print("Job final results were successfully retrieved in '%s'" % download_dir)
                else:
                    print("A snapshot of job results was successfully retrieved in '%s'" % download_dir)
                self._store.replace(app.persistent_id, app)

            except Exception, ex:
                print("Failed retrieving results of job '%s': %s"% (jobid, str(ex)))
                failed += 1
                continue

        # exit code is practically limited to 7 bits ...
        return utils.ifelse(failed < 127, failed, 126)


class cmd_gkill(_BaseCmd):
    """
Cancel a submitted job.  Given a list of jobs, try to cancel each
one of them; exit with code 0 if all jobs were cancelled
successfully, and 1 if some job was not.

The command will print an error message if a job cannot be
canceled because it's in NEW or TERMINATED state, or if some other
error occurred.
    """

    def setup_options(self):
        self.add_param("-A", action="store_true", dest="all", default=False, 
                       help="Remove all stored jobs. USE WITH CAUTION!")

    def main(self):
        if self.params.all and len(self.params.args) > 0:
            raise gc3libs.exceptions.InvalidUsage("Option '-A' conflicts with list of job IDs to remove.")
    
        if self.params.all:
            args = self._store.list()
            if len(args) == 0:
                self.log.info("No jobs in session: nothing to do.")
        else:
            args = self.params.args
            if len(args) == 0:
                self.log.warning("No job IDs given on command line: nothing to do."
                                 " Type '%s --help' for usage help." 
                                 # if we were called with an absolute path,
                                 # presume the command has been found by the
                                 # shell through PATH and just print the command name,
                                 # otherwise print the exact path name.
                                 % utils.ifelse(os.path.isabs(sys.argv[0]),
                                                os.path.basename(sys.argv[0]),
                                                sys.argv[0]))

        failed = 0
        for jobid in args:
            try:
                app = self._store.load(jobid)
                app.attach(self._core)

                self.log.debug("gkill: Job '%s' in state %s"
                               % (jobid, app.execution.state))
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
                failed += 1
                continue

        # exit code is practically limited to 7 bits ...
        return utils.ifelse(failed < 127, failed, 126)


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
        app.attach(self._core)

        try:
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
                estimated_size = gc3libs.Default.PEEK_FILE_SIZE * self.params.num_lines
                file_handle = self._core.peek(app, stream, offset=-estimated_size, size=estimated_size)
                for line in file_handle.readlines()[-(self.params.num_lines):]:
                    print line.strip()
                file_handle.close()

        except gc3libs.exceptions.InvalidOperation: # Cannot `peek()` on a task collection
            self.log.error("Task '%s' (of class '%s') has no defined output/error streams."
                           " Ignoring.", app.persistent_id, app.__class__.__name__)
            return 1

        return 0


class cmd_glist(_BaseCmd):
    """
List status of computational resources.
    """

    def setup_options(self):
        self.add_param("-n", "--no-update", action="store_false",
                       dest="update", default=True,
                       help="Do not update resource statuses;"
                       " only print what's in the local database.")
        self.add_param("-p", "--print", action="store", dest="keys", 
                       metavar="LIST", default=None, 
                       help="Only print resource attributes whose name appears in"
                       " this comma-separated list. (Attribute name is as given in"
                       " the configuration file, or listed in the middle column"
                       " in `glist` output.)")

    def main(self):
        if len(self.params.args) > 0:
            self._select_resources(* self.params.args)
            self.log.info("Retained only resources: %s",
                          str.join(",", [res['name'] for res in self._core._resources]))

        if self.params.update:
            self._core.update_resources()
        resources = self._core.get_resources()
        def cmp_by_name(x,y):
            return cmp(x.name, y.name)
        for resource in sorted(resources, cmp=cmp_by_name):
            table = Texttable(0) # max_width=0 => dynamically resize cells
            table.set_deco(Texttable.HEADER | Texttable.BORDER) # also: .VLINES, .HLINES
            table.set_cols_align(['r', 'l'])
            table.header([resource.name, ""])

            # not all resources support the same keys...
            def output_if_exists(name, print_name):
                if hasattr(resource, name) and ((not self.params.keys) or name in self.params.keys):
                    table.add_row((("%s / %s" % (print_name, name)), getattr(resource, name)))
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
