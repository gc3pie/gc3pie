#!/usr/bin/env python
#
"""
Implementation of the command-line front-ends.
"""
# Copyright (C) 2009-2019  University of Zurich. All rights reserved.
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
from __future__ import absolute_import, print_function, unicode_literals

from builtins import str
__docformat__ = 'reStructuredText'
__author__ = ', '.join([
    "Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>",
    "Riccardo Murri <riccardo.murri@uzh.ch>",
    "Antonio Messina <antonio.messina@uzh.ch>",
])

# stdlib imports
import csv
import sys
import os
import posix
import time
import types
import re
import multiprocessing as mp

# 3rd party modules
from parsedatetime.parsedatetime import Calendar
from prettytable import PrettyTable

# local modules
from gc3libs import __version__, Run
import gc3libs.defaults
from gc3libs.quantity import Duration, Memory
from gc3libs.session import Session, TemporarySession
import gc3libs.cmdline
import gc3libs.exceptions
import gc3libs.persistence
from gc3libs.url import Url
import gc3libs.utils as utils


class GC3UtilsScript(gc3libs.cmdline._Script):
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

    def __init__(self, **extra_args):
        """
        Set version string in `gc3libs.cmdline._Script`:class: to
        this package's version.
        """
        super(GC3UtilsScript, self).__init__(
            main=self.main,
            version=__version__,
            name='gc3utils',  # for logging purposes
            **extra_args)

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

    def setup(self):
        """
        Setup standard command-line parsing.

        GC3Utils scripts should probably override `setup_args`:meth:
        and `setup_options`:meth: to modify command-line parsing.
        """
        # setup of base classes (creates the argparse stuff)
        super(GC3UtilsScript, self).setup()
        # local additions
        self.add_param("-s",
                       "--session",
                       action="store",
                       required=True,
                       default=gc3libs.defaults.JOBS_DIR,
                       help="Directory where job information will be stored.")

    def pre_run(self):
        """
        Perform parsing of standard command-line options and call into
        `parse_args()` to do non-optional argument processing.
        """
        # base class parses command-line
        super(GC3UtilsScript, self).pre_run()

    ##
    # INTERNAL METHODS
    ##
    # The following methods are for internal use; they can be
    # overridden and customized in derived classes, although there
    # should be no need to do so.
    ##

    def _get_tasks(self, task_ids, ignore_failures=True):
        """
        Iterate over tasks (gc3libs.Application objects) corresponding
        to the given IDs.

        If `ignore_failures` is `True` (default), errors retrieving a
        job from the persistence layer are ignored and the jobid is
        skipped, therefore the returned list can be shorter than the
        list of Job IDs given as argument.  If `ignore_failures` is
        `False`, then any errors result in the relevant exception being
        re-raised.
        """
        for jobid in task_ids:
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

    def _get_session(self, url, **extra_args):
        """
        Return a `gc3libs.session.Session` object corresponding to the
        session identified by `url`.

        Any keyword arguments will be passed unchanged to the session
        object constructor.

        :raise gc3libs.exceptions.InvalidArgument:
          If the session cannot be loaded (e.g., does not exist).
        """
        try:
            url = Url(url)
            if url.scheme == 'file':
                extra_args.setdefault('create', False)
                return Session(url.path, **extra_args)
            else:
                return TemporarySession(url, **extra_args)
        except gc3libs.exceptions.InvalidArgument as err:
            raise RuntimeError(
                "Cannot load session `{0}`: {1}".format(url, err))

    def _list_all_tasks(self):
        try:
            return self.session.store.list()
        except NotImplementedError:
            raise NotImplementedError(
                "Task storage module does not allow listing all tasks."
                " Please specify the task IDs you wish to operate on.")


# ====== Main ========


class cmd_gclean(GC3UtilsScript):
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
        self.add_param("-A",
                       action="store_true",
                       dest="all",
                       default=False,
                       help="Remove all stored jobs.")
        self.add_param("-f", "--force",
                       action="store_true",
                       dest="force",
                       default=False,
                       help="Remove job even when not in terminal state.")

    def main(self):
        if self.params.all and len(self.params.args) > 0:
            raise gc3libs.exceptions.InvalidUsage(
                "Option '-A' conflicts with list of job IDs to remove.")

        if self.params.all:
            self.session = self._get_session(self.params.session)
            self.params.args = self._list_all_tasks()
            if len(args) == 0:
                self.log.info("No jobs in session: nothing to do.")
        else:
            if len(self.params.args) == 0:
                self.log.warning(
                    "No job IDs given on command line: nothing to do."
                    " Type '%s --help' for usage help."
                    # if we were called with an absolute path,
                    # presume the command has been found by the
                    # shell through PATH and just print the command name,
                    # otherwise print the exact path name.
                    % (os.path.basename(sys.argv[0])
                       if os.path.isabs(sys.argv[0])
                       else sys.argv[0]))
                return os.EX_USAGE
            self.session = self._get_session(self.params.session,
                                             task_ids=self.params.args)

        failed = 0
        for jobid in self.params.args:
            try:
                app = self.session.store.load(jobid)
                app.attach(self._core)

                if app.execution.state != Run.State.NEW:
                    if app.execution.state not in [Run.State.TERMINATED,
                                                   Run.State.TERMINATING]:
                        if self.params.force:
                            self.log.warning(
                                "Job '%s' not in terminal state:"
                                " attempting to kill before cleaning up.", app)
                            try:
                                app.kill()
                            except Exception as ex:
                                self.log.warning(
                                    "Killing job '%s' failed (%s: %s);"
                                    " continuing anyway, but errors might"
                                    " ensue.",
                                    app, ex.__class__.__name__, str(ex))

                                app.execution.state = Run.State.TERMINATED
                        else:
                            failed += 1
                            self.log.error(
                                "Job '%s' not in terminal state: ignoring.",
                                app)
                            continue

                    try:
                        app.free()
                    except Exception as ex:
                        if self.params.force:
                            pass
                        else:
                            failed += 1
                            self.log.warning(
                                "Freeing job '%s' failed (%s: %s);"
                                " continuing anyway, but errors might ensue.",
                                app, ex.__class__.__name__, str(ex))
                            continue

            except gc3libs.exceptions.LoadError:
                if self.params.force:
                    pass
                else:
                    failed += 1
                    self.log.error("Could not load '%s': ignoring"
                                   " (use option '-f' to remove regardless).",
                                   jobid)
                    continue

            try:
                if jobid in self.session.tasks:
                    self.session.remove(jobid)
                else:
                    # if jobid is not a toplevel job Session.remove()
                    # will raise an error.
                    self.session.store.remove(jobid)
                self.log.info("Removed job '%s'", jobid)
            except:
                failed += 1
                self.log.error("Failed removing '%s' from persistency layer."
                               " option '-f' harmless" % jobid)
                continue

        # exit code is practically limited to 7 bits ...
        return min(failed, 126)


class cmd_gclient(gc3libs.cmdline.DaemonClient):
    """
    Generic client for GC3Pie's XML-RPC server.

    Use the `help` command to display a list of remote functions
    supported by the server.
    """
    # all the functionality needed is already in the base class, no
    # need to add anything here
    pass


class cmd_ginfo(GC3UtilsScript):
    """
Print detailed information about a job.

A complete dump of all the information known about jobs listed on
the command line is printed; this will only make sense if you know
GC3Libs internals.
    """

    verbose_logging_threshold = 2

    def setup_options(self):
        self.add_param("-c", "--csv", action="store_true", dest="csv",
                       default=False,
                       help="Print attributes in CSV format,"
                       " e.g., for generating files that can be"
                       " read by a spreadsheet program."
                       " MUST be used together with '--print'.")
        self.add_param("--no-header",
                       action="store_false", dest="header", default=True,
                       help="Do *not* print table or CSV file header.")
        self.add_param("-p", "--print", action="store", dest="keys",
                       metavar="LIST", default='',
                       help="Only print job attributes whose name appears in"
                       " this comma-separated list.")
        self.add_param("-t", "--tabular", action="store_true", dest="tabular",
                       default=False,
                       help="Print attributes in table format."
                       " MUST be used together with '--print'.")
        self.add_param("-H", "--history", action="store_true", dest="history",
                       default=False,
                       help="Print job history only")

    def main(self):
        # sanity checks for the command-line options
        if self.params.csv and self.params.tabular:
            raise gc3libs.exceptions.InvalidUsage(
                "Conflicting options `-c`/`--csv` and `-t`/`--tabular`."
                " Choose either one, but not both.")

        if self.params.history and self.params.tabular:
            raise gc3libs.exceptions.InvalidUsage(
                "Conflicting options `-H`/`--history` and `-t`/`--tabular`."
                " Choose either one, but not both.")

        if self.params.history and self.params.csv:
            raise gc3libs.exceptions.InvalidUsage(
                "Conflicting options `-H`/`--history` and `-c`/`--csv`."
                " Choose either one, but not both.")

        if ((self.params.tabular or self.params.csv)
                and len(self.params.keys) == 0):
            raise gc3libs.exceptions.InvalidUsage(
                "Options '--tabular' and `--csv` only make sense"
                " in conjuction with option '--print'.")

        if posix.isatty(sys.stdout.fileno()):
            # try to screen width
            try:
                width = int(os.environ['COLUMNS'])
            except:
                width = 0
        else:
            # presume output goes to a file, so no width restrictions
            width = 0

        # start reading stuff from disk
        self.session = self._get_session(self.params.session,
                                         task_ids=self.params.args)

        if len(self.params.args) == 0:
            # if no arguments, operate on all known jobs
            self.params.args = self._list_all_tasks()

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
            # prepare table prettyprinter
            table = PrettyTable()
            table.border = True
            if self.params.header:
                table.field_names = ["Job ID"] + only_keys
                table.align = 'l'

        if self.params.csv:
            csv_output = csv.writer(sys.stdout)
            if self.params.header:
                csv_output.writerow(only_keys)

        ok = 0
        for app in sorted(self._get_tasks(self.params.args),
                          key=(lambda task: task.persistent_id)):
            # since `_get_tasks` swallows any exception raised by
            # invalid job IDs or corrupted files, let us determine the
            # number of failures by counting the number of times we
            # actually run this loop and then subtract from the number
            # of times we *should* have run, i.e., the number of
            # arguments we were passed.
            ok += 1
            if self.params.tabular or self.params.csv:
                row = [str(app)]
                for key in only_keys:
                    try:
                        row.append(gc3libs.utils.getattr_nested(app, key))
                    except AttributeError:
                        row.append("N/A")
                if self.params.tabular:
                    table.add_row(row)
                elif self.params.csv:
                    csv_output.writerow(row)
            elif self.params.history:
                print(str(app.persistent_id))
                indent = 4*' '
                for logentry in app.execution.history:
                    print(indent + logentry)
            else:
                # usual YAML-like output
                print(str(app.persistent_id))
                if self.params.verbose == 0:
                    utils.prettyprint(app.execution, indent=4,
                                      width=width, only_keys=only_keys)
                else:
                    # with `-v` and above, dump the whole `Application` object
                    utils.prettyprint(app, indent=4, width=width,
                                      only_keys=only_keys)
        if self.params.tabular:
            print(table)
        failed = len(self.params.args) - ok
        # exit code is practically limited to 7 bits ...
        return min(failed, 126)


class cmd_gresub(GC3UtilsScript):
    """
Resubmit an already-submitted job with (possibly) different parameters.

If you resubmit a job that is not in terminal state, the existing job
is canceled before re-submission.
    """

    def setup_options(self):
        self.add_param("-r", "--resource",
                       action="store",
                       dest="resource_name",
                       metavar="NAME",
                       default=None,
                       help='Select execution resource by name')
        self.add_param("-c", "--cores",
                       action="store",
                       dest="ncores",
                       type=int,
                       metavar="NUM",
                       help='Request running job on this number of CPU cores')
        self.add_param("-m", "--memory",
                       action="store",
                       dest="memory_per_core",
                       metavar="GIGABYTES",
                       help="Set the amount of memory required per execution"
                       " core; default: %(default)s. Specify this as an"
                       " integral number followed by a unit, e.g., '512MB'"
                       " or '4GB'. execution site")
        self.add_param("-w", "--walltime",
                       action="store",
                       dest="walltime",
                       metavar="DURATION",
                       help="Set the time limit for each job."
                       " Jobs exceeding this limit will be stopped and"
                       " considered as 'failed'. The duration can be expressed"
                       " as a whole number followed by a time unit, e.g.,"
                       " '3600 s', '60 minutes', '8 hours', or a combination"
                       " thereof, e.g., '2hours 30minutes'.")

    def parse_args(self):
        if self.params.walltime:
            self.params.walltime = Duration(self.params.walltime)

        if self.params.memory_per_core:
            self.params.memory_per_core = Memory(self.params.memory_per_core)

    def main(self):
        if len(self.params.args) == 0:
            self.log.error(
                "No job IDs given on command line: nothing to do."
                " Type '%s --help' for usage help."
                # if we were called with an absolute path,
                # presume the command has been found by the
                # shell through PATH and just print the command name,
                # otherwise print the exact path name.
                % (os.path.basename(sys.argv[0]) if os.path.isabs(sys.argv[0])
                   else sys.argv[0]))
            return os.EX_USAGE

        if self.params.resource_name:
            self._select_resources(self.params.resource_name)
            self.log.info(
                "Retained only resources: %s (restricted by command-line"
                " option '-r %s')",
                str.join(",",
                         [res['name'] for res in self._core.get_resources()]),
                self.params.resource_name)

        self.session = self._get_session(self.params.session,
                                         task_ids=self.params.args)
        failed = 0
        for jobid in self.params.args:
            app = self.session.load(jobid.strip())

            # Update the requested walltime, memory per core and
            # number of cores for the application
            if self.params.ncores:
                app.requested_cores = self.params.ncores
            if self.params.memory_per_core:
                app.requested_memory = self.params.memory_per_core\
                    * app.requested_cores
            if self.params.walltime:
                app.requested_walltime = self.params.walltime
            app.attach(self._core)
            try:
                app.update_state()  # update state
            except Exception as ex:
                # ignore errors, and proceed to resubmission anyway
                self.log.warning("Could not update state of %s: %s: %s",
                                 jobid, ex.__class__.__name__, str(ex))
            # kill remote job
            try:
                app.kill()
            except Exception as ex:
                # ignore errors (alert user?)
                pass

            try:
                app.submit(resubmit=True)
                print("Successfully re-submitted %s; use the 'gstat' command"
                      " to monitor its progress." % app)
                self.session.store.replace(jobid, app)
            except Exception as ex:
                failed += 1
                self.log.error("Failed resubmission of job '%s': %s: %s",
                               jobid, ex.__class__.__name__, str(ex))

        # exit code is practically limited to 7 bits ...
        return min(failed, 126)


class cmd_gstat(GC3UtilsScript):
    """
Print job state.
    """
    verbose_logging_threshold = 1

    def setup_options(self):
        self.add_param("-b", "--brief", "--summary",
                       action="store_true",
                       dest="summary",
                       help=("Only print a summary table"
                             " with count of jobs per each state."))
        self.add_param("-l", "--state",
                       action="store",
                       dest="states",
                       metavar="STATE",
                       default=None,
                       help="Only report about jobs in the given state."
                       " Multiple states are allowed: separate them with"
                       " commas.")
        self.add_param("-L", "--lifetimes", "--print-lifetimes",
                       nargs='?',
                       metavar='FILE',
                       action='store',
                       dest='lifetimes',
                       default=None,  # no option and no argument: discard data
                       const=sys.stdout,  # option given, but no argument
                       help="For each successful job, print"
                       " submission, start, and duration times."
                       " If FILE is omitted, report is printed to screen.")
        self.add_param("-u", "--update",
                       action="store_true",
                       dest="update",
                       default=False,
                       help="Update job statuses before printing results")
        self.add_param("-p", "--print",
                       action="store",
                       dest="keys",
                       metavar="LIST",
                       default=None,
                       help="Additionally print job attributes whose name"
                       " appears in this comma-separated list.")

    def main(self):
        # by default, DO NOT update job statuses

        if len(self.params.args) == 0:
            # if no arguments, operate on all known jobs
            self.session = self._get_session(self.params.session)
            self.params.args = self._list_all_tasks()
        else:
            self.session = self._get_session(self.params.session,
                                             task_ids=self.params.args)

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
            keys = []

        # init lifetimes report (if requested)
        if self.params.lifetimes is not None:
            if isinstance(self.params.lifetimes, (str,)):
                self.params.lifetimes = open(self.params.lifetimes, 'w')
            lifetimes_rows = [['JOBID',
                               'SUBMITTED_AT',
                               'RUNNING_AT',
                               'FINISHED_AT',
                               'WAIT_DURATION',
                               'EXEC_DURATION']]

        # update states and compute statistics
        stats = utils.defaultdict(lambda: 0)
        tot = 0
        rows = []
        for app in self._get_tasks(self.params.args):
            tot += 1  # one more job successfully loaded
            jobid = app.persistent_id
            if self.params.update:
                app.attach(self._core)
                app.update_state()
                self.session.store.replace(jobid, app)
            if states is None or app.execution.in_state(*states):
                try:
                    jobname = app.jobname
                except AttributeError:
                    jobname = ''

                key_values = []
                for name in keys:
                    if name in app.execution:
                        key_values.append(app.execution.get(name))
                    else:
                        key_values.append(app.get(name, "N/A"))
                rows.append([jobid,
                             jobname,
                             app.execution.state,
                             app.execution.info] + key_values)

            stats[app.execution.state] += 1
            if app.execution.state == Run.State.TERMINATED:
                if app.execution.returncode == 0:
                    stats['ok'] += 1
                    if self.params.lifetimes is not None:
                        try:
                            timestamps = app.execution.timestamp
                        except AttributeError:
                            # missing .execution or .terminated: job
                            # is malformed, skip it
                            continue
                        if Run.State.SUBMITTED in timestamps:
                            submitted_at = timestamps['SUBMITTED']
                        else:
                            # skip job: not enough info
                            continue
                        if Run.State.RUNNING in timestamps:
                            running_at = timestamps[Run.State.RUNNING]
                        else:
                            # this means that the transition from
                            # SUBMITTED to RUNNING to TERMINATED
                            # happened in between two updates; we have
                            # no idea what the update cycle was, so...
                            # there's nothing left to do but skip this job
                            continue
                        terminated_at = timestamps['TERMINATING']
                        lifetimes_rows.append(
                            [jobid,
                             submitted_at,
                             running_at,
                             terminated_at,
                             running_at - submitted_at,
                             terminated_at - running_at])
                else:
                    stats['failed'] += 1

        summary_only = (
            # requested by user on command-line
            self.params.summary
            # automatically determined based on screen size
            or (len(rows) > capacity and self.params.verbose == 0)
        )
        if summary_only:
            # only print table with statistics
            table = PrettyTable(['state', 'num/tot', 'num/tot %'])
            table.header = False
            table.align['state'] = 'r'
            table.align['num/tot'] = 'c'
            table.align['num/tot %'] = 'r'

            for state, num in sorted(stats.items()):
                if (states is None) or (str(state) in states):
                    table.add_row([
                        state,
                        "%d/%d" % (num, tot),
                        "%.2f%%" % (100.0 * num / tot)
                    ])
        else:
            # print table of job status
            table = PrettyTable(["JobID", "Job name", "State", "Info"] + keys)
            table.align = 'l'
            for row in sorted(rows):
                table.add_row(row)
        print(table)

        if self.params.lifetimes is not None and len(lifetimes_rows) > 1:
            if self.params.lifetimes is sys.stdout:
                print("")
                print("Report on the job life times:")
                lifetimes_csv = csv.writer(
                    self.params.lifetimes, delimiter='\t')
            else:
                lifetimes_csv = csv.writer(self.params.lifetimes)
            lifetimes_csv.writerows(lifetimes_rows)

        # since `_get_tasks` swallows any exception raised by invalid
        # job IDs or corrupted files, let us determine the number of
        # failures by counting the number of times we actually run
        # this loop and then subtract from the number of times we
        # *should* have run, i.e., the number of arguments we were passed.
        failed = len(self.params.args) - tot
        # exit code is practically limited to 7 bits ...
        return min(failed, 126)


class cmd_gget(GC3UtilsScript):
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
        self.add_param("-A",
                       action="store_true",
                       dest="all",
                       default=False,
                       help="Download *all* files of *all* tasks in a session."
                       " USE WITH CAUTION!")
        self.add_param("-d", "--download-dir",
                       action="store",
                       dest="download_dir",
                       default=None,
                       help="Destination directory (job id will be appended to"
                       " it); default is '.'")
        self.add_param("-f", "--overwrite",
                       action="store_true",
                       dest="overwrite",
                       default=False,
                       help="Overwrite files in destination directory")
        self.add_param("-c", "--changed-only",
                       action="store_true",
                       dest="changed_only",
                       default=False,
                       help="Only download files that were changed on remote"
                       " side.")

    def main(self):
        if self.params.all and len(self.params.args) > 0:
            raise gc3libs.exceptions.InvalidUsage(
                "Option '-A' conflicts with list of job IDs:"
                " use either '-A' or explicitly list task IDs.")

        if self.params.all:
            self.session = self._get_session(self.params.session)
            args = self._list_all_tasks()
            if len(args) == 0:
                self.log.info("No jobs in session: nothing to do.")
                return os.EX_OK
        else:
            args = self.params.args
            if len(args) == 0:
                self.log.error(
                    "No job IDs given on command line: nothing to do."
                    " Type '%s --help' for usage help."
                    # if we were called with an absolute path,
                    # presume the command has been found by the
                    # shell through PATH and just print the command name,
                    # otherwise print the exact path name.
                    % (os.path.basename(sys.argv[0])
                       if os.path.isabs(sys.argv[0])
                       else sys.argv[0]))
                return es.EX_USAGE
            self.session = self._get_session(self.params.session,
                                             task_ids=args)

        failed = 0
        download_dirs = set()
        for jobid in args:
            try:
                app = self.session.load(jobid)
                app.attach(self._core)

                if app.execution.state == Run.State.NEW:
                    raise gc3libs.exceptions.InvalidOperation(
                        "Job '%s' is not yet submitted. Output cannot be"
                        " retrieved" % app.persistent_id)
                elif app.execution.state == Run.State.TERMINATED:
                    raise gc3libs.exceptions.InvalidOperation(
                        "Output of '%s' already downloaded to '%s'"
                        % (app.persistent_id, app.output_dir))

                # XXX: this uses "private" code from `Application` and
                # `Core.fetch_output`
                app_download_dir = app._get_download_dir(
                    self.params.download_dir)
                # avoid downloading files twice for virtual tasks that
                # wrap an application (e.g., `RetryableTask`)
                if app_download_dir not in download_dirs:
                    self._core.fetch_output(
                        app,
                        download_dir=app_download_dir,
                        overwrite=self.params.overwrite,
                        changed_only=self.params.changed_only)
                    if app.changed:
                        self.session.store.replace(app.persistent_id, app)
                    # `fetch_output` is by default a no-op on tasks:
                    # try to detect this and skip all the messaging
                    # below (and also retry the directory, in case we
                    # have an Application with the same download
                    # destination)
                    if not os.path.exists(app_download_dir):
                        continue
                    # avoid downloading files twice -- see above
                    download_dirs.add(app_download_dir)
                else:
                    self.log.debug("Output directory '%s' already visited,"
                                   " not downloading again.", app_download_dir)
                # print message to user anyhow
                if app.execution.state == Run.State.TERMINATED:
                    print("Job final results were successfully retrieved in"
                          " '%s'" % (app_download_dir,))
                else:
                    print("A snapshot of job results was successfully"
                          " retrieved in '%s'" % (app_download_dir,))

            except Exception as ex:
                print("Failed retrieving results of job '%s': %s"
                      % (jobid, str(ex)))
                failed += 1
                continue

        # exit code is practically limited to 7 bits ...
        return min(failed, 126)


class cmd_gkill(GC3UtilsScript):
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
            raise gc3libs.exceptions.InvalidUsage(
                "Option '-A' conflicts with list of job IDs to remove.")

        if self.params.all:
            self.session = self._get_session(self.params.session)
            args = self._list_all_tasks()
            if len(args) == 0:
                self.log.info("No jobs in session: nothing to do.")
                return os.EX_OK
        else:
            args = self.params.args
            if len(args) == 0:
                self.log.warning(
                    "No job IDs given on command line: nothing to do."
                    " Type '%s --help' for usage help."
                    # if we were called with an absolute path,
                    # presume the command has been found by the
                    # shell through PATH and just print the command name,
                    # otherwise print the exact path name.
                    % (os.path.basename(sys.argv[0])
                       if os.path.isabs(sys.argv[0])
                       else sys.argv[0]))
                return os.EX_USAGE
            self.session = self._get_session(self.params.session,
                                             task_ids=args)

        failed = 0
        for jobid in args:
            try:
                app = self.session.load(jobid)
                app.attach(self._core)

                self.log.debug("gkill: Job '%s' in state %s"
                               % (jobid, app.execution.state))
                if app.execution.state == Run.State.NEW:
                    raise gc3libs.exceptions.InvalidOperation(
                        "Job '%s' has never been submitted." % app)
                if app.execution.state == Run.State.TERMINATED:
                    raise gc3libs.exceptions.InvalidOperation(
                        "Job '%s' is already in terminal state" % app)
                else:
                    app.kill()
                    self.session.store.replace(jobid, app)

                    # or shall we simply return an ack message ?
                    print("Sent request to cancel job '%s'." % jobid)

            except Exception as ex:
                print("Failed canceling job '%s': %s" % (jobid, str(ex)))
                failed += 1
                continue

        # exit code is practically limited to 7 bits ...
        return min(failed, 126)


class cmd_gtail(GC3UtilsScript):
    """
Display the last lines from a job's standard output or error stream.
Optionally, keep running and displaying the last part of the file
as more lines are written to the given stream.
    """
    def setup_args(self):
        """
        Override `GC3UtilsScript`:class: `setup_args` method since we
        don't operate on single jobs.
        """
        self.add_param('args',
                       nargs=1,
                       metavar='JOBID',
                       help="Job ID string identifying the single job to"
                       " operate upon.")

    def setup_options(self):
        self.add_param("-e", "--stderr",
                       action="store_true",
                       dest="stderr",
                       default=False,
                       help="show stderr of the job")
        self.add_param("-f", "--follow",
                       action="store_true",
                       dest="follow",
                       default=False,
                       help="output appended data as the file grows")
        self.add_param("-o", "--stdout",
                       action="store_true",
                       dest="stdout",
                       default=True,
                       help="show stdout of the job (default)")
        self.add_param("-n", "--lines",
                       dest="num_lines",
                       type=int,
                       default=10,
                       help="output the last N lines, instead of the last 10")

    def main(self):
        if len(self.params.args) == 0:
            self.log.error(
                "No job IDs given on command line: nothing to do."
                " Type '%s --help' for usage help."
                # if we were called with an absolute path,
                # presume the command has been found by the
                # shell through PATH and just print the command name,
                # otherwise print the exact path name.
                % (os.path.basename(sys.argv[0]) if os.path.isabs(sys.argv[0])
                   else sys.argv[0]))
            return os.EX_USAGE

        if self.params.stderr:
            stream = 'stderr'
        else:
            stream = 'stdout'

        self.session = self._get_session(self.params.session,
                                         task_ids=self.params.args)
        failed = 0
        for jobid in self.params.args:
            try:
                app = self.session.load(jobid)
                if app.execution.state == Run.State.UNKNOWN \
                        or app.execution.state == Run.State.SUBMITTED \
                        or app.execution.state == Run.State.NEW:
                    raise RuntimeError('Job output not yet available')
                app.attach(self._core)

                try:
                    if self.params.follow:
                        where = 0
                        while True:
                            file_handle = app.peek(stream)
                            self.log.debug("Seeking position %d in stream"
                                           " %s" % (where, stream))
                            file_handle.seek(where)
                            for line in file_handle.readlines():
                                print(line.strip())
                            where = file_handle.tell()
                            self.log.debug("Read up to position %d in stream"
                                           " %s" % (where, stream))
                            file_handle.close()
                            time.sleep(5)
                    else:
                        estimated_size = (
                            gc3libs.defaults.PEEK_FILE_SIZE * self.params.num_lines)
                        fh = app.peek(stream, offset=-estimated_size,
                                      size=estimated_size)
                        for line in fh.readlines()[-(self.params.num_lines):]:
                            print(line.strip())
                        fh.close()

                except gc3libs.exceptions.InvalidOperation:  # Cannot `peek()` on a task collection  # noqa
                    self.log.error(
                        "Task '%s' (of class '%s') has no defined output/error"
                        " streams. Ignoring.",
                        app.persistent_id,
                        app.__class__.__name__)
                    failed += 1
            except Exception as ex:
                print("Failed while reading content of %s for job '%s': %s"
                      % (stream, jobid, str(ex)))
                failed += 1

        # exit code is practically limited to 7 bits ...
        return min(failed, 126)


class cmd_gservers(GC3UtilsScript):
    """
List status of computational resources.
    """

    def setup_options(self):
        self.add_param("-n", "--no-update", action="store_false",
                       dest="update", default=True,
                       help="Do not update resource statuses;"
                       " only print what's in the local database.")
        self.add_param(
            "-p", "--print", action="store", dest="keys",
            metavar="LIST", default=None,
            help="Only print resource attributes whose name appears in"
            " this comma-separated list. (Attribute name is as given in"
            " the configuration file, or listed in the middle column"
            " in `gservers` output.)")

    def setup(self):
        """
        Override `GC3UtilsScript`:class: `setup` method since we
        don't need any `session` argument.
        """
        gc3libs.cmdline._Script.setup(self)

    def setup_args(self):
        """
        Override `GC3UtilsScript`:class: `setup_args` method since we
        don't operate on jobs but on resources.
        """
        self.add_param('args',
                       nargs='*',
                       metavar='RESOURCE',
                       help="Resource, string identifying the name of the"
                       " resources to check.")

    def main(self):
        if len(self.params.args) > 0:
            self._select_resources(* self.params.args)
            self.log.info(
                "Retained only resources: %s",
                str.join(",", [res['name']
                               for res in self._core.get_resources()]))

        if self.params.update:
            self._core.update_resources()

        resources = self._core.get_resources()

        for resource in sorted(resources, key=(lambda rsc: rsc.name)):
            if self.params.args and resource.name not in self.params.args:
                continue
            table = PrettyTable(['', resource.name, ' '])
            table.align = 'l'
            table.align[''] = 'r'

            # not all resources support the same keys...
            def output_if_exists(name, print_name):
                if hasattr(resource, name) and ((not self.params.keys) or
                                                name in self.params.keys):
                    table.add_row((name,
                                   ("( %s )" % print_name),
                                   getattr(resource, name)))
            output_if_exists('frontend', "Frontend host name")
            output_if_exists('type', "Access mode")
            output_if_exists('auth', "Authorization name")
            output_if_exists('updated', "Accessible?")
            output_if_exists('ncores', "Total number of cores")
            output_if_exists('queued', "Total queued jobs")
            output_if_exists('user_queued', "Own queued jobs")
            output_if_exists('user_run', "Own running jobs")
            # output_if_exists('free_slots', "Free job slots")
            output_if_exists('max_cores_per_job', "Max cores per job")
            output_if_exists('max_memory_per_core', "Max memory per core")
            output_if_exists('max_walltime', "Max walltime per job")
            output_if_exists('applications', "Supported applications")
            print(table)
            print('')


class cmd_gsession(GC3UtilsScript):
    """
`gsession` get info on a session.

Usage:

    gsession `command` [options] SESSION_DIR

commands are listed below, under `subcommands`.

To get detailed info on a specific command, run:

    gsession `command` --help
    """

    # Setup methods

    def _add_subcmd(self, name, func, help=None):
        subparser = self.subparsers.add_parser(name, help=help)
        subparser.set_defaults(func=func)
        subparser.add_argument('session')
        subparser.add_argument('-v', '--verbose', action='count')
        return subparser

    def setup(self):
        gc3libs.cmdline._Script.setup(self)

        self.subparsers = self.argparser.add_subparsers(
            title="subcommands",
            description="gsession accept the following subcommands. "
            "Each subcommand requires a `SESSION` directory as argument.")

        self._add_subcmd(
            'abort',
            self.abort_session,
            help="Kill all jobs related to a session and remove it from disk")
        self._add_subcmd(
            'delete',
            self.delete_session,
            help="Delete a session from disk.")
        subparser = self._add_subcmd(
            'list',
            self.list_jobs,
            help="Tree-like view of jobs related to a session.")
        subparser.add_argument('-r', '--recursive', action="store_true",
                               default=False,
                               help="Show all jobs contained in a task"
                               " collection, not only top-level jobs.")

        self._add_subcmd(
            'log',
            self.show_log,
            help="Show log entries for the session.")

    def setup_args(self):
        # prevent GC3UtilsScript.setup_args() to add the default JOBID
        # non optional argument
        pass

    def main(self):
        return self.params.func()

    # "working" methods
    def abort_session(self):
        """
        Called with subcommand `abort`.

        This method will open the desired session and will kill all
        the jobs which belongs to that session.

        Thiw method will return the number of jobs that have not been
        correctly aborted. If this number is greater than 125, then
        125 will be returned instead, since numbers above 125 have a
        special meaning for the shell.
        """
        self.session = self._get_session(self.params.session)

        rc = len(self.session.tasks)
        for task_id in list(self.session.tasks.keys()):
            task = self.session.tasks[task_id]
            task.attach(self._core)
            if task.execution.state == Run.State.TERMINATED:
                gc3libs.log.info("Not aborting '%s' which is already in"
                                 " TERMINATED state." % task)
                rc -= 1
                continue
            try:
                task.kill()
                task.free()
                rc -= 1
            except gc3libs.exceptions.Error as err:
                gc3libs.log.error(
                    "Could not abort task '%s': %s: %s",
                    task, err.__class__.__name__, err)

        if rc:
            gc3libs.log.error(
                "Not all tasks of the session have been aborted.")

        if rc > 125:
            # 126 and 127 error codes have special meanings.
            rc = 125
        self.session.save_all()
        return rc

    def delete_session(self):
        """
        Called with subcommand `delete`.

        This method will first call `abort` and then remove the
        current session.

        If the `abort` will fail or not all tasks are in TERMINATED
        state, it will not delete the session and will exit with the
        exit status of the `abort`.
        """
        try:
            rc = self.abort_session()
            if rc != 0:
                return rc
        except gc3libs.exceptions.InvalidArgument:
            raise RuntimeError('Session %s not found' % self.params.session)

        self.session.destroy()
        return 0

    def list_jobs(self):
        """
        Called with subcommand ``list``.

        List the content of a session, like ``gstat -n -v -s SESSION``
        does.  Unlike ``gstat``, though, display stops at the top-level
        jobs unless option `--recursive` is also given.

        With option ``--recursive``, indent job ids to show the
        tree-like organization of jobs in the task collections.
        """
        self.session = self._get_session(self.params.session)

        def print_app_table(app, indent, recursive):
            rows = []
            try:
                jobname = app.jobname
            except AttributeError:
                jobname = ''

            rows.append([indent + str(app.persistent_id),
                         jobname,
                         app.execution.state,
                         app.execution.info])
            if recursive and 'tasks' in app:
                indent = " "*len(indent) + '  '
                for task in app.tasks:
                    rows.extend(print_app_table(task, indent, recursive))
            return rows

        rows = []
        for app in list(self.session.tasks.values()):
            rows.extend(print_app_table(app, '', self.params.recursive))
        table = PrettyTable(["JobID", "Job name", "State", "Info"])
        table.align = 'l'
        for row in rows:
            table.add_row(row)
        print(table)

    def show_log(self):
        """
        Called when subcommand is `log`.

        This method will print the history of the jobs in SESSION in a
        logfile fashon
        """
        self.session = self._get_session(self.params.session)
        timestamps = []
        task_queue = list(self.session.tasks.values())
        while task_queue:
            app = task_queue.pop()
            for what, when, tags in app.execution.history._messages:
                timestamps.append((float(when), str(app), what))
            try:
                for child in app.tasks:
                    task_queue.append(child)
            except AttributeError:
                # Application class does not have a `tasks` attribute
                pass

        timestamps.sort(key=(lambda ts: ts[0]))
        for entry in timestamps:
            print("%s %s: %s" % (
                time.strftime(
                    "%b %d %H:%M:%S", time.localtime(entry[0])
                    ),
                str(entry[1]),
                entry[2]))


class cmd_gselect(GC3UtilsScript):
    """
Print IDs of jobs that match the specified criteria.
The criteria specified by command-line options will be
AND'ed together, i.e., a job must satisfy all of them
in order to be selected.
    """

    def setup_args(self):
        # No positional arguments allowed
        pass

    def setup_options(self):
        self.add_param(
            '--error-message', '--errmsg', metavar='REGEXP',
            help=("Select jobs such that a line in their error output (STDERR)"
                  " file matches the given regular expression pattern."),
        )
        self.add_param(
            '--input-file', metavar='FILENAME',
            help=("Select jobs with input file FILENAME"
                  " (only the file name is considered, not the full path)."),
            )
        self.add_param(
            '--jobname', '--job-name', metavar='REGEXP',
            dest='jobname', default='',
            help=("Select jobs whose name matches the supplied"
                  " regular expression (case insensitive).")
        )
        self.add_param(
            '--jobid', '--job-id', metavar='REGEXP',
            dest='jobid', default='',
            help=("Select jobs whose ID matches the supplied"
                  " regular expression (case insensitive).")
        )
        self.add_param(
            '-l', '--state', '--states',
            dest='states', default=None,
            help=("Select all jobs in one of the specified states"
                  " (comma-separated list). Valid states are: %s"
                  % str.join(", ", sorted(Run.State))),
            )
        self.add_param(
            '--output-file', metavar='FILENAME',
            help=("Select jobs with output file FILENAME"
                  " (only the file name is considered, not the full path)."),
            )
        self.add_param(
            '--output-message', '--outmsg', metavar='REGEXP',
            help=("Select jobs such that a line in their main output (STDOUT)"
                  " file matches the given regular expression pattern."),
        )
        self.add_param(
            '--successful', '--ok',
            action='store_true', dest='successful', default=False,
            help="Select jobs with non-zero exit code."
            )
        self.add_param(
            '--submitted-after', metavar='DATE',
            help="Select jobs submitted after the specified date.",
            )
        self.add_param(
            '--submitted-before', metavar='DATE',
            help="Select jobs submitted before the specified date.",
            )
        self.add_param(
            '--unsuccessful', '--failed',
            action='store_true', dest='unsuccessful', default=False,
            help="Select jobs with non-zero exit code."
            )

    def parse_args(self):
        self.criteria = []

        # --successful, --unsuccessful
        if self.params.successful and self.params.unsuccessful:
            raise gc3libs.exceptions.InvalidUsage(
                " Please use only one of the two options:"
                " `--successful` or `--unsuccessful`.")

        if self.params.successful:
            self.criteria.append(
                (self.params.successful, self.filter_successful, ()))

        if self.params.unsuccessful:
            self.criteria.append(
                (self.params.unsuccessful, self.filter_unsuccessful, ()))

        # --jobname, --job-name
        if self.params.jobname:
            try:
                self.jobname_re = re.compile(self.params.jobname, re.I)
                self.criteria.append(
                    (True, self.filter_by_jobname, (self.jobname_re,)))
            except re.error as err:
                raise gc3libs.exceptions.InvalidUsage(
                    "Regexp `%s` for option `--job-name` is invalid: %s"
                    % (self.params.jobname, err))

        # --jobid, --job-id
        if self.params.jobid:
            try:
                self.jobid_re = re.compile(self.params.jobid, re.I)
                self.criteria.append(
                    (True, self.filter_by_jobid, (self.jobid_re,)))

            except re.error as err:
                raise gc3libs.exceptions.InvalidUsage(
                    "Regexp `%s` for option `--job-id` is invalid: %s"
                    % (self.params.jobid, err))

        # --state
        if self.params.states is not None:
            self.allowed_states = set(i.upper()
                                      for i in self.params.states.split(','))
            invalid = self.allowed_states.difference(Run.State)
            if invalid:
                raise gc3libs.exceptions.InvalidUsage(
                    "Invalid state(s): %s" % str.join(", ", invalid))

            self.criteria.append(
                (True, self.filter_by_state, (self.allowed_states,)))

        # --submitted-after, --submitted-before
        self.submission_start = None

        # NOTE: short-cut `if self.params.submitted:` will *not* work here, as
        # the empty string is a valid value -- only `None` indicates that the
        # option was not given
        if self.params.submitted_after is not None:
            if self.params.submitted_after == '':
                gc3libs.log.warning(
                    "Empty date as argument of --submitted-after will be"
                    " interpreted as 'now'")

            try:
                self.submission_start = time.mktime(
                    Calendar().parse(self.params.submitted_after)[0])
            except Exception as ex:
                raise gc3libs.exceptions.InvalidUsage(
                    "Invalid value `%s` for --submitted-after argument: %s"
                    % (self.params.submitted_after, str(ex)))

        else:  # no `--submitted-after` option
            # a starting date is always needed; if user did not specify one,
            # then choose the beginning of time
            self.submission_start = 0.0

        # NOTE: short-cut `if self.params.submitted_before:` will *not* work
        # here, as the empty string is a valid value -- only `None` indicates
        # that the option was not given
        if self.params.submitted_before is not None:
            if self.params.submitted_before == '':
                gc3libs.log.warning(
                    "Empty date as argument of --submitted-before will be"
                    " interpreted as 'now'")
            try:
                self.submission_end = time.mktime(
                    Calendar().parse(self.params.submitted_before)[0])
            except Exception as err:
                raise gc3libs.exceptions.InvalidUsage(
                    "Invalid value `%s` for --submitted-before argument: %s"
                    % (self.params.submitted_before, err))

        else:  # no `--submitted-before` option
            # an ending date is always needed; if user did not specify one,
            # then choose the end of (UNIX) time
            self.submission_end = float(sys.maxsize)

        self.criteria.append(
            (True, self.filter_by_submission_date,
             (self.submission_start, self.submission_end)))

        # --input-file
        if self.params.input_file:
            self.criteria.append(
                (self.params.input_file or self.params.output_file,
                 self.filter_by_iofile,
                 (self.params.input_file, self.params.output_file)))

        # --error-message
        if self.params.error_message:
            self.criteria.append(
                (self.params.error_message,
                 self.filter_by_errmsg,
                 (self.params.error_message,)))

        # --output-message
        if self.params.output_message:
            self.criteria.append(
                (self.params.output_message,
                 self.filter_by_outmsg,
                 (self.params.output_message,)))

    def main(self):
        self.session = self._get_session(self.params.session)

        # Load tasks from the store
        current_jobs = list(self.session.iter_workflow())

        # pipeline of checks to perform; more expensive checks should come last
        # so they look at less jobs (do I long for LISP? Oh yes I do...)
        for cond, fn, args in self.criteria:
            if cond:
                current_jobs = fn(current_jobs, *args)
                if not current_jobs:
                    break

        # Print remaining job IDs, if any
        if current_jobs:
            print(str.join('\n',
                           (str(job.persistent_id) for job in current_jobs)))
        else:
            gc3libs.log.info("No jobs match the specified conditions.")

    @staticmethod
    def _filter_by_regexp(job_list, regexp, attribute):
        """
        Return list of items in `job_list` whose attribute
        named `attribute` matches the supplied (non-anchored) regexp.

        Jobs that do not possess an attribute with the specified name do *not*
        match.
        """
        # I know this could be written in one line but it would be
        # quite harder to read.
        matching_jobs = []
        for job in job_list:
            try:
                attrvalue = getattr(job, attribute)
            except AttributeError:
                continue
            if regexp.search(attrvalue):
                matching_jobs.append(job)
        return matching_jobs

    @staticmethod
    def filter_by_exitcode(job_list, codes):
        """
        Return list of tasks in `job_list` whose exit code
        is one of the given codes.

        If `codes` is empty, then return `job_list` unchanged.
        """
        if codes:
            return [job for job in job_list if job.execution.exitcode in codes]
        else:
            return job_list

    @staticmethod
    def filter_by_errmsg(job_list, msg):
        """
        Return list of tasks in `job_list` such that string `msg` occurs in the
        STDERR log file.
        """
        return [job for job in job_list
                if (hasattr(job, 'output_dir')
                    and utils.occurs(
                        msg, os.path.join(
                            job.output_dir,
                            (job.stdout if job.join else job.stderr)
                        )))]

    @staticmethod
    def filter_by_iofile(job_list, ifile=None, ofile=None):
        """
        Return list of items in `job_list` such that the `ifile` matches the
        *base name* of any input file, *and* `ofile` matches the base name of
        any output file.  If either one of `ifile` or `ofile` is ``None``, the
        corresponding check is turned off.
        """
        matching_jobs = []
        for job in job_list:
            # generic `Task` objects might not have `.inputs` or `.outputs`,
            # but they will be freely mixed with `Application` objects in a
            # session,xxxx so ignore errors here
            try:
                # `Application.inputs` is a `UrlKeyDict`
                inputs = [os.path.basename(url.path) for url in job.inputs]
            except AttributeError as err:
                gc3libs.log.debug(
                    "No input file data in task %s: %s."
                    " I'm turning off input file checks for this task",
                    job, err)
                inputs = []
            try:
                # `Application.inputs` is a `UrlValueDict`, so keys
                # are simple paths
                outputs = [os.path.basename(file) for file in job.outputs]
            except AttributeError as err:
                gc3libs.log.debug(
                    "No output file data in task %s: %s."
                    " I'm turning off output file checks for this task",
                    job, err)
                outputs = []
            matching = True
            if ifile and ifile not in inputs:
                matching = False
            if ofile and ofile not in outputs:
                matching = False
            if matching:
                matching_jobs.append(job)
        return matching_jobs

    @staticmethod
    def filter_by_jobid(job_list, regexp):
        """
        Return list of items in `job_list` whose ID (i.e., the `.persistent_id`
        attribute) matches the supplied regexp.  Jobs that do not have a
        `.persistent_id` attribute do *not* match.
        """
        return cmd_gselect._filter_by_regexp(job_list, regexp, 'persistent_id')

    @staticmethod
    def filter_by_jobname(job_list, regexp):
        """
        Return list of items in `job_list` whose `.jobname` attribute
        matches the supplied regexp.  Jobs that do not have a
        `jobname` attribute do *not* match.

        """
        return cmd_gselect._filter_by_regexp(job_list, regexp, 'jobname')

    @staticmethod
    def filter_by_outmsg(job_list, msg):
        """
        Return list of tasks in `job_list` such that string `msg` occurs in the
        STDOUT log file.
        """
        return [job for job in job_list
                if (hasattr(job, 'output_dir')
                    and utils.occurs(
                        msg, os.path.join(job.output_dir, job.stdout)
                    ))]

    @staticmethod
    def filter_successful(job_list):
        """
        Return list of tasks from `job_list` that were successfully terminated.
        """
        return [job for job in job_list
                if job.execution.returncode == 0]

    @staticmethod
    def filter_by_state(job_list, states):
        """
        Return list of elements in `job_list` whose state
        is one of the given states.
        """
        return [job for job in job_list if job.execution.state in states]

    @staticmethod
    def filter_unsuccessful(job_list):
        """
        Return list of tasks from `job_list` that were unsuccessfully
        terminated.
        """
        return [job for job in job_list
                if job.execution.returncode is not None
                and job.execution.returncode != 0]

    @staticmethod
    def filter_by_submission_date(job_list, start, end):
        """
        Return list of items in `job_list` which have been
        submitted within the range specified by `start` and `end`.
        """
        matching_jobs = []
        for job in job_list:
            submission_time = job.execution.timestamp.get(
                Run.State.SUBMITTED, None)
            if submission_time is None:
                # Jobs run by the ShellCmd backend transition directly to
                # RUNNING; use that timestamp if available.
                #
                # Since jobs in NEW state will not have any timestamp
                # at all, set the default to 0.0.
                submission_time = job.execution.timestamp.get(
                    Run.State.RUNNING, 0.0)

            if (submission_time <= end
                    and submission_time >= start):
                    matching_jobs.append(job)
        return matching_jobs


class cmd_gcloud(GC3UtilsScript):
    """
`gcloud` manage VMs created by the EC2 backend

Usage:

    gcloud `command` [options]

commands are listed below, under `subcommands`.

To get detailed info on a specific command, run:

    gcloud `command` --help
    """

    def _add_subcmd(self, name, func, help=None):
        subparser = self.subparsers.add_parser(name, help=help)
        subparser.set_defaults(func=func)
        subparser.add_argument('-v', '--verbose', action='count')
        return subparser

    def setup(self):
        gc3libs.cmdline._Script.setup(self)

        self.subparsers = self.argparser.add_subparsers(
            title="subcommands",
            description="gcloud accept the following subcommands.")

        listparser = self._add_subcmd(
            'list',
            self.list_vms,
            help='List VMs currently know to the Cloud backend.')
        listparser.add_argument(
            '-r', '--resource', metavar="NAME", dest="resource_name",
            default=None, help="Select resource by name.")
        listparser.add_argument(
            '-n', '--no-update', action="store_false", dest="update",
            help="Do not update job and flavor information;"
            " only print what is in the local database.")

        cleanparser = self._add_subcmd(
            'cleanup',
            self.cleanup_vms,
            help='Terminate VMs not currently running any job.')
        cleanparser.add_argument(
            '-r', '--resource', metavar="NAME", dest="resource_name",
            default=None, help="Limit to VMs running on this resource.")
        cleanparser.add_argument(
            '-n', '--dry-run',
            action="store_true", dest="dry_run", default=False,
            help="Do not perform any actual killing;"
            " just print what VMs would be terminated.")

        terminateparser = self._add_subcmd(
            'terminate',
            self.terminate_vm,
            help='Terminate a VM.')
        terminateparser.add_argument('ID', help='ID of the VM to terminate.',
                                     nargs='+')
        terminateparser.add_argument(
            '-r', '--resource', metavar="NAME", dest="resource_name",
            default=None, help="Select resource by name.")

        forgetparser = self._add_subcmd(
            'forget',
            self.forget_vm,
            help="Remove the VM from the list of known VMs, so that the EC2"
            " backend will stop submitting jobs on it. Please note that if you"
            " `forget` a VM, it will disappear from the output of"
            " `gcloud list`.")
        forgetparser.add_argument('ID',
                                  help='ID of the VM to "forget".', nargs='+')
        forgetparser.add_argument(
            '-r', '--resource', metavar="NAME", dest="resource_name",
            default=None, help="Select resource by name.")

        runparser = self._add_subcmd(
            'run',
            self.create_vm,
            help='Run a VM.')
        runparser.add_argument(
            '-r', '--resource', metavar="NAME", dest="resource_name",
            default=None, help="Select resource by name.")
        runparser.add_argument(
            '-f', '--flavor', dest="instance_type",
            default=None, help="Select instance flavor.")
        runparser.add_argument(
            '-i', '--image-id', metavar="ID", dest="image_id", default=None,
            help="Select the image id to use, if different from the one "
            "specified in the configuration file.")

    def setup_args(self):
        # prevent GC3UtilsScript.setup_args() to add the default JOBID
        # non optional argument
        pass

    def main(self):
        resources = [res for res in self._core.get_resources()
                     if res.type.startswith('ec2') or
                     res.type.startswith('openstack')]
        if self.params.resource_name:
            resources = [res for res in resources
                         if res.name == self.params.resource_name]
            if not resources:
                raise RuntimeError(
                    'No Cloud resource found matching name `%s`.'
                    '' % self.params.resource_name)

        if not resources:
            raise RuntimeError('No Cloud resource found.')

        self.resources = resources

        return self.params.func()

    @staticmethod
    def _print_vms(vms, res, header=True):
        table = PrettyTable()
        table.border = True
        if header:
            table.field_names = ["resource", "id", "state", "IP Address",
                                 "other IPs", "Nr. of jobs", "Nr. of cores",
                                 "image", "keypair"]

        images = []
        for vm in vms:
            nr_remote_jobs = 'N/A'
            ncores = 'N/A'
            status = ''
            image_name = ''
            ips = []
            if vm.id in res.subresources:
                if res.subresources[vm.id].updated:
                    nr_remote_jobs = str(res.subresources[vm.id].user_run)
                    ncores = str(res.subresources[vm.id].max_cores)

            if res.type.startswith('ec2'):
                ips = [vm.public_dns_name, vm.private_ip_address]
                status = vm.state
                image_name = vm.image_id
            elif res.type.startswith('openstack'):
                ips = vm.networks.get('private', []) + \
                    vm.networks.get('public', [])
                status = vm.status
                if not images:
                    images.extend(res._get_available_images())

                image_names = [x for x in images if x.id == vm.image['id']]
                if image_names:
                    image_name = image_names[0].name
                else:
                    image_name = 'UNKNOWN'
            if vm.preferred_ip in ips:
                ips.remove(vm.preferred_ip)

            table.add_row((res.name, vm.id, status, vm.preferred_ip,
                           str.join(', ', ips), nr_remote_jobs, ncores,
                           image_name, vm.key_name))

        print(table)

    @staticmethod
    def list_vms_in_resource(res, lock, update):
            printed = 0
            if update:
                res.get_resource_status()
            else:
                res._connect()

            vms = res._vmpool.get_all_vms()
            # draw title to separate output from different resources
            title = "VMs running on Cloud resource `%s` of type" \
                    " %s" % (res.name, res.type)
            separator = ('=' * len(title))

            # Acquire the lock before printing
            lock.acquire()
            gc3libs.log.debug(
                "list_vms: Acquiring lock for resource %s", res.name)
            print('')
            print(separator)
            print(title)
            print(separator)
            print('')

            # draw table of VMs running on resource `res`
            if vms:
                cmd_gcloud._print_vms(vms, res)
                printed += len(vms)

            if not printed:
                print("  no known VMs are currently running on this resource.")
            gc3libs.log.debug(
                "list_vms: Releasing lock for resource %s", res.name)
            lock.release()

    # Subcommand methods
    def list_vms(self):
        lock = mp.Lock()

        pool = []
        for res in self.resources:
            gc3libs.log.debug(
                "Creating a new process to get status of resource %s",
                res.name)

            def _run_list_vms():
                return self.list_vms_in_resource(res, lock, self.params.update)
            worker = mp.Process(target=_run_list_vms)
            pool.append(worker)
            worker.start()

        for worker in pool:
            worker.join()
        return 0

    def cleanup_vms(self):
        for res in self.resources:
            res.get_resource_status()

            # walk list of VMs and kill the unused ones
            vms = res._vmpool.get_all_vms()
            if vms:
                for vm in vms:
                    if res.subresources[vm.id].has_running_tasks():
                        if self.params.dry_run:
                            print("No job running on VM `%s` of resource `%s`;"
                                  " would terminate it." % (vm.id, res.name))
                        else:
                            # no dry run -- the real thing!
                            gc3libs.log.info(
                                "No job running on VM `%s` of resource `%s`;"
                                " terminating it ...", vm.id, res.name)
                            self._terminate_vm(vm.id, res)

        return 0

    def _find_resources_by_running_vm(self, vmid):
        """
        Returns a list of resources that are currently running a VM
        with id `vmid`
        """
        matching_res = []
        for resource in self.resources:
            if vmid in resource._vmpool:
                matching_res.append(resource)
        return matching_res

    def _terminate_vm(self, vmid, res=None):
        if res is None:
            matching_res = self._find_resources_by_running_vm(vmid)
        else:
            matching_res = [res]
        if len(matching_res) > 1:
            raise LookupError(
                "VM with ID `%s` have been found on multiple resources. Please"
                " specify the resource by running `gcloud terminate` with the"
                " `-r` option.")
        elif not matching_res:
            raise LookupError(
                "VM with id `%s` not found." % (vmid))

        resource = matching_res[0]
        vm = resource._vmpool.get_vm(vmid)
        gc3libs.log.info("Terminating VM `%s` on resource `%s`" %
                         (vmid, resource.name))
        if resource.type.startswith('ec2'):
            vm.terminate()
        elif resource.type.startswith('openstack'):
            vm.delete()
        else:
            gc3libs.log.error("Unsupported cloud provider %s." % resource.type)
        resource._vmpool.remove_vm(vmid)

    def terminate_vm(self):
        for resource in self.resources:
            resource.get_resource_status()
        errors = 0
        for vmid in self.params.ID:
            try:
                self._terminate_vm(vmid)
            except LookupError as ex:
                gc3libs.log.warning(str(ex))
                errors += 1
        return errors

    def _forget_vm(self, vmid):
        matching_res = self._find_resources_by_running_vm(vmid)
        if len(matching_res) > 1:
            raise LookupError("VM with ID `%s` have been found on multiple "
                              "resources. Please specify the resource by "
                              "running `grun` with the `-r` option.")
        elif not matching_res:
            raise LookupError(
                "VM with id `%s` not found." % (vmid))

        resource = matching_res[0]
        resource._vmpool.remove_vm(vmid)

    def forget_vm(self):
        for resource in self.resources:
            # calling `_connect()` will load the list of available VMs
            resource._connect()
        errors = 0
        for vmid in self.params.ID:
            try:
                self._forget_vm(vmid)
            except LookupError as ex:
                gc3libs.log.warning(str(ex))
                errors += 1
        return errors

    def create_vm(self):
        if len(self.resources) > 1:
            raise RuntimeError("Please specify the resource where you want to "
                               "create the VM by supplying the `-r` option.")

        resource = self.resources[0]
        resource._connect()
        image_id = self.params.image_id or resource.image_id
        instance_type = self.params.instance_type or resource.instance_type
        vm = resource._create_instance(image_id, instance_type=instance_type)
        resource._vmpool.add_vm(vm)
        self._print_vms([vm], resource)
