#! /usr/bin/env python
#
"""
Driver script for running the `housing` application on SMSCG.
"""
# Copyright (C) 2011, 2012  University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.sjp
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>, Benjamin Jonen < benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """
"""
__docformat__ = 'reStructuredText'


## Calls:
## -x /home/benjamin/workspace/fpProj/model/bin/forwardPremiumOut -b ../base/ para.loop  -C 1 -N -X i686
## 5-x /home/benjamin/workspace/idrisk/bin/idRiskOut -b ../base/ para.loop  -C 1 -N -X i686
## -x /home/benjamin/workspace/idrisk/model/bin/idRiskOut -b ../base/ para.loop  -C 1 -N
## Need to set path to linux kernel and apppot, e.g.: export PATH=$PATH:~/workspace/apppot:~/workspace/

# Preliminary imports
from __future__ import absolute_import, print_function
import os, shutil

## Remove all files in curPath if -N option specified.
if __name__ == '__main__':
    import sys
    if '-N' in sys.argv:

        path2Pymods = os.path.join(os.path.dirname(__file__), '../')
        if not sys.path.count(path2Pymods):
            sys.path.append(path2Pymods)
        curPath = os.getcwd()
        filesAndFolder = os.listdir(curPath)
        if 'ghousing.log' in filesAndFolder:
            if 'para.loop' in os.listdir(os.getcwd()):
                shutil.copyfile(os.path.join(curPath, 'para.loop'), os.path.join('/tmp', 'para.loop'))
                shutil.rmtree(curPath)
                shutil.copyfile(os.path.join('/tmp', 'para.loop'), os.path.join(curPath, 'para.loop'))
            else:
                shutil.rmtree(curPath)


# ugly workaround for Issue 95,
# see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import ghousing

# std module imports
import numpy as np
import re
import sys
import time
import cli

from supportGc3 import lower, flatten, str2tuple, getIndex, extractVal, str2vals
from supportGc3 import format_newVal, update_parameter_in_file, safe_eval, str2mat, mat2str, getParameter

from housing import housingApplication, housingApppotApplication
from paraLoop import paraLoop

path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
    sys.path.append(path2Pymods)

from pymods.support.support import catFile
from pymods.support.support import wrapLogger
from pymods.classes.tableDict import tableDict

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.compat import lockfile
from gc3libs.cmdline import SessionBasedScript, existing_file
import gc3libs.utils
import gc3libs.application.apppot

#import gc3libs.debug

# import personal libraries
path2SrcPy = os.path.join(os.path.dirname(__file__), '../src')
if not sys.path.count(path2SrcPy):
    sys.path.append(path2SrcPy)
from plotSimulation import plotSimulation


### Temporary evil overloads

class Error(Exception):
    pass

class Abort(Error):
    """Raised when an application exits unexpectedly.

    :class:`Abort` takes a single integer argument indicating the exit status of
    the application.

    .. versionadded:: 1.0.4
    """

    def __init__(self, status):
        self.status = status
        message = "Application terminated (%s)" % self.status
        super(Abort, self).__init__(message, self.status)

def engineProgress(self):
    """
    Update state of all registered tasks and take appropriate action.
    Specifically:

      * tasks in `NEW` state are submitted;

      * the state of tasks in `SUBMITTED`, `RUNNING` or `STOPPED` state is updated;

      * when a task reaches `TERMINATING` state, its output is downloaded.

      * tasks in `TERMINATED` status are simply ignored.

    The `max_in_flight` and `max_submitted` limits (if >0) are
    taken into account when attempting submission of tasks.
    """
    # prepare
    currently_submitted = 0
    currently_in_flight = 0
    if self.max_in_flight > 0:
        limit_in_flight = self.max_in_flight
    else:
        limit_in_flight = utils.PlusInfinity()
    if self.max_submitted > 0:
        limit_submitted = self.max_submitted
    else:
        limit_submitted = utils.PlusInfinity()

    # update status of SUBMITTED/RUNNING tasks before launching
    # new ones, otherwise we would be checking the status of
    # some tasks twice...
    #gc3libs.log.debug("Engine.progress: updating status of tasks [%s]"
    #                  % str.join(', ', [str(task) for task in self._in_flight]))
    transitioned = []
    for index, task in enumerate(self._in_flight):
        try:
            self._core.update_job_state(task)
            if self._store and task.changed:
                self._store.save(task)
            state = task.execution.state
            if state == Run.State.SUBMITTED:
                # only real applications need to be counted
                # against the limit; policy tasks are exempt
                # (this applies to all similar clause below)
                if isinstance(task, Application):
                    currently_submitted += 1
                    currently_in_flight += 1
            elif state == Run.State.RUNNING:
                if isinstance(task, Application):
                    currently_in_flight += 1
            elif state == Run.State.STOPPED:
                transitioned.append(index) # task changed state, mark as to remove
                self._stopped.append(task)
            elif state == Run.State.TERMINATING:
                transitioned.append(index) # task changed state, mark as to remove
                self._terminating.append(task)
            elif state == Run.State.TERMINATED:
                transitioned.append(index) # task changed state, mark as to remove
                self._terminated.append(task)
        #except gc3libs.exceptions.ConfigurationError:
            ## Unrecoverable; no sense in continuing -- pass
            ## immediately on to client code and let it handle
            ## this...
            #raise
        except:
            gc3libs.log.debug('Error in updating task. Raising error. ')
            raise
    # remove tasks that transitioned to other states
    for index in reversed(transitioned):
        del self._in_flight[index]

    # execute kills and update count of submitted/in-flight tasks
    #gc3libs.log.debug("Engine.progress: killing tasks [%s]"
    #                  % str.join(', ', [str(task) for task in self._to_kill]))
    transitioned = []
    for index, task in enumerate(self._to_kill):
        try:
            old_state = task.execution.state
            self._core.kill(task)
            if self._store:
                self._store.save(task)
            if old_state == Run.State.SUBMITTED:
                if isinstance(task, Application):
                    currently_submitted -= 1
                    currently_in_flight -= 1
            elif old_state == Run.State.RUNNING:
                if isinstance(task, Application):
                    currently_in_flight -= 1
            self._terminated.append(task)
            transitioned.append(index)
        except Exception, x:
            gc3libs.log.error("Ignored error in killing task '%s': %s: %s"
                              % (task, x.__class__.__name__, str(x)),
                              exc_info=True)
    # remove tasks that transitioned to other states
    for index in reversed(transitioned):
        del self._to_kill[index]

    # update state of STOPPED tasks; again need to make before new
    # submissions, because it can alter the count of in-flight
    # tasks.
    #gc3libs.log.debug("Engine.progress: updating status of stopped tasks [%s]"
    #                  % str.join(', ', [str(task) for task in self._stopped]))
    transitioned = []
    for index, task in enumerate(self._stopped):
        try:
            self._core.update_job_state(task)
            if self._store and task.changed:
                self._store.save(task)
            state = task.execution.state
            if state in [Run.State.SUBMITTED, Run.State.RUNNING]:
                if isinstance(task, Application):
                    currently_in_flight += 1
                    if task.execution.state == Run.State.SUBMITTED:
                        currently_submitted += 1
                self._in_flight.append(task)
                transitioned.append(index) # task changed state, mark as to remove
            elif state == Run.State.TERMINATING:
                self._terminating.append(task)
                transitioned.append(index) # task changed state, mark as to remove
            elif state == Run.State.TERMINATED:
                self._terminated.append(task)
                transitioned.append(index) # task changed state, mark as to remove
        except Exception, x:
            gc3libs.log.error("Ignoring error in updating state of STOPPED task '%s': %s: %s"
                              % (task, x.__class__.__name__, str(x)),
                              exc_info=True)
        # except Exception, x:
        #     gc3libs.log.error("Ignoring error in updating state of STOPPED task '%s': %s: %s"
        #                       % (task, x.__class__.__name__, str(x)),
        #                       exc_info=True)
    # remove tasks that transitioned to other states
    for index in reversed(transitioned):
        del self._stopped[index]

    # now try to submit NEW tasks
    #gc3libs.log.debug("Engine.progress: submitting new tasks [%s]"
    #                  % str.join(', ', [str(task) for task in self._new]))
    transitioned = []
    if self.can_submit:
        index = 0
        while (currently_submitted < limit_submitted
               and currently_in_flight < limit_in_flight
               and index < len(self._new)):
            task = self._new[index]
            # try to submit; go to SUBMITTED if successful, FAILED if not
            if currently_submitted < limit_submitted and currently_in_flight < limit_in_flight:
                try:
                    self._core.submit(task)
                    if self._store:
                        self._store.save(task)
                    self._in_flight.append(task)
                    transitioned.append(index)
                    if isinstance(task, Application):
                        currently_submitted += 1
                        currently_in_flight += 1
                except Exception, x:
  #                  sys.excepthook(*sys.exc_info()) # DEBUG
                    import traceback
                    traceback.print_exc()
                    gc3libs.log.error("Ignored error in submitting task '%s': %s: %s"
                                      % (task, x.__class__.__name__, str(x)))
                    task.execution.history("Submission failed: %s: %s"
                                           % (x.__class__.__name__, str(x)))
                    #raise
                #except Exception:
                    #gc3libs.log.error("Ignored error in submitting task. BJ addition")
            index += 1
    # remove tasks that transitioned to SUBMITTED state
    for index in reversed(transitioned):
        del self._new[index]

    # finally, retrieve output of finished tasks
    #gc3libs.log.debug("Engine.progress: fetching output of tasks [%s]"
    #                  % str.join(', ', [str(task) for task in self._terminating]))
    if self.can_retrieve:
        transitioned = []
        for index, task in enumerate(self._terminating):
            # try to get output
            try:
                self._core.fetch_output(task)
            except gc3libs.exceptions.UnrecoverableDataStagingError, ex:
                gc3libs.log.error("Error in fetching output of task '%s',"
                                  " will mark it as TERMINATED"
                                  " (with error exit code %d): %s: %s",
                                  task, posix.EX_IOERR,
                                  ex.__class__.__name__, str(ex), exc_info=True)
                task.execution.returncode = (Run.Signals.DataStagingFailure,
                                             posix.EX_IOERR)
                task.execution.state = Run.State.TERMINATED
                task.changed = True
            #except Exception, x:
                #gc3libs.log.error("Ignored error in fetching output of task '%s': %s: %s"
                                  #% (task, x.__class__.__name__, str(x)), exc_info=True)
            if task.execution.state == Run.State.TERMINATED:
                self._terminated.append(task)
                self._core.free(task)
                transitioned.append(index)
            if self._store and task.changed:
                self._store.save(task)
        # remove tasks for which final output has been retrieved
        for index in reversed(transitioned):
            del self._terminating[index]

import gc3libs.core
gc3libs.core.Engine.progress = engineProgress



def pre_run(self):
    """
        Temporary overload for pre_run method of gc3libs.cmdline._Script.
    """
    import cli # pyCLI
    import cli.app
    import cli._ext.argparse as argparse
    from cli.util import ifelse, ismethodof
    import logging
    ## finish setup
    self.setup_options()
    self.setup_args()

    ## parse command-line
    cli.app.CommandLineApp.pre_run(self)

    ## setup GC3Libs logging
    loglevel = max(1, logging.ERROR - 10 * max(0, self.params.verbose - self.verbose_logging_threshold))
    gc3libs.configure_logger(loglevel, self.name)
    self.log = logging.getLogger('gc3.gc3utils') # alternate: ('gc3.' + self.name)
    self.log.setLevel(loglevel)

    self.log.propagate = True
    self.log.parent.propagate = False
    # Changed to false since we want to avoid dealing with the root logger and catch the information directly.

    from logging import getLogger
    from logbook.compat import redirect_logging
    from logbook.compat import RedirectLoggingHandler
#    redirect_logging() # does the same thing as adding a RedirectLoggingHandler... might as well be explicit
    self.log.parent.handlers = []
    self.log.parent.addHandler(RedirectLoggingHandler())
    print self.log.handlers
    print self.log.parent.handlers
    print self.log.root.handlers

    self.log.critical('Successfully overridden gc3pie error handling. ')

    # interface to the GC3Libs main functionality
    self._core = self._make_core()

    # call hook methods from derived classes
    self.parse_args()

logger = wrapLogger(loggerName = 'ghousing.log', streamVerb = 'INFO', logFile = os.path.join(os.getcwd(), 'ghousing.log'))
gc3utilsLogger = wrapLogger(loggerName = 'gc3ghousing.log', streamVerb = 'INFO', logFile = os.path.join(os.getcwd(), 'ghousing.log'),
                            streamFormat = '{record.time:%Y-%m-%d %H:%M:%S} - {record.channel}: {record.message}',
                            fileFormat = '{record.time:%Y-%m-%d %H:%M:%S} - {record.channel}: {record.message}')

def dispatch_record(record):
    """Passes a record on to the handlers on the stack.  This is useful when
    log records are created programmatically and already have all the
    information attached and should be dispatched independent of a logger.
    """
#    logbook.base._default_dispatcher.call_handlers(record)
    gc3utilsLogger.call_handlers(record)

import gc3libs.cmdline
#gc3libs.cmdline._Script.pre_run = pre_run
import logbook
#logbook.dispatch_record = dispatch_record


## custom application class


class ghousing(SessionBasedScript, paraLoop):
    """
Read `.loop` files and execute the `housingOut` program accordingly.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '1.0',
            input_filename_pattern = '*.loop',
            stats_only_for = Application,
        )
        paraLoop.__init__(self, 'INFO')

    def setup_options(self):
        self.add_param("-b", "--initial", metavar="DIR",
                       dest="initial",
                       help="Include directory contents in any job's input."
                       " Use this to specify the initial guess files.")
        self.add_param("-n", "--dry-run",
                       dest = 'dryrun', action="store_true", default = False,
                       help="Take the loop for a test run")
        self.add_param("-x", "--executable", metavar="PATH",
                       dest="executable", default=os.path.join(
                           os.getcwd(), "forwardPremiumOut"),
                       help="Path to the `idRisk` executable binary"
                       "(Default: %(default)s)")
        self.add_param("-X", "--architecture", metavar="ARCH",
                       dest="architecture", default=Run.Arch.X86_64,
                       help="Processor architecture required by the executable"
                       " (one of: 'i686' or 'x86_64', without quotes)")
        self.add_param("-A", "--apppot", metavar="PATH",
                       dest="apppot",
                       type=existing_file, default=None,
                       help="Use an AppPot image to run idRisk."
                       " PATH can point either to a complete AppPot system image"
                       " file, or to a `.changes` file generated with the"
                       " `apppot-snap` utility.")
        self.add_param("-rte", "--rte", action="store_true", default=None,
                       help="Use an AppPot image to run idRisk."
                       "use predeployed image")




    def parse_args(self):
        """
        Check validity and consistency of command-line options.
        """
        if not os.path.exists(self.params.executable):
            raise gc3libs.exceptions.InvalidUsage(
                "Path '%s' to the 'housingOut' executable does not exist;"
                " use the '-x' option to specify a valid one."
                % self.params.executable)
        if os.path.isdir(self.params.executable):
            self.params.executable = os.path.join(self.params.executable,
                                                  'housing')
        gc3libs.utils.check_file_access(self.params.executable, os.R_OK|os.X_OK,
                                gc3libs.exceptions.InvalidUsage)

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
        #except Exception, ex:
            #msg = "%s: %s" % (ex.__class__.__name__, str(ex))
            #if isinstance(ex, cli.app.Abort):
                #rc = (ex.status)
            #elif isinstance(ex, EnvironmentError):
                #rc = 74  # EX_IOERR in /usr/include/sysexits.h
            #else:
                ## generic error exit
                #rc = 1
        ## output error message and -maybe- backtrace...
        #try:
            #self.log.critical(msg,
                              #exc_info=(self.params.verbose > self.verbose_logging_threshold + 2))
        #except:
            ## no logging setup, output to stderr
            #sys.stderr.write("%s: FATAL ERROR: %s\n" % (self.name, msg))
            #if self.params.verbose > self.verbose_logging_threshold + 2:
                #sys.excepthook(* sys.exc_info())
        ## ...and exit
        #return 1


    #def run(self):
        #"""
        #Execute `cli.app.Application.run`:meth: if any exception is
        #raised, catch it, output an error message and then exit with
        #an appropriate error code.
        #"""

      ##  return cli.app.CommandLineApp.run(self)
        #import cli
        #try:
            #return cli.app.CommandLineApp.run(self)
        #except gc3libs.exceptions.InvalidUsage, ex:
            ## Fatal errors do their own printing, we only add a short usage message
            #sys.stderr.write("Type '%s --help' to get usage help.\n" % self.name)
            #return 64 # EX_USAGE in /usr/include/sysexits.h
        #except KeyboardInterrupt:
            #sys.stderr.write("%s: Exiting upon user request (Ctrl+C)\n" % self.name)
            #return 13
        #except SystemExit, ex:
            #return ex.code
        ## the following exception handlers put their error message
        ## into `msg` and the exit code into `rc`; the closing stanza
        ## tries to log the message and only outputs it to stderr if
        ## this fails
        #except lockfile.Error, ex:
            #exc_info = sys.exc_info()
            #msg = ("Error manipulating the lock file (%s: %s)."
                   #" This likely points to a filesystem error"
                   #" or a stale process holding the lock."
                   #" If you cannot get this command to run after"
                   #" a system reboot, please write to gc3pie@googlegroups.com"
                   #" including any output you got by running '%s -vvvv %s'.")
            #if len(sys.argv) > 0:
                #msg %= (ex.__class__.__name__, str(ex),
                        #self.name, str.join(' ', sys.argv[1:]))
            #else:
                #msg %= (ex.__class__.__name__, str(ex), self.name, '')
            #rc = 1
        #except AssertionError, ex:
            #exc_info = sys.exc_info()
            #msg = ("BUG: %s\n"
                   #"Please send an email to gc3pie@googlegroups.com"
                   #" including any output you got by running '%s -vvvv %s'."
                   #" Thanks for your cooperation!")
            #if len(sys.argv) > 0:
                #msg %= (str(ex), self.name, str.join(' ', sys.argv[1:]))
            #else:
                #msg %= (str(ex), self.name, '')
            #rc = 1
        #except NotImplementedError, nE:
            #pass

    def new_tasks(self, extra):
        # Generating new tasks for both paramter files
        self.logger.info('\ngenParamters.in: ')
        self.logger.info('-----------------')
        catFile(os.path.join(self.params.initial, 'input', 'genParameters.in'))
        self.logger.info('\nctryParameters.in: ')
        self.logger.info('-----------------')
        catFile(os.path.join(self.params.initial, 'input', 'ctryParameters.in'))
        self.logger.info('\npara.loop: ')
        self.logger.info('-----------------')
        catFile(os.path.join(os.getcwd(), 'para.loop'))
        self.logger.info('\nPress [y] to confirm the input files and continue execution of ghousing.py. Press [q] to exit')
        #import select
        #rlist, wlist, xlist = select.select([sys.stdin], [], [], None)
        selection = raw_input()
        if selection.lower() == 'q':
            self.logger.critical('Exiting upon user request...')
            os._exit(1)
        # setup AppPot parameters
        use_apppot = False
        #bug = use_apppot[0]
        apppot_img = None
        apppot_changes = None
        if self.params.apppot:
            use_apppot = True
            if self.params.apppot.endswith('.changes.tar.gz'):
                apppot_changes = self.params.apppot
            else:
                apppot_img = self.params.apppot

        inputs = self._search_for_input_files(self.params.args)

        # create a tar.gz archive of the code
        import tarfile
        tar = tarfile.open(os.path.join(os.getcwd(), 'codeBase.tar.gz'), "w:gz")
        for name in [self.params.initial, os.path.join(self.params.initial, '../code')]:
            tar.add(name)
        tar.close()

        # Copy base dir
        localBaseDir = os.path.join(os.getcwd(), 'localBaseDir')
        shutil.copytree(self.params.initial, localBaseDir)

        # update ctry Parameters. Important, before I do the para.loop adjustments
        ctryInParaLoop = False
        for para_loop in inputs:
            if os.path.isdir(para_loop):
                para_loop = os.path.join(para_loop, 'para.loop')
            paraLoopFile = open(para_loop, 'r')
            paraLoopFile.readline()
            for line in paraLoopFile:
                if not line.rstrip():
                    continue
                eles = line.split()
                var = eles[0]
                val = eles[6]
                if var == 'ctry':
                    ctryInParaLoop = True
                    ctry = val
        localBaseDirInputFolder = os.path.join(localBaseDir, 'input')
        genParametersFile = os.path.join(localBaseDirInputFolder, 'genParameters.in')
        ctryParametersFile = os.path.join(localBaseDirInputFolder, 'ctryParameters.in')
        updateCtryParametersFile = bool(getParameter(genParametersFile, 'updateCtryParametersFile', 'space-separated'))
        if not ctryInParaLoop:
            ctry = getParameter(genParametersFile, 'ctry', 'space-separated')
        if updateCtryParametersFile:
            shutil.copy(os.path.join(localBaseDirInputFolder, ctry + 'CtryParameters.in'), os.path.join(localBaseDirInputFolder, 'ctryParameters.in'))

        for para_loop in inputs:
            path_to_base_dir = os.path.dirname(para_loop)
#            self.log.debug("Processing loop file '%s' ...", para_loop)
            for jobname, substs in self.process_para_file(para_loop):
##                self.log.debug("Job '%s' defined by substitutions: %s.",
##                               jobname, substs)
                executable = os.path.basename(self.params.executable)
                inputs = { self.params.executable:executable }
                # make a "stage" directory where input files are collected
                path_to_stage_dir = self.make_directory_path(self.params.output, jobname)
                input_dir = path_to_stage_dir #os.path.join(path_to_stage_dir, 'input')
                gc3libs.utils.mkdir(input_dir)
                prefix_len = len(input_dir) + 1
                # 2. apply substitutions to parameter files
                for (path, changes) in substs.iteritems():
                    for (var, val, index, regex) in changes:
                        update_parameter_in_file(os.path.join(localBaseDir, path),
                                                 var, index, val, regex)
                fillInputDir(localBaseDir, input_dir)
                # 3. build input file list
                for dirpath,dirnames,filenames in os.walk(input_dir):
                    for filename in filenames:
                        # cut the leading part, which is == to path_to_stage_dir
                        relpath = dirpath[prefix_len:]
                        # ignore output directory contents in resubmission
                        if relpath. startswith('output'):
                            continue
                        remote_path = os.path.join(relpath, filename)
                        inputs[os.path.join(dirpath, filename)] = remote_path
                # all contents of the `output` directory are to be fetched
                outputs = { 'output/':'' }
                kwargs = extra.copy()
                kwargs['stdout'] = os.path.join('housingStdOut.log')
                kwargs['join'] = True
                kwargs['output_dir'] = os.path.join(path_to_stage_dir, 'output')
                kwargs['requested_architecture'] = self.params.architecture

#                print 'inputs = %s' % inputs
#                print 'outputs = %s' % outputs

#                kwargs.setdefault('tags', [ ])

                # adaptions for uml
                if self.params.rte:
                    kwargs['apppot_tag'] = 'ENV/APPPOT-0.26'
#                    kwargs['tags'] = ['TEST/APPPOT-IBF-1.0']
#                    kwargs['tags'] = ['TEST/APPPOT-IBF-1.1']
                    kwargs['tags'] = ['APPS/ECON/APPPOT-IBF-1.0']

                    cls = housingApppotApplication
                    pathToExecutable = '/home/user/job/' + executable
                elif use_apppot:
                    if apppot_img is not None:
                        kwargs['apppot_img'] = apppot_img
                    if apppot_changes is not None:
                        kwargs['apppot_changes'] = apppot_changes
                    cls = housingApppotApplication
                    pathToExecutable = '/home/user/job/' + executable
                else:
                    cls = housingApplication
                    pathToExecutable = executable

#                print 'kwargs = %s' % kwargs
                # hand over job to create
                yield (jobname, cls, [pathToExecutable, [], inputs, outputs], kwargs)

def fillInputDir(baseDir, input_dir):
    '''
      Copy folder /input and all files in the base dir to input_dir.
      This is slightly more involved than before because we need to
      exclude the markov directory which contains markov information
      for all country pairs.
    '''
    shutil.copytree(baseDir, input_dir)

def combinedThresholdPlot():
    import copy
    folders = [folder for folder in os.listdir(os.getcwd()) if os.path.isdir(folder) and not folder == 'localBaseDir' and not folder == 'ghousing.jobs']
    tableList = [ (folder, os.path.join(os.getcwd(), folder, 'output', 'ownershipThreshold_1.out')) for folder in folders ]
    tableDicts = dict([ (folder, tableDict.fromTextFile(table, width = np.max([len(folder) for folder in folders]) + 5, prec = 10)) for folder, table in tableList if os.path.isfile(table)])
    tableKeys = tableDicts.keys()
    tableKeys.sort()
    if tableDicts:
        for ixTable, tableKey in enumerate(tableKeys):
#            print tableKey
#            print tableDicts[tableKey]
            table = copy.deepcopy(tableDicts[tableKey])
            table.keep(['age', 'yst1'])
            table.rename('yst1', tableKey)
            if ixTable == 0:
                fullTable = copy.deepcopy(table)
            else:
                fullTable.merge(table, 'age')
                if '_merge' in fullTable.cols:
                    fullTable.drop('_merge')
                logger.info(fullTable)
        logger.info(fullTable)
        f = open(os.path.join(os.getcwd(), 'ownerThresholds'), 'w')
        print >> f, fullTable
        f.flush()
        ax = 3
        plotSimulation(table = os.path.join(os.getcwd(), 'ownerThresholds'), xVar = 'age', yVars = list(fullTable.cols), figureFile = os.path.join(os.getcwd(), 'ownerThresholds.png'), verb = 'CRITICAL' )

def combinedOwnerSimuPlot():
    import copy
    logger.debug('starting combinedOwnerSimuPlot')
    folders = [folder for folder in os.listdir(os.getcwd()) if os.path.isdir(folder) and not folder == 'localBaseDir' and not folder == 'ghousing.jobs']
    logger.debug('folders are %s ' % folders)
    tableList = [ (folder, os.path.join(os.getcwd(), folder, 'output', 'aggregate.out')) for folder in folders ]
    tableDicts = dict([ (folder, tableDict.fromTextFile(table, width = np.max([len(folder) for folder in folders]) + 5, prec = 10)) for folder, table in tableList if os.path.isfile(table)])
    tableKeys = tableDicts.keys()
    tableKeys.sort()
    if tableDicts:
        for ixTable, tableKey in enumerate(tableKeys):
            table = copy.deepcopy(tableDicts[tableKey])
            table.keep(['age', 'owner'])
            table.rename('owner', tableKey)
            if ixTable == 0:
                fullTable = copy.deepcopy(table)
            else:
                fullTable.merge(table, 'age')
                fullTable.drop('_merge')
            logger.info(fullTable)

        empOwnershipFile = os.path.join(os.getcwd(), 'localBaseDir', 'input', 'PSIDOwnershipProfilealleduc.out')
        empOwnershipTable = tableDict.fromTextFile(empOwnershipFile, width = 20, prec = 10)
        empOwnershipTable.rename('PrOwnership', 'empOwnership')
        fullTable.merge(empOwnershipTable, 'age')
        fullTable.drop('_merge')
        if fullTable:
            logger.info(fullTable)
            f = open(os.path.join(os.getcwd(), 'ownerSimu'), 'w')
            print >> f, fullTable
            f.flush()
        else:
            logger.info('no owner simus')
        logger.debug('done combinedOwnerSimuPlot')

        plotSimulation(table = os.path.join(os.getcwd(), 'ownerSimu'), xVar = 'age', yVars = list(fullTable.cols), yVarRange = (0., 1.), figureFile = os.path.join(os.getcwd(), 'ownerSimu.png'), verb = 'CRITICAL' )

def combineRunningTimes():
    folders = [folder for folder in os.listdir(os.getcwd()) if os.path.isdir(folder) and not folder == 'localBaseDir' and not folder == 'ghousing.jobs']
    runTimeFileList = [ (folder, os.path.join(os.getcwd(), folder, 'output', 'runningTime.out')) for folder in folders ]
    print runTimeFileList
    runTimes = {} # in minutes
    for folder, fle in runTimeFileList:
        if not os.path.isfile(fle): continue
        runningTimeFile = open(fle)
        lines = runningTimeFile.readlines()
        for line in lines:
            if line.find('Full running'):
                match = re.match('(.*=)([0-9\.\s]*)(.*)', line.rstrip()).groups()
                if not match:
                    runTimeSec = 0.
                else:
                    runTimeSec = float(match [1].strip()) # in seconds
        runTimes[folder] = runTimeSec / 60.
    logger.info('running times are \n %s' % runTimes)
    f = open(os.path.join(os.getcwd(), 'runTimes.out'), 'w')
    folderKeys = runTimes.keys()
    folderKeys.sort()
    for key in folderKeys:
        print >> f, '%s = %f12.1' % (key, runTimes[key])
    f.flush()
    f.close()

def getDateTimeStr():
    import datetime
    cDate = datetime.date.today()
    cTime = datetime.datetime.time(datetime.datetime.now())
    dateString = '%04d-%02d-%02d-%02d-%02d-%02d' % (cDate.year, cDate.month, cDate.day, cTime.hour, cTime.minute, cTime.second)
    return dateString


## run scriptfg


if __name__ == '__main__':
    #logger.info('Starting: \n%s' % ' '.join(sys.argv))
    logger.info('\n%s - Starting: \n%s' % (getDateTimeStr(), ' '.join(sys.argv)))
    os.system('cat ghousing.log')
    os.system('cd ~/workspace/housingProj/model/code && hg log > ' + os.path.join(os.getcwd(), 'hgLog'))
    #os._exit(1)
    ghousing().run()
    #from guppy import hpy
    #h = hpy()
    #print h.heap()
    # create overview plots across parameter combinations
    try:
        combinedThresholdPlot()
    except:
        logger.critical('problem creating combinedThresholdPlot. Investigate...')
    try:
        combinedOwnerSimuPlot()
    except:
        logger.critical('problem creating combinedOwnerSimuPlot. Investigate...')
    try:
        combineRunningTimes()
    except:
        logger.critical('problem creating combineRunningTimes. Investigate...')
    # some find commands to copy result graphs to a common directory.
    os.system("find -maxdepth 1 -type d -iregex './p.*' -exec bash -c 'x='{}' && mkdir -p ownerPlots && y=${x#./} && echo $y && cp ${y}/output/ownership_aggregate.out.png ownerPlots/${y}.png 2>/dev/null' \; ; ld ownerPlots/")
    os.system("find -maxdepth 1 -type d -iregex './p.*' -exec bash -c 'x='{}' && mkdir -p aggregatePlots && y=${x#./} && echo $y && cp ${y}/output/aggregate.png aggregatePlots/${y}.png' \; ; ld aggregatePlots/")
    os.system("find -maxdepth 1 -type d -iregex './p.*' -exec bash -c 'x='{}' && mkdir -p ownerBdry && y=${x#./} && echo $y && cp ${y}/output/ownerBdry1.png ownerBdry/${y}.png' \; ; ld ownerBdry")

    logger.info('main done')
