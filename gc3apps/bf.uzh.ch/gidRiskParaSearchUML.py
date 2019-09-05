#! /usr/bin/env python
#
"""
Driver script for running the `forwardPremium` application on SMSCG.
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
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__author__ = 'Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'

# Call:
# -x /home/benjamin/workspace/idrisk/model/bin/idRiskOut -b /home/benjamin/workspace/idrisk/model/base para.loop -xVars 'wBarLower' -xVarsDom '-0.5 -0.35' -targetVars 'iBar_Shock0Agent0' --makePlots False -target_fx -0.5 -yC 4.9e-3 -sv info -C 10 -N -A '/home/benjamin/apppot0+ben.diskUpd.img'


# std module imports
from __future__ import absolute_import, print_function
import numpy as np
import os
import sys
import time
import copy
import shutil

# set some ugly paths
# export PYTHONPATH=$PYTHONPATH:/home/benjamin/workspace/idrisk/model/code
# sys.path.append('/home/jonen/workspace/idrisk/model/code') # cannot use tilde here for home folder

# import personal libraries
path2Src = os.path.join(os.path.dirname(__file__), '../src')
if not sys.path.count(path2Src):
    sys.path.append(path2Src)

# Remove all files in curPath if -N option specified.
if __name__ == '__main__':
    if '-N' in sys.argv:
        path2Pymods = os.path.join(os.path.dirname(__file__), '../')
        if not sys.path.count(path2Pymods):
            sys.path.append(path2Pymods)
        curPath = os.getcwd()
        filesAndFolder = os.listdir(curPath)
        if 'gc3IdRisk.csv' in filesAndFolder or 'idRiskParaSearch.csv' in filesAndFolder or 'gidRiskParaSearchUML.csv' in filesAndFolder: # if another paraSearch was run in here before, clean up.
            if 'para.loop' in os.listdir(os.getcwd()):
                shutil.copyfile(os.path.join(curPath, 'para.loop'), os.path.join('/tmp', 'para.loop'))
                shutil.rmtree(curPath)
                shutil.copyfile(os.path.join('/tmp', 'para.loop'), os.path.join(curPath, 'para.loop'))
                os.remove(os.path.join('/tmp', 'para.loop'))
            else:
                shutil.rmtree(curPath)

# ugly workaround for Issue 95,
# see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gidRiskParaSearchUML

curFileName = os.path.splitext(os.path.basename(__file__))[0]
# superclasses
from idRisk import idRiskApplication, idRiskApppotApplication
from paraLoop import paraLoop

# personal libraries
import costlyOptimization
from createTable import createOverviewTable

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, existing_file
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

import gc3libs.application.apppot

import gc3libs.debug

# logger
from pymods.support.support import fillInputDir
from pymods.support.support import wrapLogger
import pymods.support.support as support
from pymods.classes.tableDict import tableDict

from makePlots import momentPlots

### Temporary evil overloads

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
        # except gc3libs.exceptions.ConfigurationError:
        #     # Unrecoverable; no sense in continuing -- pass
        #     # immediately on to client code and let it handle
        #     # this...
        #     raise
        except:
        #     pass
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
#                    sys.excepthook(*sys.exc_info()) # DEBUG
                    import traceback
                    traceback.print_exc()
                    gc3libs.log.error("Ignored error in submitting task '%s': %s: %s"
                                      % (task, x.__class__.__name__, str(x)))
                    task.execution.history("Submission failed: %s: %s"
                                           % (x.__class__.__name__, str(x)))
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
            except Exception, x:
                gc3libs.log.error("Ignored error in fetching output of task '%s': %s: %s"
                                  % (task, x.__class__.__name__, str(x)), exc_info=True)
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

def script__init__(self, **extra_args):
    """
temporary overload for _Script.__init__
    """
    # use keyword arguments to set additional instance attrs
    for k,v in extra_args.items():
        if k not in ['name', 'description']:
            setattr(self, k, v)
    # init and setup pyCLI classes
    if not extra_args.has_key('version'):
        try:
            extra_args['version'] = self.version
        except AttributeError:
            raise AssertionError("Missing required parameter 'version'.")
    if not extra_args.has_key('description'):
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
    cli.app.CommandLineApp.__init__(
        self,
        # remove the '.py' extension, if any
        name=os.path.splitext(os.path.basename(sys.argv[0]))[0],
        reraise = Exception,
        **extra_args
        )
    # provide some defaults
    self.verbose_logging_threshold = 0

import gc3libs.cmdline
gc3libs.cmdline._Script.__init__ = script__init__


def post_run(self, returned):
    """
    temporary overload for cli.app.Application.post_run
    """
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

    # Interpret the returned value in the same way sys.exit() does.
    if returned is None:
        returned = 0
    elif isinstance(returned, Abort):
        returned = returned.status
    elif isinstance(returned, self.reraise):
        raise
    else:
        try:
            returned = int(returned)
        except:
            returned = 1

    if self.exit_after_main:
        sys.exit(returned)
    else:
        return returned
import cli.app
cli.app.Application.post_run = post_run


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

# temporarily take out logging redirection
# #    from logging import getLogger
#  #   from logbook.compat import redirect_logging
#     from logbook.compat import RedirectLoggingHandler
# #    redirect_logging() # does the same thing as adding a RedirectLoggingHandler... might as well be explicit
#     self.log.parent.handlers = []
#     self.log.parent.addHandler(RedirectLoggingHandler())
#     print self.log.handlers
#     print self.log.parent.handlers
#     print self.log.root.handlers

 #   self.log.critical('redirected gc3 log to ' + curFileName + '.log.')

    # interface to the GC3Libs main functionality
    self._core = self._get_core()

    # call hook methods from derived classes
    self.parse_args()

logger = wrapLogger(loggerName = curFileName + '.log', streamVerb = 'INFO', logFile = os.path.join(os.getcwd(), curFileName + '.log'))
gc3utilsLogger = wrapLogger(loggerName = 'gc3' + curFileName  + '.log', streamVerb = 'INFO', logFile = os.path.join(os.getcwd(), curFileName + '.log'),
                            streamFormat = '{record.time:%Y-%m-%d %H:%M:%S} - {record.channel}: {record.message}',
                            fileFormat = '{record.time:%Y-%m-%d %H:%M:%S} - {record.channel}: {record.message}')
logger.debug('hello')

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
logbook.dispatch_record = dispatch_record


## custom application class




class solveParaCombination(SequentialTaskCollection):

    def __init__(self, substs, solverParas, **sessionParas):

        logger.debug('entering solveParaCombination.__init__ for job %s' % sessionParas['jobname'])
        self.iter    = 0

        self.jobname = sessionParas['jobname']
        self.substs = substs

        self.sessionParas     = sessionParas
        self.pathToExecutable = sessionParas['pathToExecutable']
        self.architecture     = sessionParas['architecture']
        self.localBaseDir     = sessionParas['localBaseDir']

        self.paraFolder = os.path.join(os.getcwd(), sessionParas['jobname'])

        self.wBarLower_task = idRiskParaSearchDriver(self.paraFolder, self.substs, solverParas, **sessionParas)

        SequentialTaskCollection.__init__(self, self.jobname, [self.wBarLower_task])

        logger.debug('done gParaSearchDriver.__init__ for job %s' % sessionParas['jobname'])

    def __str__(self):
        return self.jobname

    def next(self, *args):
        self.iter += 1
        if self.wBarLower_task.execution.returncode == 13:
            logger.critical('wBarLower failed. terminating para combo')
            self.execution.returncode = 13
            return Run.State.TERMINATED
        elif self.wBarLower_task.execution.returncode == 0:
            wBarTable = tableDict.fromTextFile(os.path.join(self.paraFolder, 'optimwBarLower', 'overviewSimu'), width = 20, prec = 10)
            optimalRunTable = wBarTable.getSubset( np.abs(wBarTable['wBarLower'] - self.wBarLower_task.costlyOptimizer.best_x) < 1.e-7 )
            optimalRunFile = open(os.path.join(self.paraFolder, 'optimalRun'), 'w')
            print >> optimalRunFile, optimalRunTable
            self.execution.returncode = 0
            return Run.State.TERMINATED
        else:
            print 'unknown return code'
            os._exit


class idRiskParaSearchDriver(SequentialTaskCollection):
    '''
      Script that executes optimization/solving. Each self.next is one iteration in the solver.
    '''

    def __init__(self, paraFolder, substs, solverParas, **sessionParas):

        logger.debug('entering gParaSearchDriver.__init__')

        self.jobname = sessionParas['jobname'] + 'driver'
        solverParas['jobname'] = self.jobname

        self.sessionParas     = sessionParas
        self.solverParas      = solverParas
        self.substs           = substs

        self.paraFolder       = paraFolder
        self.iter             = 0

        # create a subfolder for optim over xVar
        self.optimFolder = os.path.join(paraFolder, 'optim' + str(solverParas['xVars'][0]))
        self.solverParas['optimFolder'] = self.optimFolder
        gc3libs.utils.mkdir(self.optimFolder)


        self.costlyOptimizer = costlyOptimization.costlyOptimization(solverParas)

        self.xVars       = solverParas['xVars']
        self.xParaCombos = solverParas['xInitialParaCombo']
        self.targetVars   = solverParas['targetVars']
        self.target_fx   = solverParas['target_fx']

        self.evaluator = idRiskParaSearchParallel(self.xVars, self.xParaCombos, substs, self.optimFolder, solverParas , **sessionParas)

        initial_task = self.evaluator

        SequentialTaskCollection.__init__(self, self.jobname, [initial_task])

        logger.debug('done solveParaCombination.__init__')

    def __str__(self):
        return self.jobname

    def next(self, *args):
        self.iter += 1
        logger.debug('')
        logger.debug('entering idRiskParaSearchDriver.next in iteration %s for variables %s and paraCombo %s' % (self.iter, self.solverParas['xVars'], self.jobname))
        logger.debug('')
        # sometimes next is called even though run state is terminated. In this case simply return.
        if self.costlyOptimizer.converged:
            return Run.State.TERMINATED
        self.changed = True
        if  self.execution.state == 'TERMINATED':
            logger.debug('idRiskParaSearchDriver.next already terminated. Returning.. ')
            return Run.State.TERMINATED
        logger.debug('calling self.evaluator.target to get newVals')
        newVals = self.evaluator.target(self.xVars, self.xParaCombos, self.targetVars, self.target_fx)
        if newVals is None:
            logger.critical('')
            logger.critical('FAILURE: newVals is None. Evaluating variable %s at guess \n%s failed' % (self.xVars, self.xParaCombos))
            logger.critical('')
            self.execution.returncode = 13
            self.failed = True
            return Run.State.TERMINATED
        logger.debug('calling self.costlyOptimizer.updateInterpolationPoints to update list of points. ')
        returnCode = self.costlyOptimizer.updateInterpolationPoints(self.xParaCombos, newVals)
        if returnCode != 0:
            logger.critical('')
            logger.critical('FAILURE: Critcial error in updateInterpolationPoints. ')
            logger.critical('returnCode = %s' % returnCode)
            logger.critical('')
            self.execution.returncode = 13
            self.failed = True
            return Run.State.TERMINATED
        else:
            logger.debug('updateInterpolationPoints successful')
        logger.debug('calling self.costlyOptimizer to check convergence')
        if not self.costlyOptimizer.checkConvergence():
            logger.debug('not converged yet... ')
            logger.debug('calling self.costlyOptimizer.updateApproximation')
            self.costlyOptimizer.updateApproximation()
            logger.debug('calling self.costlyOptimizer.generateNewGuess')
            try:
                self.xParaCombos = self.costlyOptimizer.generateNewGuess()
            except FloatingPointError:
                logger.critical('')
                logger.critical('FAILURE: Critcial error in self.costlyOptimizer.generateNewGuess. ')
                logger.critical('cannot compute np.exp(self.gx(xMat)), bc of FoatingPointError. ')
                logger.critical('')
                self.execution.returncode = 13
                self.failed = True
                return Run.State.TERMINATED
            logger.debug('generating a new idRiskParaSearchParallel instance to evaluate new guess')
            self.evaluator = idRiskParaSearchParallel(self.xVars, self.xParaCombos, self.substs, self.optimFolder, self.solverParas, **self.sessionParas)
            self.add(self.evaluator)
        else:
            logger.debug('')
            logger.debug('SUCCESS: converged idRiskParaSearchDriver.next in iteration %s for variables %s. Returning exit code 0. ' % (self.iter, self.solverParas['xVars']))
            logger.debug('')
            self.execution.returncode = 0
            return Run.State.TERMINATED
        logger.debug('done idRiskParaSearchDriver.next in iteration %s for variables %s' % (self.iter, self.solverParas['xVars']))
        return Run.State.RUNNING




class idRiskParaSearchParallel(ParallelTaskCollection, paraLoop):
    '''
      When initialized generates a number of Applications that are distributed on the grid.
    '''

    def __str__(self):
        return self.jobname


    def __init__(self, xVars, paraCombos, substs, optimFolder, solverParas, **sessionParas):

        logger.debug('entering idRiskParaSearchParallel.__init__')
        # create jobname
        self.jobname = 'evalSolverGuess' + '_' + sessionParas['jobname']
        for paraCombo in paraCombos:
            self.jobname += str(paraCombo)

        self.substs       = substs
        self.optimFolder  = optimFolder
        self.solverParas  = solverParas
        self.sessionParas = sessionParas
        tasks = self.generateTaskList(xVars, paraCombos, substs, sessionParas)
        ParallelTaskCollection.__init__(self, self.jobname, tasks)

        logger.debug('done idRiskParaSearchParallel.__init__')

    def target(self, xVars, xParaCombos, targetVars, target_fx):
        '''
          Method that builds an overview table for the jobs that were run and then returns the values.
        '''
        logger.debug('entering idRiskParaSearchParallel.target. Computing target for xVar = %s, xVals = \n%s, targetVars = %s' % (xVars, xParaCombos, targetVars))
        # Each line in the resulting table (overviewSimu) represents one paraCombo
        overviewTable = createOverviewTable(resultDir = self.optimFolder, outFile = 'simulation.out', exportFileName = 'overviewSimu', sortCols = [], orderCols = [], verb = 'INFO')
        if overviewTable == None:
            logger.critical('overviewTable empty')
            return None
        xVars = copy.deepcopy(xVars)
        for ixVar, xVar in enumerate(xVars):
            if xVar == 'beta':
                xVars[ixVar] = 'beta_disc'
            else:
                xVars[ixVar] = xVar
        # print table
        #overviewTable.order(['dy'] + xVars)
        #overviewTable.sort(['dy'] + xVars)
        logger.info('table for job: %s' % self.jobname)
        logger.info(overviewTable)
        # Could replace this with a check that every xVar value is in the table, then output the two relevant columns.
        result = np.array([])
        for ixParaCombo, xParaCombo in enumerate(xParaCombos):
            overviewTableSub = copy.deepcopy(overviewTable)
            for xVar, xVal in zip(xVars, xParaCombo):
                overviewTableSub = overviewTableSub.getSubset( np.abs( overviewTableSub[xVar] - xVal ) < 1.e-8 )
            if len(overviewTableSub) == 0:
                logger.critical('Cannot find value for xVal %s, i.e. overviewTableSub empty. Did you set the pythonPath?' % xVal)
                result = np.append(result, None)
#                return None
            elif len(overviewTableSub) == 1:
                #result.append(np.linalg.norm(np.array([ overviewTableSub[targetVars[ixVar]][0] for ixVar, var in enumerate(xVars) ]) - target_fx))
                result = np.append(result, [ overviewTableSub[targetVars[ixVar]][0] for ixVar, var in enumerate(xVars) ])
                logger.info('found xParaCombo = %s, result = %s' % (xParaCombo, result[ixParaCombo]))
            else:
                logger.critical('Cannot find unique value for xVal %s' % xVal)
                os._exit(1)

        logger.info('result for variables %s: xParaCombos = %s. Values =  %s' % (xVars, xParaCombos, result))
        logger.info('done target')
        logger.info('')
        return result


    def print_status(self, mins,means,vector,txt):
        print txt,mins, means, list(vector)

    def generateTaskList(self, xVars, paraCombos, substs, sessionParas):
        # setup AppPot parameters
        use_apppot = False
        apppot_img = None
        apppot_changes = None
        apppot_file = sessionParas['AppPotFile']
        if apppot_file:
            # print 'apppot_file = %s' % apppot_file
            # os._exit(1)
            use_apppot = True
            if apppot_file.endswith('.changes.tar.gz'):
                apppot_changes = apppot_file
            else:
                apppot_img = apppot_file

        pathToExecutable = sessionParas['pathToExecutable']
        localBaseDir     = sessionParas['localBaseDir']
        architecture     = sessionParas['architecture']
        jobname          = sessionParas['jobname']
        rte              = sessionParas['rte']
        # Fill the task list
        tasks = []
        for paraCombo in paraCombos:
            logger.debug('paraCombo = %s' % paraCombo)
            executable = os.path.basename(pathToExecutable)
            inputs = { pathToExecutable:executable }
            # make a "stage" directory where input files are collected
            path_to_stage_dir = os.path.join(self.optimFolder, jobname)

            # 2. apply substitutions to parameter files in local base dir
            for (path, changes) in substs.iteritems():
                for (var, val, index, regex) in changes:
                    support.update_parameter_in_file(os.path.join(localBaseDir, path), var, index, val, regex)
                # adjust xVar in parameter file
                index = 0
                regex = 'bar-separated'
                for xVar, xVal in zip(xVars, paraCombo):
                    support.update_parameter_in_file(os.path.join(localBaseDir, path), xVar, 0, xVal, regex)
                    path_to_stage_dir += '_' + xVar + '=' + str(xVal)
            gc3libs.utils.mkdir(path_to_stage_dir)
            prefix_len = len(path_to_stage_dir) + 1
            # fill stage dir
            fillInputDir(localBaseDir, path_to_stage_dir)
            # 3. build input file list
            for dirpath,dirnames,filenames in os.walk(path_to_stage_dir):
                for filename in filenames:
                    # cut the leading part, which is == to path_to_stage_dir
                    relpath = dirpath[prefix_len:]
                    # ignore output directory contents in resubmission
                    if relpath.startswith('output'):
                        continue
                    remote_path = os.path.join(relpath, filename)
                    inputs[os.path.join(dirpath, filename)] = remote_path
            # all contents of the `output` directory are to be fetched
            outputs = { 'output/':'' }
            #kwargs = sessionParas.copy()
            kwargs = {}
            kwargs['jobname'] = self.jobname
            kwargs['stdout'] = 'idriskOut.log'
            kwargs['join'] = True
            kwargs['output_dir'] = os.path.join(path_to_stage_dir, 'output')
            kwargs['requested_architecture'] = architecture

            # adaptions for uml
            if use_apppot:
                if apppot_img is not None:
                    kwargs['apppot_img'] = apppot_img
                if apppot_changes is not None:
                    kwargs['apppot_changes'] = apppot_changes
                cls = idRiskApppotApplication
                callExecutable = '/home/user/job/' + executable
            elif rte:
                kwargs['apppot_tag'] = 'ENV/APPPOT-0.26'
                kwargs['tags'] = ['TEST/APPPOT-IBF-1.0']
                cls = idRiskApppotApplication
                callExecutable = '/home/user/job/' + executable
            else:
                cls = idRiskApplication
                callExecutable = './' + executable
            kwargs.setdefault('tags', [ ])
            print 'cls = %s' % cls
            print 'callExecutable = %s' % callExecutable
            print 'kwargs = %s' % kwargs

            # hand over job to create
            curApplication = cls(callExecutable, [], inputs, outputs, **kwargs)
            tasks.append(curApplication)
        return tasks


class idRiskParaSearchScript(SessionBasedScript, paraLoop):
    """
      Read `.loop` files and execute the `forwardPremium` program accordingly.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.2',
            # only '.loop' files are considered as valid input
            input_filename_pattern = '*.loop',
            stats_only_for = Application
#idRiskApppotApplication
#idRiskApplication,
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
                       help="Path to the `forwardPremium` executable binary"
                       "(Default: %(default)s)")
        self.add_param("-X", "--architecture", metavar="ARCH",
                       dest="architecture", default=Run.Arch.X86_64,
                       help="Processor architecture required by the executable"
                       " (one of: 'i686' or 'x86_64', without quotes)")
        self.add_param("-mP", "--makePlots", metavar="ARCH", type = bool,
                       dest="makePlots", default = True,
                       help="Generate population plots each iteration.  ")
        self.add_param("-xVars", "--xVars", metavar="ARCH",
                       dest="xVars", default = 'wBarLower',
                       help="x variables over which to optimize")
        self.add_param("-xVarsDom", "--xVarsDom", metavar="ARCH",
                       dest="xVarsDom", default = '0.5 0.9',
                       help="Domain to sample x values from. Space separated list. ")
        self.add_param("-targetVars", "--target_fx", metavar="ARCH",
                       dest="targetVars", default = '0.1',
                       help="Domain to sample x values from. Space separated list. ")
        self.add_param("-target_fx", "--target_fx", metavar="ARCH",
                       dest="target_fx", default = '0.1',
                       help="Domain to sample x values from. Space separated list. ")
        self.add_param("-sv", "--solverVerb", metavar="ARCH",
                       dest="solverVerb", default = 'DEBUG',
                       help="Separate verbosity level for the global optimizer ")
        self.add_param("-yC", "--yConvCrit", metavar="ARCH", type = float,
                       dest="convCrit", default = '1.e-2',
                       help="Convergence criteria for y variables. ")
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

#-x /home/benjamin/workspace/idrisk/model/bin/idRiskOut -b /home/benjamin/workspace/idrisk/model/base para.loop -xVars 'wBarLower' -xVarsDom '-0.2 0.2 ' -target_fx '-0.1' -yC '4.9e-2' -sv info  -C 10 -N

    def parse_args(self):
        """
        Check validity and consistency of command-line options.
        """
        if not os.path.exists(self.params.executable):
            raise gc3libs.exceptions.InvalidUsage(
                "Path '%s' to the 'idrisk' executable does not exist;"
                " use the '-x' option to specify a valid one."
                % self.params.executable)
        if os.path.isdir(self.params.executable):
            self.params.executable = os.path.join(self.params.executable,
                                                  'idrisk')
        gc3libs.utils.check_file_access(self.params.executable, os.R_OK|os.X_OK,
                                gc3libs.exceptions.InvalidUsage)


    def run(self):
        """
        Execute `cli.app.Application.run`:meth: if any exception is
        raised, catch it, output an error message and then exit with
        an appropriate error code.
        """

      #  return cli.app.CommandLineApp.run(self)
        from gc3libs.compat import lockfile
        import cli
        try:
            return cli.app.CommandLineApp.run(self)
        except gc3libs.exceptions.InvalidUsage, ex:
            # Fatal errors do their own printing, we only add a short usage message
            sys.stderr.write("Type '%s --help' to get usage help.\n" % self.name)
            return 64 # EX_USAGE in /usr/include/sysexits.h
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

    def new_tasks(self, extra):

        paraLoopFile = self._search_for_input_files(self.params.args).pop()  # search_... returns set.

        # Copy base dir
        localBaseDir = os.path.join(os.getcwd(), 'localBaseDir')
        shutil.copytree(self.params.initial, localBaseDir)

        for jobname, substs in self.process_para_file(paraLoopFile):
            # yield job
            sessionParas = {}
            sessionParas['AppPotFile'] = self.params.apppot
            sessionParas['pathToExecutable'] = self.params.executable
            sessionParas['architecture'] = self.params.architecture
            sessionParas['localBaseDir'] = localBaseDir
            sessionParas['jobname'] = jobname
            sessionParas['rte'] = self.params.rte
            # Compute domain
            xVarsDom = self.params.xVarsDom.split()
            xVarsDom = [ [ ele ] for ele in xVarsDom ]
            lowerBds = np.array([xVarsDom[i] for i in range(len(xVarsDom)) if i % 2 == 0], dtype = 'float64')
            upperBds = np.array([xVarsDom[i] for i in range(len(xVarsDom)) if i % 2 == 1], dtype = 'float64')
            domain = zip(lowerBds, upperBds)
            solverParas = {}
            solverParas['xVars'] = self.params.xVars.split()
#            solverParas['xInitialParaCombo'] = np.array([lowerBds, upperBds])
            solverParas['xInitialParaCombo'] = np.array(xVarsDom, dtype = 'float64')
#            print solverParas['xInitialParaCombo']
#            os._exit(1)
            solverParas['targetVars'] = self.params.targetVars.split()
            solverParas['target_fx'] = map(float, self.params.target_fx.split())
            solverParas['plotting'] = self.params.makePlots
            solverParas['convCrit'] = self.params.convCrit
            yield (jobname, solveParaCombination, [ substs, solverParas ], sessionParas)

#def extractLinspace(strIn):
    #import re
    #if re.match('\s*linspace', strIn):
        #(linSpacePart, strIn) = re.match('(\s*linspace\(.*?\)\s*)[,\s*]*(.*)', strIn).groups()
        #args = re.match('linspace\(([(0-9\.\s-]+),([0-9\.\s-]+),([0-9\.\s-]+)\)', linSpacePart).groups()
        #args = [ float(arg) for arg in args] # assume we always want float for linspace
        #linSpaceVec = np.linspace(args[0], args[1], args[2])
        #return linSpaceVec
    #else:
        #print 'cannot find linspace in string'
        #os._exit()


def combineTables():
    print 'start combineTables'
    tableList = [ os.path.join(os.getcwd(), folder, 'optimalRun') for folder in os.listdir(os.getcwd()) if os.path.isdir(folder) and not folder == 'localBaseDir' and not folder == 'idRiskParaSearch.jobs' ]
    logger.info(tableList)
    a = [ table for table in tableList ]
    print a
    tableDicts = [ tableDict.fromTextFile(table, width = 20, prec = 10) for table in tableList if os.path.isfile(table)]
    logger.info('tableDicts=')
    logger.info(tableDicts)
    logger.info('check if tableDicts empty')
    if tableDicts:
        optimalRuns = tableDicts[0]
        for ixTable, table in enumerate(tableDicts):
            if ixTable == 0: continue
            optimalRuns = optimalRuns.getAppended(table)
        #optimalRuns.order(['dy', 'wBarLower'])
        #optimalRuns.sort(['dy'])
        logger.info(optimalRuns)
        f = open(os.path.join(os.getcwd(), 'optimalRuns'), 'w')
        print >> f, optimalRuns
        f.flush()
    #logger.info('Generating plot')
    #baseName = 'moments'
    #path = os.getcwd()
    # conditions = {}
    # overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
    # tableFile = os.path.join(os.getcwd(), 'optimalRuns')
    # figureFile = os.path.join(os.getcwd(), 'optimalRunsPlot.eps')
    # momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay, conditions = conditions, tableFile = tableFile, figureFile = figureFile)

def getDateTimeStr():
    import datetime
    cDate = datetime.date.today()
    cTime = datetime.datetime.time(datetime.datetime.now())
    dateString = '%04d-%02d-%02d-%02d-%02d-%02d' % (cDate.year, cDate.month, cDate.day, cTime.hour, cTime.minute, cTime.second)
    return dateString


if __name__ == '__main__':
#    logger.critical('\n\nhere')
    logger.info('\n%s - Starting: \n%s' % (getDateTimeStr(), ' '.join(sys.argv)))
    time.sleep(2)
#    os._exit(1)
    idRiskParaSearchScript().run()

    logger.debug('combine resulting tables')
    combineTables()
    # tableList = [ os.path.join(os.getcwd(), folder, 'optimalRun') for folder in os.listdir(os.getcwd()) if os.path.isdir(folder) and not folder == 'localBaseDir' and not folder == 'idRiskParaSearch.jobs' ]
    # tableDicts = [ tableDict.fromTextFile(table, width = 20, prec = 10) for table in tableList if os.path.isfile(table)]
    # if tableDicts:
    #     optimalRuns = tableDicts[0]
    #     for ixTable, table in enumerate(tableDicts):
    #         if ixTable == 0: continue
    #         optimalRuns = optimalRuns.getAppended(table)
    #     #optimalRuns.order(['dy', 'wBarLower'])
    #     #optimalRuns.sort(['dy'])
    #     logger.info(optimalRuns)
    #     f = open(os.path.join(os.getcwd(), 'optimalRuns'), 'w')
    #     print >> f, optimalRuns
    #     f.flush()
    # #logger.info('Generating plot')
    # #baseName = 'moments'
    # #path = os.getcwd()
    # # conditions = {}
    # # overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
    # # tableFile = os.path.join(os.getcwd(), 'optimalRuns')
    # # figureFile = os.path.join(os.getcwd(), 'optimalRunsPlot.eps')
    # # momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay, conditions = conditions, tableFile = tableFile, figureFile = figureFile)
    logger.info('%s - main done' % (getDateTimeStr()))
