#! /usr/bin/env python
#
"""
Driver script for running the `forwardPremium` application on SMSCG.
"""
# Copyright (C) 2011 University of Zurich. All rights reserved.
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
__version__ = '$Revision$'
__author__ = 'Benjamin Jonen <benjamin.jonen@bf.uzh.ch'
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'

# Call: 
# -x /home/benjamin/workspace/idrisk/model/bin/idRiskOut -b /home/benjamin/workspace/idrisk/model/base para.loop  -C 1 -N


# std module imports
import numpy as np
import os, sys
import shutil
import copy

# set some ugly paths
# export PYTHONPATH=$PYTHONPATH:/home/benjamin/workspace/idrisk/model/code
sys.path.append('/home/benjamin/workspace/idrisk/model/code')

# import personal libraries
path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
    sys.path.append(path2Pymods)
path2Src = os.path.join(os.path.dirname(__file__), '../src')
if not sys.path.count(path2Src):
    sys.path.append(path2Src)

# Remove all files in curPath if -N option specified. 
if __name__ == '__main__':    
    import sys
    if '-N' in sys.argv:
        import os, shutil
        path2Pymods = os.path.join(os.path.dirname(__file__), '../')
        if not sys.path.count(path2Pymods):
            sys.path.append(path2Pymods)
        from pymods.support.support import rmFilesAndFolders
        curPath = os.getcwd()
        filesAndFolder = os.listdir(curPath)
        if 'gc3IdRisk.csv' in filesAndFolder or 'idRiskParaSearch.csv' in filesAndFolder: # if another paraSearch was run in here before, clean up. 
            if 'para.loop' in os.listdir(os.getcwd()):
                shutil.copyfile(os.path.join(curPath, 'para.loop'), os.path.join('/tmp', 'para.loop'))
                rmFilesAndFolders(curPath)
                shutil.copyfile(os.path.join('/tmp', 'para.loop'), os.path.join(curPath, 'para.loop'))
                os.remove(os.path.join('/tmp', 'para.loop'))
            else: 
                rmFilesAndFolders(curPath)


# ugly workaround for Issue 95,
# see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import idRiskParaSearch


# superclasses
import forwardPremium

# personal libraries
import costlyOptimization
from createTable import createOverviewTable

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript
from gc3libs.dag import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

import gc3libs.debug

# logger
from pymods.support.support import wrapLogger
import pymods.support.support as support
from pymods.classes.tableDict import tableDict

from makePlots import momentPlots

# Set up logger
logger = wrapLogger(loggerName = 'idRiskParaSearchLogger', streamVerb = 'DEBUG', logFile = os.path.join(os.getcwd(), 'idRiskParaSearch.log'))


class solveParaCombination(SequentialTaskCollection):

    def __init__(self, substs, **sessionParas):

        logger.debug('entering solveParaCombination.__init__ for job %s' % sessionParas['jobname'])
        self.iter    = 0

        self.jobname = sessionParas['jobname']
        self.substs = substs

        self.sessionParas     = sessionParas
        self.pathToExecutable = sessionParas['pathToExecutable']
        self.architecture     = sessionParas['architecture']
        self.localBaseDir     = sessionParas['localBaseDir']
        
        self.paraFolder = os.path.join(os.getcwd(), sessionParas['jobname'])

        # First loop over beta
        solverParas = {}
        solverParas['xVars'] = ['beta']
        solverParas['xInitialParaCombo'] = np.array([[0.8], [1.1]])
        solverParas['targetVar'] = ['eR_b']
        solverParas['target_fx'] = [1.01]
        solverParas['plotting'] = False
        solverParas['convCrit'] = 1.e-4
        
        self.beta_task = idRiskParaSearchDriver(self.paraFolder, self.substs, solverParas, **sessionParas)

        SequentialTaskCollection.__init__(self, self.jobname, [self.beta_task])

        logger.debug('done gParaSearchDriver.__init__ for job %s' % sessionParas['jobname'])

    def __str__(self):
        return self.jobname

    def next(self, *args): 
        self.iter += 1
##        if self.beta_task.execution.returncode == 13:
##            logger.critical('beta failed. terminating para combo')
##            self.execution.returncode = 13
##            self.failed = True
##            return Run.State.TERMINATED
##        logger.debug('entering solveParaCombination.next in iteration %s' % self.iter)
##        self.changed = True
##        betaVal = self.beta_task.costlyOptimizer.best_x
##        self.substs['input/parameters.in'].append(('beta', betaVal, '(0,)', 'bar-separated'))
##        if self.iter == 1:
##            # then loop over wbar
##            self.kw['jobname'] = self.kw['jobname'] + '_' + 'beta' + '=' + str(self.beta_task.costlyOptimizer.best_x)
##            xVar = 'wBarLower'
##            xInitialGuess = [-0.15, 0.15]
##            targetVar = 'iBar_Shock0Agent0'
##            solverParas = {}
##            solverParas['plotting'] = False
##            solverParas['target_fx'] = -0.1
##            solverParas['convCrit'] = 1.e-4
##            self.wBarLower_task = idRiskParaSearchDriver(xVar, xInitialGuess, targetVar, self.paraFolder, self.pathToExecutable, self.architecture, self.localBaseDir, self.substs, solverParas, **self.kw)
##            self.add(self.wBarLower_task)
##        else:
##            if self.wBarLower_task.execution.returncode == 13:
##                logger.critical('beta failed. terminating para combo')
##                self.execution.returncode = 13
##                self.failed = True
##                return Run.State.TERMINATED
##            logger.debug('converged for beta and wBarLower for job %s' % self.kw['jobname'])
##            wBarTable = tableDict.fromTextFile(os.path.join(self.paraFolder, 'optimwBarLower', 'overviewSimu'), width = 20, prec = 10)
##            optimalRunTable = wBarTable.getSubset( np.abs(wBarTable['wBarLower'] - self.wBarLower_task.costlyOptimizer.best_x) < 1.e-7 )
##            optimalRunFile = open(os.path.join(self.paraFolder, 'optimalRun'), 'w')
##            print >> optimalRunFile, optimalRunTable
##            return Run.State.TERMINATED
        return Run.State.RUNNING


class idRiskParaSearchDriver(SequentialTaskCollection):
    '''
      Script that executes optimization/solving. Each self.next is one iteration in the solver. 
    '''

    def __init__(self, paraFolder, substs, solverParas, **sessionParas):

        logger.debug('entering gParaSearchDriver.__init__')

        self.jobname = sessionParas['jobname'] + 'driver'
        
        self.sessionParas     = sessionParas
        self.solverParas      = solverParas
        self.substs           = substs
 
        self.paraFolder       = paraFolder
        self.iter             = 0

        # create a subfolder for optim over xVar
        self.optimFolder = os.path.join(paraFolder, 'optim' + str(solverParas['xVars'][0]))
        gc3libs.utils.mkdir(self.optimFolder)

        self.costlyOptimizer = costlyOptimization.costlyOptimization(solverParas)
        
        self.xVars       = solverParas['xVars']
        self.xParaCombos = solverParas['xInitialParaCombo']
        self.targetVar   = solverParas['targetVar']

        self.evaluator = idRiskParaSearchParallel(self.xVars, self.xParaCombos, substs, self.optimFolder, solverParas , **sessionParas)

        initial_task = self.evaluator

        SequentialTaskCollection.__init__(self, self.jobname, [initial_task])

        logger.debug('done solveParaCombination.__init__')

    def __str__(self):
        return self.jobname

    def next(self, *args): 
        self.iter += 1
        logger.debug('entering idRiskParaSearchDriver.next in iteration %s for variables %s' % (self.iter, self.solverParas['xVars']))
        # sometimes next is called even though run state is terminated. In this case simply return. 
        if self.costlyOptimizer.converged:
            return Run.State.TERMINATED
        self.changed = True
        if  self.execution.state == 'TERMINATED':
            logger.debug('idRiskParaSearchDriver.next already terminated. Returning.. ')
            return Run.State.TERMINATED
        newVals = self.evaluator.target(self.xVars, self.xParaCombos, self.targetVar)
        if newVals is None:
            logger.critical('evaluating variable %s at guess %s failed' % (self.xVar, self.xGuess))	
            self.execution.returncode = 13
            self.failed = True
            return Run.State.TERMINATED
        self.costlyOptimizer.updateInterpolationPoints(self.xParaCombos, newVals)
        if not self.costlyOptimizer.checkConvergence():
            self.costlyOptimizer.updateApproximation()
            self.xGuess = self.costlyOptimizer.generateNewGuess()
            self.evaluator = idRiskParaSearchParallel(self.xVar, self.xGuess, self.targetVar, self.pathToExecutable, self.architecture, self.localBaseDir, self.substs, self.optimFolder, **self.kw)
            self.add(self.evaluator)
        else:
            self.execution.returncode = 0
            return Run.State.TERMINATED
        logger.debug('done idRiskParaSearchDriver.next in iteration %s for variable %s' % (self.iter, self.xVar))
        return Run.State.RUNNING




class idRiskParaSearchParallel(ParallelTaskCollection, forwardPremium.paraLoop_fp):
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
        forwardPremium.paraLoop_fp.__init__(self, verbosity = 'INFO')
        tasks = self.generateTaskList(xVars, paraCombos, substs, sessionParas)
        ParallelTaskCollection.__init__(self, self.jobname, tasks)

        logger.debug('done idRiskParaSearchParallel.__init__')

    def target(self, xVars, xParaCombos, targetVars):
        '''
          Method that builds an overview table for the jobs that were run and then returns the values. 
        '''
        logger.debug('entering idRiskParaSearchParallel.target. Computing target for xVar = %s, xVals = %s' % (xVars, xParaCombos))
        # Each line in the resulting table (overviewSimu) represents one paraCombo
##        if xVar == 'wBarLower' and xVals[0] == -0.10085714:
##            print 'here'
        overviewTable = createOverviewTable(resultDir = self.optimFolder, outFile = 'simulation.out', exportFileName = 'overviewSimu', sortCols = [], orderCols = [], verb = 'INFO')
        if overviewTable == None:
            logger.critical('overviewTable empty')
            os._exit(1)
        logger.info('table for job: %s' % self.jobname)
        logger.info(overviewTable)
        for ixVar, xVar in enumerate(xVars):
            if xVar == 'beta':
                xVars[ixVar] = 'beta_disc'
            else:
                xVars[ixVar] = xVar
        # Could replace this with a check that every xVar value is in the table, then output the two relevant columns.
        result = []
        for xParaCombo in xParaCombos:
            overviewTableSub = copy.deepcopy(overviewTable)        
            for xVar, xVal in zip(xVars, xParaCombo):
                overviewTableSub = overviewTableSub.getSubset( np.abs( overviewTableSub[xVar] - xVal ) < 1.e-8 )
            if len(overviewTableSub) == 0:
                logger.critical('Cannot find value for xVal %s, i.e. overviewTableSub empty' % xVal)
                return None
            elif len(overviewTableSub) == 1:
                result.append(np.linalg.norm(np.array([ overviewTableSub[targetVars[ixVar]][0] for ixVar, var in enumerate(xVars) ])))
            else:
                logger.critical('Cannot find unique value for xVal %s' % xVal)
                os._exit(1)
        logger.info('Computed target: Returning result to solver\n')
        return result
        #return result


    def print_status(self, mins,means,vector,txt):
        print txt,mins, means, list(vector)

    def generateTaskList(self, xVars, paraCombos, substs, sessionParas):
        pathToExecutable = sessionParas['pathToExecutable']
        localBaseDir     = sessionParas['localBaseDir']
        architecture     = sessionParas['architecture']
        jobname          = sessionParas['jobname']
        # Fill the task list
        tasks = []
        for paraCombo in paraCombos:
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
            self.fillInputDir(localBaseDir, path_to_stage_dir)
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
##            kwargs.pop('requested_memory')
##            kwargs.pop('requested_walltime')
##            kwargs.pop('requested_cores')
            kwargs['stdout'] = 'forwardPremiumOut.log'
            kwargs['join'] = True
            kwargs['output_dir'] = os.path.join(path_to_stage_dir, 'output')
            kwargs['requested_architecture'] = architecture

            # hand over job to create
            tasks.append(forwardPremium.GPremiumApplication('./' + executable, [], inputs, outputs, **kwargs)) 
        return tasks


class idRiskParaSearchScript(SessionBasedScript, forwardPremium.paraLoop_fp):
    """
      Read `.loop` files and execute the `forwardPremium` program accordingly.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.2',
            # only '.loop' files are considered as valid input
            input_filename_pattern = '*.loop',
        )
        forwardPremium.paraLoop_fp.__init__(self, verbosity = 'INFO')

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


    def parse_args(self):
        """
        Check validity and consistency of command-line options.
        """
        if not os.path.exists(self.params.executable):
            raise gc3libs.exceptions.InvalidUsage(
                "Path '%s' to the 'forwardPremium' executable does not exist;"
                " use the '-x' option to specify a valid one."
                % self.params.executable)
        if os.path.isdir(self.params.executable):
            self.params.executable = os.path.join(self.params.executable,
                                                  'forwardPremium')
        gc3libs.utils.test_file(self.params.executable, os.R_OK|os.X_OK,
                                gc3libs.exceptions.InvalidUsage)



    def new_tasks(self, extra):     

        paraLoopFile = self._search_for_input_files(self.params.args).pop()  # search_... returns set.

        # Copy base dir
        localBaseDir = os.path.join(os.getcwd(), 'localBaseDir')
        gc3libs.utils.copytree(self.params.initial, localBaseDir)


        for jobname, substs in self.process_para_file(paraLoopFile):
            # yield job            
            kwargs = {}
            sessionParas = {}
            sessionParas['pathToExecutable'] = self.params.executable
            sessionParas['architecture'] = self.params.architecture
            sessionParas['localBaseDir'] = localBaseDir
            sessionParas['jobname'] = jobname 
            yield (jobname, solveParaCombination, [ substs ], sessionParas)


if __name__ == '__main__':
    logger.info('Starting: \n%s' % ' '.join(sys.argv))
    idRiskParaSearchScript().run()
    logger.debug('combine resulting tables')    
    tableList = [ os.path.join(os.getcwd(), folder, 'optimalRun') for folder in os.listdir(os.getcwd()) if os.path.isdir(folder) and not folder == 'localBaseDir' and not folder == 'idRiskParaSearch.jobs' ]
    tableDicts = [ tableDict.fromTextFile(table, width = 20, prec = 10) for table in tableList if os.path.isfile(table)]
    if tableDicts:
        optimalRuns = tableDicts[0]
        for ixTable, table in enumerate(tableDicts):
            if ixTable == 0: continue
            optimalRuns = optimalRuns.getAppended(table)
        optimalRuns.order(['dy', 'beta_disc', 'wBarLower'])
        optimalRuns.sort(['dy'])
        logger.info(optimalRuns)
        f = open(os.path.join(os.getcwd(), 'optimalRuns'), 'w')  
        print >> f, optimalRuns
        f.flush()
    logger.info('Generating plot')
    baseName = 'moments'
    path = os.getcwd()
    conditions = {}
    overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
    tableFile = os.path.join(os.getcwd(), 'optimalRuns')
    figureFile = os.path.join(os.getcwd(), 'optimalRunsPlot.eps')
    momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay, conditions = conditions, tableFile = tableFile, figureFile = figureFile)
    logger.info('main done')


logger.info('done')