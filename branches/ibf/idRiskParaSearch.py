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

sys.path.append('/home/benjamin/workspace/idrisk/model/code/gPremiumScripts')
sys.path.append('/home/benjamin/workspace/idrisk/model/code/')

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

# Set up logger
logger = wrapLogger(loggerName = 'idRiskParaSearchLogger', streamVerb = 'DEBUG', logFile = os.path.join(os.getcwd(), 'idRiskParaSearch.log'))


np.seterr(over='raise')

class solveParaCombination(SequentialTaskCollection):

    def __init__(self, pathToExecutable, architecture, localBaseDir, substs, solverParas, **kw):

        logger.debug('entering solveParaCombination.__init__ for job %s' % kw['jobname'])

        self.jobname = kw['jobname'] + 'solveParaCombination'
        self.iter    = 0
        self.pathToExecutable = pathToExecutable
        self.architecture  = architecture
        self.localBaseDir = localBaseDir
        self.substs = substs
        self.solverParas = solverParas
        self.kw = kw
        self.paraFolder = os.path.join(os.getcwd(), kw['jobname'])

        # First loop over beta
        xVar = 'beta'
        xInitialGuess = [0.9, 0.99]
        targetVar = 'eR_b'
        solverParas = {}
        solverParas['plotting'] = False
        solverParas['target_fx'] = 1.01
        solverParas['convCrit'] = 1.e-4
        self.beta_task = idRiskParaSearchDriver(xVar, xInitialGuess, targetVar, self.paraFolder, self.pathToExecutable, self.architecture, self.localBaseDir, self.substs, solverParas, **self.kw)

        SequentialTaskCollection.__init__(self, self.jobname, [self.beta_task])

        logger.debug('done gParaSearchDriver.__init__ for job %s' % kw['jobname'])

    def __str__(self):
        return self.jobname

    def next(self, *args): 
	self.iter += 1
        logger.debug('entering solveParaCombination.next in iteration %s' % self.iter)
	self.changed = True
        logger.debug('check whether beta worked')
        if self.beta_task.failed == True:
            return Run.State.TERMINATED
        betaVal = self.beta_task.costlyOptimizer.best_x
        self.substs['input/parameters.in'].append(('beta', betaVal, '(0,)', 'bar-separated'))
        if self.iter == 1:
            # then loop over wbar
	    self.kw['jobname'] = self.kw['jobname'] + '_' + 'beta' + '=' + str(self.beta_task.costlyOptimizer.best_x)
            xVar = 'wBarLower'
            xInitialGuess = [-0.1, -0.04]
            targetVar = 'iBar_Shock0Agent0'
            solverParas = {}
            solverParas['plotting'] = False
            solverParas['target_fx'] = -0.1
            solverParas['convCrit'] = 1.e-4
            self.wBarLower_task = idRiskParaSearchDriver(xVar, xInitialGuess, targetVar, self.paraFolder, self.pathToExecutable, self.architecture, self.localBaseDir, self.substs, solverParas, **self.kw)
            self.add(self.wBarLower_task)
        else:
            if self.wBarLower_task.failed == True:
                return Run.State.TERMINATED
            logger.debug('converged for beta and wBarLower for job %s' % self.kw['jobname'])
       	    wBarTable = tableDict.fromTextFile(os.path.join(self.paraFolder, 'optimwBarLower', 'overviewSimu'), width = 20, prec = 10)
	    optimalRunTable = wBarTable.getSubset( np.abs(wBarTable['wBarLower'] - self.wBarLower_task.costlyOptimizer.best_x) < 1.e-7 )
            optimalRunFile = open(os.path.join(self.paraFolder, 'optimalRun'), 'w')
            print >> optimalRunFile, optimalRunTable
            return Run.State.TERMINATED
        return Run.State.RUNNING


class idRiskParaSearchDriver(SequentialTaskCollection):

    def __init__(self, xVar, xInitialGues, targetVar, paraFolder, pathToExecutable, architecture, localBaseDir, substs, solverParas, **kw):

        logger.debug('entering gParaSearchDriver.__init__')

        self.jobname = kw['jobname'] + 'driver'
 
        self.pathToExecutable = pathToExecutable
        self.architecture     = architecture
        self.localBaseDir     = localBaseDir
        self.substs           = substs
        self.kw               = kw
        self.xVar             = xVar
        self.xGuess           = xInitialGues
        self.targetVar        = targetVar
        self.paraFolder       = paraFolder
	self.iter             = 0
        self.failed           = False
        
        # create a subfolder for optim over xVar
        self.optimFolder = os.path.join(paraFolder, 'optim' + xVar)
        gc3libs.utils.mkdir(self.optimFolder)

        self.costlyOptimizer = costlyOptimization.costlyOptimization(solverParas)
   
        self.evaluator = idRiskParaSearchParallel(self.xVar, self.xGuess, self.targetVar, pathToExecutable, architecture, localBaseDir, substs, self.optimFolder, **kw)

        initial_task = self.evaluator

        SequentialTaskCollection.__init__(self, self.jobname, [initial_task])

        logger.debug('done solveParaCombination.__init__')

    def __str__(self):
        return self.jobname

    def next(self, *args): 
	self.iter += 1
        logger.debug('entering idRiskParaSearchDriver.next in iteration %s for variable %s' % (self.iter, self.xVar))
	# sometimes next is called even though run state is terminated. In this case simply return. 
	if self.costlyOptimizer.converged:
	    return Run.State.TERMINATED
        self.changed = True
        newVals = self.evaluator.target(self.xVar, self.xGuess)
        if newVals is None:
            logger.critical('optimizatio failed bc one job could not be finished')
            self.failed = True
            return Run.State.TERMINATED
                
        self.costlyOptimizer.updateInterpolationPoints(self.xGuess, newVals)
        if not self.costlyOptimizer.checkConvergence():
            self.costlyOptimizer.updateApproximation()
            self.xGuess = self.costlyOptimizer.generateNewGuess()
            self.evaluator = idRiskParaSearchParallel(self.xVar, self.xGuess, self.targetVar, self.pathToExecutable, self.architecture, self.localBaseDir, self.substs, self.optimFolder, **self.kw)
            self.add(self.evaluator)
        else:
            self.execution.returncode = 0
            return Run.State.TERMINATED
        return Run.State.RUNNING
        logger.debug('done idRiskParaSearchDriver.next in iteration %s for variable %s' % (self.iter, self.xVar))



class idRiskParaSearchParallel(ParallelTaskCollection, forwardPremium.paraLoop_fp):    

    def __str__(self):
        return self.jobname


    def __init__(self, xVar, xVals, targetVar, pathToExecutable, architecture, localBaseDir, substs, optimFolder, **kw):

        logger.debug('entering idRiskParaSearchParallel.__init__')

        self.jobname = 'evalSolverGuess' + '_' + kw['jobname'] + '-' + xVar + '=' + str(xVals)
        self.verbosity = 'DEBUG'
        self.kw = kw
        self.targetVar = targetVar
        self.optimFolder = optimFolder
        forwardPremium.paraLoop_fp.__init__(self, verbosity = self.verbosity)
        tasks = self.generateTaskList(xVar, xVals, pathToExecutable, architecture, kw['jobname'], localBaseDir, substs)
        ParallelTaskCollection.__init__(self, self.jobname, tasks)
	
	logger.debug('done idRiskParaSearchParallel.__init__')

    def target(self, xVar, xVals):

        logger.debug('entering idRiskParaSearchParallel.target. Computing target for xVar = %s, xVals = %s' % (xVar, xVals))
        # Each line in the resulting table (overviewSimu) represents one paraCombo
        overviewTable = createOverviewTable(resultDir = self.optimFolder, outFile = 'simulation.out', exportFileName = 'overviewSimu', sortCols = [], orderCols = [], verb = 'INFO')
        if overviewTable == None:
            logger.critical('overviewTable empty. Check if pythonpath is set. Seems like all jobs are failing. Exiting.. ')
            os._exit(1)
	logger.info('table for job: %s' % self.jobname)
        logger.info(overviewTable)
        if xVar == 'beta':
            xVarAdj = 'beta_disc'
        else:
            xVarAdj = xVar
        result = []
        # Could replace this with a check that every xVar value is in the table, then output the two relevant columns. 
        for xVal in xVals:
            overviewTableSub = overviewTable.getSubset( np.abs( overviewTable[xVarAdj] - xVal ) < 1.e-8 )
            if len(overviewTableSub)   == 0:
                logger.critical('job failed. Didnt get simulation.out')
                return None
            elif len(overviewTableSub) == 1:
                result.append(overviewTableSub[self.targetVar][0])
            elif len(overviewTableSub) > 1:
                result.append(overviewTableSub[self.targetVar][0])
                logger.critical('Cannot find unique value for xVal %s' % xVal)
#                os._exit(1)
        logger.info('Computed target: Returning result to solver\n')
        return result
        #return result


    def print_status(self, mins,means,vector,txt):
        print txt,mins, means, list(vector)

    def generateTaskList(self, xVar, xVals, pathToExecutable, architecture, jobname, localBaseDir, substs):
        # Fill the task list
        tasks = []
        for xVal in xVals:
            executable = os.path.basename(pathToExecutable)
            inputs = { pathToExecutable:executable }        
            # make a "stage" directory where input files are collected
            path_to_stage_dir = os.path.join(self.optimFolder, jobname + '_' + xVar + '=' + str(xVal))
            gc3libs.utils.mkdir(path_to_stage_dir)
            prefix_len = len(path_to_stage_dir) + 1
            # 2. apply substitutions to parameter files in local base dir
            for (path, changes) in substs.iteritems():
                for (var, val, index, regex) in changes:
                    support.update_parameter_in_file(os.path.join(localBaseDir, path), var, index, val, regex)
                # adjust xVar in parameter file
                index = 0
                regex = 'bar-separated'
                support.update_parameter_in_file(os.path.join(localBaseDir, path), xVar, 0, xVal, regex)
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
            kwargs = self.kw.copy()
            kwargs['jobname'] = self.jobname
            kwargs.pop('requested_memory')
            kwargs.pop('requested_walltime')
            kwargs.pop('requested_cores')
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
##        inputs = self._search_for_input_files(self.params.args)
        
##        # Copy base dir
##        localBaseDir = os.path.join(os.getcwd(), 'localBaseDir')
###        gc3libs.utils.copytree(self.params.initial, '/mnt/shareOffice/ForwardPremium/Results/sensitivity/wGridSize/dfs')
##        gc3libs.utils.copytree(self.params.initial, localBaseDir)

##        for path in inputs:
##            para_loop = path
##            path_to_base_dir = os.path.dirname(para_loop)
###            self.log.debug("Processing loop file '%s' ...", para_loop)
##            for jobname, substs in self.process_para_file(para_loop):
####                self.log.debug("Job '%s' defined by substitutions: %s.",
####                               jobname, substs)
##                executable = os.path.basename(self.params.executable)
##                inputs = { self.params.executable:executable }
##                # make a "stage" directory where input files are collected
##                path_to_stage_dir = self.make_directory_path(
##                    self.params.output, jobname, path_to_base_dir)
##                input_dir = path_to_stage_dir #os.path.join(path_to_stage_dir, 'input')
##                gc3libs.utils.mkdir(input_dir)
##                prefix_len = len(input_dir) + 1
##                # 2. apply substitutions to parameter files
##                for (path, changes) in substs.iteritems():
##                    for (var, val, index, regex) in changes:
##                        update_parameter_in_file(os.path.join(localBaseDir, path),
##                                                 var, index, val, regex)
##                self.fillInputDir(localBaseDir, input_dir)
##                # 3. build input file list
##                for dirpath,dirnames,filenames in os.walk(input_dir):
##                    for filename in filenames:
##                        # cut the leading part, which is == to path_to_stage_dir
##                        relpath = dirpath[prefix_len:]
##                        # ignore output directory contents in resubmission
##                        if relpath.startswith('output'):
##                            continue
##                        remote_path = os.path.join(relpath, filename)
##                        inputs[os.path.join(dirpath, filename)] = remote_path
##                # all contents of the `output` directory are to be fetched
##                outputs = { 'output/':'' }
##                kwargs = extra.copy()
##                kwargs['stdout'] = 'forwardPremiumOut.log'
##                kwargs['join'] = True
##                kwargs['output_dir'] = os.path.join(path_to_stage_dir, 'output')
##                kwargs['requested_architecture'] = self.params.architecture
##                # hand over job to create
##                yield (jobname, forwardPremium.GPremiumApplication,
##                       ['./' + executable, [], inputs, outputs], kwargs) 
        
        
        
        paraLoopFile = self._search_for_input_files(self.params.args).pop()  # search_... returns set.

        # Copy base dir
        localBaseDir = os.path.join(os.getcwd(), 'localBaseDir')
        gc3libs.utils.copytree(self.params.initial, localBaseDir)


        for jobname, substs in self.process_para_file(paraLoopFile):
            solverParas = {}
            solverParas['plotting'] = False
            solverParas['target_fx'] = -0.1
            solverParas['convCrit'] = 1.e-4
            # yield job            
            kwargs = {'jobname': jobname}
            xVar = 'beta'
            xVar = 'wBarLower'
            xInitialGuess = [0.9, 0.99]
            xInitialGuess = [-0.1, -0.04]
            targetVar = 'eR_b'
            targetVar = 'iBar_Shock0Agent0'
#            yield (jobname, idRiskParaSearchDriver, [ xVar, xInitialGuess, targetVar, self.params.executable, self.params.architecture, localBaseDir, substs, solverParas ], kwargs)
            yield (jobname, solveParaCombination, [ self.params.executable, self.params.architecture, localBaseDir, substs, solverParas ], kwargs)


if __name__ == '__main__':
    logger.info('Starting: \n%s' % ' '.join(sys.argv))
    idRiskParaSearchScript().run()
    # combine resulting tables
    tableList = [ os.path.join(os.getcwd(), folder, 'optimalRun') for folder in os.listdir(os.getcwd()) if os.path.isdir(folder) and not folder == 'localBaseDir' and not folder == 'idRiskParaSearch.jobs' ]
    tableDicts = [ tableDict.fromTextFile(table, width = 20, prec = 10) for table in tableList ]
    fullTable = tableDicts[0]
    for ixTable, table in enumerate(tableDicts):
        if ixTable == 0: continue
        fullTable = fullTable.getAppended(table)
    logger.info(fullTable)
    logger.info('main done')


print 'done'
