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




# Calls: -x /home/benjamin/workspace/fpProj/model/bin/forwardPremiumOut -b ../base/ para.loop  -C 1 -N -X i686
# -x /home/benjamin/workspace/idrisk/bin/idRiskOut -b ../base/ para.loop  -C 1 -N -X i686


# std module imports
import numpy as np
import os, sys
import shutil

# import personal libraries
path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
    sys.path.append(path2Pymods)

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
        if 'idRiskParaSearch.csv' in filesAndFolder: # if another paraSearch was run in here before, clean up. 
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
import forwardPremium.paraLoop
import forwardPremium.GPremiumApplication

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript
from gc3libs.dag import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

import gc3libs.debug

# logger
from pymods.support.support import wrapLogger

# Set up logger
#self.streamVerb = solverVerb
logger = wrapLogger(loggerName = 'idRiskParaSearchLogger', streamVerb = 'INFO', logFile = os.path.join(os.getcwd(), 'idRiskParaSearch.log'))

class idRiskParaSearchDriver(SequentialTaskCollection):

    def __init__(self, executable, architecture, jobname, localBaseDir, substs, **kw):

        logger.debug('entering gParaSearchDriver.__init__')


##        # Initialize solver 
##        self.deSolver = deKenPrice(S_struct) 

##        # Set constraint function in solver. 
##        self.deSolver.nlc = nlc

##        # create initial task and register it
##        if initialPop:
##            self.deSolver.newPop = np.loadtxt(initialPop, delimiter = '  ')
##        else:
##            self.deSolver.newPop = self.deSolver.drawInitialSample()

##        #self.deSolver.plotPopulation(self.deSolver.newPop)
##        self.deSolver.I_iter += 1
##        self.evaluator = gParaSearchParallel(self.deSolver.newPop, self.deSolver.I_iter, self.pathToExecutable, self.pathToStageDir, 
##                                             self.architecture, self.baseDir, self.xVars, self.verbosity, self.problemType, self.analyzeResults, 
##                                             self.ctryList, **self.kw)
        xVar = 'beta'
        xVals = [ 0.95, 0.96 ]
        self.evaluator = idRiskParaSearchParallel(xVar, xVals, executable, architecture, jobname, localBaseDir, substs, kw)

##        initial_task = self.evaluator

##        SequentialTaskCollection.__init__(self, self.jobname, [initial_task], grid)

        logger.debug('done gParaSearchDriver.__init__')

    def __str__(self):
        return self.jobname

    def next(self, *args): 
        logger.debug('entering gParaSearchDriver.next')

##        self.changed = True
##        newVals = self.evaluator.target(self.deSolver.newPop)
##        self.deSolver.updatePopulation(self.deSolver.newPop, newVals)
##        # Stats for initial population: 
##        self.deSolver.printStats() 
##        # make full overview table
##        self.combOverviews(runDir = self.pathToStageDir, tablePath = self.pathToStageDir)

##        # make plots
##        if self.deSolver.I_plotting:
##            self.deSolver.plotPopulation(self.deSolver.FM_pop) 
##            self.plot3dTable()

##        if not self.deSolver.checkConvergence():
##            self.deSolver.newPop = self.deSolver.evolvePopulation(self.deSolver.FM_pop)
##            # Check constraints and resample points to maintain population size. 
##            self.deSolver.newPop = self.deSolver.enforceConstrReEvolve(self.deSolver.newPop)    
##            self.deSolver.I_iter += 1
##            self.evaluator = gParaSearch.gParaSearchParallel(self.deSolver.newPop, self.deSolver.I_iter, self.pathToExecutable, self.pathToStageDir, 
##                                             self.architecture, self.baseDir, self.xVars, self.verbosity, self.problemType, self.analyzeResults, 
##                                             self.ctryList, **self.kw)
##            self.add(self.evaluator)
##        else: 
##            # post processing
##            if self.deSolver.I_plotting:
##                self.plot3dTable()

##            open(os.path.join(self.pathToStageDir, 'jobDone'), 'w')
##            # report success of sequential task
##            self.execution.returncode = 0
##            return Run.State.TERMINATED
##        return Run.State.RUNNING



class idRiskParaSearchParallel(ParallelTaskCollection, paraLoop_fp):    

    def __str__(self):
        return self.jobname


    def __init__(self, xVar, xVals, executable, architecture, jobname, localBaseDir, substs, **kw):

        logger.debug('entering gParaSearchParalell.__init__')


        forwardPremium.paraLoop_fp.__init__(self, verbosity = self.verbosity)
        tasks = self.generateTaskList(xVar, xVals, executable, architecture, jobname, localBaseDir, substs)
        ParallelTaskCollection.__init__(self, self.jobname, tasks)

##    def target(self, inParaCombos):

##        logger.debug('entering gParaSearchParallel.target')
##        # Each line in the resulting table (overviewSimu) represents one paraCombo
##        overviewTable = createOverviewTable(resultDir = self.iterationFolder, outFile = 'simulation.out', slUIPFile = 'slUIP.mat', 
##                                            exportFileName = 'overviewSimu', sortTable = False, 
##                                            logLevel = self.verbosity, logFile = os.path.join(self.pathToStageDir, 'createOverviewTable.log'))

##        result = self.analyzeResults(tableIn = overviewTable, varsIn = self.variables, valsIn = self.paraCombos, 
##                             targetVar = 'normDev', logLevel = self.verbosity, 
##                             logFile = os.path.join(self.iterationFolder, os.path.join(self.pathToStageDir, 'analyzeOverview.log')))
##        #result = [ ele[0] for ele in inParaCombos ]
##        logger.info('Computed target: Returning result to solver')
##        self.iteration += 1
##        return result


    def print_status(self, mins,means,vector,txt):
        print txt,mins, means, list(vector)

    def generateTaskList(self, xVar, xVals, executable, architecture, jobname, localBaseDir, substs):
        # Fill the task list
        tasks = []
        inputs = { executable:os.path.basename(executable) }
        # make a "stage" directory where input files are collected
        path_to_stage_dir = os.path.join(os.getcwd(), jobname + xVar)
        gc3libs.utils.mkdir(path_to_stage_dir)
        prefix_len = len(path_to_stage_dir) + 1
        for xVal in xVals:
            # 2. apply substitutions to parameter files in local base dir
            for (path, changes) in substs.iteritems():
                for (var, val, index, regex) in changes:
                    update_parameter_in_file(os.path.join(localBaseDir, path), var, index, val, regex)
                # adjust xVar in parameter file
                index = 0
                regex = 'bar-separated'
                update_parameter_in_file(os.path.join(localBaseDir, path), xVar, 0, xVal, regex)
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
            kwargs = extra.copy()
            kwargs['stdout'] = 'forwardPremiumOut.log'
            kwargs['join'] = True
            kwargs['output_dir'] = os.path.join(path_to_stage_dir, 'output')
            kwargs['requested_architecture'] = self.params.architecture

            # hand over job to create
            tasks.append(forwardPremium.GPremiumApplication('./' + executable, [], inputs, outputs, **kwargs)) 
        return tasks


class idRiskParaSearchScript(SessionBasedScript, paraLoop_fp):
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
        paraLoop_fp.__init__(self, verbosity = 'INFO')

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
##        if not os.path.exists(self.params.executable):
##            raise gc3libs.exceptions.InvalidUsage(
##                "Path '%s' to the 'forwardPremium' executable does not exist;"
##                " use the '-x' option to specify a valid one."
##                % self.params.executable)
##        if os.path.isdir(self.params.executable):
##            self.params.executable = os.path.join(self.params.executable,
##                                                  'forwardPremium')
##        gc3libs.utils.test_file(self.params.executable, os.R_OK|os.X_OK,
##                                gc3libs.exceptions.InvalidUsage)



    def new_tasks(self, extra):
        paraLoopFile = self._search_for_input_files(self.params.args).pop()  # search_... returns set.

        # Copy base dir
        localBaseDir = os.path.join(os.getcwd(), 'localBaseDir')
        gc3libs.utils.copytree(self.params.initial, localBaseDir)

        for jobname, substs in self.process_para_file(paraLoopFile):
            # yield job
            kwargs = {}
            yield (jobname, idRiskParaSearchDriver, [ self.params.executable, self.params.architecture, jobname, localBaseDir, substs ], kwargs)



if __name__ == '__main__':
    logger.info('Starting: \n%s' % ' '.join(sys.argv))
    idRiskParaSearchScript().run()
    logger.info('main done')


print 'done'