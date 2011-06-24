#! /usr/bin/env python
#
"""
Driver script for performing an global optimization over the parameter space. 
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
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>, Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'



if __name__ == "__main__":
    import gpremium


# std module imports
import numpy as np
import os
import re
import shutil
import sys
import time

# optimizer import
from difEvo import differential_evolution_optimizer

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript
from gc3libs.dag import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

# import logger
import logbook

#import gc3libs.debug

path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
    sys.path.append(path2Pymods)

from forwardPremium import paraLoop_fp, GPremiumTaskMods
from supportGc3 import update_parameter_in_file
from pymods.support.support import rmFilesAndFolders

sys.path.append(os.path.join(os.path.dirname(__file__), '../generateResults/'))
from createOverviewTable_gc3 import createOverviewTable
from analyzeOverviewTable import oneCtryPair
from difEvoKenPrice import *

# verbosity levels for logbook
##CRITICAL = 6
##ERROR = 5
##WARNING = 4
##NOTICE = 3
##INFO = 2
##DEBUG = 1
##NOTSET = 0

class gParaSearchParallel(ParallelTaskCollection, paraLoop_fp, GPremiumTaskMods):
 
    def __init__(self, pathToExecutable, architecture, logger, baseDir, xVars, 
                 nPopulation, xVarsDom, solverVerb, output_dir = '/tmp', grid = None, **kw):
        # Remove all files in curPath
        curPath = os.getcwd()
        filesAndFolder = os.listdir(curPath)
        if 'gParaSearch.log' in filesAndFolder: # if another paraSearch was run in here before, clean up. 
            rmFilesAndFolders(curPath)      
        
        # Set up initial variables
        self.grid = grid
        self.executable = pathToExecutable
        self.architecture = architecture
        self.baseDir = baseDir
        self.log = logger
        self.verbosity = solverVerb
        self.jobname = 'evaluateSolverGuess'
        tasks = []
        self.xVars = xVars
        self.xVarsDom = xVarsDom.split()
        lowerBds = [self.xVarsDom[i] for i in range(len(self.xVarsDom)) if i % 2 == 0]
        upperBds = [self.xVarsDom[i] for i in range(len(self.xVarsDom)) if i % 2 == 1]
        self.domain = [ (x, y) for x in lowerBds for y in upperBds ]
        self.n = len(self.xVars)
        self.x = None
        self.iteration = 0
#        self.domain = [ (0.5, 0.9) ] * len(self.parameters)
        # Call differential_evolution_optimizer, where self (gParaSearchParallel) is sent as evaluator class. 
##        self.optimizer =  differential_evolution_optimizer(self, population_size = 5, # pop_size must be > 3
##                                                           n_cross = self.n,
##                                                           cr = 0.9, eps = 1e-12, 
##                                                           show_progress = True)
        S_struct = {}
        S_struct['I_NP']         = int(nPopulation)
        S_struct['F_weight']     = 0.85
        S_struct['F_CR']         = 1 
        S_struct['I_D']          = 1 
        S_struct['FVr_minbound'] = 0.5
        S_struct['FVr_maxbound'] = 0.9
        S_struct['I_bnd_constr'] = 0
        S_struct['I_itermax']    = 50
        S_struct['F_VTR']        = 1.e-3
        S_struct['I_strategy']   = 1
        S_struct['I_refresh']    = 1
        S_struct['I_plotting']   = 0
        
        deKenPrice(self, S_struct)

    def target(self, inParaCombos):
                   
        mySH = logbook.StreamHandler(stream = sys.stdout, level = 'DEBUG', format_string = '{record.message}', bubble = True)
        mySH.format_string = '{record.message}'
 #       mySH.push_application()
        myFH = logbook.FileHandler(filename = 'gParaSearch.log', level = 'DEBUG', bubble = True)
        myFH.format_string = '{record.message}'
#        myFH.push_application()   
        logger = logbook.Logger(name = 'target.log')
#        list(logbook.handlers.Handler.stack_manager.iter_context_objects()))[0].pop_application()
        try:
            stdErr = list(logbook.handlers.Handler.stack_manager.iter_context_objects())[0]
            stdErr.pop_application()
        except: 
            pass
        logger.handlers.append(mySH)
        logger.handlers.append(myFH)
        
        logger.debug('Entering target')
          
        ## Enter an iteration specific folder
        self.iteration += 1
        iterationFolder = os.path.join(os.getcwd(), 'Iteration-' + str(self.iteration))
        os.mkdir(iterationFolder)
        
        ## Establish vals vector
        vals = []
        nVariables = range(len(inParaCombos[0]))
        for ixVar in nVariables:
            varValString = ''
            for ixParaCombo, paraCombo in enumerate(inParaCombos):
                ### Should make more precise string conversion. 
                varValString += str(paraCombo[ixVar])
                if ixParaCombo < len(inParaCombos) - 1: 
                    varValString += ', '
            vals.append(varValString)
            
        # Check if EA or sigmaA are alone in the specified parameters. If so make diagnol adjustments
        if 'EA' in self.xVars and not 'EB' in self.xVars:
            variables = [ 'EA', 'EB' ]
            groups = [ '0', '0' ]
            groupRestrs = [ 'diagnol', 'diagnol' ]
            vals = vals * 2
            paraCombos = [  np.append(ele, ele) for ele in inParaCombos ]
        
        # Write a para.loop file to generate grid jobs
        para_loop = self.writeParaLoop(variables = variables, 
                                       groups = groups, 
                                       groupRestrs = groupRestrs, 
                                       vals = vals, 
                                       desPath = os.path.join(iterationFolder, 'para.loopTmp'))
        
        tasks = self.generateTaskList(para_loop, iterationFolder)
        ## Take list of tasks and potentially split into parts if requested. Could loop over whole block
        ParallelTaskCollection.__init__(self, self.jobname, tasks, self.grid)
        
##        self.wait()
        self.submit()
        curState = self._state()
        logger.info('state = %s' % curState)
        while curState != 'TERMINATED':
            self.progress()
            time.sleep(15)
            curState = self._state()
            logger.info('state = %s' % curState)
        print 'done submitting'
        taskStats = self.stats()
        keyList = taskStats.keys()
        keyList = [ key.lower() for key in keyList ]
        keyList.sort()
        for key in keyList:
            logger.info(key + '   ' + str(taskStats[key]))
#        logger.info(self.stats())
        
        ## Each line in the resulting table (overviewSimu) represents one paraCombo
        overviewTable = createOverviewTable(resultDir = iterationFolder, outFile = 'simulation.out', slUIPFile = 'slUIP.mat', 
                                            exportFileName = 'overviewSimu', sortTable = False, 
                                            logLevel = self.verbosity, logFile = 'overTableLog.log')

        result = oneCtryPair(tableIn = overviewTable, varsIn = variables, valsIn = paraCombos, 
                             targetVar = 'normDev', logLevel = self.verbosity, 
                             logFile = os.path.join(iterationFolder, 'oneCtryPairLog.log'))
        logger.info('returning result to solver')
        logger.handlers = []
        return result

    def print_status(self, mins,means,vector,txt):
        print txt,mins, means, list(vector)

    def generateTaskList(self, para_loop, iterationFolder):
        # Fill the task list
        tasks = []
        for jobname, substs in self.process_para_file(para_loop):
            self.log.debug("Job '%s' defined by substitutions: %s.",
                       jobname, substs)
            executable = os.path.basename(self.executable)
            # start the inputs dictionary with syntax: client_path: server_path
            inputs = { self.executable:executable }
            # make a "stage" directory where input files are collected on the client machine. 
            #path_to_stage_dir = self.make_directory_path(os.path.join(iterationFolder, 'NAME'), jobname)
            path_to_stage_dir = os.path.join(iterationFolder, jobname)
            # input_dir is cwd/jobname (also referred to as "stage" dir. 
            input_dir = path_to_stage_dir
            gc3libs.utils.mkdir(input_dir)
            prefix_len = len(input_dir) + 1
            # 1. files in the "initial" dir are copied verbatim
            self.getCtryParas(self.baseDir)
            self.fillInputDir( self.baseDir, input_dir)
            # 2. apply substitutions to parameter files
            for (path, changes) in substs.iteritems():
                for (var, val, index, regex) in changes:
                    update_parameter_in_file(os.path.join(input_dir, path),
                                             var, index, val, regex)
            # 3. build input file list
            for dirpath,dirnames,filenames in os.walk(input_dir):
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
            kwargs = {}
            kwargs['stdout'] = 'forwardPremiumOut.log'
            kwargs['join'] = True
            kwargs['output_dir'] = os.path.join(path_to_stage_dir, 'output')
            kwargs['requested_architecture'] = self.architecture
            kwargs['requested_cores'] = 1
            # hand over job to create
            tasks.append(Application('./' + executable, [], inputs, outputs, **kwargs)) 
        return tasks





class gParaSearch(SessionBasedScript):
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
        self.add_param("-NP", "--nPopulation", metavar="ARCH",
                       dest="nPopulation", default=5,
                       help="Population size for global optimizer")
        self.add_param("-xVars", "--xVars", metavar="ARCH",
                       dest="xVars", default = 'EA',
                       help="x variables over which to optimize")
        self.add_param("-xVarsDom", "--xVarsDom", metavar="ARCH",
                       dest="xVarsDom", default = '0.5 0.9',
                       help="Domain to sample x values from. Space separated list. ")
        self.add_param("-sv", "--solverVerb", metavar="ARCH",
                       dest="solverVerb", default = '0.5 0.9',
                       help="Separate verbosity level for the global optimizer ")

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
        jobname = 'globalOptimization'
        executable = os.path.basename(self.params.executable)
        inputs = { self.params.executable:executable }
        outputs = { 'output/':'' }
        kwargs = extra.copy()
        kwargs['stdout'] = 'forwardPremiumOut.log'
        kwargs['join'] = True
        path_to_stage_dir = '/home/benjamin/workspace/fpProj/model/results/minitest'
        kwargs['output_dir'] = os.path.join(path_to_stage_dir, 'output')
        kwargs['requested_architecture'] = self.params.architecture
        # hand over job to create
        # application.init signature: 
        # def __init__(self, executable, arguments, inputs, outputs, output_dir, **kw):
##        yield (jobname, Application,
##               ['./' + executable, [], inputs, outputs], kwargs)
        kwargs = {}
        # interface to the GC3Libs main functionality
        coreInstance = self._get_core(gc3libs.Default.CONFIG_FILE_LOCATIONS)
        kwargs['grid'] = coreInstance
        # Check if number of population coincides with desired cores
        if self.params.max_running < self.params.nPopulation:
            self.params.max_running = self.params.nPopulation
        
        
        yield (jobname, gParaSearchParallel, 
               [ self.params.executable, self.params.architecture, 
                 self.log, self.params.initial, self.params.xVars, 
                 self.params.nPopulation, self.params.xVarsDom, self.params.solverVerb], kwargs)



## run script

if __name__ == '__main__':
    gParaSearch().run()
    
    
    
    
    
    
    