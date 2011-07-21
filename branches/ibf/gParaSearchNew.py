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
import datetime

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript
from gc3libs.dag import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

# import logger
import logbook
from supportGc3 import StatefulStreamHandler, StatefulFileHandler

#import gc3libs.debug

path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
    sys.path.append(path2Pymods)

from forwardPremium import paraLoop_fp, GPremiumApplication
from supportGc3 import update_parameter_in_file, getParameter, getIndex

from pymods.support.support import rmFilesAndFolders
from pymods.classes.tableDict import tableDict

sys.path.append(os.path.join(os.path.dirname(__file__), '../generateResults/'))
from createOverviewTable import createOverviewTable
from analyzeOverviewTable import anaOne4eachPair, anaOne4eachCtry, anaOne4all, nlcOne4eachCtry, nlcOne4eachPair, nlcOne4all
from difEvoKenPrice import *

# verbosity levels for logbook
##CRITICAL = 6
##ERROR = 5
##WARNING = 4
##NOTICE = 3
##INFO = 2
##DEBUG = 1
##NOTSET = 0

class gParaSearchDriver(SequentialTaskCollection):
    
    def __init__(self, pathToExecutable, pathToStageDir, architecture, baseDir, xVars, 
                 nPopulation, xVarsDom, solverVerb, problemType, pathEmpirical, 
                 itermax, xConvCrit, yConvCrit, 
                 makePlots, optStrategy, fWeight, fCritical, countryList, analyzeResults, nlc, 
                 output_dir = '/tmp', grid = None, **kw):
        
        self.pathToStageDir = pathToStageDir
        
        # set up logger
        self.mySH = StatefulStreamHandler(stream = sys.stdout, level = solverVerb.upper(), format_string = '{record.message}', bubble = True)
        self.mySH.format_string = '{record.message}'
        self.myFH = StatefulFileHandler(filename = os.path.join(self.pathToStageDir, 'gParaSearch.log'), level = 'DEBUG', bubble = True)
        self.myFH.format_string = '{record.message}' 
        self.logger = logbook.Logger(name = 'target.log')

        self.logger.handlers.append(self.mySH)
        self.logger.handlers.append(self.myFH)   

        try:
            stdErr = list(logbook.handlers.Handler.stack_manager.iter_context_objects())[0]
            stdErr.pop_application()
        except: 
            pass
        
        # Set up initial variables and set the correct methods. 
        self.problemType = problemType
        self.pathToExecutable = pathToExecutable
        self.architecture = architecture
        self.baseDir = baseDir
        self.verbosity = solverVerb.upper()
        self.jobname = 'iterationSolver'
        self.countryList = countryList.split()
        tasks = []
        self.xVars = xVars
        self.xVarsDom = xVarsDom.split()
        lowerBds = np.array([self.xVarsDom[i] for i in range(len(self.xVarsDom)) if i % 2 == 0], dtype = 'float64')
        upperBds = np.array([self.xVarsDom[i] for i in range(len(self.xVarsDom)) if i % 2 == 1], dtype = 'float64')
        self.domain = zip(lowerBds, upperBds)
        self.n = len(self.xVars.split())
        self.x = None
        self.analyzeResults = analyzeResults
          
        S_struct = {}
        S_struct['I_NP']         = int(nPopulation)
        S_struct['F_weight']     = float(fWeight)
        S_struct['F_CR']         = float(fCritical)
        S_struct['I_D']          = self.n
        S_struct['lowerBds']     = lowerBds
        S_struct['upperBds']     = upperBds
        S_struct['I_itermax']    = int(itermax)
        S_struct['F_VTR']        = float(yConvCrit)
        S_struct['I_strategy']   = int(optStrategy)
        S_struct['I_plotting']   = int(makePlots)
        S_struct['xConvCrit']    = float(xConvCrit)
        S_struct['workingDir']   = pathToStageDir
        S_struct['verbosity']    = self.verbosity

        # Initialize solver 
        self.deSolver = deKenPrice(S_struct) 
        
        # Set constraint function in solver. 
        self.deSolver.nlc = nlc
        
        # create initial task and register it
        self.deSolver.newPop = self.deSolver.drawInitialSample()
        
        self.deSolver.I_iter += 1
        self.evaluator = gParaSearchParallel(self.deSolver.newPop, self.deSolver.I_iter, self.pathToExecutable, self.pathToStageDir, 
                                             self.architecture, self.baseDir, self.xVars, self.verbosity, self.problemType, self.analyzeResults)
        
        initial_task = self.evaluator
        
        SequentialTaskCollection.__init__(self, self.jobname, [initial_task], grid)
        
        
    def __str__(self):
        return self.jobname
        
    def next(self, *args): 
        newVals = self.evaluator.target(self.deSolver.newPop)
        self.deSolver.updatePopulation(self.deSolver.newPop, newVals)
        # Stats for initial population: 
        self.deSolver.printStats()
        # make plots
        if self.deSolver.I_plotting:
            self.deSolver.plotPopulation(self.deSolver.FM_pop) 
        if not self.deSolver.checkConvergence():
            self.deSolver.newPop = self.deSolver.evolvePopulation(self.deSolver.FM_pop)
            # Check constraints and resample points to maintain population size. 
            self.deSolver.newPop = self.deSolver.enforceConstrReEvolve(self.deSolver.newPop)    
            self.deSolver.I_iter += 1
            self.evaluator = gParaSearchParallel(self.deSolver.newPop, self.deSolver.I_iter, self.pathToExecutable, self.pathToStageDir,  
                                             self.architecture, self.baseDir, self.xVars, self.verbosity, self.problemType, self.analyzeResults)
            self.add(self.evaluator)
        else: 
            return Run.State.TERMINATED
        return Run.State.RUNNING
        

class gParaSearchParallel(ParallelTaskCollection, paraLoop_fp):
 
    def __init__(self, inParaCombos, iteration, pathToExecutable, pathToStageDir, architecture, baseDir, xVars, 
                 solverVerb, problemType, analyzeResults, **kw):

        self.pathToStageDir = pathToStageDir
        # set up logger
        self.mySH = StatefulStreamHandler(stream = sys.stdout, level = solverVerb.upper(), format_string = '{record.message}', bubble = True)
        self.mySH.format_string = '{record.message}'
        self.myFH = StatefulFileHandler(filename = os.path.join(self.pathToStageDir, 'gParaSearch.log'), level = 'DEBUG', bubble = True)
        self.myFH.format_string = '{record.message}' 
        self.logger = logbook.Logger(name = 'target.log')

        self.logger.handlers.append(self.mySH)
        self.logger.handlers.append(self.myFH)   
    
        try:
            stdErr = list(logbook.handlers.Handler.stack_manager.iter_context_objects())[0]
            stdErr.pop_application()
        except: 
            pass
        
        # Set up initial variables and set the correct methods. 
        self.jobname = 'evaluateSolverGuess'
        self.problemType = problemType
        self.executable = pathToExecutable
        self.architecture = architecture
        self.baseDir = baseDir
        self.verbosity = solverVerb.upper()
        tasks = []
        self.xVars = xVars
        self.n = len(self.xVars.split())
        self.analyzeResults = analyzeResults        
        self.iteration = iteration

        # --- createJobs_x ---
        
        # Log activity
        cDate = datetime.date.today()
        cTime = datetime.datetime.time(datetime.datetime.now())
        dateString = '{0:04d}-{1:02d}-{2:02d}-{3:02d}-{4:02d}-{5:02d}'.format(cDate.year, cDate.month, cDate.day, cTime.hour, cTime.minute, cTime.second)
        #self.logger.debug('Entering target on %s' % dateString)
        
        # Enter an iteration specific folder
        self.iterationFolder = os.path.join(self.pathToStageDir, 'Iteration-' + str(self.iteration))

        os.mkdir(self.iterationFolder)
        
        # Establish vals vector
        vals = []
        nVariables = range(len(inParaCombos[0]))
        for ixVar in nVariables:
            varValString = ''
            for ixParaCombo, paraCombo in enumerate(inParaCombos):
                ### Should make more precise string conversion. 
                varValString += str(paraCombo[ixVar])
                if ixParaCombo < len(inParaCombos) - 1: 
                    varValString += ', '
            vals.append( [varValString ])
            
        # Check if EA or sigmaA are alone in the specified parameters. If so make diagnol adjustments
        writeVals = []
        if 'EA' in self.xVars and not 'EB' in self.xVars:
            variables = [ 'EA', 'EB' ]
            groups = [ '0', '0' ]
            groupRestrs = [ 'diagnol', 'diagnol' ]
            
            writeVals.append(vals[0][0])
            writeVals.append(vals[0][0])
            paraCombosEA = [  np.append(ele[0], ele[0]) for ele in inParaCombos ]
        if 'sigmaA' in self.xVars and not 'sigmaB' in self.xVars:
            variables.append( 'sigmaA')
            variables.append('sigmaB')
            groups.append( '0')
            groups.append('0')
            groupRestrs.append( 'diagnol')
            groupRestrs.append( 'diagnol' )
            writeVals.append(vals[1][0])
            writeVals.append(vals[1][0])
            paraCombosSigmaA = [  np.append(ele[1], ele[1]) for ele in inParaCombos ]
            
        self.variables = variables
            
        # Prepare paraCombos matching to resulting table. Used in analyzeOverviewTable
        # !!! This should be dependent on problem type or on missing variables in xvars. !!!
        paraCombos = []
        for EA,sA in zip(paraCombosEA, paraCombosSigmaA):
            paraCombo = np.append(EA, sA)
            paraCombos.append(paraCombo)
        self.paraCombos = paraCombos

        # Write a para.loop file to generate grid jobs
        para_loop = self.writeParaLoop(variables = variables, 
                                       groups = groups, 
                                       groupRestrs = groupRestrs, 
                                       vals = writeVals, 
                                       desPath = os.path.join(self.iterationFolder, 'para.loopTmp'))
        
        tasks = self.generateTaskList(para_loop, self.iterationFolder)
        ParallelTaskCollection.__init__(self, self.jobname, tasks)
        
        
        
    def target(self, inParaCombos):
        ## ---- ideally replace this block with self.wait(). At the moment there is a bug in dag.py. time module is used but not imported. 
        #self.submit()
        #curState = self._state()
        ##self.logger.info('state = %s' % curState)
        #while curState != 'TERMINATED':
            #self.progress()
            #time.sleep(15)
            #curState = self._state()
            ##self.logger.info('state = %s' % curState)
        #print 'done submitting'
        #taskStats = self.stats()
        #keyList = taskStats.keys()
        #keyList = [ key.lower() for key in keyList ]
        #keyList.sort()
        #for key in keyList:
            #self.logger.info(key + '   ' + str(taskStats[key]))
         ##--- 
        #self.wait()
        
        # Each line in the resulting table (overviewSimu) represents one paraCombo
        overviewTable = createOverviewTable(resultDir = self.iterationFolder, outFile = 'simulation.out', slUIPFile = 'slUIP.mat', 
                                            exportFileName = 'overviewSimu', sortTable = False, 
                                            logLevel = self.verbosity, logFile = 'overTableLog.log')

        result = self.analyzeResults(tableIn = overviewTable, varsIn = self.variables, valsIn = self.paraCombos, 
                             targetVar = 'normDev', logLevel = self.verbosity, 
                             logFile = os.path.join(self.iterationFolder, 'oneCtryPairLog.log'))
        #result = [ ele[0] for ele in inParaCombos ]
        self.logger.info('returning result to solver')
        self.iteration += 1
        return result
        

    def print_status(self, mins,means,vector,txt):
        print txt,mins, means, list(vector)

    def generateTaskList(self, para_loop, iterationFolder):
        # Fill the task list
        tasks = []
        for jobname, substs in self.process_para_file(para_loop):
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
            #self.getCtryParas(self.baseDir)
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
            #kwargs = extra.copy()
            kwargs = {}
            kwargs['stdout'] = 'forwardPremiumOut.log'
            kwargs['join'] = True
            kwargs['output_dir'] = os.path.join(path_to_stage_dir, 'output')
            kwargs['requested_architecture'] = self.architecture
            kwargs['requested_cores'] = 1
            # hand over job to create
            tasks.append(GPremiumApplication('./' + executable, [], inputs, outputs, **kwargs)) 
        return tasks

class gParaSearch(SessionBasedScript, paraLoop_fp):
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
        self.add_param("-t", "--problemType", metavar="ARCH",
                       dest="problemType", default = 'one4eachPair',
                       help="Problem type for gParaSearch. Must be one of: one4eachPair, one4all, one4eachCtry. ")
        self.add_param("-e", "--pathEmpirical", metavar="PATH",
                       dest="pathEmpirical", default = '',
                       help="Path to empirical analysis folder")
        self.add_param("-i", "--itermax", metavar="ARCH", type = int, 
                       dest="itermax", default = '50',
                       help="Maximum number of iterations of solver. ")
        self.add_param("-xC", "--xConvCrit", metavar="ARCH", type = float, 
                       dest="xConvCrit", default = '1.e-8',
                       help="Convergence criteria for x variables. ")
        self.add_param("-yC", "--yConvCrit", metavar="ARCH", type = float, 
                       dest="yConvCrit", default = '1.e-3',
                       help="Convergence criteria for y variables. ")
        self.add_param("-mP", "--makePlots", metavar="ARCH", type = bool, 
                       dest="makePlots", default = True,
                       help="Generate population plots each iteration.  ")
        self.add_param("-oS", "--optStrategy", metavar="ARCH", type = int, 
                       dest="optStrategy", default = '1',
                       help="Which differential evolution technique to use. ")
        self.add_param("-fW", "--fWeight", metavar="ARCH", type = float, 
                       dest="fWeight", default = '0.85',
                       help="Weight of differential vector. ")
        self.add_param("-fC", "--fCritical", metavar="ARCH", type = float,
                       dest="fCritical", default = '1.0',
                       help="Fraction of new population to use.  ")
        self.add_param("-cL", "--countryList", metavar="ARCH", type = str,
                       dest="countryList", default = 'AU UK',
                       help="List of countries to analyze. ")

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
        
        baseDir = self.params.initial
        xVarsDom = self.params.xVarsDom.split()
        lowerBds = np.array([xVarsDom[i] for i in range(len(xVarsDom)) if i % 2 == 0], dtype = 'float64')
        upperBds = np.array([xVarsDom[i] for i in range(len(xVarsDom)) if i % 2 == 1], dtype = 'float64')
        domain = zip(lowerBds, upperBds)
        countryList = self.params.countryList.split()
        
        ctryIndices = getIndex(base = [len(countryList), len(countryList)], restr = 'lowerTr')
        for ctryIndex in ctryIndices:
            print(countryList[ctryIndex[0]], countryList[ctryIndex[1]])
        
        # Make problem type specific adjustments. 
        if self.params.problemType == 'one4eachCtry':
            self.gdpTable = tableDict.fromTextFile(fileIn = os.path.join(self.params.pathEmpirical, 'outputInput/momentTable/Gdp/gdpMoments.csv'),
                                              delim = ',', width = 20)
            #self.analyzeResults = anaOne4eachCtry
          #  self.nlc = nlcOne4eachCtry

            print(self.gdpTable)
        elif self.params.problemType == 'one4eachPair':
            for ctryIndex in ctryIndices:
                Ctry1 = countryList[ctryIndex[0]]
                Ctry2 = countryList[ctryIndex[1]]
                print(Ctry1, Ctry2)
                jobname = Ctry1 + '-' + Ctry2
                # set stage dir. 
                path_to_stage_dir = self.make_directory_path(self.params.output, jobname)
                gc3libs.utils.mkdir(path_to_stage_dir)
                #path_to_stage_dir = os.path.join(iterationFolder, jobname)
                # Get moments table from empirical analysis
                gdpTable = tableDict.fromTextFile(fileIn = os.path.join(self.params.pathEmpirical, 'outputInput/momentTable/Gdp/gdpMoments.csv'),
                                                  delim = ',', width = 20)
                analyzeResults = anaOne4eachPair

                # Set Ctry information for this run. 
                update_parameter_in_file(os.path.join(baseDir, 'input/markovA.in'), 'Ctry',
                                          0,  Ctry1,  'space-separated')
                update_parameter_in_file(os.path.join(baseDir, 'input/markovB.in'), 'Ctry',
                                          0,  Ctry2,  'space-separated')
                # Get the correct Ctry Paras into base dir. 
                self.getCtryParas(baseDir, Ctry1, Ctry2)
                EA = getParameter(fileIn = os.path.join(baseDir, 'input/parameters.in'), varIn = 'EA', 
                                     regexIn = 'bar-separated')
                EB = getParameter(fileIn = os.path.join(baseDir, 'input/parameters.in'), varIn = 'EB', 
                                     regexIn = 'bar-separated')
                sigmaA = getParameter(fileIn = os.path.join(baseDir, 'input/parameters.in'), varIn = 'sigmaA', 
                                     regexIn = 'bar-separated')
                sigmaB = getParameter(fileIn = os.path.join(baseDir, 'input/parameters.in'), varIn = 'sigmaB', 
                                     regexIn = 'bar-separated')
                # Pass ctry information to nlc
                nlc = nlcOne4eachPair(gdpTable = gdpTable, ctryPair = [Ctry1, Ctry2], domain = domain)              
                
                executable = os.path.basename(self.params.executable)
                kwargs = extra.copy()
                kwargs['output_dir'] = path_to_stage_dir
                # Check if number of population coincides with desired cores
##                if self.params.max_running < self.params.nPopulation:
##                    self.params.max_running = self.params.nPopulation
                
                # yield job
                yield (jobname, gParaSearchDriver, 
                       [ self.params.executable, path_to_stage_dir, self.params.architecture, 
                         self.params.initial, self.params.xVars, 
                         self.params.nPopulation, self.params.xVarsDom, self.params.solverVerb, self.params.problemType,
                         self.params.pathEmpirical, self.params.itermax, self.params.xConvCrit, self.params.yConvCrit,
                         self.params.makePlots, self.params.optStrategy, self.params.fWeight, self.params.fCritical, self.params.countryList,
                         analyzeResults, nlc
                       ], kwargs)
        
        elif self.params.problemType == 'one4all':
            pass        
        
    




# run script

if __name__ == '__main__':    
    # Remove all files in curPath
    curPath = os.getcwd()
    filesAndFolder = os.listdir(curPath)
    if 'gParaSearchNew.csv' in filesAndFolder: # if another paraSearch was run in here before, clean up. 
        rmFilesAndFolders(curPath)  
    gParaSearch().run()
    print 'done'
    