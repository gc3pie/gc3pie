#! /usr/bin/env python
#
"""
Driver script for performing an global optimization over the parameter space.
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
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>, Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'

# std module imports
import numpy as np
import os
import re
import shutil
import sys
import time
import datetime

# Calling syntax one4all:
# -b ~/workspace/fpProj/model/base/ -x ~/workspace/fpProj/model/bin/forwardPremiumOut -e ~/workspace/fpProj/empirical/ -C 16 -NP 8 -xVars 'EA sigmaA' -xVarsDom '0.8 0.94 0.004 0.006' -sv info -t one4all --itermax 1 --countryList 'AU DE' -J 1000 -X i686 -r localhost --norm "fpSqr2" --initialPop "" -N


# Remove all files in curPath if -N option specified.
if __name__ == '__main__':
    import sys
    if '-N' in sys.argv:
        import os
        path2Pymods = os.path.join(os.path.dirname(__file__), '../')
        if not sys.path.count(path2Pymods):
            sys.path.append(path2Pymods)
        curPath = os.getcwd()
        filesAndFolder = os.listdir(curPath)
        if 'gParaSearch.csv' in filesAndFolder: # if another paraSearch was run in here before, clean up.
            shutil.rmtree(curPath)

if __name__ == "__main__":
    import gParaSearch


# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

# import logger
import logbook

path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
    sys.path.append(path2Pymods)

from forwardPremium import paraLoop_fp, GPremiumApplication
from supportGc3 import update_parameter_in_file, getParameter, getIndex
from supportGc3 import getLogger, wrapLogger, emptyFun


from pymods.classes.tableDict import tableDict

sys.path.append(os.path.join(os.path.dirname(__file__), '../generateResults/'))
from createOverviewTable import createOverviewTable
from analyzeOverviewTable import anaOne4eachPair, anaOne4eachCtry, anaOne4all, nlcOne4eachCtry, nlcOne4eachPair, nlcOne4all, plotPopOne4eachCtry
import combineOverviews
from difEvoKenPrice import *

# verbosity levels for logbook
##CRITICAL = 6
##ERROR = 5
##WARNING = 4
##NOTICE = 3
##INFO = 2
##DEBUG = 1
##NOTSET = 0

# Set up logger
#self.streamVerb = solverVerb
logger = wrapLogger(loggerName = 'gParaSearchLogger', streamVerb = 'INFO', logFile = os.path.join(os.getcwd(), 'gParaSearch.log'))


class gParaSearchDriver(SequentialTaskCollection):

    def __init__(self, pathToExecutable, pathToStageDir, architecture, baseDir, xVars, initialPop,
                 nPopulation, domain, solverVerb, problemType, pathEmpirical,
                 itermax, xConvCrit, yConvCrit,
                 makePlots, optStrategy, fWeight, fCritical, ctryList, analyzeResults, nlc, plot3dTable, combOverviews,
                 output_dir = '/tmp', **extra_args):

        '''
          pathToExecutable: Path to main executable. 
          pathToStageDir  : Path to directory in which the action takes place. Usually set to os.getcwd(). 
          architecture    : Set the architecture used. 
          baseDir         : Directory in which the input files are assembled. This directory is sent as input to the cluseter. 
          xVars           : Names of the x variables.
          initialPop      : The initial population if recovering from earlier failure. 
          nPopulation     : Population size. 
          domain          : The domain of the x variables. List of (lowerbound, upperbound) tuples. 
          solverVerb      : Verbosity of the solver. 
          problemType     : The problem type of the forward premium optimization. One4all, one4each etc. 
          pathEmpirical   : Path to the empirical results of the forward premium project. 
          itermax         : Maximum # of iterations of the solver. 
          xConvCrit       : Convergence criteria for x variables. 
          yConvCrit       : Convergence criteria for the y variables. 
          makePlots       : Make plots? 
          optStrategy     : The kind of differential evolution strategy to use. 
          fWeight         : 
          fCritical       : 
          ctryList        : The list of ctrys analyzed. 
          analyzeResults  : Function to analyze the output retrieved from the servers. 
          nlc             : Constraint function. 
          plot3dTable     : Function to generate 3d plots. 
          combOverviews   : Function to combine overviews. 
        '''

        logger.debug('entering gParaSearchDriver.__init__')

        # Set up initial variables and set the correct methods.
        self.pathToStageDir = pathToStageDir
        self.problemType = problemType
        self.pathToExecutable = pathToExecutable
        self.architecture = architecture
        self.baseDir = baseDir
        self.verbosity = solverVerb.upper()
        self.jobname = extra_args['jobname']
        self.ctryList = ctryList.split()
        self.xVars = xVars
        self.domain = domain
        self.n = len(self.xVars.split())
        self.extra_args = extra_args
        self.analyzeResults = analyzeResults
        self.plot3dTable    = plot3dTable
        self.combOverviews = combOverviews

        # Set solver options
        S_struct = {}
        S_struct['I_NP']         = int(nPopulation)
        S_struct['F_weight']     = float(fWeight)
        S_struct['F_CR']         = float(fCritical)
        S_struct['I_D']          = self.n
        S_struct['lowerBds']     = np.array([ element[0] for element in domain ], dtype = 'float64')
        S_struct['upperBds']     = np.array([ element[1] for element in domain ], dtype = 'float64')
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
        if initialPop:
            self.deSolver.newPop = np.loadtxt(initialPop, delimiter = '  ')
        else:
            self.deSolver.newPop = self.deSolver.drawInitialSample()

        #self.deSolver.plotPopulation(self.deSolver.newPop)
        self.deSolver.I_iter += 1
        self.evaluator = gParaSearchParallel(self.deSolver.newPop, self.deSolver.I_iter, self.pathToExecutable, self.pathToStageDir,
                                             self.architecture, self.baseDir, self.xVars, self.verbosity, self.problemType, self.analyzeResults,
                                             self.ctryList, **self.extra_args)

        initial_task = self.evaluator

        SequentialTaskCollection.__init__(self, self.jobname, [initial_task])


    def __str__(self):
        return self.jobname

    def next(self, *args):
        logger.debug('entering gParaSearchDriver.next')

        self.changed = True
        newVals = self.evaluator.target(self.deSolver.newPop)
        self.deSolver.updatePopulation(self.deSolver.newPop, newVals)
        # Stats for initial population:
        self.deSolver.printStats()
        # make full overview table
        self.combOverviews(runDir = self.pathToStageDir, tablePath = self.pathToStageDir)

        # make plots
        if self.deSolver.I_plotting:
            self.deSolver.plotPopulation(self.deSolver.FM_pop)
            self.plot3dTable()

        if not self.deSolver.checkConvergence():
            self.deSolver.newPop = self.deSolver.evolvePopulation(self.deSolver.FM_pop)
            # Check constraints and resample points to maintain population size.
            self.deSolver.newPop = self.deSolver.enforceConstrReEvolve(self.deSolver.newPop)
            self.deSolver.I_iter += 1
            self.evaluator = gParaSearch.gParaSearchParallel(self.deSolver.newPop, self.deSolver.I_iter, self.pathToExecutable, self.pathToStageDir,
                                             self.architecture, self.baseDir, self.xVars, self.verbosity, self.problemType, self.analyzeResults,
                                             self.ctryList, **self.extra_args)
            self.add(self.evaluator)
        else:
            # post processing
            if self.deSolver.I_plotting:
                self.plot3dTable()

            open(os.path.join(self.pathToStageDir, 'jobDone'), 'w')
            # report success of sequential task
            self.execution.returncode = 0
            return Run.State.TERMINATED
        return Run.State.RUNNING


class gParaSearchParallel(ParallelTaskCollection, paraLoop_fp):

    def __str__(self):
        return self.jobname


    def __init__(self, inParaCombos, iteration, pathToExecutable, pathToStageDir, architecture, baseDir, xVars,
                 solverVerb, problemType, analyzeResults, ctryList, **extra_args):
        
        '''
          Generate a list of tasks and initialize a ParallelTaskCollection with them. 
          Uses paraLoop class to generate a list of (descriptions, substitutions for the input files). Descriptions are generated from
          variable names that are hard coded in this method right now. 
          Uses method generateTaskList to create a list of GPremiumApplication's which are invoked from a list of inputs (appropriately adjusted input files), 
          the output directory and some further settings for each run. 
          
          inParaCombos:      List of tuples defining the parameter combinations.
          iteration:         Current iteration number. 
          pathToExecutable:  Path to the executable (the external program to be called). 
          pathToStageDir:    Root path. Usually os.getcwd()
          architecture:      32 or 64 bit.
          baseDir:           Directory in which the input files are located. 
          xVars:             Names of the x variables. 
          solverVerb:        Logger verbosity. 
          problemType:       Forward premium specific flag to determine which case to look at. 
          analyzeResults:    Function to use to analyze the emerging output. 
          ctryList:          Forward premium specific list of ctrys to look at. 
        '''

        logger.debug('entering gParaSearchParalell.__init__')


        # Set up initial variables and set the correct methods.
        self.pathToStageDir = pathToStageDir
        self.problemType = problemType
        self.executable = pathToExecutable
        self.architecture = architecture
        self.baseDir = baseDir
        self.verbosity = solverVerb.upper()
        self.xVars = xVars
        self.n = len(self.xVars.split())
        self.analyzeResults = analyzeResults
        self.ctryList = ctryList
        self.iteration = iteration
        self.jobname = 'evalSolverGuess' + '-' + extra_args['jobname'] + '-' + str(self.iteration)
        self.extra_args = extra_args
        tasks = []

        # --- createJobs_x ---

        # Log activity
        cDate = datetime.date.today()
        cTime = datetime.datetime.time(datetime.datetime.now())
        dateString = '{0:04d}-{1:02d}-{2:02d}-{3:02d}-{4:02d}-{5:02d}'.format(cDate.year, cDate.month, cDate.day, cTime.hour, cTime.minute, cTime.second)
        logger.debug('Establishing parallel task on %s' % dateString)

        # Enter an iteration specific folder
        self.iterationFolder = os.path.join(self.pathToStageDir, 'Iteration-' + str(self.iteration))
        try:
            os.mkdir(self.iterationFolder)
        except OSError:
            print '%s already exists' % self.iterationFolder

        # save population to file
        np.savetxt(os.path.join(self.iterationFolder, 'curPopulation'), inParaCombos, delimiter = '  ')

        # Take the list of parameter combinations and translate them in a comma separated list of values for each variable to be fed into paraLoop file.
        # This can be done much more elegantly with ','.join() but it works...
        vals = []
        nVariables = range(len(inParaCombos[0]))
        for ixVar in nVariables:
            varValString = ''
            for ixParaCombo, paraCombo in enumerate(inParaCombos):
                ### Should make more precise string conversion.
                varValString += str(paraCombo[ixVar])
                if ixParaCombo < len(inParaCombos) - 1:
                    varValString += ', '
            vals.append( varValString )


        # Make problem specific adjustments to the paraLoop file.
        if self.problemType == 'one4all':
            print 'one4all'
            variables = ['Ctry', 'Ctry', 'EA', 'EB', 'sigmaA', 'sigmaB']
            groups    = [ 0, 0, 1, 1, 1, 1 ]
            groupRestrs = [ 'lowerTr', 'lowerTr', 'diagnol', 'diagnol', 'diagnol', 'diagnol' ]
            writeVals = [ ", ".join(self.ctryList), ", ".join(self.ctryList), vals[0], vals[0], vals[1], vals[1] ]
            self.variables = ['EA','sigmaA']
            self.paraCombos = inParaCombos
            paraFiles = [ 'input/markovA.in', 'input/markovB.in', 'input/parameters.in', 'input/parameters.in', 'input/parameters.in', 'input/parameters.in' ]
            paraFileRegex = [ 'space-separated', 'space-separated', 'bar-separated', 'bar-separated' , 'bar-separated' , 'bar-separated'  ]
            self.analyzeResults.tablePath = self.iterationFolder

        elif self.problemType == 'one4eachPair':
            print 'one4eachPair'
            # Check if EA or sigmaA are alone in the specified parameters. If so make diagnol adjustments
            writeVals = []
            if 'EA' in self.xVars and not 'EB' in self.xVars:
                variables = [ 'EA', 'EB' ]
                groups = [ '0', '0' ]
                groupRestrs = [ 'diagnol', 'diagnol' ]

                writeVals.append(vals[0])
                writeVals.append(vals[0])
                paraCombosEA = [  np.append(ele[0], ele[0]) for ele in inParaCombos ]
            if 'sigmaA' in self.xVars and not 'sigmaB' in self.xVars:
                variables.append( 'sigmaA')
                variables.append('sigmaB')
                groups.append( '0')
                groups.append('0')
                groupRestrs.append( 'diagnol')
                groupRestrs.append( 'diagnol' )
                writeVals.append(vals[1])
                writeVals.append(vals[1])
                paraCombosSigmaA = [  np.append(ele[1], ele[1]) for ele in inParaCombos ]

            # match ctry with val
            ctryVals = {}
            for ixCtry, ctry in enumerate(ctryList):
                ctryVals[ctry] = vals

            self.variables = variables

            # Prepare paraCombos matching to resulting table. Used in analyzeOverviewTable
            # !!! This should be dependent on problem type or on missing variables in xvars. !!!
            paraCombos = []
            for EA,sA in zip(paraCombosEA, paraCombosSigmaA):
                paraCombo = np.append(EA, sA)
                paraCombos.append(paraCombo)
            self.paraCombos = paraCombos
            paraFiles = [ 'input/parameters.in', 'input/parameters.in', 'input/parameters.in', 'input/parameters.in' ]
            paraFileRegex = [  'bar-separated', 'bar-separated' , 'bar-separated' , 'bar-separated'  ]

        elif self.problemType == 'one4eachCtry':
            print 'one4eachCtry'

            ctry1List  = []
            ctry2List  = []
            EAList     = []
            EBList     = []
            sigmaAList = []
            sigmaBList = []
            self.paraCombos = []

            ctryIndices = getIndex([len(ctryList), len(ctryList)], 'lowerTr')
            for ixCombo in range(len(inParaCombos)):
                ctry1ListCombo = []
                ctry2ListCombo = []
                EAListCombo    = []
                EBListCombo    = []
                sigmaAListCombo = []
                sigmaBListCombo = []
                for ctryIndex in ctryIndices:
                    ctry1ListCombo.append(ctryList[ctryIndex[0]])
                    ctry2ListCombo.append(ctryList[ctryIndex[1]])
                    EAListCombo.append(inParaCombos[ixCombo][0 + 2 * ctryIndex[0]])
                    sigmaAListCombo.append(inParaCombos[ixCombo][1 + 2 * ctryIndex[0]])
                    EBListCombo.append(inParaCombos[ixCombo][0 + 2 *ctryIndex[1]])
                    sigmaBListCombo.append(inParaCombos[ixCombo][1 + 2 * ctryIndex[1]])
                self.paraCombos.append(zip(ctry1ListCombo, ctry2ListCombo, EAListCombo, sigmaAListCombo, EBListCombo, sigmaBListCombo))
                ctry1List.extend(ctry1ListCombo)
                ctry2List.extend(ctry2ListCombo)
                EAList.extend(map(str, EAListCombo))
                EBList.extend(map(str, EBListCombo))
                sigmaAList.extend(map(str, sigmaAListCombo))
                sigmaBList.extend(map(str, sigmaBListCombo))



            variables = ['Ctry', 'Ctry', 'EA', 'EB', 'sigmaA', 'sigmaB']
            groups    = [ 0, 0, 0, 0, 0, 0 ]
            groupRestrs = [ 'diagnol', 'diagnol', 'diagnol', 'diagnol', 'diagnol', 'diagnol' ]
            writeVals = [ ", ".join(ctry1List), ", ".join(ctry2List), ", ".join(EAList), ", ".join(EBList), ", ".join(sigmaAList),", ".join(sigmaBList)]
            paraFiles = [ 'input/markovA.in', 'input/markovB.in', 'input/parameters.in', 'input/parameters.in', 'input/parameters.in', 'input/parameters.in' ]
            paraFileRegex = [ 'space-separated', 'space-separated', 'bar-separated', 'bar-separated' , 'bar-separated' , 'bar-separated'  ]
            #self.paraCombos = inParaCombos
            self.analyzeResults.tablePath = self.iterationFolder
            # variable list passed to analyzeOverviewTables
            self.variables = ['EA', 'sigmaA', 'EB', 'sigmaB']
            print 'Done setting up one4eachCtry. '




        # Write a para.loop file to generate grid jobs
        para_loop = self.writeParaLoop(variables = variables,
                                       groups = groups,
                                       groupRestrs = groupRestrs,
                                       vals = writeVals,
                                       desPath = os.path.join(self.iterationFolder, 'para.loopTmp'),
                                       paraFiles = paraFiles,
                                       paraFileRegex = paraFileRegex)

        paraLoop_fp.__init__(self, verbosity = self.verbosity)
        tasks = self.generateTaskList(para_loop, self.iterationFolder)
        ParallelTaskCollection.__init__(self, self.jobname, tasks)

    def target(self, inParaCombos):

        logger.debug('entering gParaSearchParallel.target')
        # Each line in the resulting table (overviewSimu) represents one paraCombo
        overviewTable = createOverviewTable(resultDir = self.iterationFolder, outFile = 'simulation.out', slUIPFile = 'slUIP.mat',
                                            exportFileName = 'overviewSimu', sortTable = False,
                                            logLevel = self.verbosity, logFile = os.path.join(self.pathToStageDir, 'createOverviewTable.log'))

        result = self.analyzeResults(tableIn = overviewTable, varsIn = self.variables, valsIn = self.paraCombos,
                             targetVar = 'normDev', logLevel = self.verbosity,
                             logFile = os.path.join(self.iterat
                                                    ionFolder, os.path.join(self.pathToStageDir, 'analyzeOverview.log')))
        #result = [ ele[0] for ele in inParaCombos ]
        logger.info('Computed target: Returning result to solver')
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
            # ugly work around to determine the correct base dir in case of one4all.
            baseDir = os.path.join(os.getcwd(), 'base')
            for (path, changes) in substs.iteritems():
                for (var, val, index, regex) in changes:
                    if var == 'Ctry':
                        baseDir += val
            if baseDir == 'base':
                baseDir = self.baseDir
            self.fillInputDir( baseDir, input_dir)
            # 2. apply substitutions to parameter files
            for (path, changes) in substs.iteritems():
                for (var, val, index, regex) in changes:
                    # new. make adjustments in the base dir itself.
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
            kwargs = self.extra_args
            kwargs['stdout'] = 'forwardPremiumOut.log'
            kwargs['join'] = True
            kwargs['output_dir'] = os.path.join(path_to_stage_dir, 'output')
            kwargs['requested_architecture'] = self.architecture
            kwargs['requested_cores'] = 1
            # hand over job to create
            tasks.append(GPremiumApplication('./' + executable, [], inputs, outputs, **kwargs))
        return tasks

class gParaSearchScript(SessionBasedScript, paraLoop_fp):
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
        self.add_param("-nN", "--norm", metavar="ARCH", type = str,
                       dest="norm", default = 2,
                       help="Which norm to apply for one4all and one4eachCtry")
        self.add_param("-iP", "--initialPop", metavar="ARCH", type = str,
                       dest="initialPop", default = "",
                       help="File name that contains a space separated list of initial pop. ")

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
        gc3libs.utils.check_file_access(self.params.executable, os.R_OK|os.X_OK,
                                gc3libs.exceptions.InvalidUsage)


    def new_tasks(self, extra):

        logger.info('entering new_tasks')

        # Adjust logging level for stream handler. Could be extracted into a general function.
        for handler in logger.handlers:
            if not isinstance(handler, logbook.FileHandler):
                logger.handlers.remove(handler)
        mySH = logbook.StreamHandler(stream = sys.stdout, level = self.params.solverVerb.upper(), format_string = '{record.message}', bubble = True)
        logger.handlers.append(mySH)

        baseDir = self.params.initial
        xVars    = self.params.xVars

        countryList = self.params.countryList.split()

        ctryIndices = getIndex(base = [len(countryList), len(countryList)], restr = 'lowerTr')
        for ctryIndex in ctryIndices:
            logger.info(countryList[ctryIndex[0]] + countryList[ctryIndex[1]])


        # Compute domain
        xVarsDom = self.params.xVarsDom.split()
        lowerBds = np.array([xVarsDom[i] for i in range(len(xVarsDom)) if i % 2 == 0], dtype = 'float64')
        upperBds = np.array([xVarsDom[i] for i in range(len(xVarsDom)) if i % 2 == 1], dtype = 'float64')
        domain = zip(lowerBds, upperBds)


        # Make problem type specific adjustments.
        if self.params.problemType == 'one4all':
            gdpTable = tableDict.fromTextFile(fileIn = os.path.join(self.params.pathEmpirical, 'outputInput/momentTable/Gdp/gdpMoments.csv'),
                                              delim = ',', width = 20)

            jobname = 'one4all'

            logger.info('%s' % self.params.norm)
            norm = self.params.norm
            try:
                norm = int(norm)
            except ValueError:
                if norm == "np.inf" or norm == "inf":
                    norm = np.inf
                else:
                    pass # in particular custom norms

            logger.info('using norm %s' % (norm))


            path_to_stage_dir = os.getcwd()
            executable = os.path.basename(self.params.executable)
            analyzeResults = anaOne4all(len(list(ctryIndices)), norm = norm)
            nlc            = nlcOne4all(gdpTable = gdpTable, ctryList = countryList, domain = domain, logFile = os.path.join(path_to_stage_dir, 'nlc.log'))
            combOverviews  = combineOverviews.combineOverviews(overviewSimuFile = 'eSigmaTable', tableName = 'ag_eSigmaTable', sortKeys = ['norm'])
            plot3dTable    = combineOverviews.plotTable(tablePath =os.path.join(path_to_stage_dir, 'ag_eSigmaTable'), savePath = os.path.join(path_to_stage_dir, 'scatter3d'))
            plot3dTable.columnNames = ['E', 'sigma', 'norm']
            for ctryIndex in ctryIndices:
                Ctry1 = countryList[ctryIndex[0]]
                Ctry2 = countryList[ctryIndex[1]]
                # Set Ctry information for this run.
                update_parameter_in_file(os.path.join(baseDir, 'input/markovA.in'), 'Ctry',
                                          0,  Ctry1,  'space-separated')
                update_parameter_in_file(os.path.join(baseDir, 'input/markovB.in'), 'Ctry',
                                          0,  Ctry2,  'space-separated')
                # Get the correct Ctry Paras into base dir.
                self.getCtryParas(baseDir, Ctry1, Ctry2)
                # Copy base dir
                ctryBaseDir = os.path.join(path_to_stage_dir, 'base' + Ctry1 + Ctry2)
                try:
                    shutil.copytree(baseDir, ctryBaseDir)
                except:
                    logger.info('%s already exists' % baseDir)

            kwargs = extra.copy()
            kwargs['output_dir'] = path_to_stage_dir

            # yield job
            yield (jobname, gParaSearchDriver,
                   [ self.params.executable, path_to_stage_dir, self.params.architecture,
                     baseDir, self.params.xVars, self.params.initialPop,
                     self.params.nPopulation, domain, self.params.solverVerb, self.params.problemType,
                     self.params.pathEmpirical, self.params.itermax, self.params.xConvCrit, self.params.yConvCrit,
                     self.params.makePlots, self.params.optStrategy, self.params.fWeight, self.params.fCritical, self.params.countryList,
                     analyzeResults, nlc, plot3dTable, combOverviews
                   ], kwargs)



        elif self.params.problemType == 'one4eachPair':
            for ctryIndex in ctryIndices:
                Ctry1 = countryList[ctryIndex[0]]
                Ctry2 = countryList[ctryIndex[1]]
                logger.info(Ctry1 + Ctry2)
                jobname = Ctry1 + '-' + Ctry2
                # set stage dir.
                path_to_stage_dir = self.make_directory_path(self.params.output, jobname)
                gc3libs.utils.mkdir(path_to_stage_dir)
                #path_to_stage_dir = os.path.join(iterationFolder, jobname)
                # Get moments table from empirical analysis
                gdpTable = tableDict.fromTextFile(fileIn = os.path.join(self.params.pathEmpirical, 'outputInput/momentTable/Gdp/gdpMoments.csv'),
                                                  delim = ',', width = 20)

                # Set Ctry information for this run.
                update_parameter_in_file(os.path.join(baseDir, 'input/markovA.in'), 'Ctry',
                                          0,  Ctry1,  'space-separated')
                update_parameter_in_file(os.path.join(baseDir, 'input/markovB.in'), 'Ctry',
                                          0,  Ctry2,  'space-separated')
                # Get the correct Ctry Paras into base dir.
                self.getCtryParas(baseDir, Ctry1, Ctry2)
                # Copy base dir
                ctryBaseDir = os.path.join(path_to_stage_dir, 'base')
                try:
                    shutil.copytree(baseDir, ctryBaseDir)
                except:
                    logger.info('%s already exists' % baseDir)
                EA = getParameter(fileIn = os.path.join(baseDir, 'input/parameters.in'), varIn = 'EA',
                                     regexIn = 'bar-separated')
                EB = getParameter(fileIn = os.path.join(baseDir, 'input/parameters.in'), varIn = 'EB',
                                     regexIn = 'bar-separated')
                sigmaA = getParameter(fileIn = os.path.join(baseDir, 'input/parameters.in'), varIn = 'sigmaA',
                                     regexIn = 'bar-separated')
                sigmaB = getParameter(fileIn = os.path.join(baseDir, 'input/parameters.in'), varIn = 'sigmaB',
                                     regexIn = 'bar-separated')
                # Pass ctry information to nlc
                analyzeResults = anaOne4eachPair
                nlc = nlcOne4eachPair(gdpTable = gdpTable, ctryPair = [Ctry1, Ctry2], domain = domain, logFile = os.path.join(path_to_stage_dir, 'nlc.log'))
                combOverviews = combineOverviews.combineOverviews(overviewSimuFile = 'overviewSimu', tableName = 'agTable', sortKeys = ['normDev'])
                plot3dTable    = combineOverviews.plotTable(tablePath =os.path.join(path_to_stage_dir, 'agTable'), savePath = os.path.join(path_to_stage_dir, 'scatter3d'))
                plot3dTable.columnNames = ['EA', 'sigmaA', 'normDev']

                executable = os.path.basename(self.params.executable)
                kwargs = extra.copy()
                kwargs['output_dir'] = path_to_stage_dir

                # yield job
                yield (jobname, gParaSearchDriver,
                       [ self.params.executable, path_to_stage_dir, self.params.architecture,
                         ctryBaseDir, self.params.xVars, self.params.initialPop,
                         self.params.nPopulation, domain, self.params.solverVerb, self.params.problemType,
                         self.params.pathEmpirical, self.params.itermax, self.params.xConvCrit, self.params.yConvCrit,
                         self.params.makePlots, self.params.optStrategy, self.params.fWeight, self.params.fCritical, self.params.countryList,
                         analyzeResults, nlc, plot3dTable, combOverviews
                       ], kwargs)

        elif self.params.problemType == 'one4eachCtry':
            gdpTable = tableDict.fromTextFile(fileIn = os.path.join(self.params.pathEmpirical, 'outputInput/momentTable/Gdp/gdpMoments.csv'),
                                              delim = ',', width = 20)

            if len(countryList) > len(self.params.xVars.split()) / 2 and len(xVars.split()) == 2:
                if self.params.xVars[-1:] != " ":
                    xVars = ( self.params.xVars + ' ' ) * len(countryList)
                    xVars = xVars[:-1]
                else:
                    xVars = self.params.xVars * len(countryList)
                if self.params.xVarsDom[-1:] != " ":
                    xVarsDom = ( self.params.xVarsDom + ' ' ) * len(countryList)
                    xVarsDom = xVarsDom[:-1].split()
                else:
                    xVarsDom = self.params.xVarsDom * len(countryList)
                    xVarsDom = xVarsDom.split()
            else:
                xVars = self.params.xVars
                xVarsDom = self.params.xVarsDom[:-1].split()
            jobname = 'one4eachCtry'

            lowerBds = np.array([xVarsDom[i] for i in range(len(xVarsDom)) if i % 2 == 0], dtype = 'float64')
            upperBds = np.array([xVarsDom[i] for i in range(len(xVarsDom)) if i % 2 == 1], dtype = 'float64')
            domain = zip(lowerBds, upperBds)

            norm = self.params.norm
            try:
                norm = int(norm)
            except ValueError:
                if norm == "np.inf" or norm == "inf":
                    norm = np.inf
                else:
                    pass # in particular custom norms

            logger.info('using norm %s' % (norm))

            path_to_stage_dir = os.getcwd()
            executable = os.path.basename(self.params.executable)
            analyzeResults = anaOne4eachCtry(countryList, len(list(ctryIndices)), norm = norm)
            nlc            = nlcOne4eachCtry(gdpTable = gdpTable, ctryList = countryList, domain = domain, logFile = os.path.join(path_to_stage_dir, 'nlc.log'))
            combOverviews  = combineOverviews.combineOverviews(overviewSimuFile = 'eSigmaTable', tableName = 'ag_eSigmaTable', sortKeys = ['norm'])
            deKenPrice.plotPopulation = plotPopOne4eachCtry(countryList)


##            # Set solver variables
##            nXvars = len(xVars.split())
##            deKenPrice.I_NP         = int(self.params.nPopulation)
##            deKenPrice.F_weight     = float(self.params.fWeight)
##            deKenPrice.F_CR         = float(self.params.fCritical)
##            deKenPrice.I_D          = int(nXvars)
##            deKenPrice.lowerBds     = np.array([ element[0] for element in domain ], dtype = 'float64')
##            deKenPrice.upperBds     = np.array([ element[1] for element in domain ], dtype = 'float64')
##            deKenPrice.I_itermax    = int(self.params.itermax)
##            deKenPrice.F_VTR        = float(self.params.yConvCrit)
##            deKenPrice.I_strategy   = int(self.params.optStrategy)
##            deKenPrice.I_plotting   = int(self.params.makePlots)
##            deKenPrice.xConvCrit    = float(self.params.xConvCrit)
##            deKenPrice.workingDir   = path_to_stage_dir
##            deKenPrice.verbosity    = self.params.solverVerb


            plot3dTable    = emptyFun
#            plot3dTable    = combineOverviews.plotTable(tablePath =os.path.join(path_to_stage_dir, 'ag_eSigmaTable'), savePath = os.path.join(path_to_stage_dir, 'scatter3d'))
#            plot3dTable.columnNames = ['E', 'sigma', 'norm']
            for ctryIndex in ctryIndices:
                Ctry1 = countryList[ctryIndex[0]]
                Ctry2 = countryList[ctryIndex[1]]
                # Set Ctry information for this run.
                update_parameter_in_file(os.path.join(baseDir, 'input/markovA.in'), 'Ctry',
                                          0,  Ctry1,  'space-separated')
                update_parameter_in_file(os.path.join(baseDir, 'input/markovB.in'), 'Ctry',
                                          0,  Ctry2,  'space-separated')
                # Get the correct Ctry Paras into base dir.
                self.getCtryParas(baseDir, Ctry1, Ctry2)
                # Copy base dir
                ctryBaseDir = os.path.join(path_to_stage_dir, 'base' + Ctry1 + Ctry2)
                try:
                    shutil.copytree(baseDir, ctryBaseDir)
                except:
                    logger.info('%s already exists' % baseDir)

            kwargs = extra.copy()
            kwargs['output_dir'] = path_to_stage_dir

            # yield job

            yield (jobname, gParaSearchDriver,
                   [ self.params.executable, path_to_stage_dir, self.params.architecture,
                     baseDir, xVars, self.params.initialPop,
                     self.params.nPopulation, domain, self.params.solverVerb, self.params.problemType,
                     self.params.pathEmpirical, self.params.itermax, self.params.xConvCrit, self.params.yConvCrit,
                     self.params.makePlots, self.params.optStrategy, self.params.fWeight, self.params.fCritical, self.params.countryList,
                     analyzeResults, nlc, plot3dTable, combOverviews
                   ], kwargs)

        # Set solver variables
        nXvars = len(xVars.split())
        deKenPrice.I_NP         = int(self.params.nPopulation)
        deKenPrice.F_weight     = float(self.params.fWeight)
        deKenPrice.F_CR         = float(self.params.fCritical)
        deKenPrice.I_D          = int(nXvars)
        if self.params.problemType == 'one4eachCtry':
            deKenPrice.lowerBds = np.array([xVarsDom[i] for i in range(len(xVarsDom)) if i % 2 == 0], dtype = 'float64')
            deKenPrice.upperBds = np.array([xVarsDom[i] for i in range(len(xVarsDom)) if i % 2 == 1], dtype = 'float64')
        else:
            deKenPrice.lowerBds     = np.array([ element[0] for element in domain ], dtype = 'float64')
            deKenPrice.upperBds     = np.array([ element[1] for element in domain ], dtype = 'float64')
        deKenPrice.I_itermax    = int(self.params.itermax)
        deKenPrice.F_VTR        = float(self.params.yConvCrit)
        deKenPrice.I_strategy   = int(self.params.optStrategy)
        deKenPrice.I_plotting   = int(self.params.makePlots)
        deKenPrice.xConvCrit    = float(self.params.xConvCrit)
        deKenPrice.workingDir   = path_to_stage_dir
        deKenPrice.verbosity    = self.params.solverVerb

        logger.info('done with new_tasks')


# run script

if __name__ == '__main__':
    logger.info('Starting: \n%s' % ' '.join(sys.argv))
    gParaSearchScript().run()
    logger.info('main done')
