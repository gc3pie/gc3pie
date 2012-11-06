#! /usr/bin/env python
#
"""
  Class to perform global optimization
"""

# Copyright (C) 2011, 2012 University of Zurich. All rights reserved.
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
__author__ = 'Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'


# For now use export PYTHONPATH=/home/benjamin/workspace/housingProj/model/code/ to allow import 
# of pymods


import os, sys
import numpy as np
from pymods.support.support import wrapLogger, update_parameter_in_file
sys.path.append('/home/benjamin/workspace/globalOpt/gc3pie/gc3apps/bf.uzh.ch')
from difEvoKenPrice import deKenPrice
sys.path.append('/home/benjamin/workspace/globalOpt/gc3pie')
import logging
import gc3libs
import gc3libs.config
import gc3libs.core
#from gc3libs.dag import SequentialTaskCollection, ParallelTaskCollection
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
from gc3libs import Application, Run
from paraLoop import paraLoop
import datetime
import glob
import time

gc3libs.configure_logger(level=logging.DEBUG)
logger = wrapLogger(loggerName = 'gParaSearchLogger', streamVerb = 'INFO', logFile = os.path.join(os.getcwd(), 'gParaSearch.log'))
 
class GlobalOptimizer(SequentialTaskCollection):

    def __init__(self, jobname, pathToStageDir, optSettings, targetSettings, **extra_args):
    #, nPopulation, initialPop, domain, itermax, xConvCrit, yConvCrit, makePlots, optStrategy, fWeight, fCritical, nlc, solverVerb, 
            #pathToStageDir, pathToExecutable, baseDir, xVars, computeTarget,
            #**extra_args):

        '''
          Main loop for the global optimizer. 

          pathToStageDir  : Directory in which to perform the optimization. 

          optSettings is a dictionary of optimizer settings. Key values can be. 

          nDim            : Dimensionality. 
          nPopulation     : Population size. 
          initialPop      : The initial population if recovering from earlier failure. 
          domain          : The domain of the x variables. List of (lowerbound, upperbound) tuples. 
          itermax         : Maximum # of iterations of the solver. 
          xConvCrit       : Convergence criteria for x variables. 
          yConvCrit       : Convergence criteria for the y variables. 
          makePlots       : Make plots? 
          optStrategy     : The kind of differential evolution strategy to use.           
          fWeight         : 
          fCritical       : 
          nlc             : Constraint function. 
          solverVerb      : Verbosity of the solver. 

          targetSettings is a dictionary of settings that specify how to compute the target values for one agent. Key values are: 

          pathToExecutable: Path to main executable. 
          baseDir         : Directory in which the input files are assembled. This directory is sent as input to the cluseter. 
          xVars           : Names of the x variables.
          computeTarget   : Function to analyze the output retrieved from the servers. Problem specific. 

        '''

        logger.debug('entering globalOptimizer.__init__')

        # Set up initial variables and set the correct methods.
        self.pathToStageDir    = pathToStageDir

        self.optSettings       = optSettings                
        self.targetSettings    = targetSettings
        self.target            = self.targetSettings['computeTarget']

        self.jobname           = jobname
        self.extra_args        = extra_args

        # Initialize solver
        self.deSolver = deKenPrice(optSettings)             

        if not 'initialPop' in optSettings:
            self.deSolver.newPop = self.deSolver.drawInitialSample()
        else:
            self.deSolver.newPop = optSettings['initialPop']

        self.deSolver.I_iter += 1

        self.evaluator = computePhenotypes(self.deSolver.newPop, self.jobname, 
                                             self.deSolver.I_iter, pathToStageDir, optSettings, targetSettings) # need to specify this

        initial_task = self.evaluator

        SequentialTaskCollection.__init__(self, self.jobname, [initial_task])
        
    def next(self, *args):
        logger.debug('entering gParaSearchDriver.next')

        self.changed = True
        # pass on (popMem, location)
        popLocationTuple = [(popEle, os.path.join(self.pathToStageDir, 'Iteration-' + str(self.deSolver.I_iter), 
                                'para_' + '_'.join([(var + '=' + ('%25.15f' % val).strip()) for (var,val) in zip(self.targetSettings['xVars'], popEle) ] ) ) ) for popEle in self.deSolver.newPop]
        newVals = self.target(popLocationTuple)
        self.deSolver.updatePopulation(self.deSolver.newPop, newVals)
        # Stats for initial population:
        self.deSolver.printStats()
        ## make full overview table
        #self.combOverviews(runDir = self.pathToStageDir, tablePath = self.pathToStageDir)

        ## make plots
        #if self.deSolver.I_plotting:
            #self.deSolver.plotPopulation(self.deSolver.FM_pop)
            #self.plot3dTable()

        if not self.deSolver.checkConvergence():
            self.deSolver.newPop = self.deSolver.evolvePopulation(self.deSolver.FM_pop)
            # Check constraints and resample points to maintain population size.
            self.deSolver.newPop = self.deSolver.enforceConstrReEvolve(self.deSolver.newPop)
            self.deSolver.I_iter += 1
            self.evaluator = computePhenotypes(self.deSolver.newPop, self.jobname, 
                                             self.deSolver.I_iter, self.pathToStageDir, self.optSettings, self.targetSettings)
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
        
        
        
        
    def __str__(self):
        return self.jobname

class computePhenotypes(ParallelTaskCollection, paraLoop):

    def __str__(self):
        return self.jobname


    def __init__(self, inParaCombos, jobname, iteration, pathToStageDir, optSettings, targetSettings, **extra_args):

        """
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
        """

        logger.debug('entering gParaSearchParalell.__init__')


        # Set up initial variables and set the correct methods.
        self.pathToStageDir = pathToStageDir
        self.optSettings = optSettings
        self.targetSettings = targetSettings
        self.iteration = iteration
        self.jobname = 'evalSolverGuess' + '-' + jobname + '-' + str(self.iteration)
        self.verbosity = self.optSettings['verbosity']
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
            varValString = ', '.join([('%25.15f' % paraCombo[ixVar]).strip() for paraCombo in inParaCombos])
            #varValString = ''
            #for ixParaCombo, paraCombo in enumerate(inParaCombos):
                #### Should make more precise string conversion.
                #varValString += str(paraCombo[ixVar])
                #if ixParaCombo < len(inParaCombos) - 1:
                    #varValString += ', '
            vals.append( varValString )
        
        variables = self.targetSettings['xVars']
        groups    = [0] * len(self.targetSettings['xVars'])
        groupRestrs = ['diagnol'] * len(self.targetSettings['xVars'])
        writeVals = vals
        self.variables = variables
        self.paraCombos = inParaCombos
        paraFiles = self.targetSettings['paraFiles']
        paraFileRegex = self.targetSettings['paraFileFormat']

        # Write a para.loop file to generate grid jobs
        para_loop = self.writeParaLoop(variables = variables,
                                       groups = groups,
                                       groupRestrs = groupRestrs,
                                       vals = writeVals,
                                       desPath = os.path.join(self.iterationFolder, 'para.loopTmp'),
                                       paraFiles = paraFiles,
                                       paraFileRegex = paraFileRegex)

        paraLoop.__init__(self, verbosity = self.verbosity)
        tasks = self.generateTaskList(para_loop, self.iterationFolder)
        ParallelTaskCollection.__init__(self, self.jobname, tasks)
        
    def generateTaskList(self, para_loop, iterationFolder):
        # Fill the task list
        tasks = []
        for jobname, substs in self.process_para_file(para_loop):
            executable = os.path.basename(self.targetSettings['pathToExecutable'])
            # start the inputs dictionary with syntax: client_path: server_path
            inputs = { self.targetSettings['pathToExecutable']:executable }
            # make a "stage" directory where input files are collected on the client machine.
            #path_to_stage_dir = self.make_directory_path(os.path.join(iterationFolder, 'NAME'), jobname)
            path_to_stage_dir = os.path.join(iterationFolder, jobname)
            # input_dir is cwd/jobname (also referred to as "stage" dir.
            path_to_stage_base_dir = os.path.join(path_to_stage_dir, 'base')
            gc3libs.utils.mkdir(path_to_stage_base_dir)
            prefix_len = len(path_to_stage_base_dir) + 1
            # 1. files in the "initial" dir are copied verbatim
            baseDir = self.targetSettings['baseDir']
            gc3libs.utils.copytree(baseDir , path_to_stage_base_dir) # copy entire input directory
            # 2. apply substitutions to parameter files
            for (path, changes) in substs.iteritems():
                for (var, val, index, regex) in changes:
                    # new. make adjustments in the base dir itself.
                    update_parameter_in_file(os.path.join(path_to_stage_base_dir, path),
                                             var, index, val, regex)
            # 3. build input file list
            for dirpath,dirnames,filenames in os.walk(path_to_stage_base_dir):
                for filename in filenames:
                    # cut the leading part, which is == to path_to_stage_dir
                    relpath = dirpath[prefix_len:]
                    # ignore output directory contents in resubmission
                    if relpath.startswith('output'):
                        continue
                    remote_path = os.path.join(relpath, filename)
                    inputs[os.path.join(dirpath, filename)] = remote_path
            # all contents of the `output` directory are to be fetched
           # outputs = { 'output/':'' }
            outputs = gc3libs.ANY_OUTPUT
            #{ '*':'' }
            #kwargs = extra.copy()
            kwargs = self.extra_args
            kwargs['stdout'] = executable + '.log'
            kwargs['join'] = True
            # kwargs['output_dir'] = '/home/benjamin/Desktop'
            kwargs['output_dir'] =  os.path.join(path_to_stage_dir, 'output')
            gc3libs.log.debug("Output dir: %s" % kwargs['output_dir'])
            kwargs['requested_architecture'] = 'x86_64'
            kwargs['requested_cores'] = 1
            # hand over job to create
            tasks.append(Application('./' + executable, [], inputs, outputs, **kwargs))
        return tasks




if __name__ == '__main__':
    logger.info('Starting: \n%s' % ' '.join(sys.argv))
    # clean up
    os.system('rm -r Iteration*')
    os.system('rm *.log')

    def computeTarget(popLocationTuple):
        '''
          Given a list of (population, location) compute and return list of target values. 
        '''
        fxVals = []
        for (pop, loc) in popLocationTuple:
            outputDir = os.path.join(loc, 'output')
            f = open(os.path.join(outputDir, 'test.out'))
            line = f.readline().strip()
            fxVal = float(line)
            fxVals.append(fxVal)
        return fxVals
    
    # Somehow execute globalOpt.__init__()...    gParaSearchScript().run()
    optSettings = {}
    optSettings['nDim']         = 2
    optSettings['nPopulation']  = 100
    optSettings['F_weight']     = 0.85
    optSettings['F_CR']         = 1.
    optSettings['lowerBds']     = -2 * np.ones(optSettings['nDim']) 
    optSettings['upperBds']     = 2 * np.ones(optSettings['nDim']) 
    optSettings['I_bnd_constr'] = 0
    optSettings['itermax']      = 200
    optSettings['F_VTR']        = 1.e-8
    optSettings['optStrategy']  = 1
    optSettings['I_refresh']    = 1
    optSettings['I_plotting']   = 0
    optSettings['verbosity']    = 'DEBUG'
    targetSettings = {}
    targetSettings['pathToExecutable'] = os.path.join(os.getcwd(), 'bin', 'a.out')
    targetSettings['baseDir']          = os.path.join(os.getcwd(), 'base')
    targetSettings['xVars']            = ['x1', 'x2']
    targetSettings['paraFiles']        = ['test.in', 'test.in']
    targetSettings['paraFileFormat']   = ['space-separated'] * 2
    targetSettings['computeTarget']    = computeTarget
    globalOptObj = globalOptimizer(jobname = 'globalOptRun', pathToStageDir = os.getcwd(), optSettings = optSettings, targetSettings = targetSettings)
    # create an instance of GdemoSimpleApp
    app = globalOptObj
    
    # create an instance of Core. Read configuration from your default
    # configuration file
    cfg = gc3libs.config.Configuration(*gc3libs.Default.CONFIG_FILE_LOCATIONS,
                                       **{'auto_enable_auth': True})
    g = gc3libs.core.Core(cfg)
    engine = gc3libs.core.Engine(g)
    engine.add(app)
    
    # in case you want to select a specific resource, call
    # `Core.select_resource(...)`
    # if len(sys.argv)>1:
    #     g.select_resource(sys.argv[1])
    
    # Submit your application.
    # g.submit(app)
    
    # After submssion, you have to check the application for its state:
    #print  "Job id: %s" % app.execution.lrms_jobid
    
    # Periodically check the status of your application.
    while app.execution.state != gc3libs.Run.State.TERMINATED:
        try:
            print "Job in status %s " % app.execution.state
            time.sleep(5)
            # This call will contact the resource(s) and get the current
            # job state
            # g.update_job_state(app)
            engine.progress()
            sys.stdout.write("[ %s ]\r" % app.execution.state)
            sys.stdout.flush()
        except:
            raise
    
    print "Job is now in state %s. Fetching output." % app.execution.state
    
    # You can specify a different `download_dir` option if you want to
    # override the value used in the GdemoSimpleApp initialization (app.output_dir).
    
    # By default overwrite is False. If the output directory exists, it
    # will be renamed by appending a unique numerical suffix in the form
    # of output_dir.~N~ with N the first available number.
    # g.fetch_output(app, overwrite=False)
    
    # print "Done. Results are in %s" % app.output_dir    
    logger.info('main done')
