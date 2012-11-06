#! /usr/bin/env python
#
"""
  Class to perform global optimization. 
  
  An implementation of Ken Price's Differnetial Evolution algorithm generates
  guesses that are evaluated in parallel using gc3pie. The objective function
  must be an executable file receiving inputs through a parameter file. 
  
  An instance of `GlobalOptimizer` will perform the entire optimization
  in a directory on the local machine named `path_to_stage_dir`. 
  
  At each iteration an instance of 'ComputePhenotypes' will prepare input files
  in an input-specific directory and execute the objective function. When all
  jobs are complete, the objective's output is analyzed with the user-supplied
  function `target_fun'. This function returns the function value for all
  analyzed input vectors. 
  
  With this information, the optimizer generates a new guess. The instance of
  `GlobalOptimizer' iterates until some convergence criteria is satisfied. 
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

# For now use export export PYTHONPATH=~/workspace/globalOpt/gc3pie/ to allow import 
# of gc3libs

# General imports
import os
import sys
import datetime
import glob
import time
import logging
import numpy as np

# gc3libs imports
import gc3libs
import gc3libs.debug
import gc3libs.config
import gc3libs.core
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
from gc3libs.optimizer.examples.rosenbrock.opt_rosenbrock import compute_target_rosenbrock
from gc3libs import Application, Run

# optimizer specific imports
from support_gc3 import update_parameter_in_file
from dif_evolution import DifferentialEvolution
from para_loop import ParaLoop

# For now use __file__ to determine path to the example files. Could also use pkg_resources. 
path_to_rosenbrock_example = os.path.join(os.path.dirname(__file__), 'examples/rosenbrock/')

# Perform basic configuration for gc3libs logger. Adjust level to logging.DEBUG if necessary. 
gc3libs.configure_logger(level=logging.CRITICAL)

# Generate a separate logging instance. Careful, running gc3libs.configure_logger again will 
log = logging.getLogger('gc3.gc3libs.optimizer')
log.setLevel(logging.DEBUG)
log.propagate = 0
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
log_file_name = os.path.join(gc3libs.Default.RCDIR, 'optimizer.log')
file_handler = logging.FileHandler(log_file_name, mode = 'w')
file_handler.setLevel(logging.DEBUG)
log.addHandler(stream_handler)
log.addHandler(file_handler)

float_fmt = '%25.15f'

class GlobalOptimizer(SequentialTaskCollection):

    def __init__(self, jobname = 'rosenbrock', path_to_stage_dir = '/tmp/rosenbrock',
                 n_dim = 2, n_population = 100, initial_pop = [], lower_bds = [-2, -2], upper_bds = [2, 2], bnd_constr = 0, iter_max = 200, 
                 x_crit = None, y_crit = 1.e-8, opt_strat = 1, f_weight = 0.85, f_cross = 1., nlc = None, plot_output = 0, verbosity = 'DEBUG', 
                 path_to_executable = os.path.join(path_to_rosenbrock_example, 'bin/rosenbrock'),
                 base_dir = os.path.join(path_to_rosenbrock_example, 'base'), 
                 x_vars = ['x1', 'x2'], para_files = ['parameters.in', 'parameters.in'], para_file_formats = ['space-separated', 'space-separated'],
                 target_fun = compute_target_rosenbrock, **extra_args ):
                 
        '''
          Main loop for the global optimizer. 
          
          Keyword arguments:
          jobname -- string that labels this optimization case. 
          path_to_stage_dir -- directory in which to perform the optimization. 
          
          n_dim -- Dimension of the objective function.
          n_population -- Size of the populatin. 
          initial_pop -- Population to start with. 
          lower_bds -- Lower bounds for the input variables when drawing initial guess. 
          upper_bds -- Upper bounds for the input variables when drawing initial guess. 
          iter_max -- Maximum # of iterations of the solver. 
          x_crit -- Convergence criteria for x variables. 
          y_crit -- Convergence criteria for the y variables. 
          plot_output -- Generate plots. 
          opt_strat -- The kind of differential evolution strategy to use. Ranges from 1 to 6. See difEvoKenPrice.py for description. 
          f_weight -- DE-stepsize F_weight ex [0, 2]
          f_cross -- Crossover probabililty constant ex [0, 1]
          nlc -- Constraint function. 
          verbosity -- Verbosity of the solver. 

          path_to_executable -- Path to main executable. 
          base_dir -- Directory in which the input files are assembled. This directory is sent as input to the cluseter. 
          x_vars -- Names of the x variables.
          para_files -- List of parameter files corresponding to the x variables. 
          para_file_formats -- List of format strings for parameter files. Values can be: space-separated, bar-separated or a regular expression. 
          target_fun --  Function to analyze the output retrieved from the servers and generate list of f values. 
        '''

        log.debug('entering globalOptimizer.__init__')

        # Set up initial variables and set the correct methods.
        self.jobname = jobname
        self.path_to_stage_dir = path_to_stage_dir

        self.path_to_executable = path_to_executable
        self.base_dir = base_dir
        self.x_vars = x_vars
        self.para_files = para_files
        self.para_file_formats = para_file_formats
        self.target_fun = target_fun
        
        self.extra_args = extra_args
        
        # Initialize stage dir
        if not os.path.isdir(self.path_to_stage_dir):
            os.makedirs(self.path_to_stage_dir)

        # Initialize solver
        opt_settings = {}
        opt_settings['nDim']         = n_dim
        opt_settings['nPopulation']  = n_population
        opt_settings['F_weight']     = f_weight
        opt_settings['F_CR']         = f_cross
        opt_settings['lowerBds']     = lower_bds
        opt_settings['upperBds']     = upper_bds
        opt_settings['I_bnd_constr'] = bnd_constr
        opt_settings['itermax']      = iter_max
        opt_settings['F_VTR']        = y_crit
        opt_settings['optStrategy']  = opt_strat
        opt_settings['I_refresh']    = 0
        opt_settings['I_plotting']   = plot_output
        opt_settings['verbosity']    = verbosity
        opt_settings['workingDir']   = self.path_to_stage_dir
        
        self.deSolver = DifferentialEvolution(opt_settings)             

        if not initial_pop:
            self.deSolver.newPop = self.deSolver.drawInitialSample()
        else:
            self.deSolver.newPop = initial_pop

        self.deSolver.I_iter += 1

        self.evaluator = ComputePhenotypes(self.deSolver.newPop, self.jobname, self.deSolver.I_iter, path_to_stage_dir, 
                                           self.path_to_executable, self.base_dir, self.x_vars, self.para_files, self.para_file_formats) 

        initial_task = self.evaluator

        SequentialTaskCollection.__init__(self, self.jobname, [initial_task])
        
    def next(self, *args):
        log.debug('entering gParaSearchDriver.next')

        self.changed = True
        # pass on (popMem, location)
        pop_location_tuple = [(popEle, 
                             os.path.join(self.path_to_stage_dir, 'Iteration-' + str(self.deSolver.I_iter), 'para_' + '_'.join([(var + '=' + 
                                                               (float_fmt % val).strip()) for (var,val) in zip(self.x_vars, popEle) ] ) ) ) for popEle in self.deSolver.newPop]
        newVals = self.target_fun(pop_location_tuple)
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
            self.evaluator = ComputePhenotypes(self.deSolver.newPop, self.jobname, self.deSolver.I_iter, self.path_to_stage_dir, 
                                           self.path_to_executable, self.base_dir, self.x_vars, self.para_files, self.para_file_formats)
            #computePhenotypes(self.deSolver.newPop, self.jobname, 
                                             #self.deSolver.I_iter, self.pathToStageDir, self.optSettings, self.targetSettings)
            self.add(self.evaluator)
        else:
            # post processing
            if self.deSolver.I_plotting:
                self.plot3dTable()

            open(os.path.join(self.path_to_stage_dir, 'jobDone'), 'w')
            # report success of sequential task
            self.execution.returncode = 0
            return Run.State.TERMINATED
        return Run.State.RUNNING
  
        
    def __str__(self):
        return self.jobname

class ComputePhenotypes(ParallelTaskCollection, ParaLoop):

    def __str__(self):
        return self.jobname


    def __init__(self, inParaCombos, jobname, iteration, path_to_stage_dir, 
                 path_to_executable, base_dir, x_vars, para_files, para_file_formats, **extra_args):

        """
          Generate a list of tasks and initialize a ParallelTaskCollection with them. 
          
          Uses paraLoop class to generate a list of (descriptions, substitutions for the input files). 
          Uses method generateTaskList to create a list of GPremiumApplication's which are invoked from a list of inputs (appropriately adjusted input files), 
          the output directory and some further settings for each run. 

          Keyword arguments: 
          inParaCombos -- List of tuples defining the parameter combinations.
          jobname -- Name of GlobalOptimizer instance driving the optimization. 
          iteration -- Current iteration number. 
          path_to_stage_dir -- Path to directory in which optimization takes place. 
          path_to_executable --  Path to the executable (the external program to be called). 
          base_dir -- Directory in which the input files are located. 
          x_vars -- Names of the x variables. 
          para_files -- List of parameter files corresponding to the x variables. 
          para_file_formats -- List of format strings for parameter files. Values can be: space-separated, bar-separated or a regular expression. 
        """

        log.debug('entering gParaSearchParalell.__init__')

        # Set up initial variables and set the correct methods.
        self.jobname = 'evalSolverGuess' + '-' + jobname + '-' + str(iteration)
        self.iteration = iteration        

        self.path_to_stage_dir = path_to_stage_dir
        self.path_to_executable = path_to_executable
        self.base_dir = base_dir
        self.x_vars = x_vars
        self.para_files = para_files
        self.para_file_formats = para_file_formats
        self.verbosity = 'DEBUG'
        self.extra_args = extra_args

        # Log activity
        cDate = datetime.date.today()
        cTime = datetime.datetime.time(datetime.datetime.now())
        date_string = '%04d--%02d--%02d--%02d--%02d--%02d' % (cDate.year, cDate.month, cDate.day, cTime.hour, cTime.minute, cTime.second)
        gc3libs.log.debug('Establishing parallel task on %s', date_string)

        # Enter an iteration specific folder
        self.iterationFolder = os.path.join(self.path_to_stage_dir, 'Iteration-' + str(self.iteration))
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
            vals.append([paraCombo[ixVar] for paraCombo in inParaCombos])
     
        params = {}
        n_vars = len(self.x_vars)
        params['variables'] = self.x_vars
        params['indices'] = np.array(['(0)'] * n_vars)
        params['paraFiles'] = self.para_files
        params['paraProps'] = np.array(['None'] * n_vars)
        params['groups'] = np.array([0] * n_vars)
        params['groupRestrs'] = np.array(['diagnol'] * n_vars)
        params['paraFileRegex'] = self.para_file_formats
        params['vals'] = np.array(vals)

        ParaLoop.__init__(self, verbosity = 'CRITICAL')
        tasks = self.gen_task_list(self.iterationFolder, params)
        ParallelTaskCollection.__init__(self, self.jobname, tasks)
        
    def gen_task_list(self, iterationFolder, params):
        # Fill the task list
        tasks = []
        for jobname, substs in self.process_para_file(path_to_para_loop = None, params = params):
            executable = os.path.basename(self.path_to_executable)
            # start the inputs dictionary with syntax: client_path: server_path
            inputs = { self.path_to_executable:executable }
            # make a "stage" directory where input files are collected on the client machine.
            #path_to_stage_dir = self.make_directory_path(os.path.join(iterationFolder, 'NAME'), jobname)
            path_to_stage_dir = os.path.join(iterationFolder, jobname)
            # input_dir is cwd/jobname (also referred to as "stage" dir.
            path_to_stage_base_dir = os.path.join(path_to_stage_dir, 'base')
            gc3libs.utils.mkdir(path_to_stage_base_dir)
            prefix_len = len(path_to_stage_base_dir) + 1
            # 1. files in the "initial" dir are copied verbatim
            base_dir = self.base_dir
            gc3libs.utils.copytree(base_dir , path_to_stage_base_dir) # copy entire input directory
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
            kwargs['output_dir'] =  os.path.join(path_to_stage_dir, 'output')
            gc3libs.log.debug("Output dir: %s" % kwargs['output_dir'])
            kwargs['requested_architecture'] = 'x86_64'
            kwargs['requested_cores'] = 1
            # hand over job to create
            tasks.append(Application('./' + executable, [], inputs, outputs, **kwargs))
        return tasks
    

if __name__ == '__main__':
    log.info('Starting: \n%s' % ' '.join(sys.argv))
    # clean up
    os.system('rm -r /tmp/rosenbrock')

    # create an instance globalObt
    globalOptObj = GlobalOptimizer(y_crit=0.1)
    app = globalOptObj
    
    # create an instance of Core. Read configuration from your default
    # configuration file
    cfg = gc3libs.config.Configuration(*gc3libs.Default.CONFIG_FILE_LOCATIONS,
                                       **{'auto_enable_auth': True})
    g = gc3libs.core.Core(cfg)
    engine = gc3libs.core.Engine(g)
    engine.add(app)
    
    # Periodically check the status of your application.
    while app.execution.state != gc3libs.Run.State.TERMINATED:
        try:
            print "Job in status %s " % app.execution.state
            time.sleep(5)
            engine.progress()
        except:
            raise
    
    print "Job is now in state %s. Fetching output." % app.execution.state
    
    log.info('main done')
