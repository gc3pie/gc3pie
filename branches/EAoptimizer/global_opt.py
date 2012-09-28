#! /usr/bin/env python
#
"""
  Class to perform global optimization. 
  
  Some optimization algorithm (for example Ken Price's Differnetial 
  Evolution algorithm) generates guesses that are evaluated in parallel 
  using gc3pie. 
  
  An instance of :class:`GlobalOptimizer` will perform the entire optimization
  in a directory on the local machine named `path_to_stage_dir`. 
  
  At each iteration an instance of 'ComputePhenotypes' lets the user-defined 
  function `task_constructor` generate :class:`Application` instances that are
  used to execute the jobs in parallel on the grid. When all
  jobs are complete, the objective's output is analyzed with the user-supplied
  function `target_fun'. This function returns the function value for all
  analyzed input vectors. 
  
  With this information, the optimizer generates a new guess. The instance of
  class:`GlobalOptimizer' iterates until some convergence criteria is satisfied. 
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
import shutil

# gc3libs imports
import gc3libs
import gc3libs.debug
import gc3libs.config
import gc3libs.core
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
from gc3libs.optimizer.examples.rosenbrock.opt_rosenbrock import compute_target_rosenbrock
from gc3libs import Application, Run

# optimizer specific imports
from dif_evolution import DifferentialEvolution

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

class GlobalOptimizer(SequentialTaskCollection):

    def __init__(self, jobname = '', path_to_stage_dir = '',
                 optimizer = None, task_constructor = None,
                 target_fun = None, **extra_args ):
                 
        '''
          Main loop for the global optimizer. 
          
          Keyword arguments:
          jobname -- string that labels this optimization case. 
          path_to_stage_dir -- directory in which to perform the optimization. 
          
          optimizer -- Optimizer instance that conforms to the abstract class optimization algorithm. 
          task_constructor -- Takes a list of x vectors and the path to the current iteration directory. 
                              Returns Application instances that can be executed on the grid. 
          target_fun -- Takes a list of (x_vector, application_instance) tuples and returns the corresponding
                        function value for the x_vector. 
        '''

        log.debug('entering globalOptimizer.__init__')

        # Set up initial variables and set the correct methods.
        self.jobname = jobname
        self.path_to_stage_dir = path_to_stage_dir
        self.optimizer = optimizer
        self.target_fun = target_fun
        self.task_constructor = task_constructor
        self.extra_args = extra_args

        self.optimizer.I_iter += 1

        self.evaluator = ComputePhenotypes(self.optimizer.newPop, self.jobname, self.optimizer.I_iter, path_to_stage_dir, task_constructor)

        initial_task = self.evaluator

        SequentialTaskCollection.__init__(self, self.jobname, [initial_task])
        
    def next(self, *args):
        log.debug('entering gParaSearchDriver.next')

        self.changed = True
        # pass on (popMem, Application)
        pop_task_tuple = [(popEle, task) for (popEle, task) in zip(self.optimizer.newPop, self.evaluator.tasks)]

        newVals = self.target_fun(pop_task_tuple)
        self.optimizer.updatePopulation(self.optimizer.newPop, newVals)
        # Stats for initial population:
        self.optimizer.printStats()

        ## make plots
        #if self.optimizer.I_plotting:
            #self.optimizer.plotPopulation(self.optimizer.FM_pop)
            #self.plot3dTable()

        if not self.optimizer.checkConvergence():
            self.optimizer.newPop = self.optimizer.evolvePopulation(self.optimizer.FM_pop)
            # Check constraints and resample points to maintain population size.
            self.optimizer.newPop = self.optimizer.enforceConstrReEvolve(self.optimizer.newPop)
            self.optimizer.I_iter += 1
            self.evaluator = ComputePhenotypes(self.optimizer.newPop, self.jobname, self.optimizer.I_iter, self.path_to_stage_dir, self.task_constructor)
            self.add(self.evaluator)
        else:
            # post processing
            if self.optimizer.I_plotting:
                self.plot3dTable()

            open(os.path.join(self.path_to_stage_dir, 'jobDone'), 'w')
            # report success of sequential task
            self.execution.returncode = 0
            return Run.State.TERMINATED
        return Run.State.RUNNING
  
        
    def __str__(self):
        return self.jobname

class ComputePhenotypes(ParallelTaskCollection):

    def __str__(self):
        return self.jobname


    def __init__(self, inParaCombos, jobname, iteration, path_to_stage_dir, task_constructor, **extra_args):

        """
          Generate a list of tasks and initialize a ParallelTaskCollection with them. 

          Keyword arguments: 
          inParaCombos -- List of tuples defining the parameter combinations.
          jobname -- Name of GlobalOptimizer instance driving the optimization. 
          iteration -- Current iteration number. 
          path_to_stage_dir -- Path to directory in which optimization takes place. 
          task_constructor -- Takes a list of x vectors and the path to the current iteration directory. 
                              Returns Application instances that can be executed on the grid. 
        """

        log.debug('entering gParaSearchParalell.__init__')

        # Set up initial variables and set the correct methods.
        self.jobname = 'evalSolverGuess' + '-' + jobname + '-' + str(iteration)
        self.iteration = iteration        

        self.path_to_stage_dir = path_to_stage_dir
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

        self.tasks = [ task_constructor(x_vec, self.iterationFolder) for x_vec in inParaCombos ]
        ParallelTaskCollection.__init__(self, self.jobname, self.tasks)

