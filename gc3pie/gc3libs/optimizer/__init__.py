#! /usr/bin/env python
#
"""
Class to perform global optimization.

Optimization algorithm (for example Ken Price's Differential
Evolution algorithm) generates guesses that are evaluated in parallel
using gc3pie.

An instance of :class:`GlobalOptimizer` will perform the entire optimization
in a directory on the local machine named `path_to_stage_dir`.

At each iteration an instance of 'ComputeTargetVals' lets the user-defined
function `task_constructor` generate :class:`Application` instances that are
used to execute the jobs in parallel on the grid. When all
jobs are complete, the objective's output is analyzed with the user-supplied
function `target_fun`. This function returns the function value for all
analyzed input vectors.

With this information, the optimizer generates a new guess. The instance of
:class:`GlobalOptimizer` iterates until the sepcified convergence criteria
is satisfied.
"""
# Copyright (C) 2011, 2012, 2013 University of Zurich. All rights reserved.
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
from gc3libs import Application, Run, Task

# Perform basic configuration for gc3libs logger. Adjust level to logging.DEBUG if necessary.
gc3libs.configure_logger(level=logging.CRITICAL)

# Generate a separate logging instance. Careful, running gc3libs.configure_logger again will
log = logging.getLogger('gc3.gc3libs.GlobalOptimizer')
log.setLevel(logging.DEBUG)
log.propagate = 0
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
log_file_name = os.path.join(gc3libs.Default.RCDIR, 'GlobalOptimizer.log')
file_handler = logging.FileHandler(log_file_name, mode = 'w')
file_handler.setLevel(logging.DEBUG)
log.addHandler(stream_handler)
log.addHandler(file_handler)


class GlobalOptimizer(SequentialTaskCollection):
    """Main loop for the global optimizer.

    :param str jobname:       string that labels this optimization case.

    :param path_to_stage_dir: directory in which to perform the optimization.

    :param optimizer: Optimizer instance that conforms to the abstract class
     optimization algorithm.

    :param task_constructor: A function that takes a list of x vectors
                             and the path to the current iteration
                             directory, and returns Application
                             instances that can be executed on the
                             grid.

    :param target_fun:       Takes a list of (x_vector,
                             application_instance) tuples and returns
                             the corresponding function value for the
                             x_vector.

    :param cur_pop_file:     Filename under which the population is stored
                             in the current iteration dir. The
                             population is discarded if no file is
                             specified.

    """

    def __init__(self, jobname = '', path_to_stage_dir = '',
                 optimizer = None, task_constructor = None,
                 target_fun = None, cur_pop_file = '',
                 **extra_args ):

        log.debug('entering GlobalOptimizer.__init__')

        # Set up initial variables and set the correct methods.
        self.jobname = jobname
        self.path_to_stage_dir = path_to_stage_dir
        self.optimizer = optimizer
        self.target_fun = target_fun
        self.task_constructor = task_constructor
        self.cur_pop_file = cur_pop_file
        self.extra_args = extra_args
        self.output_dir = os.getcwd()

        self.evaluator = ComputeTargetVals(self.optimizer.new_pop, self.jobname, self.optimizer.cur_iter,
                                           path_to_stage_dir, self.cur_pop_file, task_constructor)

        initial_task = self.evaluator

        SequentialTaskCollection.__init__(self,  [initial_task], **extra_args)


    def next(self, *args):
        log.debug('entering GlobalOptimizer.next')

        self.changed = True
        # pass on (popMem, Application)
        pop_task_tuple = [(popEle, task) for (popEle, task) in zip(self.optimizer.new_pop, self.evaluator.tasks)]

        newVals = self.target_fun(pop_task_tuple)
        self.optimizer.update_opt_state(newVals)

        if not self.optimizer.has_converged():
            self.optimizer.new_pop = self.optimizer.evolve()
            # Check constraints and resample points to maintain population size.
  #          self.optimizer.new_pop = self.optimizer.enforce_constr_re_evolve(self.optimizer.new_pop)
            self.evaluator = ComputeTargetVals(self.optimizer.new_pop, self.jobname, self.optimizer.cur_iter,
                                               self.path_to_stage_dir, self.cur_pop_file, self.task_constructor)
            self.add(self.evaluator)
        else:
            # !! save should be optional !!
            open(os.path.join(self.path_to_stage_dir, 'job_done'), 'w')
            # report success of sequential task
            self.execution.returncode = 0
            return Run.State.TERMINATED
        return Run.State.RUNNING


    def __str__(self):
        return self.jobname

    # Adjustments for pickling
    # def __getstate__(self):
    #     state = Task.__getstate__(self)
    #     # Check that there are no functions in state.
    #     #for attr in ['optimizer']:
    #         ## 'task_constructor', 'target_fun', 'tasks',
    #         #del state[attr]
    #    # state = None
    #     return state

    # def __setstate__(self, state):
    #     # restore _grid, etc.
    #     Task.__setstate__(self, state)
    #     # restore loggers
    #     #self._setup_logging()


class ComputeTargetVals(ParallelTaskCollection):

    """
    Generate a list of tasks and initialize a ParallelTaskCollection with them.

    :param inParaCombos: List of tuples defining the parameter combinations.
    
    :param jobname: Name of GlobalOptimizer instance driving the
     optimization. 
    
    :param iteration: Current iteration number. 
    
    :param path_to_stage_dir: Path to directory in which optimization takes
    place.

    :param cur_pop_file: Filename under which the population is stored in the
    current iteration dir. The population is discarded if no file is
    specified. :param task_constructor: Takes a list of x vectors and the
    path to the current iteration directory. Returns Application instances
    that can be executed on the grid.
    """

    def __str__(self):
        return self.jobname


    def __init__(self, inParaCombos, jobname, iteration, path_to_stage_dir,
                 cur_pop_file, task_constructor, **extra_args):

        log.debug('entering ComputeTargetVals.__init__')

        # Set up initial variables and set the correct methods.
        self.jobname = 'evalSolverGuess' + '-' + jobname + '-' + str(iteration)
        self.iteration = iteration
        self.output_dir = os.getcwd()

        self.path_to_stage_dir = path_to_stage_dir
        self.cur_pop_file = cur_pop_file
        self.verbosity = 'DEBUG'
        self.extra_args = extra_args

        # Log activity
        cDate = datetime.date.today()
        cTime = datetime.datetime.time(datetime.datetime.now())
        date_string = '%04d--%02d--%02d--%02d--%02d--%02d' % (cDate.year, 
                                                cDate.month, cDate.day, cTime.hour, 
                                                cTime.minute, cTime.second)
        gc3libs.log.debug('Establishing parallel task on %s', date_string)

        # Enter an iteration specific folder
        self.iterationFolder = os.path.join(self.path_to_stage_dir,
                                            'Iteration-' + str(self.iteration))
        try:
            os.mkdir(self.iterationFolder)
        except OSError:
            print '%s already exists' % self.iterationFolder

        # save population to file
        if cur_pop_file:
            np.savetxt(os.path.join(self.iterationFolder, cur_pop_file),
                       inParaCombos, delimiter = ' ')

        self.tasks = [
            task_constructor(x_vec, self.iterationFolder) for x_vec in inParaCombos
        ]
        ParallelTaskCollection.__init__(self, self.tasks, **extra_args)


class EvolutionaryAlgorithm(object):
    '''
    Base class for building an evolutionary algorithm for global optimization.
    '''

    def __init__(self, whatever):
        """Document what this method should do."""
        raise NotImplementedError("Abstract method `LRMS.free()` called - this should have been defined in a derived class.")

    def update_opt_state(self, new_vals = None):
        '''
          Updates the solver with the newly evaluated population and the corresponding
          new_vals.
        '''
        pass

    def has_converged(self):
        '''
          Check all specified convergence criteria and return whether converged.
        '''
        return False

    def evaluate(self, pop):
        # For each indivdual in self.population evaluate individual
        return fitness_vector

    def select(self, pop, fitness_vec):
        pass # return a matrix of size self.size

    # a list of modified population, for example mutated, recombined, etc.
    def evolve(self, offspring):
        return modified_population # a mixture of different variations


def draw_population(lower_bds, upper_bds, dim, size, filter_fn = None):
    '''
      Check that each ele satisfies fullfills all constraints. If not, then draw a new population memeber and check constraint.
    '''

    pop = lower_bds + np.random.random_sample( (size, dim) ) * ( upper_bds - lower_bds )

    # If a filter function is specified, resample until a sample fullfilling the filter
    # is found.
    if filter_fn:
        ctr = 0
        max_n_resample = 100
        dim = self.dim
        # check filter_fn | should I use pop or self.pop here?
        pop_valid = self.filter_fn(pop)
        n_invalid_pop = (pop_valid == False).sum()
        while n_invalid_pop > 0 and ctr < max_n_resample:
            resampled_pop = lower_bds + np.random.random_sample( (n_invalid_pop, dim) ) * ( upper_bds - lower_bds )
            #draw_population(self.lower_bds, self.upper_bds, n_invalid_pop, self.dim)
            pop[~pop_valid] = resampled_pop
            pop_valid = self.filter_fn(pop)
            n_invalid_pop = (pop_valid == False).sum()

    return pop

def populate(create_fn, filter_fn=None, max_n_resample=100):
    pop = create_fn()
    if filter_fn:
        # re-evolve if some members do not fullfill fiter_fn
        pop_valid_orig = filter_fn(pop)
        n_invalid_orig = (pop_valid_orig == False).sum()
        fillin_pop = pop[~pop_valid_orig]
        total_filled = 0
        ctr = 0
        while total_filled < n_invalid_orig and ctr < max_n_resample:
            new_pop = create_fn()
            new_pop_valid = filter_fn(new_pop)
            n_pop_valid = (new_pop_valid == True).sum()
            new_total_filled = min(total_filled + n_pop_valid, len(fillin_pop))
            fillin_pop[total_filled:new_total_filled] = new_pop[new_pop_valid]
            total_filled = new_total_filled
        if total_filled < n_invalid_orig:
            self.logger.warning(
                "%d population members are invalid even after re-sampling %d times."
                "  You might want to increase `max_n_resample`.",
                (n_invalid_orig - total_filled), max_n_resample)
        pop[~pop_valid_orig] = fillin_pop
    return pop


def draw_population(lower_bds, upper_bds, dim, size, filter_fn = None):
    return populate(create_fn=lambda:(lower_bds + np.random.random_sample( (size, dim) ) * ( upper_bds - lower_bds )), 
                    filter_fn=filter_fn)