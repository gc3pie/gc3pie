#! /usr/bin/env python
#
"""
Class to perform global optimization.

Optimization algorithm (for example Ken Price's Differential
Evolution algorithm) generates guesses that are evaluated in parallel
using gc3pie.

An instance of :class:`GridOptimizer` will perform the entire optimization
in a directory on the local machine named `path_to_stage_dir`.

At each iteration an instance of 'ComputeTargetVals' lets the user-defined
function `task_constructor` generate :class:`Application` instances that are
used to execute the jobs in parallel on the grid. When all
jobs are complete, the objective's output is analyzed with the user-supplied
function `target_fun`. This function returns the function value for all
analyzed input vectors.

With this information, the optimizer generates a new guess. The instance of
:class:`GridOptimizer` iterates until the sepcified convergence criteria
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


# Could call this GridOptimizer

# class LocalOptimizer
# SequentialOptimizer
# OptimizeLocal

# gc3Optimizer
# DistributedOptimizer
class GridOptimizer(SequentialTaskCollection):
    """Main loop for the global optimizer.

    :param str jobname:       string that labels this optimization case.

    :param path_to_stage_dir: directory in which to perform the optimization.

    :param opt_algorithm:    Evolutionary algorithm instance that conforms
                             to :class:`EvolutionaryAlgorithm`.

    :param task_constructor: A function that takes a list of x vectors
                             and the path to the current iteration
                             directory, and returns Application
                             instances that can be executed on the
                             grid.

    :param extract_value_fn: Takes an `Application`:class: instance returns
                             the function value computed in that task.
                             The default implementation just looks for a
                             `.value` attribute on the application instance.

    :param cur_pop_file:     Filename under which the population is stored
                             in the current iteration dir. The
                             population is discarded if no file is
                             specified.

    """

    def __init__(self, jobname = '', path_to_stage_dir = '',
                 opt_algorithm = None, task_constructor = None,
                 extract_value_fn = (lambda app: app.value),
                 cur_pop_file = '',
                 **extra_args ):

        log.debug('entering GridOptimizer.__init__')

        # Set up initial variables and set the correct methods.
        self.jobname = jobname
        self.path_to_stage_dir = path_to_stage_dir
        self.opt_algorithm = opt_algorithm
        self.extract_value_fn = extract_value_fn
        self.task_constructor = task_constructor
        self.cur_pop_file = cur_pop_file
        self.extra_args = extra_args
        self.output_dir = os.getcwd()

        self.new_pop = self.opt_algorithm.pop
        initial_task = ComputeTargetVals(
            self.opt_algorithm.pop, self.jobname, self.opt_algorithm.cur_iter,
            path_to_stage_dir, self.cur_pop_file, task_constructor)

        SequentialTaskCollection.__init__(self,  [initial_task], **extra_args)

    def next(self, done):
        log.debug('entering GridOptimizer.next(%d)', done)

        # feed back results from the evaluation just completed
        new_pop = self.new_pop
        new_vals = [ self.extract_value(task) for task in self.tasks[done].tasks ]
        self.opt_algorithm.update_opt_state(new_pop, new_vals)

        self.changed = True

        if opt_algorithm.cur_iter > opt_algorithm.itermax:
            # maximum number of iterations exceeded
            # XXX: what return code is appropriate here?
            self.execution.exitcode = os.EX_TEMPFAIL
            return Run.State.TERMINATED

        # still within allowed number of iterations, check convergence
        if self.opt_algorithm.has_converged():
            # report success of sequential task
            self.execution.returncode = 0
            return Run.State.TERMINATED
        else:
            # prepare next evaluation
            self.new_pop = self.opt_algorithm.evolve()
            self.add(
                ComputeTargetVals(
                    self.new_pop, self.jobname, self.opt_algorithm.cur_iter,
                    self.path_to_stage_dir, self.cur_pop_file, self.task_constructor))
            return Run.State.RUNNING


    def __str__(self):
        return self.jobname

    # Adjustments for pickling
    # def __getstate__(self):
    #     state = Task.__getstate__(self)
    #     # Check that there are no functions in state.
    #     #for attr in ['opt_algorithm']:
    #         ## 'task_constructor', 'extract_value_fn', 'tasks',
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

    :param jobname: Name of GridOptimizer instance driving the
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

    def __init__(self, initial_pop,
                 # criteria for convergence
                 itermax = 100, dx_conv_crit = None, y_conv_crit = None,
                 # hooks for "extra" functions, e.g., printg/logging/plotting
                 logger=None, after_update_opt_state=[]):
        """Document what this method should do."""

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('gc3.gc3libs')

        # save parameters
        self.pop = initial_pop
        self.dim = len(initial_pop[0])
        self.pop_size = len(initial_pop)

        self.y_conv_crit = y_conv_crit
        self.dx_conv_crit = dx_conv_crit

        self.itermax = itermax
        self.cur_iter = 0

        self.after_update_opt_state = after_update_opt_state


    def has_converged(self):
        '''
        Checks convergence based on two criteria:

        1) Is the lowest target value in the population below `y_conv_crit`.
        2) Are all population members within `dx_conv_crit` from the first population member.
        '''
        converged = False
        # Check `y_conv_crit`
        if self.best_y < self.y_conv_crit:
            converged = True
            self.logger.info('Converged: self.best_y < self.y_conv_crit')

        # Check `dx_conv_crit`
        dxs = np.abs(self.pop[:, :] - self.pop[0, :])
        has_dx_converged = (dxs <= self.dx_conv_crit).all()
        if has_dx_converged:
            converged = True
            self.logger.info('Converged: All population members within `dx_conv_crit` from the first population member. ')
        return converged

    def update_opt_state(self, new_pop, new_vals):
        '''
        Stores set of function values corresponding to the current
        population, then updates optimizer state in many ways:

        * update the `.best*` variables accordingly;
        * merges the two populations (old and new), keeping only the members with lower corresponding value;
        * advances iteration count.
        '''

        self.logger.debug('entering update_opt_state')

        # In variable names `best` refers to a population member with the
        # lowest target function value within some group:

        # best_x: Coordinates of the best population member since the optimization started.
        # best_y: Val of the best population member since the optimization started.

        new_vals = np.array(new_vals)
        # determine the member with the lowest target value
        best_ix = np.argmin(new_vals)
        if self.cur_iter == 0 or new_vals[best_ix] < self.best_y:
            # store the best population members
            self.best_x = new_pop[best_ix, :].copy()
            self.best_y = new_vals[best_ix].copy()

        if self.cur_iter > 0:
            # update self.pop and self.vals
            self.select(new_pop, new_vals)
        else:
            self.pop = new_pop
            self.vals = new_vals

        self.logger.debug('new values %s', new_vals)
        self.logger.debug('best value %s', self.best_y)

        for fn in self.after_update_opt_state:
            fn(self)

        self.cur_iter += 1


    def select(self, new_pop, new_vals):
        """
        Update `self.pop` and `self.vals` given the new population
        and the corresponding fitness vector.
        """
        raise NotImplemented(
            "Method `EvolutionaryAlgorithm.select` should be implemented in subclasses!")


    def evolve(self):
        '''
        Generates a new population fullfilling `filter_fn`.
        '''
        raise NotImplemented(
            "Method `EvolutionaryAlgorithm.evolve` should be implemented in subclasses!")


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