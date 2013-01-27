#! /usr/bin/env python
#
"""
Support for running optimizations with the GC3Libs.
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
import re
import shutil

import numpy as np

# gc3libs imports
import gc3libs
import gc3libs.debug
import gc3libs.config
import gc3libs.core
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
from gc3libs import Application, Run, Task

class EvolutionaryAlgorithm(object):
    '''
    Base class for building an evolutionary algorithm for global optimization.

    :param list initial_pop: Initial population for the optimization.
    :param int `itermax`: Maximum # of iterations.
    :param float `dx_conv_crit`: Abort optimization if all population members are within a certain distance to each other.
    :param float `y_conv_crit`: Declare convergence when the target function is below a `y_conv_crit`.
    :param obj `logger`: Configured logger to use.
    :param list `after_update_opt_state`: Functions that are called at the end of
                `update_opt_state`:meth:. Use this list
                to provide problem-specific printing and plotting routines. Examples can be found
                in `gc3libs.optimizer.extra`:mod:.
    '''

    def __init__(self, initial_pop,
                 # criteria for convergence
                 itermax = 100, dx_conv_crit = None, y_conv_crit = None,
                 # hooks for "extra" functions, e.g., printg/logging/plotting
                 logger=None, after_update_opt_state=[]):

        if logger:
            self.logger = logger
        else:
            self.logger = gc3libs.log

        # save parameters
        self.pop = np.array(initial_pop)
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
            self.logger.info('Converged: self.best_y[%s] < self.y_conv_crit[%s]',
                             self.best_y, self.y_conv_crit)

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
        * uses :meth:`select` to determine the surviving population.
        * advances iteration count.
        '''

        gc3libs.log.debug('Entering update_opt_state ...')
        # XXX: `new_vals` is a NumPy array, so the way it is printed
        # is influenced by Numpy's `set_printoptions()` -- the default
        # settings introduce line breaks at every 75th column, so the
        # following results in a multi-line log even for moderate-size
        # populations...  You might want to
        # `np.set_printoptions(linewidth=1024)` or so to prevent this.
        self.logger.debug('Updating optimizer state with new values: %s', str(new_vals))

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

        self.logger.debug('Computed best value: %s (at index %d)', self.best_y, best_ix)

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
        Generates a new population fullfilling :func:`filter_fn`.
        '''
        raise NotImplemented(
            "Method `EvolutionaryAlgorithm.evolve` should be implemented in subclasses!")


def populate(create_fn, filter_fn=None, max_n_resample=100):
    '''
    Uses :func:`create_fn` to generate a new population. If :func:`filter_fn` is not
    fulfilled, :func:`create_fn` is called repeatedly. Invalid population members are
    replaced until reaching the desired valid population size or
    `max_n_resample` calls to :func:`create_fn`. If `max_n_resample` is reached, a
    warning is issued and the optimization continues with the remaining
    "invalid" members.

    :param fun create_fn: Generates a new population. Takes no arguments.
    :param fun filter_fn: Determines population's validity.
                          Takes no arguments and returns a list of bools
                          indicating each members validity.
    :param int max_n_resample: Maximum number of resamples to be drawn to
                               satisfy :func:`filter_fn`.
    '''
    pop = create_fn()
    if filter_fn:
        # re-evolve if some members do not fullfill fiter_fn
        pop_valid_orig = np.array(filter_fn(pop))
        n_invalid_orig = (~pop_valid_orig).sum()
        fillin_pop = pop[~pop_valid_orig]
        n_to_fill = len(fillin_pop)
        total_filled = 0
        ctr = 0
        while total_filled < n_invalid_orig and ctr < max_n_resample:
            new_pop = create_fn()
            new_pop_valid = np.array(filter_fn(new_pop))
            n_pop_valid = new_pop_valid.sum()
            new_total_filled = min(total_filled + n_pop_valid, n_to_fill)
            n_new_recruits = new_total_filled - total_filled
            ix_new_recruits = np.where(new_pop_valid)[0][0:n_new_recruits]
            fillin_pop[total_filled:new_total_filled] = new_pop[ix_new_recruits]
            total_filled = new_total_filled
        if total_filled < n_invalid_orig:
            self.logger.warning(
                "%d population members are invalid even after re-sampling %d times."
                "  You might want to increase `max_n_resample`.",
                (n_invalid_orig - total_filled), max_n_resample)
        pop[~pop_valid_orig] = fillin_pop
    return pop


def draw_population(lower_bds, upper_bds, dim, size, filter_fn = None, seed = None):
    '''
    Draw a random population with the following criteria:

    :param list lower_bds: List of length `dim` indicating the lower bound in each dimension.
    :param list upper_bds: List of length `dim` indicating the upper bound in each dimension.
    :param int dim: Dimension of each population member.
    :param int size: Population size.
    :param fun filter_fn: Determines population's validity.
                          Takes no arguments and returns a list of bools
                          indicating each members validity.
    :param float `seed`: Seed to initialize NumPy's random number generator.
    '''
    np.random.seed(seed)
    return populate(create_fn=lambda:(lower_bds + np.random.random_sample( (size, dim) ) * ( upper_bds - lower_bds )),
                    filter_fn=filter_fn)

@gc3libs.debug.trace
def update_parameter_in_file(path, var_in, new_val, regex_in):
    '''
    Updates a parameter value in a parameter file using predefined regular
    expressions in `_loop_regexps`.
    
    :param path: Full path to the parameter file. 
    :param var_in: The variable to modify. 
    :param new_val: The updated parameter value. 
    :param regex: Name of the regular expression that describes the format of the parameter file. 
    '''
    _loop_regexps = {
        'bar-separated':(r'([a-z]+[\s\|]+)'
                         r'(\w+)' # variable name
                         r'(\s*[\|]+\s*)' # bars and spaces
                         r'([\w\s\.,;\[\]\-]+)' # value
                         r'(\s*)'),
        'space-separated':(r'(\s*)'
                           r'(\w+)' # variable name
                           r'(\s+)' # spaces (filler)
                           r'([\w\s\.,;\[\]\-]+)' # values
                           r'(\s*)'), # spaces (filler)
    }
    isfound = False
    if regex_in in _loop_regexps.keys():
        regex_in = _loop_regexps[regex_in]
    para_file_in = open(path, 'r')
    para_file_out = open(path + '.tmp', 'w')
    for line in para_file_in:
        #print "Read line '%s' " % line
        if not line.rstrip(): continue
        (a, var, b, old_val, c) = re.match(regex_in, line.rstrip()).groups()
        gc3libs.log.debug("Read variable '%s' with value '%s' ...", var, old_val)
        if var == var_in:
            isfound = True
            upd_val = new_val
        else:
            upd_val = old_val
        para_file_out.write(a + var + b + upd_val + c + '\n')
    para_file_out.close()
    para_file_in.close()
    # move new modified content over the old
    os.rename(path + '.tmp', path)
    if not isfound:
        gc3libs.log.critical('update_parameter_in_file could not find parameter in sepcified file')