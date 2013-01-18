#! /usr/bin/env python
#
"""

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


def draw_population(lower_bds, upper_bds, dim, size, filter_fn = None, seed = None):
    np.random.seed(seed)
    return populate(create_fn=lambda:(lower_bds + np.random.random_sample( (size, dim) ) * ( upper_bds - lower_bds )),
                    filter_fn=filter_fn)