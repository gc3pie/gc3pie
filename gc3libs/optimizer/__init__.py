#! /usr/bin/env python

"""
Support for finding minima of functions with GC3Pie.

GC3Pie can run a large number of :class:`~gc3libs.Application` instances in
parallel. The idea of this optimization module is to use these core
capabilities to perform optimization, which is particularly effective for
optimization using evolutionary algorithms, as they require several independent
evaluations of the target function.

The optimization module has two main components, the driver and the algorithm.
You need both an instance of a driver and an instance of an algorithm to
perform optimization of a given function.

Drivers perform optimization following a specific algorithm. Two drivers are
currently implemented: :class:`drivers.SequentialDriver` that runs the entire
algorithm on the local computer (hence, all the evaluations of the target
function required by the algorithm are performed one after the other), and
:class:`drivers.ParallelDriver` splits the evaluations into tasks that are
executed in parallel using GC3Pie's remote execution facilities.

This module implements a generic framework for evolutionary algorithms, and one
particular type of global optimization algorithm called `Differential
Evolution`_ is worked out in full. Other Evolutionary Algorithms can easily be
incorporated by subclassing :class:`EvolutionaryAlgorithm`. (Different
optimization algorithms, for example gradient based methods such as
quasi-newton methods, could be implemented but likely require adaptations in
the driver classes.)

.. _`differential evolution`: http://stackoverflow.com/a/7519536

The module is organized as follows:

* :mod:`~gc3libs.optimizer.drivers`: Set of drivers that interface with GC3Libs
  to automatically drive the optimization process following a specified
  algorithm. :class:`~gc3libs.optimizer.drivers.ParallelDriver` is the core of
  the optimization module, performing optimization using an algorithm based on
  :class:`EvolutionaryAlgorithm`.

* :mod:`~gc3libs.optimizer.dif_evolution`: Implements the Differential
  Evolution algorithm, in particular the evolution and selection step, based on
  :class:`EvolutionaryAlgorithm`. See the module for details on the algorithm.

* :mod:`~gc3libs.optimizer.extra`: Provides tools to printing, plotting etc. that can be
  used as addons to :class:`~gc3libs.optimizer.EvolutionaryAlgorithm`.

"""

# Copyright (C) 2011, 2012, 2013  University of Zurich. All rights reserved.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
from builtins import object
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
from gc3libs import Application, Run, Task, utils
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection


class EvolutionaryAlgorithm(object):

    '''
    Base class for building an evolutionary algorithm for global optimization.

    :param initial_pop: Initial population for the optimization.
      The value can be any sequence that can be passed to `np.array()`
    :param int `itermax`: Maximum # of iterations.
    :param float `dx_conv_crit`: Abort optimization if all population members are within a certain distance to each other.
    :param float `y_conv_crit`: Declare convergence when the target function is below a `y_conv_crit`.
    :param obj `logger`: Configured logger to use.
    :param `after_update_opt_state`: List of functions that are called
      at the end of `update_opt_state`:meth:.
      Use this list to provide problem-specific printing and plotting routines.
      Examples can be found in `gc3libs.optimizer.extra`:mod:.
    '''

    def __init__(self, initial_pop,
                 # criteria for convergence
                 itermax=100, dx_conv_crit=None, y_conv_crit=None,
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

        :rtype: bool
        '''
        converged = False
        # Check `y_conv_crit`
        if self.best_y < self.y_conv_crit:
            converged = True
            self.logger.info(
                'Converged: self.best_y[%s] < self.y_conv_crit[%s]',
                self.best_y,
                self.y_conv_crit)

        # Check `dx_conv_crit`
        dxs = np.abs(self.pop[:, :] - self.pop[0, :])
        if self.dx_conv_crit is not None:
            has_dx_converged = (dxs <= self.dx_conv_crit).all()
        else:
            has_dx_converged = False
        if has_dx_converged:
            converged = True
            self.logger.info(
                'Converged: All population members within `dx_conv_crit` from the first population member. ')
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
        self.logger.debug(
            'Updating optimizer state with new values: %s',
            str(new_vals))

        # In variable names `best` refers to a population member with the
        # lowest target function value within some group:

        # best_x: Coordinates of the best population member since the optimization started.
        # best_y: Val of the best population member since the optimization
        # started.

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

        self.logger.debug(
            'Computed best value: %s (at index %d)',
            self.best_y,
            best_ix)

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
        Generates a new population fullfilling :func:`in_domain`.
        :rtype list of population members
        '''
        raise NotImplemented(
            "Method `EvolutionaryAlgorithm.evolve` should be implemented in subclasses!")


def populate(create_fn, in_domain=None, max_n_resample=100):
    '''
    Generate a new population.

    Uses :func:`create_fn` to generate a new population. If :func:`in_domain` is not
    fulfilled, :func:`create_fn` is called repeatedly. Invalid population members are
    replaced until reaching the desired valid population size or
    `max_n_resample` calls to :func:`create_fn`. If `max_n_resample` is reached, a
    warning is issued and the optimization continues with the remaining
    "invalid" members.

    :param fun create_fn: Generates a new population. Takes no arguments.
    :param fun in_domain: Determines population's validity.
                          Takes no arguments and returns a list of bools
                          indicating each members validity.
    :param int max_n_resample: Maximum number of resamples to be drawn to
                               satisfy :func:`in_domain`

    :rtype: list of population members
    '''
    pop = create_fn()
    if in_domain:
        # re-evolve if some members do not fullfill fiter_fn
        pop_valid_orig = np.array(in_domain(pop))
        n_invalid_orig = (~pop_valid_orig).sum()
        fillin_pop = pop[~pop_valid_orig]
        n_to_fill = len(fillin_pop)
        total_filled = 0
        ctr = 0
        while total_filled < n_invalid_orig and ctr < max_n_resample:
            new_pop = create_fn()
            new_pop_valid = np.array(in_domain(new_pop))
            n_pop_valid = new_pop_valid.sum()
            new_total_filled = min(total_filled + n_pop_valid, n_to_fill)
            n_new_recruits = new_total_filled - total_filled
            ix_new_recruits = np.where(new_pop_valid)[0][0:n_new_recruits]
            fillin_pop[
                total_filled:new_total_filled] = new_pop[ix_new_recruits]
            total_filled = new_total_filled
        if total_filled < n_invalid_orig:
            self.logger.warning(
                "%d population members are invalid even after re-sampling %d times."
                "  You might want to increase `max_n_resample`.",
                (n_invalid_orig - total_filled),
                max_n_resample)
        pop[~pop_valid_orig] = fillin_pop
    return pop


def draw_population(
        lower_bds,
        upper_bds,
        dim,
        size,
        in_domain=None,
        seed=None):
    '''
    Draw a random population with the following criteria:

    :param lower_bds: List of length `dim` indicating the lower bound in each dimension.
    :param upper_bds: List of length `dim` indicating the upper bound in each dimension.
    :param int dim: Dimension of each population member.
    :param int size: Population size.
    :param fun in_domain: Determines population's validity.
                          Takes no arguments and returns a list of bools
                          indicating each members validity.
    :param float `seed`: Seed to initialize NumPy's random number generator.
    :rtype: list of population members
    '''
    np.random.seed(seed)
    return populate(create_fn=lambda: (lower_bds +
                                       np.random.random_sample((size, dim)) *
                                       (upper_bds -
                                        lower_bds)), in_domain=in_domain)
