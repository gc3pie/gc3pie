#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, 2012, 2013, GC3, University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
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
__docformat__ = 'reStructuredText'
__version__ = '$Revision$'

import os
import logging

from nose.tools import raises
from nose.plugins.skip import SkipTest

import numpy as np

from gc3libs.optimizer.dif_evolution import DifferentialEvolutionSequential, DifferentialEvolutionAlgorithm
from gc3libs.optimizer import draw_population

def rosenbrock_fn(vectors):
    result = []
    for vector in vectors:
        #---Rosenbrock saddle-------------------------------------------
        F_cost = 100 * ( vector[1] - vector[0]**2 )**2 + ( 1 - vector[0] )**2

        result.append(F_cost)
    return np.array(result)


def test_differential_evolution_sequential_with_rosenbrock():

    # FVr_minbound,FVr_maxbound   vector of lower and bounds of initial population
    #               the algorithm seems to work especially well if [FVr_minbound,FVr_maxbound]
    #               covers the region where the global minimum is expected
    #               *** note: these are no bound constraints!! ***
    dim = 2
    pop_size = 100
    lower_bounds = -2 * np.ones(dim)
    upper_bounds = +2 * np.ones(dim)
    prob_cross = 0.8

    log = logging.getLogger("gc3.gc3libs")

    initial_pop = draw_population(lower_bounds, upper_bounds, dim, pop_size)

    algo = DifferentialEvolutionAlgorithm(
        initial_pop = initial_pop,
        de_step_size = 0.85,# DE-stepsize ex [0, 2]
        prob_crossover = prob_cross, # crossover probabililty constant ex [0, 1]
        itermax = 1000,      # maximum number of iterations (generations)
        dx_conv_crit = None, # stop when variation among x's is < this
        y_conv_crit = 1e-5, # stop when ofunc < y_conv_crit
        de_strategy = 'DE_local_to_best',
        logger = log
        )
    assert algo.de_step_size == 0.85
    assert algo.prob_crossover == prob_cross
    assert algo.itermax == 1000
    assert algo.dx_conv_crit == None
    assert algo.y_conv_crit == 1e-5
    assert algo.de_strategy == 'DE_local_to_best'
    assert algo.logger == log

    opt = DifferentialEvolutionSequential(algo, target_fn=rosenbrock_fn)
    assert opt.target_fn == rosenbrock_fn

    # run the Diff.Evo. algorithm
    opt.de_opt()

    assert algo.has_converged()
    assert (algo.best_y - 0.) < algo.y_conv_crit
    assert (algo.best_x[0] - 1.) < 1e-3
    assert (algo.best_x[1] - 1.) < 1e-3


def test_differential_evolution_parallel_with_rosenbrock():

    # FVr_minbound,FVr_maxbound   vector of lower and bounds of initial population
    #               the algorithm seems to work especially well if [FVr_minbound,FVr_maxbound]
    #               covers the region where the global minimum is expected
    #               *** note: these are no bound constraints!! ***
    dim = 2
    pop_size = 100
    lower_bounds = -2 * np.ones(dim)
    upper_bounds = +2 * np.ones(dim)

    log = logging.getLogger("gc3.gc3libs")

    initial_pop = draw_population(lower_bounds, upper_bounds, dim, pop_size)

    opt = DifferentialEvolutionAlgorithm(
        initial_pop = initial_pop,
        de_step_size = 0.85,# DE-stepsize ex [0, 2]
        prob_crossover = 0.8, # crossover probabililty constant ex [0, 1]
        itermax = 1000,      # maximum number of iterations (generations)
        dx_conv_crit = None, # stop when variation among x's is < this
        y_conv_crit = 1e-5, # stop when ofunc < y_conv_crit
        de_strategy = 'DE_local_to_best',
        logger = log
        )

    new_pop = opt.pop
    newVals = rosenbrock_fn(opt.pop)
    opt.update_opt_state(new_pop, newVals)

    has_converged = False
    while not has_converged:
        new_pop = opt.evolve()
        ### The evaluation needs to be parallelized
        newVals = rosenbrock_fn(new_pop)
        opt.update_opt_state(new_pop, newVals)
        has_converged = opt.has_converged()

    assert opt.has_converged()
    assert (opt.best_y - 0.) < opt.y_conv_crit
    assert (opt.best_x[0] - 1.) < 1e-3
    assert (opt.best_x[1] - 1.) < 1e-3
