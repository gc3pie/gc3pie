#! /usr/bin/env python
#
"""
Unit tests for the `gc3libs.optimizer.dif_evolution` module.

Test using the 2-d Rosenbrock function: 
    f(x,y) = (1-x)**2 + 100 * (y - x**2)**2
The function has a global minimum at (x,y) = (1,1) with f(x,y) = 0
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
import tempfile
import shutil

from nose.tools import raises
from nose.plugins.skip import SkipTest

import numpy as np

from gc3libs import configure_logger
from gc3libs.optimizer.dif_evolution import DifferentialEvolutionAlgorithm
from gc3libs.optimizer.drivers import LocalDriver
from gc3libs.optimizer import draw_population
from gc3libs.optimizer.extra import print_stats, log_stats, plot_population

# Create a temporary stage directory
temp_stage_dir = tempfile.mkdtemp(prefix = 'LocalDriver_Rosenbrock_')

np.set_printoptions(linewidth=300, precision=8, suppress=True)

# Test parameters
magic_seed = 100
dim = 2
pop_size = 100
lower_bounds = -2 * np.ones(dim)
upper_bounds = +2 * np.ones(dim)
prob_cross = 0.8
filter_pop_sum = 3.                # x[0] + x[1] <= filter_pop_sum


configure_logger(level=logging.DEBUG)
log = logging.getLogger("gc3.gc3libs")

def rosenbrock_fn(vectors):
    result = []
    for vector in vectors:
        #---Rosenbrock saddle-------------------------------------------
        F_cost = 100 * ( vector[1] - vector[0]**2 )**2 + ( 1 - vector[0] )**2

        result.append(F_cost)
    return np.array(result)

def rosenbrock_sample_filter(pop):
    '''
    Sample filter function. 
    In optimum x[0] + x[1] = 2. 
    '''
    return [ x[0] + x[1] <= filter_pop_sum for x in pop ]

class TestLocalDriver(object):
    def tearDown(self):
        # Remove Rosenbrock output
        log.debug('Removing temporary directory: \n%s', temp_stage_dir)
        shutil.rmtree(temp_stage_dir)
    
    def test_LocalDriver_with_rosenbrock(self):
        '''
        Unit test for LocalDriver class. 
        '''
        initial_pop = draw_population(lower_bds = lower_bounds, upper_bds = upper_bounds, dim = dim, size = pop_size, 
                                      filter_fn = rosenbrock_sample_filter, seed = magic_seed)
    
        algo = DifferentialEvolutionAlgorithm(
            initial_pop = initial_pop,
            de_step_size = 0.85,# DE-stepsize ex [0, 2]
            prob_crossover = prob_cross, # crossover probabililty constant ex [0, 1]
            itermax = 1000,      # maximum number of iterations (generations)
            dx_conv_crit = None, # stop when variation among x's is < this
            y_conv_crit = 1e-5, # stop when ofunc < y_conv_crit
            de_strategy = 'DE_local_to_best',
            logger = log, 
            filter_fn=rosenbrock_sample_filter,
            seed = magic_seed,
            after_update_opt_state=[print_stats, log_stats, plot_population(os.getcwd())]
            )
        assert algo.de_step_size == 0.85
        assert algo.prob_crossover == prob_cross
        assert algo.itermax == 1000
        assert algo.dx_conv_crit == None
        assert algo.y_conv_crit == 1e-5
        assert algo.de_strategy == 'DE_local_to_best'
        assert algo.logger == log
    
        opt = LocalDriver(algo, target_fn=rosenbrock_fn)
        assert opt.target_fn == rosenbrock_fn
    
        # run the Diff.Evo. algorithm
        opt.de_opt()
    
        assert algo.has_converged()
        assert (algo.best_y - 0.) < algo.y_conv_crit
        assert (algo.best_x[0] - 1.) < 1e-3
        assert (algo.best_x[1] - 1.) < 1e-3


