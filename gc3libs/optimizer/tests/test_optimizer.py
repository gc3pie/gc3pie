#! /usr/bin/env python
#
"""
Unit tests for the `gc3libs.optimizer.drivers` module.
"""
# Copyright (C) 2011, 2012, 2013,  University of Zurich. All rights reserved.
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
from __future__ import absolute_import, print_function, unicode_literals
from builtins import object
__docformat__ = 'reStructuredText'

import os
import sys
import logging
import tempfile
import shutil

import cli.test

import numpy as np

import gc3libs
from gc3libs import Application, configure_logger
from gc3libs.cmdline import SessionBasedScript

# optimizer specific imports
from gc3libs.optimizer import draw_population
from gc3libs.utils import update_parameter_in_file
from gc3libs.optimizer.drivers import ParallelDriver, SequentialDriver
from gc3libs.optimizer.dif_evolution import DifferentialEvolutionAlgorithm
from gc3libs.optimizer.extra import print_stats, log_stats, plot_population


class TestSequentialDriver(object):

    def setUp(self):
        # Create a temporary stage directory
        self.temp_stage_dir = tempfile.mkdtemp(
            prefix='SequentialDriver_Rosenbrock_')

    def tearDown(self):
        # Remove Rosenbrock output
        shutil.rmtree(self.temp_stage_dir)

    def test_SequentialDriver_with_rosenbrock(self):
        """Test :class:`gc3libs.optimizer.drivers.SequentialDriver
        """

        # Test parameters
        magic_seed = 100
        dim = 2
        pop_size = 100
        lower_bounds = -2 * np.ones(dim)
        upper_bounds = +2 * np.ones(dim)
        prob_cross = 0.8

        configure_logger(level=logging.CRITICAL)
        log = logging.getLogger("gc3.gc3libs")

        initial_pop = draw_population(
            lower_bds=lower_bounds,
            upper_bds=upper_bounds,
            dim=dim,
            size=pop_size,
            in_domain=self.rosenbrock_sample_filter,
            seed=magic_seed)

        algo = DifferentialEvolutionAlgorithm(
            initial_pop=initial_pop,
            de_step_size=0.85,  # DE-stepsize ex [0, 2]
            prob_crossover=prob_cross,
            # crossover probabililty constant ex [0, 1]
            itermax=1000,      # maximum number of iterations (generations)
            dx_conv_crit=None,  # stop when variation among x's is < this
            y_conv_crit=1e-5,  # stop when ofunc < y_conv_crit
            de_strategy='DE_local_to_best',
            logger=log,
            in_domain=self.rosenbrock_sample_filter,
            seed=magic_seed,
            after_update_opt_state=[print_stats, log_stats]
            #, plot_population(temp_stage_dir)]
        )
        assert algo.de_step_size == 0.85
        assert algo.prob_crossover == prob_cross
        assert algo.itermax == 1000
        assert algo.dx_conv_crit is None
        assert algo.y_conv_crit == 1e-5
        assert algo.de_strategy == 'DE_local_to_best'
        assert algo.logger == log

        opt = SequentialDriver(
            algo,
            target_fn=self.rosenbrock_fn,
            fmt="%12.8f")
        assert opt.target_fn == self.rosenbrock_fn

        # run the Diff.Evo. algorithm
        opt.de_opt()

        assert algo.has_converged()
        assert (algo.best_y - 0.) < algo.y_conv_crit
        assert (algo.best_x[0] - 1.) < 1e-3
        assert (algo.best_x[1] - 1.) < 1e-3

    @staticmethod
    def rosenbrock_fn(vectors):
        result = []
        for vector in vectors:
            #---Rosenbrock saddle-------------------------------------------
            F_cost = 100 * \
                (vector[1] - vector[0] ** 2) ** 2 + (1 - vector[0]) ** 2

            result.append(F_cost)
        return np.array(result)

    @staticmethod
    def rosenbrock_sample_filter(pop):
        '''
        Sample filter function.
        In optimum x[0] + x[1] = 2.
        '''
        filter_pop_sum = 3.                # x[0] + x[1] <= filter_pop_sum
        return [x[0] + x[1] <= filter_pop_sum for x in pop]


class TestParallelDriver(cli.test.FunctionalTest):
    CONF = """
[resource/localhost_test]
type=shellcmd
transport=local
time_cmd=/usr/bin/time
max_cores=2
max_cores_per_job=2
max_memory_per_core=2
max_walltime=2
architecture=x64_64
auth=noauth
enabled=True
resourcedir=%s

[auth/noauth]
type=none
"""

    def __init__(self, *args, **extra_args):
        cli.test.FunctionalTest.__init__(self, *args, **extra_args)
        self.scriptdir = os.path.join(
            os.path.dirname(
                os.path.abspath(__file__)),
            '../../../examples/optimizer/rosenbrock')

    def setUp(self):
        cli.test.FunctionalTest.setUp(self)

        # Create stage_dir to run optimization in.
        self.temp_stage_dir = tempfile.mkdtemp(
            prefix='ParallelDriver_Rosenbrock_')

        # Create a sample user config file.
        (fd, cfgfile) = tempfile.mkstemp()
        resourcedir = cfgfile + '.d'
        f = os.fdopen(fd, 'w+')
        f.write(TestParallelDriver.CONF % resourcedir)
        f.close()
        sys.argv += ['--config-files', cfgfile, '-r', 'localhost_test']
        self.files_to_remove = [cfgfile, resourcedir]

        self.cfg = gc3libs.config.Configuration()
        self.cfg.merge_file(cfgfile)

        self.core = gc3libs.core.Core(self.cfg)
        self.backend = self.core.get_backend('localhost_test')

        # Create base dir with sample parameter file
        self.temp_base_dir = os.path.join(self.temp_stage_dir, 'base')
        os.mkdir(self.temp_base_dir)
        para_file = open(
            os.path.join(
                self.temp_base_dir,
                'parameters.in'),
            'w')
        para_file.write('x1    100.0\nx2    50.0\n')

    def cleanup_file(self, fname):
        self.files_to_remove.append(fname)

    def tearDown(self):
        cli.test.FunctionalTest.tearDown(self)

        for fname in self.files_to_remove:
            if os.path.isdir(fname):
                shutil.rmtree(fname)
            elif os.path.exists(fname):
                os.remove(fname)

        # Remove Rosenbrock output
        shutil.rmtree(self.temp_stage_dir)

    def test_ParallelDriver(self):
        """Test :class:`gc3libs.optimizer.drivers.ParallelDriver`

        The script is found in ``examples/optimizer/rosenbrock/opt_rosenbrock.py``
        """
        # Run Rosenbrock
        result = self.run_script('python',
                                 os.path.join(
                                     self.scriptdir,
                                     'opt_rosenbrock.py'),
                                 '-C', '1',
                                 '-r', 'localhost',
                                 '--pop_size', 5,
                                 '--y_conv_crit', 1000.0,
                                 '--path_to_stage_dir', self.temp_stage_dir,
                                 '--path_to_base_dir', self.temp_base_dir,
                                 '--path_to_executable', os.path.join(
                                     os.path.dirname(__file__), 'fitness_func', 'rosenbrock.py')
                                 #  ,'-vvvv'
                                 )

        assert result.stderr.find(b'Converged:')


# main: run tests

if "__main__" == __name__:
    import pytest
    pytest.main(["-v", __file__])
