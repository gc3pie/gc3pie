#! /usr/bin/env python
#
"""
Unit tests for the `gc3libs.optimizer.drivers` module.
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
import sys
import logging
import tempfile
import shutil

from nose.tools import raises, assert_true
from nose.plugins.skip import SkipTest

import cli.test

import numpy as np

import gc3libs
from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript

# optimizer specific imports
from gc3libs.optimizer import draw_population, update_parameter_in_file
from gc3libs.optimizer.drivers import ParallelDriver
from gc3libs.optimizer.dif_evolution import DifferentialEvolutionAlgorithm


# Nose will add command line arguments that cannot be interpreted by
# the SessionBasedScript. To avoid error, override sys.argv.
sys.argv = ['test_drivers_rosenbrock.py' ]

class TestParallelDriver(cli.test.FunctionalTest):
    CONF="""
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

[auth/noauth]
type=none
"""
    def __init__(self, *args, **extra_args):
        cli.test.FunctionalTest.__init__(self, *args, **extra_args)
        self.scriptdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../examples/optimizer/rosenbrock')

    def setUp(self):
        cli.test.FunctionalTest.setUp(self)
        
        self.temp_stage_dir = tempfile.mkdtemp(prefix = 'ParallelDriver_Rosenbrock_')
        
        (fd, cfgfile) = tempfile.mkstemp()
        f = os.fdopen(fd, 'w+')
        f.write(TestParallelDriver.CONF)
        f.close()
        sys.argv += ['--config-files', cfgfile, '-r', 'localhost_test']
        self.files_to_remove = [cfgfile]

        self.cfg = gc3libs.config.Configuration()
        self.cfg.merge_file(cfgfile)

        self.core = gc3libs.core.Core(self.cfg)
        self.backend = self.core.get_backend('localhost_test')

        # -- Rosenbrock setup --
        # Create base dir with sample parameter file
        self.temp_base_dir = os.path.join(self.temp_stage_dir, 'base')
        os.mkdir(self.temp_base_dir)
        para_file = open(os.path.join(self.temp_base_dir, 'parameters.in'), 'w')
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
        """Test :class:`gc3libs.optimizer.test_ParallelDriver`

        The script is found in ``examples/optimizer/rosenbrock/rosenbrock.py``
        """
        # Run Rosenbrock
        result = self.run_script('python',
                                 os.path.join(self.scriptdir, 'opt_rosenbrock.py'),
                                 '-C', '1',
#                                 '-s', 'TestOne',
#                                 '--config-files', self.cfgfile,
                                 '-r', 'localhost',
                                 '--pop_size', 5,
                                 '--y_conv_crit', 50.0,
                                 '--path_to_stage_dir', self.temp_stage_dir, 
                                 '--path_to_base_dir', self.temp_base_dir,
                                 '--path_to_executable', os.path.join(os.getcwd(), 'rosenbrock.py'),#os.path.join(self.scriptdir, 'bin/rosenbrock'),
                                 '-v')
        
        assert_true(result.stderr.find('Converged:'))


## main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()