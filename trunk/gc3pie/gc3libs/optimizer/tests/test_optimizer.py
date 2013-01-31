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
import logging

import cli.test

from nose.tools import raises, assert_true
from nose.plugins.skip import SkipTest

import numpy as np

import gc3libs
from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript
from gc3libs.optimizer import update_parameter_in_file

# optimizer specific imports
from gc3libs.optimizer.drivers import ParallelDriver
from gc3libs.optimizer.dif_evolution import DifferentialEvolutionAlgorithm
from gc3libs.optimizer import draw_population

# Create a temporary stage directory
temp_stage_dir = tempfile.mkdtemp(prefix = 'ParallelDriver_Rosenbrock_')
optimization_dir = os.path.join(temp_stage_dir, 'rosenbrock_output_dir')

# Nose will add command line arguments that cannot be interpreted by
# the SessionBasedScript. To avoid error, override sys.argv.
sys.argv = ['test_drivers_rosenbrock.py' ]

# General settings
float_fmt = '%25.15f'
magic_seed = 100
pop_size = 5
itermax = 2
dim = 2
lower_bounds = -2 * np.ones(dim)
upper_bounds = +2 * np.ones(dim)

# Set up logger
log_file_name = os.path.join(temp_stage_dir, 'DifferentialEvolutionAlgorithm.log')
log = logging.getLogger("gc3.gc3libs")
log.setLevel(logging.CRITICAL)

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
        print self.scriptdir
        pass

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
        # Create base dir
        temp_base_dir = os.path.join(self.temp_stage_dir, 'base')
        temp_bin_dir = os.path.join(self.temp_stage_dir, 'bin')
        os.mkdir(temp_base_dir)
        para_file = open(os.path.join(temp_base_dir, 'parameters.in'), 'w')
        para_file.write('x1    100.0\nx2    50.0\n')
        # Create base dir
        os.mkdir(temp_bin_dir)
        cpp_file = open(os.path.join(temp_bin_dir, 'rosenbrock.cpp'),'w')
        cpp_file.write(
        """
#include "math.h"
#include <fstream>
#include <iostream>
#include <ostream>
#include <stdio.h>


using std::cout;
using std::endl;

using std::ifstream;
using std::ofstream;

int main()
{
  cout << "Compute Rosenbrock function" << endl;

  ifstream indata; // indata is like cin
  double x; // variable for input value
  double y;
  std::string a;
  indata.open("parameters.in"); // opens the file
  if(!indata) { // file couldn't be opened
  std::cerr << "Error: file could not be opened" << endl;
  }
  indata >> a >> x;
  indata >> a >> y;
  indata.close();

  double fun = 100.0 * pow( y - pow(x, 2), 2) + pow( 1 - x, 2);
  cout << "fun val = " << fun << endl;

  ofstream myfile;
  myfile.open ("rosenbrock.out");
  myfile << fun << endl;
  myfile.close();

  cout << "main ended" << endl;
}
        """
        )
        cpp_file.flush()
        # Generate Rosenbrock binary
        compile_str = 'g++ ' + os.path.join(temp_bin_dir, 'rosenbrock.cpp') + ' -o ' + os.path.join(temp_bin_dir, 'rosenbrock')
        os.system(compile_str)

        if os.path.isdir(optimization_dir):
            shutil.rmtree(optimization_dir)
        os.mkdir(optimization_dir)


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
                                 '--path_to_base_dir', os.path.join(self.scriptdir, 'base'),
                                 '--path_to_executable', os.path.join(self.scriptdir, 'bin/rosenbrock'),
                                 '-v')
        
        assert_true(result.stderr.find('Converged:'))


## main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()