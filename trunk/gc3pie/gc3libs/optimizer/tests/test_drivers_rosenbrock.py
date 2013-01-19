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

from nose.tools import raises
from nose.plugins.skip import SkipTest

import numpy as np

import gc3libs
from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript
from gc3libs.optimizer.utils import update_parameter_in_file

# optimizer specific imports
from gc3libs.optimizer.drivers import GridDriver
from gc3libs.optimizer.dif_evolution import DifferentialEvolutionAlgorithm
from gc3libs.optimizer import draw_population

# General settings
float_fmt = '%25.15f'
optimization_dir = os.path.join(os.getcwd(), 'optimizeRosenBrock')
pop_size = 10
itermax = 2
dim = 2
lower_bounds = -2 * np.ones(dim)
upper_bounds = +2 * np.ones(dim)

os.system('rm -r bin base optimizeRosenBrock/ EvolutionaryAlgorithm.log parameters.in')

class TestGridDriver(object):
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

    def setUp(self):
        # change command line options
        sys.argv = ['test_drivers_rosenbrock.py', '-N', '-C 10']
        (fd, cfgfile) = tempfile.mkstemp()
        f = os.fdopen(fd, 'w+')
        f.write(TestGridDriver.CONF)
        f.close()
        self.files_to_remove = [cfgfile]

        self.cfg = gc3libs.config.Configuration()
        self.cfg.merge_file(cfgfile)

        self.core = gc3libs.core.Core(self.cfg)
        self.backend = self.core.get_backend('localhost_test')
        
        # -- Rosenbrock setup --
        # Create base dir
        os.mkdir('base')
        para_file = open('base/parameters.in', 'w')
        para_file.write('x1    100.0\nx2    50.0\n')
        # Create base dir
        os.mkdir('bin')
        cpp_file = open('bin/rosenbrock.cpp','w')
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
        os.system('g++ bin/rosenbrock.cpp -o bin/rosenbrock')
        
        print sys.argv
        if os.path.isdir(optimization_dir):
            import shutil
            shutil.rmtree(optimization_dir)
        os.mkdir(optimization_dir)
        
                

    def cleanup_file(self, fname):
        self.files_to_remove.append(fname)

    def tearDown(self):
        for fname in self.files_to_remove:
            if os.path.isdir(fname):
                shutil.rmtree(fname)
            elif os.path.exists(fname):
                os.remove(fname)
                
        # Remove Rosenbrock output
        print 'not implemented yet. '

    def test_optimizer(self):
    # basically, adapt `opt_rosenb4rock.py`
        RosenbrockScript().run()
        print 'done succesffully'
    

class RosenbrockScript(SessionBasedScript):
    """
    Execute Rosenbrock optimization.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.2',
            stats_only_for = Application,
            exit_after_main = False
        )

    def new_tasks(self, extra):

        path_to_stage_dir = os.getcwd()

        import logging
        log = logging.getLogger('gc3.gc3libs.EvolutionaryAlgorithm')
        log.setLevel(logging.DEBUG)
        log.propagate = 0
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        log_file_name = os.path.join(os.getcwd(), 'EvolutionaryAlgorithm.log')
        file_handler = logging.FileHandler(log_file_name, mode = 'w')
        file_handler.setLevel(logging.DEBUG)
        log.addHandler(stream_handler)
        log.addHandler(file_handler)


        initial_pop = draw_population(lower_bds=lower_bounds, upper_bds=upper_bounds, size=pop_size, dim=dim)

        de_solver = DifferentialEvolutionAlgorithm(
            initial_pop = initial_pop,
            de_step_size = 0.85,# DE-stepsize ex [0, 2]
            prob_crossover = 1, # crossover probabililty constant ex [0, 1]
            itermax = itermax,      # maximum number of iterations (generations)
            dx_conv_crit = None, # stop when variation among x's is < this
            y_conv_crit = 0.5, # stop when ofunc < y_conv_crit
            de_strategy = 'DE_local_to_best',
            logger = log,
            )

        # create an instance globalObt

        jobname = 'rosenbrock'
        kwargs = extra.copy()
        kwargs['path_to_stage_dir'] = optimization_dir
        kwargs['opt_algorithm'] = de_solver
        kwargs['task_constructor'] = task_constructor_rosenbrock
        kwargs['extract_value_fn'] = compute_target_rosenbrock
        kwargs['cur_pop_file'] = 'cur_pop'

        return [GridDriver(jobname=jobname, **kwargs)]    


def task_constructor_rosenbrock(x_vals, iteration_directory, **extra_args):
    """
    Given solver guess `x_vals`, return an instance of :class:`Application`
    set up to produce the output :def:`target_fun` of :class:`GridOptimizer`
    analyzes to produce the corresponding function values.
    """
    import shutil

    # Set some initial variables
    path_to_rosenbrock_example = os.getcwd()
    path_to_executable = os.path.join(path_to_rosenbrock_example, 'bin/rosenbrock')
    base_dir = os.path.join(path_to_rosenbrock_example, 'base')

    x_vars = ['x1', 'x2']
    para_files = ['parameters.in', 'parameters.in']
    para_file_formats = ['space-separated', 'space-separated']
    index = 0 # We are dealing with scalar inputs

    jobname = 'para_' + '_'.join(['%s=' % var + ('%25.15f' % val).strip() for (var, val) in zip(x_vars, x_vals)])
    path_to_stage_dir = os.path.join(optimization_dir, iteration_directory, jobname)

    executable = os.path.basename(path_to_executable)
    # start the inputs dictionary with syntax: client_path: server_path
    inputs = { path_to_executable:executable }
    path_to_stage_base_dir = os.path.join(path_to_stage_dir, 'base')
    shutil.copytree(base_dir, path_to_stage_base_dir, ignore=shutil.ignore_patterns('.svn'))
    for var, val, para_file, para_file_format in zip(x_vars, x_vals, para_files, para_file_formats):
        val = (float_fmt % val).strip()
        update_parameter_in_file(os.path.join(path_to_stage_base_dir, para_file),
                                 var, index, val, para_file_format)

    prefix_len = len(path_to_stage_base_dir) + 1
    for dirpath,dirnames,filenames in os.walk(path_to_stage_base_dir):
        for filename in filenames:
            # cut the leading part, which is == to path_to_stage_base_dir
            relpath = dirpath[prefix_len:]
            # ignore output directory contents in resubmission
            if relpath.startswith('output'):
                continue
            remote_path = os.path.join(relpath, filename)
            inputs[os.path.join(dirpath, filename)] = remote_path
    # all contents of the `output` directory are to be fetched
    # outputs = { 'output/':'' }
    outputs = gc3libs.ANY_OUTPUT
    #{ '*':'' }
    #kwargs = extra.copy()
    kwargs = extra_args # e.g. architecture
    kwargs['stdout'] = executable + '.log'
    kwargs['join'] = True
    kwargs['output_dir'] =  os.path.join(path_to_stage_dir, 'output')
    gc3libs.log.debug("Output dir: %s" % kwargs['output_dir'])
    kwargs['requested_architecture'] = 'x86_64'
    kwargs['requested_cores'] = 1
    # hand over job to create
    return Application(['./' + executable], inputs, outputs, **kwargs)

def compute_target_rosenbrock(task):
    '''
    Extract and return the target value computed by a single run of
    the `rosenbrock` program.
    '''
    outputDir = task.output_dir
    f = open(os.path.join(outputDir, 'rosenbrock.out'))
    line = f.readline().strip()
    return float(line)
    
print 'done'