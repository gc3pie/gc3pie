#! /usr/bin/env python
#
#   gscr.py -- Front-end script for running ParRecoveryFun Matlab
#   function with a given combination of reference models.
#
#   Copyright (C) 2014, 2015  University of Zurich. All rights reserved.
#
#   This program is free software: you can redistribute it and/or
#   modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
"""

It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gscr.py --help`` for program usage
instructions.

Input parameters consists of:
@param_folder
@data_folder
@sound_index
...

Options:
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2014-11-13:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gscr
    gscr.GscrScript().run()

import os
import sys
import time
import tempfile
import re

import shutil
import random

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask


EXP_INDEX = [0,1]

## custom application class
class GscrApplication(Application):
    """
    Custom class to wrap the execution of the Matlab script.
    """
    application_name = 'scr_analysis'

    def __init__(self, param_file, data_file, **extra_args):

        self.output_dir = extra_args['output_dir']
        self.result_dir = extra_args['result_dir']

        inputs = dict()
        outputs = dict()
        executables = []

        # Check if binary to be executed is provided as part of input arguments
        if 'run_binary' in extra_args:
            inputs[os.path.abspath(extra_args["run_binary"])] = "estimate_DCM.m"

        arguments = "matlab -nodesktop -nosplash -nodisplay -r \"estimate_DCM " \
                    "%s %s results;quit;\"" % (os.path.basename(param_file),
                                          os.path.basename(data_file))

        inputs[param_file] = os.path.basename(param_file)
        inputs[data_file] = os.path.basename(data_file)

        # Set output
        outputs['results/'] = 'results/'

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gscr.log',
            join=True,
            executables = executables,
            **extra_args)


class GscrScript(SessionBasedScript):
    """
    For each param file (with '.mat' extension) found in the 'param folder',
    GscrScript extracts the corresponding index (from filename) and searches for
    the associated file in 'data folder'. For each pair ('param_file','data_file'),
    GscrScript generates execution Tasks.

    The ``gscr`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gscr`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GscrApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GscrApplication,
            )

    def setup_args(self):

        self.add_param('params', type=str,
                       help="Path to the param files. "
                       " Files with extension .m will be considered.")

        self.add_param('data', type=str,
                       help="Path to the data files. "
                       " Data files will be associated to param files "
                       "using their index number. Es. param file: "
                       " .DCM_s24_can_159e-4Hz_bi_depth2_newVBA.mat will "
                       "be associated to a data file: "
                       "tscr_soundexp_scbd24.mat (index is 24).")

    def setup_options(self):
        self.add_param("-b", "--binary", metavar="[STRING]",
                       dest="run_binary", default=None,
                       help="Location of the Matlab compiled binary "
                       "version of the ParRecoveryFun. Default: None.")

        self.add_param("-S", "--store_results", type=str, metavar="[STRING]",
                       dest="store_results", default=None,
                       help="Location where all results will be aggregated. "
                       "Default: (session folder).")

    def parse_args(self):
        """
        Check validity of input parameters and selected benchmark.
        """

        if not os.path.isdir(self.params.params):
            raise OSError("No such file or directory: %s ",
                          os.path.abspath(self.params.params))

        if not os.path.isdir(self.params.data):
            raise OSError("No such file or directory: %s ",
                          os.path.abspath(self.params.data))

        if self.params.run_binary:
            if not os.path.isfile(self.params.run_binary):
                raise gc3libs.exceptions.InvalidUsage("Estimate function binary "
                                                      " file %s not found"
                                                      % self.params.run_binary)

    def new_tasks(self, extra):
        """
        For each of the network data and for each of the selected benchmarks,
        create a GscrApplication.

        First loop the input files, then loop the selected benchmarks
        """
        tasks = []

        for (param,data,dcm_index) in self.pair_param_data(self.params.params,self.params.data):

            jobname = "SCR-%d" % (dcm_index)

            extra_args = extra.copy()
            extra_args['dcm_index'] = dcm_index

            if self.params.run_binary:
                extra_args['run_binary'] = self.params.run_binary

            extra_args['jobname'] = jobname

            if self.params.store_results:
                if not os.path.isdir(self.params.store_results):
                    os.makedirs(self.params.store_results)
                extra_args['result_dir'] = self.params.store_results
            else:
                extra_args['result_dir'] = self.params.output
                extra_args['result_dir'] = extra_args['result_dir'].replace('NAME', self.params.session)

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', 'SCR-%d' % (dcm_index))

            gc3libs.log.info("Creating GscrApplication with: %s %s %d",
                             param,
                             data,
                             dcm_index)
            tasks.append(GscrApplication(
                param,
                data,
                **extra_args))

        return tasks

    def pair_param_data(self, param, data):
        """
        Walk through 'param'
        for each .m extract index
        find corresponding data file in 'data'
        return triple: [param_file, data_file, index]
        Agreed filename structures and string pattern:
        param: DCM_s[index]_can_159e-4Hz_bi_depth2_newVBA.mat
        data: tscr_HRA_1_[index].mat
        pattern: match the first number occourrance in param file with the
        last number occurrance in data file.
        """
        # Generate index of data files
        # Data file example: tscr_HRA_1_12.mat
        data_files = dict()

        for data_file in os.listdir(data):
            if os.path.isfile(os.path.join(data,data_file)) and \
            re.findall(r'\d+',os.path.basename(data_file)) :
                index = re.findall(r'\d+',os.path.basename(data_file))[-1]
                data_files[index] = os.path.join(data,data_file)

        processes = []

        for param_file in os.listdir(param):
            if param_file.endswith('.mat') and \
               re.findall(r'\d+',os.path.basename(param_file)):
                # Valid Matlab parameter file found
                # DCM_s24_can_159e-4Hz_bi_depth2_newVBA.mat
                index = re.findall(r'\d+',os.path.basename(param_file))[0]

                # Search corresponding file in data folder
                if index in data_files:
                    # yield (param_file, data_files[index], index)
                    gc3libs.log.info("Found new parameter/data pair, index %s." %
                                     index)
                    processes.append((os.path.join(param,param_file),
                                      data_files[index],
                                      int(index)))
                else:
                    gc3libs.log.error("No data file associated to: %s index: %s" %
                                      (param_file, index))

        return processes
