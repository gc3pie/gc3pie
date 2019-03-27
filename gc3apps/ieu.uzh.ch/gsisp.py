#! /usr/bin/env python
#
#   gsisp.py -- Front-end script for running sisp
#   function with a given combination of input parameters.
#
#   Copyright (C) 2015, 2016  University of Zurich. All rights reserved.
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

See the output of ``gsisp.py --help`` for program usage
instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2016-01-15:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gsisp
    gsisp.Gsispingsispipt().run()

import os
import sys
import time
import tempfile
import re

import shutil
import random
import posix

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

PARAMETERS_FILE = "parameters.in"

## custom application class
class GsispingApplication(Application):
    """
    Custom class to wrap the execution of the sisp script.
    """
    application_name = 'gsisp'

    def __init__(self, input_folder, param_file, **extra_args):

        self.output_dir = extra_args['results_dir']

        inputs = dict()
        outputs = dict()
        executables = []
        self.jobname = extra_args['jobname']

        # Check if binary to be executed is provided as part of input arguments
        if 'sisp' in extra_args:
            inputs[os.path.abspath(extra_args["sisp"])] = "./sisp"
            executables.append("./sisp")

        arguments = "./sisp"
        inputs[param_file] = PARAMETERS_FILE

        # Set output
        outputs['output/'] = os.path.join(self.output_dir,"./output")

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gsisp.log',
            join=True,
            executables = executables,
            **extra_args)

    def termianted(self):
        """
        Check output folder
        """
        try:
            assert os.path.isdir(os.path.join(self.output_dir,"./output"))
        except AssertionError as ex:
            self.execution.returncode = (0, posix.EX_OSFILE)
            gc3libs.log.error("Job %s failed: %s" (self.jobname,ex.message))

class Gsispingsispipt(SessionBasedScript):
    """
    For each param file (with '.in' extension) found in the 'param folder',
    GsispScript generates execution Tasks.

    The ``gsisp`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gsisp`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GsispingApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GsispingApplication,
            )

    def setup_args(self):

        self.add_param('parameters', type=str,
                       help="Root localtion of input parameters. "
                       "Note: only folders containing `%s` files will be "
                       " considered." % PARAMETERS_FILE)

    def setup_options(self):
        self.add_param("-S", "--sisp", metavar="PATH",
                       dest="sisp", default=None,
                       help="Location of the sisp binary file.")

    def parse_args(self):
        """
        Check validity of input parameters and selected benchmark.
        """

        if not os.path.isdir(self.params.parameters):
            raise OSError("No such file or directory: %s ",
                          os.path.abspath(self.params.parameters))

        if self.params.sisp:
            if not os.path.isfile(self.params.sisp):
                raise gc3libs.exceptions.InvalidUsage("Sisp binary "
                                                      " file %s not found"
                                                      % self.params.sisp)
            else:
                self.params.sisp = os.path.abspath(self.params.sisp)

        # Walk through the input parameters folder and record all folders with `parameters.in` file

        try:
            self.folders = dict()
            for root,dir,files in os.walk(self.params.parameters):
                if PARAMETERS_FILE in files:
                    self.folders[os.path.abspath(root)] = os.path.join(root,files[files.index(PARAMETERS_FILE)])
            assert len(self.folders) > 0, "No valid input paramenters found"
        except AssertionError as ex:
            gc3libs.log.error(ex.message)

    def new_tasks(self, extra):
        """
        For each of the network data and for each of the selected benchmarks,
        create a GsispApplication.

        First loop the input files, then loop the selected benchmarks
        """
        tasks = []

        for parameter_folder in self.folders.keys():

            # Extract foldername
            jobname = os.path.basename(parameter_folder)

            extra_args = extra.copy()

            if self.params.sisp:
                extra_args['sisp'] = self.params.sisp

            extra_args['jobname'] = jobname
            extra_args['results_dir'] = parameter_folder
            tasks.append(GsispingApplication(
                parameter_folder,
                self.folders[parameter_folder],
                **extra_args))

        return tasks
