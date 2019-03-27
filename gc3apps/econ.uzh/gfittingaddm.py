#! /usr/bin/env python
#
#   gfittingaddm.py -- Front-end script for running MCMC with opneBUGS
#   function throguh R over a large table set.
#
#   Copyright (C) 2018, 2019  University of Zurich. All rights reserved.
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
Front-end script for submitting multiple `R` jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gfittingaddm.py --help`` for program usage
instructions.

arguments for the 'main' R script to be executed:
@int:subject_number
@int:number_of_simulations_per_subject
@int:number_of_iteration_per_simulation
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2018-01-10:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'
__version__ = '0.1.0'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gfittingaddm
    gfittingaddm.GfittingaddmScript().run()

import os
import sys
import time
import tempfile

import shutil

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file, positive_int, existing_directory
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask, StagedTaskCollection, ParallelTaskCollection

DEFAULT_CORES = 1
DEFAULT_MEMORY = Memory(1500,MB)
DEFAULT_WALLTIME = Duration(300,hours)
DEFAULT_RESULTS = "Results"
DEFAULT_SIMULATIONS=100
DEFAULT_ITERATIONS=150

## custom application class
class GfittingaddmApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'gfittingaddm'

    def __init__(self, subject_number,  rscript_folder, main_function, n_simulations, n_iterations, **extra_args):

        inputs = {}
        outputs = {}
        executables = []

        if rscript_folder:
            inputs[rscript_folder] = "./"

        outputs[DEFAULT_RESULTS] = DEFAULT_RESULTS

        arguments = "Rscript --vanilla {0}.R {1} {2} {3}".format(main_function,
                                                                 subject_number,
                                                                 n_simulations,
                                                                 n_iterations)

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gfittingaddm.log',
            executables = executables,
            join=True,
            **extra_args)

class GfittingaddmScript(SessionBasedScript):
    """
    The ``gfittingaddm`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gfittingaddm`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gfittingaddm``
    aggregates them into a single larger output file located in
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            application = GfittingaddmApplication,
            stats_only_for = GfittingaddmApplication,
            )

    def setup_args(self):
        self.add_param('number_of_subjects', type=positive_int,
                       help="Number of subjects to be analysed")

    def setup_options(self):
        self.add_param("-M", "--Rscripts", metavar="[PATH]",
                       type=existing_directory,
                       dest="Rscripts", default=None,
                       help="Location of R Main scripts. Default: '%(default)s'.")

        self.add_param("-F", "--main_function", metavar="[PATH]",
                       dest="main_function", default="runme",
                       help="R script main function to be invoked. Default: '%(default)s'.")

        self.add_param("-R", "--repeat", metavar="[INT]",
                       type=positive_int,
                       dest="repeat", default=1,
                       help="Repeat each subject simulation. Default: '%(default)s'.")

        self.add_param("-S", "--simulations", metavar="[INT]",
                       type=positive_int,
                       dest="simulations", default=DEFAULT_SIMULATIONS,
                       help="Number of simulations for each individual subject." \
                       " Default: '%(default)s'.")

        self.add_param("-I", "--iterations", metavar="[INT]",
                       type=positive_int,
                       dest="iterations", default=DEFAULT_ITERATIONS,
                       help="Number of iterations for each individual simulation." \
                       " Default: '%(default)s'.")

        self.add_param("-F", "--follow",
                       dest="follow",
                       action="store_true",
                       default=False,
                       help="Periodically fetch job's output folder and copy locally." \
                       " Default: '%(default)s'.")

    def before_main_loop(self):
        # XXX: should this be done with `make_controller` instead?
        self._controller.retrieve_running = self.params.follow
        self._controller.retrieve_overwrites = self.params.follow
        self._controller.retrieve_changed_only = self.params.follow

    def new_tasks(self, extra):
        """
        Chunk initial input file
        For each chunked fule, generate a new GfittingaddmTask
        """
        tasks = []

        for subject_number in range(1,self.params.number_of_subjects+1):
            for rep in range(1,self.params.repeat+1):
                extra_args = extra.copy()
                extra_args['jobname'] = "subject{0}-rep{1}".format(subject_number,
                                                                   rep)
                gc3libs.log.info("Creating task for subject number {0}".format(subject_number))

                tasks.append(GfittingaddmApplication(subject_number,
                                                     self.params.Rscripts,
                                                     self.params.main_function,
                                                     self.params.simulations,
                                                     self.params.iterations,
                                                     **extra_args))

        return tasks
