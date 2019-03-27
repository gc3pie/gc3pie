#! /usr/bin/env python
#
#   gbraunian.py -- Front-end script for evaluating Matlab function
#   `braunian` over large number of events.
#
#   Copyright (C) 2016, 2017  University of Zurich. All rights reserved.
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
Front-end script for submitting multiple `Matlab` jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gbraunian.py --help`` for program usage
instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2016-07-14:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gbraunian
    gbraunian.GbraunianScript().run()

import os
import sys
import time
import tempfile

import tarfile
import shutil
import pandas
import csv

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

DEFAULT_REMOTE_OUTPUT_FILE = "workflow.mat"
DEFAULT_FUNCTION = "brownian_cloud"
DEFAULT_CASE_FILE="3d_case1.txt"
DEFAULT_CHUNK=1
MATLAB_CMD="matlab -nosplash -nodisplay -nodesktop -r \"{main_function}({events},'{case_file}','{output_file}');quit\""

## custom application class
class GbraunianApplication(Application):
    """
    Custom class to wrap the execution of the Matlab function
    over a subset of the total number of events.
    """
    application_name = 'gbraunian'

    def __init__(self, events, matlab_file, case_file, **extra_args):

        executables = []
        inputs = dict()
        outputs = dict()

        inputs[matlab_file] = os.path.basename(matlab_file)
        matlab_function = inputs[matlab_file].split('.')[0]
        inputs[case_file] = os.path.basename(case_file)

        arguments = MATLAB_CMD.format(main_function=matlab_function,
                                      events=events,
                                      case_file=os.path.basename(case_file),
                                      output_file=DEFAULT_REMOTE_OUTPUT_FILE)

        # Set output
        outputs[DEFAULT_REMOTE_OUTPUT_FILE] = DEFAULT_REMOTE_OUTPUT_FILE

        gc3libs.log.debug("Creating application for executing: %s",
                          arguments)

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gbraunian.log',
            join=True,
            executables = executables,
            **extra_args)


class GbraunianScript(SessionBasedScript):
    """
    Take total number of events and create a list of chunked events.
    For each chunk, run the provided Matlab function.

    The ``gbraunian`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gbraunian`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gbraunian``
    aggregates them into a single larger output file located in
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GbraunianApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GbraunianApplication,
            )

    def setup_args(self):

        self.add_param('events', type=int,
                       help="Total number of events.")

        self.add_param('main',
                       help="Matlab function to execute.")

        self.add_param('case',
                       help="case file.")

    def setup_options(self):
        self.add_param("-k", "--chunk", metavar="INT", type=int,
                       dest="chunk", default=DEFAULT_CHUNK,
                       help="How to split the edges input data set. "
                       "Default: %(default)s.")

    def parse_args(self):
        """
        Check for validity of input arguments
        """
        try:
            assert isinstance(self.params.events,int), \
                "Invalid number of events '%s'. Must be positive integer." % str(self.params.events)

            assert os.path.isfile(self.params.case), \
                "case file '%s' not found" % self.params.case

            assert os.path.isfile(self.params.main), \
                "Matlab function file '%s' not found" % self.params.main

            gc3libs.log.info("Using matlab function name: '%s'" % os.path.basename(self.params.main).split('.')[0])

        except AssertionError as ex:
            raise ValueError(ex.message)

    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new Application
        """
        tasks = []

        for event in get_events(self.params.events,self.params.chunk):
            extra_args = extra.copy()
            tasks.append(GbraunianApplication(
                event,
                self.params.main,
                self.params.case,
                **extra_args))

        return tasks

def get_events(events,chunk):
    event_list = [chunk for elem in range(1,events/chunk+1)]
    if events % chunk:
        event_list.append(events % chunk)
    return event_list
