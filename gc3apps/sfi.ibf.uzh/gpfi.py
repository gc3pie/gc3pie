#! /usr/bin/env python
#
#   gpfi.py -- Front-end script for evaluating Matlab functions
#   function over a large number of parameters.
#
#   Copyright (C) 2015, 2016  University of Zurich. All rights reserved.
#
#   This program is free software: you can redistribute it and/or
#   modify it under the terms of the GNU General Public License as
#   published by the Free Software Foundation, either version 3 of
#   the License, or (at your option) any later version.
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
Front-end script for submitting multiple Matlab jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gpfi.py --help`` for program usage instructions.

Input parameters consists of: model input_csv: Path to an .csv file
containing input parameters
Example:
0.1 0.9
0.2 0.8
...
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2016-04-20:
  * Initial version
"""
__author__ = 'Tyanko Aleksiev <tyanko.aleksiev@uzh.ch>'
__docformat__ = 'reStructuredText'


if __name__ == "__main__":
    import gpfi
    gpfi.GpfiScript().run()

import os
import sys
import time
import tempfile

import shutil

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

## custom application class
class GpfiApplication(Application):
    """
    Custom class to wrap the execution of the matlab script passed in src_dir.
    """
    application_name = 'gpfi'

    def __init__(self, parameter, model, **extra_args):

        inputs = dict()
        outputs = dict()

        # execution wrapper needs to be added anyway
        gpfi_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gpfi.sh")
        inputs[gpfi_wrapper_sh] = os.path.basename(gpfi_wrapper_sh)
        inputs[model] = os.path.basename(model)

        _command = "%s %s" % (os.path.basename(gpfi_wrapper_sh), ' '.join(str(x) for x in parameter))

        Application.__init__(
            self,
            arguments = _command,
            inputs = inputs,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'gpfi.log',
            join=True,
            executables = "./%s" % os.path.basename(gpfi_wrapper_sh),
            **extra_args)


class GpfiScript(SessionBasedScript):
    """
    Parse the input .csv file. For each line in the input .csv file
    create a different Matlab execution.

    The ``gpfi`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gpfi`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gpfi``
    aggregates them into a single larger output file located in
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            application = GpfiApplication,
            stats_only_for = GpfiApplication,
            )

    def setup_args(self):
        self.add_param('model', type=str, help="Location of the matlab scripts and related MAtlab functions. Default: None")

    def setup_args(self):
        self.add_param('csv_input_file', type=str, help="Input .csv file")

    def parse_args(self):
        """
        Check presence of input folder (should contains matlab scripts).
        path to command_file should also be valid.
        """
        assert os.path.isfile(self.params.csv_input_file), \
        "Input CSV file %s not found" % self.params.csv_input_file

    def new_tasks(self, extra):
        """
        For each line of the input .csv file generate an execution Task
        """
        tasks = []

        for parameter in self._enumerate_csv(self.params.csv_input_file):
            parameter_str = '.'.join(str(x) for x in parameter)
            jobname = "gpfi-%s" % parameter_str

            extra_args = extra.copy()

            extra_args['jobname'] = jobname

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

            self.log.debug("Creating Application for parameter : %s" %
                           (parameter_str))

            tasks.append(GpfiApplication(
                    parameter,
                    self.param.model,
                    **extra_args))

        return tasks

    def _enumerate_csv(self, csv_input):
        """
        For each line of the input .csv file return list of parameters
        """
        csv_file = open(csv_input, 'rb')
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)
        for row in reader:
            yield row
