#! /usr/bin/env python
#
#   gfsurfer.py -- Front-end script for running the docking program rDock
#   over a list of ligand files.
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

See the output of ``gfsurfer.py --help`` for program usage instructions.

Input parameters consists of:
...

Options:
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2015-02-17:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gfsurfer
    gfsurfer.GfsurferScript().run()

import os
import sys
import time
import tempfile
import re

import shutil
# import csv

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

DEFAULT_CORES = 1
DEFAULT_MEMORY = Memory(3000,MB)

DEFAULT_REMOTE_OUTPUT_FOLDER="./output/"

INPUT_LIST_PATTERNS = [".nii",".nii.tgz",".nii.gz",".nii.tar.gz"]
FREESURFER_STEPS = ['cross','long']

## custom application class
class GfsurferApplication(Application):
    """
    """
    application_name = 'gfsurfer'

    def __init__(self, subject_name, input_nifti, freesurfer_steps, **extra_args):

        output_dir = DEFAULT_REMOTE_OUTPUT_FOLDER + subject_name + ".crossTP1"

        inputs = dict()
        outputs = dict()

        gfsurfer_wrapper = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gfsurfer_wrapper.py")
        inputs[gfsurfer_wrapper] = os.path.basename(gfsurfer_wrapper)
        inputs[input_nifti] = os.path.basename(input_nifti)
        outputs[output_dir] = output_dir

        arguments = "./%s %s %s %s" % (inputs[gfsurfer_wrapper],
                                       subject_name,
                                       os.path.basename(input_nifti),
                                       DEFAULT_REMOTE_OUTPUT_FOLDER)

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gfsurfer.log',
            join=True,
            executables = [os.path.basename(gfsurfer_wrapper)],
            **extra_args)

class GfsurferScript(SessionBasedScript):
    """

    The ``gfsurfer`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gfsurfer`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GfsurferApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GfsurferApplication,
            )

    def setup_args(self):
        self.add_param('input_data', type=str,
                       help="Root localtion of input data. "
                       "Note: expected folder structure: "
                       " - input files in case of cross sectional runs. "
                       " - subdirectories with input files for more TP in case of longitudinal ")

        self.add_param("-P", "--pipeline", metavar="STRING", type=str,
                       dest="pipeline",
                       default="cross",
                       help="Comma separated list of Freesurfer steps to be "
                       " executed on each valid input file. "
                       " Valid values: %s." % FREESURFER_STEPS)

    def parse_args(self):
        try:
            assert os.path.isdir(self.params.input_data), "Input folder %s not found" % self.params.input_data
        except AssertionError as ex:
            raise OSError(ex.message)

        self.freesurfer_pipeline = [ step for step in self.params.pipeline if step in FREESURFER_STEPS ]
        gc3libs.log.debug("Accepted following Freesurfer steps: %s" % self.freesurfer_pipeline)

    def new_tasks(self, extra):
        """
        For each input folder, create an instance of GfsurferApplication
        """
        tasks = []

        for input_nifti in [ nifti for nifti in os.listdir(self.params.input_data) if
                             any(nifti.endswith(pattern) for pattern in INPUT_LIST_PATTERNS) ]:

            # filename example: 0103645_anat.nii.gz
            # extract root folder name to be used as jobname
            subject_name = input_nifti.split(".")[0]

            extra_args = extra.copy()
            extra_args['jobname'] = subject_name

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                        'run_%s' % subject_name)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                        'run_%s' % subject_name)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                        'run_%s' % subject_name)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                        'run_%s' % subject_name)

            tasks.append(GfsurferApplication(
                subject_name,
                os.path.join(self.params.input_data,input_nifti),
                self.freesurfer_pipeline,
                **extra_args))

        return tasks
