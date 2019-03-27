#! /usr/bin/env python
#
#   gnift.py -- Front-end script for running the docking program rDock
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

See the output of ``gnift.py --help`` for program usage
instructions.

Input parameters consists of:

...

Options:
"""

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
from __future__ import absolute_import, print_function
    import gnift
    gnift.GniftScript().run()

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

DEFAULT_REMOTE_INPUT_FOLDER="./input/"
DEFAULT_REMOTE_OUTPUT_FOLDER="./output"

## custom application class
class GniftApplication(Application):
    """
    """
    application_name = 'gnifti'
    
    def __init__(self, subject, subject_folder, **extra_args):

        self.output_dir = extra_args['output_dir']

        inputs = dict()
        outputs = dict()

        gnift_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gnift_wrapper.py")
        inputs[gnift_wrapper_sh] = os.path.basename(gnift_wrapper_sh)
        inputs[subject_folder] = DEFAULT_REMOTE_INPUT_FOLDER

        arguments = "./%s %s %s %s" % (inputs[gnift_wrapper_sh],
                                    subject,
                                    DEFAULT_REMOTE_INPUT_FOLDER,
                                    DEFAULT_REMOTE_OUTPUT_FOLDER)

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = [DEFAULT_REMOTE_OUTPUT_FOLDER],
            stdout = 'gnift.log',
            join=True,
            **extra_args)        

class GniftScript(SessionBasedScript):
    """
    
    The ``gnift`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gnift`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GniftApplication, 
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GniftApplication,
            )
 
    def setup_args(self):

        self.add_param('input_data', type=str,
                       help="Root localtion of input data. "
                       "Note: expected folder structure: "
                       " 1 subfodler for each subject. "
                       " In each subject folder, " 
                       " 1 subfolder for each TimePoint. "
                       " Each TimePoint folder should contain 2 input "
                       "NFTI files.")

    def new_tasks(self, extra):
        """
        For each input folder, create an instance of GniftApplication
        """
        tasks = []

        for subject_folder in self.get_input_subject_folder(self.params.input_data):
        
            # extract root folder name to be used as jobname
            subjectname = subject_folder

            extra_args = extra.copy()
            extra_args['jobname'] = subjectname

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', 
                                                                        'run_%s' % subjectname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', 
                                                                        'run_%s' % subjectname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', 
                                                                        'run_%s' % subjectname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', 
                                                                        'run_%s' % subjectname)
            
            tasks.append(GniftApplication(
                subjectname,
                os.path.join(self.params.input_data,subject_folder),
                **extra_args))
            
        return tasks

    def get_input_subject_folder(self, input_folder):
        """
        Check and validate input subfolders
        XXX: for the time being just pass
        """
        return os.listdir(input_folder)
        # for folder in os.listdir(input_folder):
        #     yield os.path.abspath(os.path.join(input_folder,folder))
            
