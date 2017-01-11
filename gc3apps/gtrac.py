#! /usr/bin/env python
#
#   gtrac.py -- Front-end script for running the docking program rDock
#   over a list of ligand files.
#
#   Copyright (C) 2014, 2015 S3IT, University of Zurich
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

See the output of ``gtrac.py --help`` for program usage
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
    import gtrac
    gtrac.GtracScript().run()

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

DEFAULT_REMOTE_INPUT_FOLDER="./"
DEFAULT_REMOTE_OUTPUT_FOLDER="./output"
DMRIC_PATTERN = "dmrirc"

## custom application class
class GtracApplication(Application):
    """
    """
    application_name = 'gtraci'
    
    def __init__(self, subject, subject_folder, dmrirc, **extra_args):

        self.output_dir = extra_args['output_dir']

        inputs = dict()
        outputs = dict()

        inputs[subject_folder] = DEFAULT_REMOTE_INPUT_FOLDER

        arguments = "trac-all -prep -c %s -debug" % dmrirc

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = [DEFAULT_REMOTE_OUTPUT_FOLDER],
            stdout = 'gtrac.log',
            join=True,
            **extra_args)        

class GtracScript(SessionBasedScript):
    """
    
    The ``gtrac`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gtrac`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GtracApplication, 
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GtracApplication,
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
        For each input folder, create an instance of GtracApplication
        """
        tasks = []

        for (subject_folder,subject_name, dmric) in self.get_input_subject_folder(self.params.input_data):
        
            # extract root folder name to be used as jobname
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
            
            tasks.append(GtracApplication(
                subject_name,
                os.path.join(self.params.input_data,subject_folder),
                dmric,
                **extra_args))
            
        return tasks

    def get_input_subject_folder(self, input_folder):
        """
        Check and validate input subfolders
        XXX: for the time being just pass
        """

        for r,d,f in os.walk(input_folder):
            for infile in f:
                if infile.startswith(DMRIC_PATTERN):
                    yield (os.path.abspath(r),r,infile)
        
        # return os.listdir(input_folder)            
