#! /usr/bin/env python
#
#   gtraclong.py -- Front-end script for running the Tracula
#   over a list of subject files.
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

It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gtraclong.py --help`` for program usage
instructions.
"""

# summary of user-visible changes
__changelog__ = """
  2016-07-05:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gtraclong
    gtraclong.GtraclongScript().run()

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
DEFAULT_TRAC_COMMAND="trac-all -prep -c {dmrirc} -debug"

def _correlate(subjects, freesurfers):
    """
    correlate every subject in 'subjects' with subfolders in 'freesurfers'
    """    
    for subject in subjects:
        freesurfer_list = list()
        for freesurfer in os.listdir(freesurfers):
            if freesurfer.split('.')[0] == subject:
                freesurfer_list.append(os.path.join(freesurfers,freesurfer))
        yield (subject,freesurfer_list)

## custom application class
class GtraclongApplication(Application):
    """
    """
    application_name = 'gtraclong'
    
    def __init__(self, subject, subject_folder, **extra_args):

        self.output_dir = extra_args['output_dir']

        inputs = dict()
        outputs = dict()

        inputs[subject_folder] = DEFAULT_REMOTE_INPUT_FOLDER

        wrapper = resource_filename(Requirement.parse("gc3pie"),
                                    "gc3libs/etc/gtraclong_wrapper.py")
        inputs[wrapper] = os.path.basename(wrapper)

        arguments = "./%s" % (inputs[wrapper])

        if extra_args['requested_memory'] < DEFAULT_MEMORY:
            gc3libs.log.warning("GtraclongApplication for subject %s running with memory allocation " \
                                "'%d GB' lower than suggested one: '%d GB'," % (subject,
                                                                                extra_args['requested_memory'].amount(unit=GB),
                                                                                DEFAULT_MEMORY.amount(unit=GB)))
        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = [DEFAULT_REMOTE_OUTPUT_FOLDER],
            stdout = 'gtraclong.log',
            join=True,
            **extra_args)        

class GtraclongScript(SessionBasedScript):
    """
    
    The ``gtraclong`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gtraclong`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GtraclongApplication, 
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GtraclongApplication,
            )
 
    # def setup_args(self):

    #     self.add_param('input_data', type=str,
    #                    help="Root location of input data. "
    #                    "Note: expected folder structure: "
    #                    " 1 subfodler for each subject. "
    #                    " In each subject folder, " 
    #                    " 1 subfolder for each TimePoint. "
    #                    " Each TimePoint folder should contain 2 input "
    #                    "NFTI files.")

    def setup_options(self):
        self.add_param("-F", "--fs", metavar="[PATH]", 
                       dest="freesurfer", default=None,
                       help="Location of Freesurfer data folders.")
    
    def parse_args(self):
        self.params.subjects.append = list()
        try:
            for input_folder in self.params.args:
                assert os.path.isdir(input_folder), \
                    "Input subject forler %s not found" % input_folder
                self.params.subjects.append(input_folder)
        except ValueError as vx:
            gc3libs.log.warning(vx)
    
    def new_tasks(self, extra):
        """
        For each input folder, create an instance of GtraclongApplication
        """
        tasks = []
        for (subject_name,freesurfers) in _correlate(self.params.subjects, self.params.freesurfer)
        
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
            
            tasks.append(GtraclongApplication(
                subject_name,
                os.path.join(self.params.subjects,subject_name),
                freesurfers,
                **extra_args))
            
        return tasks

    def get_input_subject_folder(self, input_folder):
        """
        Check and validate input subfolders
        """

        for r,d,f in os.walk(input_folder):
            for infile in f:
                if infile.startswith(DMRIC_PATTERN):
                    yield (os.path.abspath(r),os.path.basename(r),infile)

