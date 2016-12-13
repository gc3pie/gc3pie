#! /usr/bin/env python
#
#   gnift.py -- Front-end script for running the docking program rDock
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

See the output of ``gnift.py --help`` for program usage
instructions.

Input parameters consists of:

...

Options:
"""

__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2016-12-12:
  * Initial version
"""
__author__ = 'Franz Liem <franziskus.liem@uzh.ch>'
__docformat__ = 'reStructuredText'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import bidsapps
    bidsapps.BidsAppsScript().run()

import os
import sys
import time
import tempfile
import re

import shutil
# import csv
from bids.grabbids import BIDSLayout

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, hours, minutes, \
    seconds
from gc3libs.workflow import RetryableTask

DEFAULT_CORES = 1
DEFAULT_MEMORY = Memory(3000, MB)

DEFAULT_REMOTE_INPUT_FOLDER = "./input/"
DEFAULT_REMOTE_OUTPUT_FOLDER = "./output"


## custom application class
class BidsAppsApplication(Application):
    """
    """
    application_name = 'bidsapps'

    def __init__(self, subject, bids_input_folder,
                 #output_folder,
                 #docker_container, runscript, runscript_args,
                 **extra_args):
        self.output_dir = []  # extra_args['output_dir']

        inputs = dict()
        outputs = dict()
        self.output_dir = extra_args['output_dir']

        #fixme
        # docker_input_folder_str = "{bids_input_folder}:/bids_dataset:ro" \
        #     .format(bids_input_folder=bids_input_folder)
        #
        # docker_output_folder_str = "{output_folder}:/outputs" \
        #     .format(output_folder=output_folder)
        #
        # arguments = "docker run --rm -ti " + \
        #             "-v {} ".format(docker_input_folder_str) + \
        #             "-v {} ".format(docker_output_folder_str) + \
        #             "{docker_container} {runscript} {runscript_args}".format(
        #                 docker_container=docker_container,
        #                 runscript=runscript, runscript_args=runscript_args)

        arguments = "docker run --rm -ti  {bids_input_folder}".format(
            bids_input_folder=bids_input_folder)

        # fixme
        arguments = "echo %s > output/text.txt"%arguments
        print(arguments)
        # ""./%s %s %s %s" % (inputs[gnift_wrapper_sh],
        #                             subject,
        #                             DEFAULT_REMOTE_INPUT_FOLDER,
        #                             DEFAULT_REMOTE_OUTPUT_FOLDER)

        Application.__init__(
            self,
            arguments=arguments,
            inputs=[],
            outputs=[DEFAULT_REMOTE_OUTPUT_FOLDER],
            stdout='bidsapps.log',
            join=True,
            **extra_args)


#
class BidsAppsScript(SessionBasedScript):
    """
    
    The ``gnift`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gnift`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    This class is called when bidsapps command is executed.
    Loops through subjects in bids input folder and starts instance
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version=__version__,  # module version == script version
            application=BidsAppsApplication,
            stats_only_for=BidsAppsApplication,
        )

    def setup_args(self):
        self.add_param("-i", "--bids_input_folder", type=str,
                       dest="bids_input_folder", default=None,
                       help="Root localtion of input data. "
                            "Note: expects folder in BIDS format.")
       # self.add_param("-x", "--executable", metavar="PATH", #type=executable_file,
       #                 dest="executable", default=None,
       #                 help="Path to the GEOtop executable file.")
    def new_tasks(self, extra):
        """
        For each input folder, create an instance of GniftApplication
        """
        tasks = []

        for subject_id in self.get_input_subjects(
                self.params.bids_input_folder):
            extra_args = extra.copy()
            extra_args['jobname'] = subject_id

            tasks.append(BidsAppsApplication(
                subject_id,
                **extra_args))

        return tasks

    def get_input_subjects(self, bids_input_folder):
        """
        """
        layout = BIDSLayout(bids_input_folder)
        return layout.get_subjects()
