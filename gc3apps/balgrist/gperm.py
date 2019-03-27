#! /usr/bin/env python
#
#   gperm.py -- Front-end script for running Docker BIDS apps
#   function over a large dataset.
#
#   Copyright (C) 2017, 2018  University of Zurich. All rights reserved.
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
Front-end script for submitting multiple `BIDS apps` jobs fetching
docker images from the `BIDS apps` repository.

It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gperm.py --help`` for program usage
instructions.

Example of docker execution:
docker run -i --rm -v /mnt/filo/data/ds005:/bids_dataset:ro -v /mnt/filo/outputs:/outputs bids/fmriprep /bids_dataset /outputs participant --participant_label 01

gperm takes BIDS files as input.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2018-03-10:
  * result of each subject get stored back to the corresponding input folder.
  2018-01-10:
  * added support for freesurfer license file to be passed as part of the docker invokation. see: https://fmriprep.readthedocs.io/en/latest/installation.html#the-freesurfer-license
  2017-04-18:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'
__version__ = '1.0'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gperm
    gperm.GpermScript().run()

import os
import sys
import time
import tempfile
import mimetypes
import random
import shutil

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file, positive_int, existing_file, existing_directory
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

PHOENY_OUTPUT_DIR="output"
DEFAULT_BIDS_FOLDER="data/"
DEFAULT_RESULT_FOLDER="output/"
DEFAULT_DOCKER_BIDS_ARGS="--no-submm-recon"
DEFAULT_FREESURFER_LICENSE_FILE="license.txt"
DEFAULT_DOCKER_BIDS_APP="poldracklab/fmriprep "+DEFAULT_DOCKER_BIDS_ARGS
DEFAULT_REPETITIONS=1

DOCKER_RUN_COMMAND="sudo docker run -i --rm {DOCKER_MOUNT} {DOCKER_TO_RUN} /bids /output participant --participant_label {SUBJECT_NAME}"

## Utility methods
def _get_subjects(input_folder):
    """
    return tuple ([list .json and .tsv files],[list of sub-folders])
    Assumptions:
    * each sub-folder contains a valid subject's data
    * each .json and .tsv file found in root folder will be made available
    to all Applications.
    """
    control_files_list = []
    subjects_folders_list = []

    for element in os.listdir(input_folder):
        full_element = os.path.abspath(os.path.join(input_folder,element))
        if element.endswith(".json") or element.endswith(".tsv"):
            # Valid control file
            control_files_list.append(full_element)

        elif os.path.isdir(full_element):
            # Valid subject folder
            subjects_folders_list.append(full_element)

    return (control_files_list,subjects_folders_list)

## custom application class
class GpermApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'gperm'

    def __init__(self, subject, subject_name, control_files, repeat_index, docker_run, freesurfer_license, **extra_args):

        executables = []
        inputs = dict()
        outputs = dict()

        self.subject_dir = subject
        inputs[subject] = os.path.join(DEFAULT_BIDS_FOLDER,
                                       os.path.basename(subject))

        # add all control files to 'data' folder
        for element in control_files:
            inputs[element] = os.path.join(DEFAULT_BIDS_FOLDER,
                                       os.path.basename(element))

        if not os.path.isdir(DEFAULT_RESULT_FOLDER):
            os.mkdir(DEFAULT_RESULT_FOLDER)
        inputs[DEFAULT_RESULT_FOLDER] = DEFAULT_RESULT_FOLDER

        # Define mount points
        DOCKER_MOUNT=" -v $PWD/{SUBJECT_DIR}:/bids:ro -v $PWD/{OUTPUT_DIR}:/output ".format(SUBJECT_DIR=DEFAULT_BIDS_FOLDER,
                                                                                     OUTPUT_DIR=DEFAULT_RESULT_FOLDER)

        if freesurfer_license:
            inputs[freesurfer_license] = os.path.basename(freesurfer_license)
            DOCKER_MOUNT+=" -v $PWD/{0}:/opt/freesurfer/license.txt ".format(inputs[freesurfer_license])

        arguments = DOCKER_RUN_COMMAND.format(DOCKER_MOUNT=DOCKER_MOUNT,
                                              DOCKER_TO_RUN=docker_run,
                                              SUBJECT_NAME=subject_name)

        gc3libs.log.debug("Creating application for executing: %s", arguments)

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = [DEFAULT_RESULT_FOLDER],
            stdout = 'gperm.log',
            join=True,
            executables = executables,
            **extra_args)

    def terminated(self):
        """
        Move output file in 'result_dir'
        """
        gc3libs.log.info("Moving results from {0} to {1}".format(self.output_dir,
                                                                 self.subject_dir))

        for elem in os.listdir(self.output_dir):
            shutil.move(os.path.join(self.output_dir,elem),
                        os.path.join(self.subject_dir,elem))


class GpermScript(SessionBasedScript):
    """
    The ``gperm`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gperm`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gperm``
    aggregates them into a single larger output file located in
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            application = GpermApplication,
            stats_only_for = GpermApplication,
            )

    def setup_args(self):
        self.add_param('input_folder',
                       type=existing_directory,
                       help="Path to input folder containing valid input .bids files.")

    def setup_options(self):
        self.add_param("-R", "--repeat", metavar="[INT]",
                       type=positive_int,
                       dest="repeat", default=DEFAULT_REPETITIONS,
                       help="Repeat analysis. Default: %(default)s.")

        self.add_param("-L", "--license", metavar="[PATH]",
                       type=existing_file,
                       dest="freesurfer_license", default=None,
                       help="Location of freesurfer license file. Default: %(default)s.")

        self.add_param("-D", "--docker", metavar="[PATH]",
                       dest="docker", default=DEFAULT_DOCKER_BIDS_APP,
                       help="BIDS app docker image and execution arguments. Default: '%(default)s'.")


    def parse_args(self):
        """
        parse 'docker' command and separate docker image reference
        from execution arguments
        """

        self.docker_image = self.params.docker.split(' ')[0]
        self.docker_args =  self.params.docker.split(' ')[1:]

    def new_tasks(self, extra):
        """
        For each valid input file create a new GpermRetryableTask
        """
        tasks = []

        control_files,subjects_list = _get_subjects(self.params.input_folder)
        for subject in subjects_list:
            for repeat in range(0,self.params.repeat):
                subject_name = os.path.basename(subject)
                jobname = "{subject}-{rep}".format(subject=subject_name,
                                                   rep=repeat)

                extra_args = extra.copy()

                extra_args['jobname'] = jobname
                extra_args['output_dir'] = self.params.output
                extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

                self.log.debug("Creating Application for subject %s repetition : %d" % (subject_name,
                                                                                        repeat))

                tasks.append(GpermApplication(
                    subject,
                    subject_name,
                    control_files,
                    repeat,
                    self.params.docker,
                    self.params.freesurfer_license,
                    **extra_args))

        return tasks
