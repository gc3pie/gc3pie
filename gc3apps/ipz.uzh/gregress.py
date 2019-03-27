#! /usr/bin/env python
#
#   gregress.py -- Front-end script for running REMDataset pipeline.
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

It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2017-07-03:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'
__version__ = '1.0.0'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gregress
    gregress.GenREMScript().run()

import os
import sys
import shutil
import random
from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file, \
    existing_directory
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, \
    hours, minutes, seconds
from gc3libs.workflow import StagedTaskCollection, \
    ParallelTaskCollection, SequentialTaskCollection

# Defaults

S0_REMOTE_INPUT_FILENAME = "./input.stata"
S0_REMOTE_OUTPUT_FILENAME = "./output.RData"
RSCRIPT_COMMAND="Rscript --vanilla {src}/script.R {method} {src} {data}"
STATS = ["stat-inertia", "stat-reciprocity", "stat-similarity", "stat-triad", "stat-degree"]
METHODS = ['remdataset', 'merge'] + STATS
S0_OUTPUT=""
S1_OUTPUT=""
S2_OUTPUT=""
REMOTE_RESULT_FOLDER="result"
REMOTE_DATA_FOLDER="data"
REMOTE_SCRIPTS_FOLDER="src"




# Utility methods

class GenREMDatasetApplication(Application):
    """
    Execute main workflow script and pass the workflow step
    as input argument.
    """
    application_name = 'genREM'

    def __init__(self, method, data_file_list, source_folder, **extra_args):

        inputs = dict()

        self.output = extra_args['results']

        for data_folder in data_file_list:
            for data_file in os.listdir(data_folder):
                inputs[os.path.join(data_folder,
                                    data_file)] = os.path.join(REMOTE_DATA_FOLDER,
                                                               data_file)

        for script_file in os.listdir(source_folder):
            inputs[os.path.join(source_folder,
                                script_file)] = os.path.join(REMOTE_SCRIPTS_FOLDER,
                                                             os.path.basename(script_file))

        arguments = RSCRIPT_COMMAND.format(method=method,
                                           src=REMOTE_SCRIPTS_FOLDER,
                                           data=REMOTE_DATA_FOLDER)

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = [REMOTE_RESULT_FOLDER],
            stdout = 'genREM.log',
            join=True,
            **extra_args)

    def terminated(self):
        """
        Move results to corresponding 'result' folder
        """

        if not os.path.isdir(self.output):
            os.makedirs(self.output)
        for data in os.listdir(os.path.join(self.output_dir,
                                            REMOTE_RESULT_FOLDER)):
            shutil.move(os.path.join(self.output_dir,
                                     REMOTE_RESULT_FOLDER,
                                     data),
                        os.path.join(self.output,
                                     data))

class GenREMStagedTaskCollection(StagedTaskCollection):
    """
    Staged collection:
    Step 0: Generate REMDataset
    Step 1: For each available statistical method, run independent application
    Step 2: Merge all results together
    """
    def __init__(self, data_folder, source_folder, **extra_args):

        self.data_folder = data_folder
        self.source_folder = source_folder
        self.extra = extra_args
        self.s0_outputfolder = os.path.join(extra_args['result'],"S0")
        self.s1_outputfolder = os.path.join(extra_args['result'],"S1")
        self.s2_outputfolder = os.path.join(extra_args['result'],"S2")
        StagedTaskCollection.__init__(self)

    def stage0(self):
        """
        Step 0: Generate REMDataset
        """

        extra_args = self.extra.copy()
        extra_args['jobname'] = "remdataset"
        extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                    extra_args['jobname'])
        extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                    extra_args['jobname'])
        extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                    extra_args['jobname'])
        extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                    extra_args['jobname'])

        # gc3libs.log.debug("Creating Stage0 task for : %s" % os.path.basename(self.input_stata_file))
        extra_args['results'] = self.s0_outputfolder

        return GenREMDatasetApplication("remdataset",[self.data_folder],self.source_folder,**extra_args)


    def stage1(self):
        """
        Step 1: For each available statistical method, run independent application
        """
        tasks = []

        for method in STATS:
            extra_args = self.extra.copy()
            extra_args['jobname'] = method
            extra_args['results'] = self.s1_outputfolder
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                        extra_args['jobname'])
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                        extra_args['jobname'])
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                        extra_args['jobname'])
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                        extra_args['jobname'])

            tasks.append(GenREMDatasetApplication(method,[self.s0_outputfolder],self.source_folder,**extra_args))
        return ParallelTaskCollection(tasks)

    def stage2(self):
        """
        Step 2: Merge all results together
        """
        extra_args = self.extra.copy()
        extra_args['jobname'] = "merge"
        extra_args['results'] = self.s2_outputfolder
        extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                    extra_args['jobname'])
        extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                    extra_args['jobname'])
        extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                    extra_args['jobname'])
        extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                    extra_args['jobname'])

        return GenREMDatasetApplication("merge",[self.s0_outputfolder, self.s1_outputfolder],self.source_folder,**extra_args)


class GenREMScript(SessionBasedScript):
    """
    Take initial RData DataFrame input file and run the full workflow by
    passing to the main execution script the workflow steps as part of the input
    arguments.

    The ``gregress`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gscr`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            application = GenREMDatasetApplication,
            stats_only_for = GenREMDatasetApplication
            )

    def setup_args(self):

        self.add_param('data_folder', type=existing_directory,
                       help="Location of initial RData file.")

    def setup_options(self):
        self.add_param("-x", "--source", metavar="PATH",
                       dest="src", default=None,
                       type=existing_directory,
                       help="Location of source R scripts.")

        self.add_param("-R", "--results", metavar="PATH",
                       dest="result", default='results',
                       help="Location of results.")

    def new_tasks(self, extra):
        """
        Just start the StageCollection with the passed input RData file in
        self.params.data_folder.
        """
        extra_args = extra.copy()
        extra_args['result'] = self.params.result
        return [GenREMStagedTaskCollection(self.params.data_folder,
                                           self.params.src,
                                           **extra_args)]
