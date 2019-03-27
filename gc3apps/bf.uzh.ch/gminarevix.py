#! /usr/bin/env python
#
#   gminarevix.py -- Front-end script for running Matlab MinAREVIX_Cloud
#   function over a set of structure data and structure models.
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

See the output of ``gminarevix.py --help`` for program usage
instructions.

Input parameters consists of:

...

Options:
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2014-03-14:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gminarevix
    gminarevix.GminarevixScript().run()

import os
import sys
import time
import tempfile

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

# Resource requirements depending on Model type (only # cores)
MODELS_SPECS = {'Wishart': {'requested_cores': 8, 'requested_memory': Memory(12*GB)},
                'SVJJ': {'requested_cores': 16, 'requested_memory': Memory(32*GB)},
                'SV2F': {'requested_cores': 8, 'requested_memory': Memory(12*GB)}}

## custom application class
class GminarevixApplication(Application):
    """
    Custom class to wrap the execution of the Matlab script.
    Binary is called 'minarevix_cloud'; it can be replaced by what is passed as
    'run_binary' option.
    Binary takes a single 'model_name' and a data folder containing 1 structure_data
    in .mat format.
    Output will be written in 'data' folder (this is hardcoded in the Matlab
    differential evolutionary alghorithm.
    """
    application_name = 'matlab-mcr'

    def __init__(self, model_name, structure_data, **extra_args):

        self.output_dir = extra_args['output_dir']

        inputs = dict()
        outputs = dict()

        data_name = os.path.basename(structure_data)
        inputs[structure_data] = "./data/%s" % data_name
        outputs["./data/"] = "results/"

        if extra_args.has_key('run_binary'):
            inputs[os.path.abspath(extra_args['run_binary'])] = './minarevix_cloud'
            arguments = "./minarevix_cloud "
        else:
            arguments = "minarevix_cloud "

        arguments += "%s ./data/%s" % (model_name, data_name)

        if extra_args.has_key('requested_cores'):
            extra_args['requested_cores'] = max(MODELS_SPECS[model_name]['requested_cores'],
                                                extra_args['requested_cores'])
        else:
            extra_args['requested_cores'] = MODELS_SPECS[model_name]['requested_cores']

        if extra_args.has_key('requested_memory'):
            extra_args['requested_memory'] = max(MODELS_SPECS[model_name]['requested_memory'],
                                                 extra_args['requested_memory'])
        else:
            extra_args['requested_memory'] = MODELS_SPECS[model_name]['requested_memory']

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gminarevix.log',
            join=True,
            **extra_args)

class GminarevixScript(SessionBasedScript):
    """
    Fro each structure_data file (with '.mat' extension) found in the 'input folder',
    GminarevixScript generates as many Tasks as 'models' defined.

    The ``gminarevix`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gminarevix`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GminarevixApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GminarevixApplication,
            )

    def setup_args(self):


        self.add_param('models', type=str,
                       help="Comma separated list of model names. "
                       "Allowed models: %s" % str(MODELS_SPECS.keys()))

        self.add_param('structure_data', type=str,
                       help="Path to the folder containing structure data files.")

    def setup_options(self):
        self.add_param("-b", "--binary", metavar="[STRING]",
                       dest="run_binary", default=None,
                       help="Location of the Matlab compiled binary "
                       "version of the MinArevix.")

    def parse_args(self):
        """
        Check validity of input parameters and selected benchmark.
        """

        if self.params.run_binary:
            if not os.path.isfile(self.params.run_binary):
                raise gc3libs.exceptions.InvalidUsage("MinArevix binary "
                                                      " file %s not found"
                                                      % self.params.run_binary)


        # check args:
        if not os.path.isdir(self.params.structure_data):
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid path to structure data: '%s'. Folder not found"
                % self.params.structure_data)

        for model in self.params.models.split(','):
            if not model in MODELS_SPECS.keys():
                raise gc3libs.exceptions.InvalidUsage(
                    "Invalid model name: %s. Authorized model names: %s"
                    % (model, str(MODELS_SPECS.keys())))
            else:
                gc3libs.log.info("valid model: %s", model)

    def new_tasks(self, extra):
        """
        For each of the network data and for each of the selected benchmarks,
        create a GminarevixApplication.

        First loop the input files, then loop the selected benchmarks
        """
        tasks = []

        for input_file_name in os.listdir(self.params.structure_data):

            # Rule-out files without '.dat' extension
            if not input_file_name.endswith(".mat"):
                continue

            input_file = os.path.abspath(os.path.join(self.params.structure_data, input_file_name))

            for model in self.params.models.split(','):
                # extract numerical value from filename given the structure:
                # VIX_Call_Options_15102008.mat
                (a,b,c,data_index) = input_file_name.split('_')

                # XXX: need to find a more compact name
                # jobname = "gminarevix-%s-%s" % (model,data_index)
                jobname = "gminarevix-%s-%s" % (model,(input_file_name))

                extra_args = extra.copy()

                extra_args['jobname'] = jobname
                # extra_args['model_name'] = model

                if self.params.run_binary:
                    extra_args['run_binary'] = self.params.run_binary

                extra_args['output_dir'] = self.params.output
                extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                            os.path.join(model,
                                                                                         input_file_name))
                extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                            os.path.join(model,
                                                                                         input_file_name))
                extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                            os.path.join(model,
                                                                                         input_file_name))
                extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                            os.path.join(model,
                                                                                         input_file_name))

                tasks.append(GminarevixApplication(
                    model,
                    input_file,
                    **extra_args))

        return tasks
