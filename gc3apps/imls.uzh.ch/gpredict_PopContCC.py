#! /usr/bin/env python
#
#   gpredict_PopContCC.py -- Front-end script for running Matlab function
#   function over a large parameter range.
#
#   Copyright (C) 2016  University of Zurich. All rights reserved.
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
Front-end script for running Matlab function
#   function over a large parameter range.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gpredict_PopContCC.py --help`` for program usage
instructions.

"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2016-08-17:
  * Initial version
  2016-08-19:
  * add '-f <function name>' option
  2016-09-30:
  * add extra option 'bundle' and option value
    to the matlab function to be called
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gpredict_PopContCC
    gpredict_PopContCC.Gpredict_PopContCCScript().run()

import os
import sys
import time
import tempfile

import tarfile
import shutil

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file, positive_int, existing_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

DEFAULT_MATLAB_FUNCTION="predict_PopContCC"
DEFAULT_REMOTE_OUTPUT_FOLDER = "./results"
TARFILE="source.tgz"
TEMP_FOLDER="/var/tmp"
DEFAULT_REPETITIONS = 100
DEFAULT_BUNDLE = 10
MATLAB_COMMAND="matlab -nodesktop -nodisplay -nosplash -r \'{mfunct} {MatPredictor} " \
    "{VecResponse} {numberOfSamples} {numberOfTrees} {bundles} {results} ;quit()'"

## utility funtions
def _get_iterations(repetitions,bundle):
    """
    Yield iterator number and bundle size.
    Last bundle value may be shorter than 'bundle'
    """
    total_iterations = repetitions/bundle
    last_iteration = repetitions%bundle

    for iteration in range(1,total_iterations+1):
        yield (iteration,bundle)

    if repetitions%bundle > 0:
        yield (total_iterations+1,repetitions%bundle)


def _scanandtar(dir_to_scan, temp_folder=TEMP_FOLDER):
    try:
        gc3libs.log.debug("Compressing input folder '%s'" % dir_to_scan)
        cwd = os.getcwd()
        os.chdir(dir_to_scan)

        if not os.path.isdir(temp_folder):
            os.mkdir(temp_folder)

        with tarfile.open(os.path.join(temp_folder,TARFILE), "w:gz") as tar:

            tar.add(dir_to_scan, arcname=".")
            os.chdir(cwd)

            gc3libs.log.info("Created tar file '%s'" % TARFILE)
            return tar.name

    except Exception, x:
        gc3libs.log.error("Failed creating input archive '%s': %s %s",
                          os.path.join(dir_to_scan,),
                          type(x),x.message)
        raise


## custom application class

class Gpredict_PopContCCApplication(Application):
    """
    Custom class to wrap the execution of the matlab function.
    """
    application_name = 'gpredictpopcontcc'

    def __init__(self, Mfunct, MatPredictor_file, VecResponse_file,
                 numberOfSamples, numberOfTrees, bundles, iteration, **extra_args):

        executables = []
        inputs = dict()
        outputs = dict()

        self.iteration = iteration
        self.results = extra_args['session_output_dir']

        inputs[Mfunct] = os.path.basename(Mfunct)
        function_name = inputs[Mfunct].split('.')[0]
        inputs[MatPredictor_file] = os.path.basename(MatPredictor_file)
        inputs[VecResponse_file] = os.path.basename(VecResponse_file)

        arguments = MATLAB_COMMAND.format(mfunct=function_name,
                                          MatPredictor=inputs[MatPredictor_file],
                                          VecResponse=inputs[VecResponse_file],
                                          numberOfSamples=numberOfSamples,
                                          numberOfTrees=numberOfTrees,
                                          bundles=bundles,
                                          results=DEFAULT_REMOTE_OUTPUT_FOLDER)

        # Set output
        outputs[DEFAULT_REMOTE_OUTPUT_FOLDER] = DEFAULT_REMOTE_OUTPUT_FOLDER

        gc3libs.log.debug("Creating application for executing: %s",
                          arguments)

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gpredict_PopContCC.log',
            join=True,
            executables = executables,
            **extra_args)

    def terminated(self):
        folder_files = os.path.join(self.output_dir, DEFAULT_REMOTE_OUTPUT_FOLDER)
        for f in os.listdir(folder_files):
            os.rename(os.path.join(folder_files ,f),
                      os.path.join(self.results, "%d_%s" % (self.iteration,f)))

class Gpredict_PopContCCScript(SessionBasedScript):
    """
    Takes a matlab function file as first argument, then MatPredictor .mat file
    then a VecResponse .mat file, then numberOfSamples and numberOfTrees.
    Additionally takes the number of repetitions as option.

    For each repetitions, the script executed the function specified in the matlab
    function.
    expected function signature:
    predict_PopContCC(matPredictor,vecResponse,numberOfSamples,numberOfTrees,resultFolder)

    The ``gpredict_PopContCC`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gpredict_PopContCC`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gpredict_PopContCC``
    aggregates them into a single larger output file located in
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = Gpredict_PopContCCApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = Gpredict_PopContCCApplication,
            )

    def setup_args(self):
        self.add_param('Mfunct',
                       type=existing_file,
                       help="Path to the gpredict_PopContCC file.")

        self.add_param('MatPredictor',
                       type=existing_file,
                       help="Path to the MatPredictor file.")

        self.add_param('VecResponse',
                       type=existing_file,
                       help="Path to the VecResponse file.")


        self.add_param('numberOfSamples',
                       type=positive_int,
                       help="Number of samples.")

        self.add_param('numberOfTrees',
                       type=positive_int,
                       help="Number of trees.")

    def setup_options(self):
        self.add_param("-R", "--repetitions",
                       metavar="[INT]",
                       type=positive_int,
                       dest="repetitions",
                       default=DEFAULT_REPETITIONS,
                       help="Number of repetitions. "
                       " Repeat Matlab execution. "
                       " Default: repeat '%(default)s' times.")

        self.add_param("-B", "--bundle",
                       metavar="[INT]",
                       type=positive_int,
                       dest="bundle",
                       default=DEFAULT_BUNDLE,
                       help="Group execution of repetitions in bundles. "
                       " Total executions: repetitions / bundle. "
                       " Default: bundle in group of '%(default)s' repetitions.")


    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new Application
        """
        tasks = []

        for iteration,bundle_size in _get_iterations(self.params.repetitions,self.params.bundle):
            jobname = "gpredict_PopContCC-%d" % (iteration)

            extra_args = extra.copy()
            extra_args['jobname'] = jobname
            extra_args['output_dir'] = self.params.output.replace('NAME', jobname)
            extra_args['session_output_dir'] = os.path.dirname(self.params.output)

            self.log.debug("Creating Application for iteration : %d" % iteration)

            tasks.append(Gpredict_PopContCCApplication(
                os.path.abspath(self.params.Mfunct),
                os.path.abspath(self.params.MatPredictor),
                os.path.abspath(self.params.VecResponse),
                self.params.numberOfSamples,
                self.params.numberOfTrees,
                bundle_size,
                iteration,
                **extra_args))

        return tasks
