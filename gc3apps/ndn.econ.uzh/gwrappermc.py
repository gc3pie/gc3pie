#! /usr/bin/env python
#
#   gwrappermc.py -- Front-end script for evaluating R-based 'weight'
#   function over a large dataset.
#
#   Copyright (C) 2011, 2012  University of Zurich. All rights reserved.
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
Front-end script for submitting multiple `R` jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gwrappermc.py --help`` for program usage
instructions.

Input parameters consists of:
:param str edges file: Path to an .csv file containing input data in
the for of:
    X1   X2
1  id1  id2
2  id1  id3
3  id1  id4

...
2015-09-29: aggregated result file should be named after the `-o` option


XXX: To be clarified:
. What happen if an error happen at merging time ?
. Should be possible to re-run a subset of the initial chunk list
without re-creating a new session ?
e.g. adding a new argument accepting chunk ranges (-R 3000:7500)
This would trigger the re-run of the whole workflow only
for lines between 3000 and 7500
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2013-07-03:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gwrappermc
    gwrappermc.GwrappermcScript().run()

import os
import sys
import time
import tempfile

import shutil
import pandas

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

## custom application class
class GwrappermcApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'adimat'

    def __init__(self, input_file, **extra_args):

        inputs = dict()
        inputs[input_file] = "./input.csv"

        self.output_folder = "./results"

        # arguments = "./MCSpecs ./input.csv"
        # $ arguments = "matlab -nodesktop -nodisplay -nosplash -r \'Main_loop input.csv results; quit()\'"

        gwrappermc_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gwrappermc_wrapper.sh")

        inputs[gwrappermc_wrapper_sh] = os.path.basename(gwrappermc_wrapper_sh)

        arguments = "./%s " % inputs[gwrappermc_wrapper_sh]
        if 'main_loop_folder' in extra_args:
            inputs[extra_args['main_loop_folder']] = './data/'
            arguments += "-m ./data "
        arguments += " -i %s " % extra_args['index_chunk']
        arguments += "input.csv results"

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = ['results/'],
            stdout = 'gwrappermc.log',
            join=True,
            executables = "./%s " % inputs[gwrappermc_wrapper_sh],
            **extra_args)


class GwrappermcScript(SessionBasedScript):
    """
    Splits input .csv file into smaller chunks, each of them of size
    'self.params.chunk_size'.
    Then it submits one execution for each of the created chunked files.

    The ``gwrappermc`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gwrappermc`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gwrappermc``
    aggregates them into a single larger output file located in
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GwrappermcApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GwrappermcApplication,
            )

    def setup_options(self):
        self.add_param("-k", "--chunk", metavar="INT", type=int,
                       dest="chunk_size", default=2,
                       help="How to split the edges input data set.")

        self.add_param("-d", "--data", metavar="PATH", type=str,
                       dest="main_loop", default=None,
                       help="Location of the Main_Loop.m script and "
                       "related MAtlab functions. Default: None")

        self.add_param("-R", "--result-file", metavar="STRING", type=str,
                       dest="merged_result", default="result.csv",
                       help="Name of merged result file. Default: result.csv")

    def setup_args(self):

        self.add_param('csv_input_file', type=str,
                       help="Input .csv file")

    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """

        # check args:
        # XXX: make them position independent
        try:
            assert os.path.isfile(self.params.csv_input_file)
        except ValueError:
            raise gc3libs.exceptions.InvalidUsage(
                "Input CSV file %s not found" % self.params.csv_input_file)

        # Verify that 'self.params.chunk_size' is int
        try:
            assert int(self.params.chunk_size)
        except ValueError:
            raise gc3libs.exceptions.InvalidUsage(
                "-k option accepts only numbers")

            if self.params.main_loop:
                if not os.path.isdir(self.params.main_loop):
                    raise gc3libs.exceptions.InvalidUsage(
                        "Main_Loop.m location %s not found" % self.params.main_loop)

    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new GcgpsTask
        """
        tasks = []

        for (input_file, index_chunk) in self._generate_chunked_files_and_list(self.params.csv_input_file,
                                                                              self.params.chunk_size):
            jobname = "gwrappermc-%s" % (str(index_chunk))

            extra_args = extra.copy()

            extra_args['index_chunk'] = str(index_chunk)
            extra_args['chunk_size'] = int(self.params.chunk_size)

            extra_args['jobname'] = jobname

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

            if self.params.main_loop:
                extra_args['main_loop_folder'] = self.params.main_loop

            self.log.debug("Creating Application for index : %d - %d" %
                           (index_chunk, (index_chunk + self.params.chunk_size)))

            tasks.append(GwrappermcApplication(
                    input_file,
                    **extra_args))

        return tasks

    def after_main_loop(self):
        """
        Merge all result files together
        Then clean up tmp files
        """

        output_file_name_prefix = "Results"

        # init result .csv file
        merged_csv = "result-%s" % os.path.basename(self.params.csv_input_file)
        result_columns = dict()

        try:
            fout=open(merged_csv,"w+")
            for task in self.session:
                if isinstance(task,GwrappermcApplication) and task.execution.returncode == 0:
                    # get index reference
                    # index = task.index_chunk
                    # chunk_size = task.chunk_size

                    for index in range(int(task.index_chunk)+1, int(task.index_chunk) + int(task.chunk_size) + 1):

                        result_file = os.path.join(task.output_dir,
                                                   "%s_%d.csv" % (output_file_name_prefix,
                                                                  index))
                        if os.path.isfile(result_file):
                            data = pandas.read_csv(result_file, header=None)
                            result_columns[index] = data
            if result_columns:
                result = pandas.concat(result_columns, axis=1, ignore_index=True)
                result.to_csv("result.csv", header=False, index=False)
            else:
                gc3libs.log.warning("No results found")
        except OSError, osx:
            gc3libs.log.critical("Failed while merging result files. " +
                                 "Error %s" % str(osx))
            raise
        finally:
            fout.close()



    def _generate_chunked_files_and_list(self, file_to_chunk, chunk_size=2):
        """
        Takes a file_name as input and a defined chunk size
        ( uses 1000 as default )
        returns a list of filenames 1 for each chunk created and a corresponding
        index reference
        e.g. ('/tmp/chunk2800.csv,2800) for the chunk segment that goes
        from 2800 to (2800 + chunk_size)
        """

        chunk = []
        chunk_files_dir = os.path.join(self.session.path,"tmp")

        # creating 'chunk_files_dir'
        if not(os.path.isdir(chunk_files_dir)):
            try:
                os.mkdir(chunk_files_dir)
            except OSError, osx:
                gc3libs.log.error("Failed while creating tmp folder %s. " % chunk_files_dir +
                                  "Error %s." % str(osx) +
                                  "Using default '/tmp'")
                chunk_files_dir = "/tmp"

        cc = pandas.read_csv(file_to_chunk, header=None)
        for index in range(0, len(cc.columns), chunk_size):
            filename = "%s/chunk_%s.csv" % (chunk_files_dir,index)
            cc.to_csv(filename ,columns=range(index,index+chunk_size), header=False, index=False)
            chunk.append((filename,index))

        return chunk
