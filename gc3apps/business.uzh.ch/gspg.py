#! /usr/bin/env python
#
#   gspg.py -- Front-end script for evaluating R-based 'weight'
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

See the output of ``gspg.py --help`` for program usage
instructions.

Input parameters consists of:
:param str edges file: Path to an .csv file containing input data in
the for of:
    X1   X2
1  id1  id2
2  id1  id3
3  id1  id4

...

XXX: To be clarified:
. When input files should be removed ?
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
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gspg
    gspg.GspgScript().run()

import os
import sys
import time
import tempfile

import shutil
import pandas
# import csv

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

## custom application class
class GspgApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'gspg'

    def __init__(self, input_file, **extra_args):


        executables = []
        inputs = dict()

        inputs[input_file] = "./input.csv"
        gspg_wrapper = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gspg_wrapper.py")

        inputs[gspg_wrapper] = "./wrapper.py"


        self.output_folder = "./results"

        # setup output references
        # self.result_dir = result_dir
        self.output_dir = extra_args['output_dir']
        self.output_filename = 'results-%s.csv' % extra_args['index_chunk']
        self.output_file = os.path.join(self.output_dir,self.output_filename)

        outputs = [("./results.csv",self.output_filename)]

        arguments ="python ./wrapper.py ./input.csv "

        # check the optional inputs

        if extra_args.has_key('ctx'):
            inputs[extra_args['ctx']] = "./bin/ctx.p4"
            arguments +=  "--ctx ./bin/ctx.p4 "
            executables.append("./bin/ctx.p4")

        gc3libs.log.debug("Creating application for executing: %s",
                          arguments)

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gspg.log',
            join=True,
            executables = executables,
            **extra_args)

    def terminated(self):
        """
        Check whether output file has been properly created
        """
        pass

class GspgScript(SessionBasedScript):
    """
    Takes 1 .csv input file containing the list of parameters to be passed
    to the `ctx-linkdyn-ordprm-sirs.p4` application.
    Each line of the input .csv file correspond to the parameter list to be
    passed to a single `ctx-linkdyn-ordprm-sirs.p4` execution. For each line
    of the input .csv file a GspgApplication needs to be generated (depends on
    chunk value passed as part of the input options).
    Splits input .csv file into smaller chunks, each of them of size
    'self.params.chunk_size'.
    Then submits one execution for each of the created chunked files.

    The ``gspg`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gspg`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gspg``
    aggregates them into a single larger output file located in
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GspgApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GspgApplication,
            )

    def setup_args(self):

        self.add_param('csv_input_file', type=str,
                       help="Input .csv file")

    def setup_options(self):

        self.add_param("-M", "--ctx", metavar="[PATH]",
                       dest="ctx", default=None,
                       help="Location of local copy of ctx binary")

        self.add_param("-k", "--chunk", metavar="INT", type=int,
                       dest="chunk_size", default=1000,
                       help="How to split the edges input data set.")

    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """

        try:
            assert os.path.isfile(self.params.csv_input_file)
        except ValueError:
            raise gc3libs.exceptions.InvalidUsage(
                "Input CSV file %s not found" % self.params.csv_input_file)

        if self.params.ctx:
            try:
                assert os.path.isfile(self.params.ctx)
            except ValueError:
                raise gc3libs.exceptions.InvalidUsage(
                    "CTX binary file %s not found" % self.params.ctx)

        # Verify that 'self.params.chunk_size' is int
        try:
            assert int(self.params.chunk_size)
        except ValueError:
            raise gc3libs.exceptions.InvalidUsage(
                "-k option accepts only numbers")

    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new GcgpsTask
        """
        tasks = []

        for (input_file, index_chunk) in self._generate_chunked_files_and_list(self.params.csv_input_file,
                                                                              self.params.chunk_size):

            jobname = "gspg-%s" % (str(index_chunk))

            extra_args = extra.copy()

            extra_args['index_chunk'] = str(index_chunk)
            extra_args['chunk_size'] = int(self.params.chunk_size)

            extra_args['jobname'] = jobname

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

            if self.params.ctx:
                extra_args['ctx'] = self.params.ctx

            self.log.debug("Creating Application for index : %d - %d" %
                           (index_chunk, (index_chunk + self.params.chunk_size)))

            tasks.append(GspgApplication(
                    input_file,
                    **extra_args))

        return tasks

    def _generate_chunked_files_and_list(self, file_to_chunk, chunk_size=2):
        """
        Takes a file_name as input and a defined chunk size
        ( uses 1000 as default )
        returns a list of filenames 1 for each chunk created and a corresponding
        index reference
        e.g. ('/tmp/chunk2800.csv,2800) for the chunk segment that goes
        from 2800 to (2800 + chunk_size)
        """

        chunks = []
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

        # XXX: by convenction, 1st row contains headers
        reader = pandas.read_csv(file_to_chunk, header=0, chunksize=chunk_size)

        index = 0
        for chunk in reader:
            index += 1
            filename = "%s/chunk_%s.csv" % (chunk_files_dir,index)
            chunk.to_csv(filename, header=True, index=False)
            chunks.append((filename,index))

        return chunks
