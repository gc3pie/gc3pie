#! /usr/bin/env python
#
#   gbugs.py -- Front-end script for running MCMC with opneBUGS
#   function throguh R over a large table set.
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

See the output of ``gbugs.py --help`` for program usage
instructions.

Input parameters consists of:
:param str table input file: Path to an .txt file containing input data in table form

"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2015-04-10:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gbugs
    gbugs.GBugsScript().run()

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
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask, StagedTaskCollection, ParallelTaskCollection

DEFAULT_CORES = 1
DEFAULT_MEMORY = Memory(1500,MB)
DEFAULT_WALLTIME = Duration(300,hours)

def generate_chunked_files_and_list(file_to_chunk, chunk_size=1000, tmp_folder=os.getcwd()):
        """
        Takes a file_name as input and a defined chunk size
        ( uses 1000 as default )
        returns a list of filenames 1 for each chunk created and a corresponding
        index reference
        e.g. ('/tmp/chunk2800.csv,2800) for the chunk segment that goes
        from 2800 to (2800 + chunk_size)
        """
        index = 0
        chunk = []
        failure = False
        chunk_files_dir = os.path.join(tmp_folder,"tmp")

        # creating 'chunk_files_dir'
        if not(os.path.isdir(chunk_files_dir)):
            try:
                os.mkdir(chunk_files_dir)
            except OSError, osx:
                gc3libs.log.error("Failed while creating tmp folder %s. " % chunk_files_dir +
                                  "Error %s." % str(osx) +
                                  "Using default '/tmp'")
                chunk_files_dir = "/tmp"

        try:
            fd = open(file_to_chunk,'rb')

            fout = None

            for (i, line) in enumerate(fd):
                if i % chunk_size == 0:
                    if fout:
                        fout.close()
                    (handle, tmp_filename) = tempfile.mkstemp(dir=chunk_files_dir,
                                                                    prefix=
                                                                   'gbugs-',
                                                                   suffix=
                                                                   "%d.txt" % i)
                    fout = open(tmp_filename,'w')
                    chunk.append((fout.name,i))
                fout.write(line)
            fout.close()
        except OSError, osx:
            gc3libs.log.critical("Failed while creating chunk files." +
                                 "Error %s", (str(osx)))
            failure = True
        finally:
            if failure:
                # remove all tmp file created
                gc3libs.log.info("Could not generate full chunk list. "
                                 "Removing all existing tmp files... ")
                for (cfile, index) in chunk:
                    try:
                        os.remove(cfile)
                    except OSError, osx:
                        gc3libs.log.error("Failed while removing " +
                                          "tmp file %s. " +
                                          "Message %s" % osx.message)

        return chunk


## custom application class
class GBugsApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'gbugs'

    def __init__(self, input_table_filename, **extra_args):

        # setup output references
        # self.result_dir = result_dir
        self.output_dir = extra_args['output_dir']
        self.output_filename = 'result-%s.Rdata' % extra_args['index_chunk']
        self.output_file = os.path.join(self.output_dir,self.output_filename)
        outputs = [("./output.Rdata",self.output_filename)]
        # setup input references
        inputs = dict()
        inputs[input_table_filename] = "./input.txt"
        arguments = "Rscript --vanilla "

        # check the optional inputs

        if extra_args.has_key('driver_script'):
            inputs[extra_args['driver_script']] = "./run.R"

        arguments +=  "run.R "
        arguments += "input.txt output.Rdata"

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gbugs.log',
            join=True,
            **extra_args)

    def terminated(self):
        """
        Check whether output file has been properly created
        """
        # XXX: TBD, work on a more precise checking of the output file
        gc3libs.log.info("Application terminated with exit code %s" % self.execution.exitcode)
        if (not os.path.isfile(self.output_file)):
            gc3libs.log.error("Failed while checking outputfile %s." % self.output_file)
            self.execution.returncode = (0, 99)

class OpenBUGSCollection(StagedTaskCollection):
    def __init__(self, input_table_file, chunk_size, driver_script, stats_script, **extra_args):
        self.name = os.path.basename(input_table_file)
        self.input_table_file = input_table_file
        self.chunk_size = chunk_size
        self.driver_script = driver_script
        self.stats_script = stats_script
        self.output_dir = extra_args['output_dir']
        self.extra = extra_args
        StagedTaskCollection.__init__(self)

    def stage0(self):
        """
        Chunk input table and run chunks in parallel
        """
        tasks = []
        for (input_file, index_chunk) in generate_chunked_files_and_list(self.input_table_file,
                                                                              self.chunk_size):
            jobname = "gbugs-%s" % (str(index_chunk))
            extra_args = self.extra.copy()
            extra_args['index_chunk'] = str(index_chunk)
            extra_args['jobname'] = jobname

            # extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

            if self.driver_script:
                extra_args['driver_script'] = self.driver_script

            gc3libs.log.debug("Creating Task for index : %d - %d" %
                           (index_chunk, (index_chunk + self.chunk_size)))

            tasks.append(GBugsApplication(
                    input_file,
                    **extra_args))
        return ParallelTaskCollection(tasks)

class GBugsScript(SessionBasedScript):
    """
    Splits input .csv file into smaller chunks, each of them of size
    'self.params.chunk_size'.
    Then it submits one execution for each of the created chunked files.

    The ``gbugs`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gbugs`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gbugs``
    aggregates them into a single larger output file located in
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            application = GBugsApplication,
            stats_only_for = GBugsApplication,
            )

    def setup_options(self):
        self.add_param("-k", "--chunk", metavar="[NUM]",
                       dest="chunk_size", default="1000",
                       help="How to split the edges input data set.")

        self.add_param("-M", "--master", metavar="[PATH]",
                       dest="driver_script", default=None,
                       help="Location of master driver R script.")

        self.add_param("-S", "--statistic", metavar="[PATH]",
                       dest="stats_script", default=None,
                       help="Location of statistic R script.")
    def setup_args(self):

        self.add_param('input_table', type=str,
                       help="Input input table full path name.")

    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """

        if not os.path.isfile(self.params.input_table):
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid path to input table: '%s'. File not found"
                % self.params.input_table)

        self.table_filename = os.path.basename(self.params.input_table)

        # Verify that 'self.params.chunk_size' is int
        int(self.params.chunk_size)
        self.params.chunk_size = int(self.params.chunk_size)

    def new_tasks(self, extra):
        """
        Chunk initial input file
        For each chunked fule, generate a new GbugsTask
        """
        extra_args = extra.copy()
        extra_args['output_dir'] = self.params.output
        return [OpenBUGSCollection(self.params.input_table,
                                   self.params.chunk_size,
                                   self.params.driver_script,
                                   self.params.stats_script,
                                   **extra_args)]
