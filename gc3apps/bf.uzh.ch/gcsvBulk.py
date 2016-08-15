#! /usr/bin/env python
#
#   gcsvBulk.py -- Front-end script for running a MATLAB function
#   with a given csv file as input parameters. Additionally a block
#   size for the csv file can be specified to devide the csv in blocks
#   that should yield as input for each node. In other words, the block size
#   determines the number of nodes to run
#
#   Copyright (C) 2014, 2015 GC3, University of Zurich
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

See the output of ``gcsvBulk.py --help`` for program usage
instructions.

Input parameters consists of:

...

Options:
"""

__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2016-05-11:
  * Initial version
"""
__author__ = 'Adrian Etter <adrian.etter@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gcsvBulk
    gcsvBulk.GcsvBulkScript().run()

from argparse import ArgumentError
import os
from os.path import basename, exists, join, realpath
import sys
import time
import tempfile

from math import ceil

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

DEFAULT_CORES = 4
DEFAULT_MEMORY = Memory(7,GB)
DEFAULT_WALLTIME = Duration(200,hours)

## custom application class
class GcsvBulkApplication(Application):
    """
    Custom class to wrap the execution of a matlab script with a csv as input.
    A convenient wrapper script is provided to facilitate how the command file
    is executed on the remote end.
    """
    application_name = 'gcsvBulk'

    def __init__(self, input_file, input_function, input_csvfile, limit, skip, **extra_args):

        self.output_dir = extra_args['output_dir']
        self.input_function = input_function
        #
        inputs = dict()

        gcsvBulk_wrapper_m = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gcsvBulk_wrapper.m")
        #
        inputs[gcsvBulk_wrapper_m] = os.path.basename(gcsvBulk_wrapper_m)
        # inputs['foo'] = 'foo'
        #
        inputs[input_file] = "%s" % os.path.basename(input_file)
        inputs[input_csvfile] = "%s" % os.path.basename(input_csvfile)
        #
        arguments = "matlab -nodesktop -nodisplay -nojvm -nosplash -r \"%s('%s', '%s', %d, %d);\"" % (inputs[gcsvBulk_wrapper_m][:-2], input_function, inputs[input_csvfile], limit, skip)
        # print 'hello'
        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = ["./results"],
            stdout = 'gcsvBulk.log',
            join=True,
            executables = "./%s" % os.path.basename(input_function),
            **extra_args)

    def terminated(self):
        """
        Move results into original results folder
        """
        gc3libs.log.info("Application terminated with exit code %s" % self.execution.exitcode)
        for result in os.listdir(os.path.join(self.output_dir,"results/")):
            shutil.move(os.path.join(self.output_dir,"results/",result),
                        os.path.join(self.input_folder,"results",os.path.basename(result)))
        # Cleanup
        os.removedirs(os.path.join(self.output_dir,"results/"))

class GcsvBulkScript(SessionBasedScript):
    """
    The script takes as input a mfunction, the number of parallel jobs to run and the
    dataset to be processed

    The ``gcsvBulk`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gcsvBulk`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GcsvBulkApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GcsvBulkApplication,
            )

    def setup_args(self):
        self.add_param('mfunction', help=(
            "Name of the matlab function to run."
            "A corresponding `.m` file must exist in the current directory."))
        self.add_param('csvfile', help=(
            "Name of the csv filte to be processed."))
        self.add_param('N', default=0, type=int, help=("Value for the number of nodes."))

    def get_function_name_and_file(self, mfunction):
        if mfunction.endswith('.m'):
            mfunction_m = mfunction
            mfunction = mfunction[:-len('.m')]
        else:
            mfunction_m = mfunction + '.m'
        m_file = join(os.getcwd(), mfunction_m)
        if not exists(m_file):
            raise ArgumentError(
                self.actions['mfunction'],
                ("Cannot read file '{m_file}'"
                 " providing matlab function '{mfunction}'.")
                .format(**locals()))
        return mfunction, m_file

    def get_csv_file(self, csvFile):
        if not csvFile.endswith('.csv'):
            csvFile += '.csv'
        csv_file = join(os.getcwd(), csvFile)
        if not exists(csv_file):
            raise ArgumentError(
                self.actions['csvfile'],
                ("Cannot read file '{csv_file}.")
                .format(**locals()))
        return csv_file

    def get_BlockSize(self, nrBlocks, csvFile):
        with open(csvFile) as f:
            for i, l in enumerate(f, 1):
                pass
        return ceil(i/nrBlocks)



    def new_tasks(self, extra):
        """
        For each N, create an instance of GcsvBulkApplication
        """
        tasks = []

        input_function, input_file = self.get_function_name_and_file(self.params.mfunction)
        csv_file = self.get_csv_file(self.params.csvfile)

        blockSize = self.get_BlockSize(self.params.N, csv_file)

        # for node in xrange(self.params.N):
        extra_args = extra.copy()
        for node in xrange(self.params.N):
            jobname = "gcsvBulk-%s" % node

            extra_args = extra.copy()

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', 'run_%s' % jobname)
            tasks.append(GcsvBulkApplication(input_file, input_function, csv_file, blockSize, node * blockSize, **extra_args))
        return tasks
