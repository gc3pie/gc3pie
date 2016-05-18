#! /usr/bin/env python
#
#   gtree.py -- Front-end script for running an R script
#   over a defined Data set on a given number of nodes .
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

See the output of ``gtree.py --help`` for program usage
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
    import gtree
    gtree.GtreeScript().run()

from argparse import ArgumentError
import os
from os.path import basename, exists, join, realpath
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

DEFAULT_CORES = 4
DEFAULT_MEMORY = Memory(7,GB)
DEFAULT_WALLTIME = Duration(200,hours)

## custom application class
class GtreeApplication(Application):
    """
    Custom class to wrap the execution of a R script
    A convenient wrapper script is provided to facilitate how the command file
    is executed on the remote end.
    """
    application_name = 'gtree'

    def __init__(self, input_function, input_dataSet, **extra_args):

        self.output_dir = extra_args['output_dir']
        self.input_function = input_function
        #
        inputs = dict()
        # outputs = dict()
        #
        # gtree_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
        #                                   "gc3libs/etc/gtree_wrapper.sh")
        gtree_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gtree_wrapper.sh")
        #
        inputs[gtree_wrapper_sh] = os.path.basename(gtree_wrapper_sh)
        # inputs['foo'] = 'foo'
        #
        inputs[input_function] = "%s" % os.path.basename(input_function)
        inputs[input_dataSet] = "%s" % os.path.basename(input_dataSet)

        #
        arguments = "./%s %s %s" % (inputs[gtree_wrapper_sh], inputs[input_function], inputs[input_dataSet])
        # print 'hello'
        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = ["./results"],
            stdout = 'gtree.log',
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

class GtreeScript(SessionBasedScript):
    """
    The script takes as input a rscript, the number of parallel jobs to run and the
    dataset to be processed

    The ``gtree`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gtree`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GtreeApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GtreeApplication,
            )

    def setup_args(self):
        self.add_param('rscript', help=(
            "Name of the R script to apply to the dataset."
            "A corresponding `.R` file must exist in the current directory."))
        self.add_param('P', type=int, help=("Value for the number of parallel executions"))
        self.add_param('dataset', help=(
            "Name of the dataset to be processed."))
    # def parse_args(self):
    #     """
    #     Check that each element in args is at least a valid folder
    #     """
    #     for folder_name in self.params.args:
    #         if not os.path.isdir(folder_name):
    #             gc3libs.log.error(
    #                 "Invalid input folder: {folder_name}."
    #                 " Removing it from input list"
    #                 .format(folder_name=folder_name))
    #             self.params.args.remove(folder_name)

    def get_function_name_and_file(self, rscript):
        if rscript.endswith('.R'):
            rscript_r = rscript
            rscript = rscript[:-len('.R')]
        else:
            rscript_r = rscript + '.R'
        r_file = join(os.getcwd(), rscript_r)
        if not exists(r_file):
            raise ArgumentError(
                self.actions['rscript'],
                ("Cannot read file '{r_file}'"
                 " providing R-script '{rscript}'.")
                .format(**locals()))
        return rscript, r_file

    def new_tasks(self, extra):
        """
        For each N, create an instance of GtreeApplication
        """
        tasks = []

        input_function, input_file = self.get_function_name_and_file(self.params.rscript)

        # for node in xrange(self.params.N):
        extra_args = extra.copy()
        for node in xrange(self.params.P):
            jobname = "gtree-%s" % node

            extra_args = extra.copy()

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', 'run_%s' % jobname)
            tasks.append(GtreeApplication(input_file, self.params.dataset, **extra_args))
        return tasks
