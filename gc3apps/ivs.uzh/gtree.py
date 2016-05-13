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

    def __init__(self, **extra_args):

        self.output_dir = extra_args['output_dir']
        input_folder = '.'
        self.input_folder = input_folder
        #
        inputs = dict()
        # outputs = dict()
        #
        print Requirement.parse("gc3pie")
        # gtree_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
        #                                   "gc3libs/etc/gtree_wrapper.sh")
        gtree_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gtree_wrapper.sh")
        #
        inputs[gtree_wrapper_sh] = os.path.basename(gtree_wrapper_sh)
        # inputs['foo'] = 'foo'
        #
        inputs[input_folder] = "%s/" % os.path.basename(input_folder)
        # outputs[os.path.join(os.path.basename(input_folder),"results")] = "results/"
        #
        arguments = "./%s %s" % (inputs[gtree_wrapper_sh],inputs[input_folder])
        # print 'hello'
        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = ["./results"],
            stdout = 'gtree.log',
            join=True,
            executables = "./%s" % os.path.basename(input_folder),
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
    The script takes as input a comma separated list of input folders.
    Each input folder contains all the information to run a fitModel with a given
    Dataset and a defined model.
    The input folder will contain a command file that will mimic exactly the invocation
    of fitModel.R for the given input folder as executed on a local computer.
    For each input folder, `gdnd` creates an instance of GtreeApplication.

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

    def new_tasks(self, extra):
        """
        For each N, create an instance of GtreeApplication
        """
        tasks = []

        # for node in xrange(self.params.N):
        for node in xrange(1):
            jobname = "gtree-%s" % node

            extra_args = extra.copy()

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', 'run_%s' % jobname)
            tasks.append(GtreeApplication(**extra_args))
        # for input_folder in self.params.args:
        #
        #     # extract root folder name to be used as jobname
        #     jobname = os.path.basename(input_folder)
        #
        #     extra_args = extra.copy()
        #
        #     extra_args['output_dir'] = self.params.output
        #     extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', 'run_%s' % jobname)
        #     extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', 'run_%s' % jobname)
        #     extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', 'run_%s' % jobname)
        #     extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', 'run_%s' % jobname)
        #
        #     tasks.append(GtreeApplication(
        #         os.path.abspath(input_folder),
        #         **extra_args))

        return tasks
