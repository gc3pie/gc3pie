#! /usr/bin/env python
#
#   gsPhenotypicalHomologyExample.py -- Front-end script for running sheepriver simulation
#   written in java.
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

It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gsPhenotypicalHomologyExample.py --help`` for program usage
instructions.

Input argument consists of:
- hunting range: integer range (e.g. 1:430)
...

Options:
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2016-03-29:
  * Initial version
  2016-05-06:
  * TODO: allow 0 as hunting value
  * TODO: check consistency of seed file
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gsPhenotypicalHomologyExample
    gsPhenotypicalHomologyExample.GsPhenotypicalHomologyExampleScript().run()

import os
import sys
import time
import tempfile
import re

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

DEFAULT_ITERATIONS=10000
DEFAULT_OUTPUT_FOLDER="results"
DEFAULT_OUTPUT_ARCHIVE="results.tgz"

## custom application class
class GsPhenotypicalHomologyExampleApplication(Application):
    """
    """
    application_name = 'gsphenotypicalhomologyexample'

    def __init__(self, hunting, **extra_args):
        """
        Remote execution: ./sheepriver_wrapper.sh 77
                          -p param_SheepRiver_wear
                          -m listOfRandomSeeds
                          -i 10
                          -j src/StartSimulation.jar
        """

        inputs = dict()
        outputs = dict()

        wrapper = resource_filename(Requirement.parse("gc3pie"),
                                    "gc3libs/etc/sheepriver_wrapper.sh")
        inputs[wrapper] = "./wrapper.sh"

        arguments = "./wrapper.sh %d " % hunting

        if 'sources' in extra_args:
            sources = os.path.basename(extra_args['sources'])
            inputs[extra_args['sources']] = sources
            arguments += " -s %s " % sources

        if 'sheepriver' in extra_args:
            sheepriver = os.path.basename(extra_args['sheepriver'])
            inputs[extra_args['sheepriver']] = sheepriver
            arguments += " -p %s " % sheepriver

        if 'seeds' in extra_args:
            seeds = os.path.basename(extra_args['seeds'])
            inputs[extra_args['seeds']] = seeds
            arguments += " -m %s " % seeds

        if 'jar' in extra_args:
            jar = os.path.basename(extra_args['jar'])
            inputs[extra_args['jar']] = jar
            arguments += " -j %s " % jar

        if 'iterations' in extra_args:
            arguments += " -i %d " % extra_args['iterations']

        # set output folder
        arguments += " -o %s " % DEFAULT_OUTPUT_ARCHIVE

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = [DEFAULT_OUTPUT_ARCHIVE],
            stdout = 'gsPhenotypicalHomologyExample.log',
            join=True,
            executables = ["./wrapper.sh"],
            **extra_args)

class GsPhenotypicalHomologyExampleScript(SessionBasedScript):
    """

    The ``gsPhenotypicalHomologyExample`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gsPhenotypicalHomologyExample`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GsPhenotypicalHomologyExampleApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GsPhenotypicalHomologyExampleApplication,
            )

    def setup_args(self):

        self.add_param('range', type=str,
                       help="hunting pressures range. "
                       "Format: [int],[int]|[int]:[int]. E.g 1:432|3|6,9")

    def setup_options(self):
        self.add_param("-S", "--source", metavar="STRING", type=str,
                       dest="sources",
                       default=None,
                       help="Location of java scripts to drive the "
                       " execution of sheepriver. Default: %(default)s")

        self.add_param("-j", "--jar", metavar="STRING", type=str,
                       dest="jar",
                       default=None,
                       help="Location of .jar package to drive the "
                       " execution of sheepriver. Default: %(default)s")

        self.add_param("-P", "--param_SheepRiver_wear", metavar="PATH", type=str,
                       dest="param_SheepRiver_wear",
                       default=None,
                       help="Location of the 'param_SheepRiver_wear' input file. "
                       "Default: %(default)s")

        self.add_param("-M", "--seeds", metavar="PATH", type=str,
                       dest="seeds",
                       default=None,
                       help="Location of the seeds file. "
                       "Default: %(default)s")

        self.add_param("-I", "--replications", metavar="INT", type=int,
                       dest="iterations",
                       default=DEFAULT_ITERATIONS,
                       help="Number of repeating iterations. "
                       "Default: %(default)s")

    def parse_args(self):
        try:
            if self.params.sources:
                assert os.path.isdir(self.params.sources), \
                    "Simulation source folder %s not found" % self.params.sources


            if self.params.param_SheepRiver_wear:
                assert os.path.isfile(self.params.param_SheepRiver_wear), \
                    "SheepRiver file %s not found" % self.params.param_SheepRiver_wear

            if self.params.seeds:
                assert os.path.isfile(self.params.seeds), \
                    "Seeds file %s not found" % self.params.seeds

            if self.params.jar:
                assert os.path.isfile(self.params.jar), \
                    "Jar file %s not found" % self.params.jar


            assert isinstance(int(self.params.iterations),int), \
                "Iterations should be a positive integer"

            # Validate month range
            try:
                # Check whether only single value has been passed
                try:
                    assert isinstance(int(self.params.range),int)
                    self.input_range = [int(self.params.range)]
                except ValueError as ex:
                    # Identify the separator
                    if len(self.params.range.split(":")) == 2:
                        # Use ':' as separator
                        try:
                            start,end = [ int(mrange) for mrange in self.params.range.split(":") ]
                            if end <= start:
                                raise ValueError("No valid input range. "
                                                 "Format: [int],[int]|[int]:[int]. E.g 1:432|3")
                            self.input_range = range(start,end+1)
                        except (TypeError, ValueError) as ex:
                            gc3libs.log.critical(ex.message)
                            raise ValueError("No valid input range. "
                                             "Format: [int],[int]|[int]:[int]. E.g 1:432|3")
                    elif len(self.params.range.split(",")) > 0:
                        # Use ',' as separator
                        self.input_range = [ int(mrange) for mrange in self.params.range.split(",") ]
                    else:
                        # Anything else should fail
                        raise ValueError("No valid input range. "
                                         "Format: [int],[int]|[int]:[int]. E.g 1:432|3")

            except ValueError as ex:
                gc3libs.log.debug(ex.message)
                raise AttributeError("No valid input range. "
                                     "Format: [int],[int]|[int]:[int]. E.g 1:432|3")

        except AssertionError as ex:
            raise OSError(ex.message)

    def new_tasks(self, extra):
        """
        For each input folder, create an instance of GsPhenotypicalHomologyExampleApplication
        """
        tasks = []

        for hunting in self.input_range:

            # filename example: 0103645_anat.nii.gz
            # extract root folder name to be used as jobname
            extra_args = extra.copy()
            extra_args['jobname'] = 'sheepriver-%s' % str(hunting)

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                        'run_%s' % hunting)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                        'run_%s' % hunting)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                        'run_%s' % hunting)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                        'run_%s' % hunting)

            if self.params.sources:
                extra_args['sources'] = os.path.abspath(self.params.sources)

            if self.params.param_SheepRiver_wear:
                extra_args['sheepriver'] = os.path.abspath(self.params.param_SheepRiver_wear)

            if self.params.seeds:
                extra_args['seeds'] = os.path.abspath(self.params.seeds)

            if self.params.jar:
                extra_args['jar'] = os.path.abspath(self.params.jar)

            extra_args['iterations'] = self.params.iterations

            tasks.append(GsPhenotypicalHomologyExampleApplication(
                hunting,
                **extra_args))

        return tasks
