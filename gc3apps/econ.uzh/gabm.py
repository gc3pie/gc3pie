#! /usr/bin/env python
#
#   gabm.py -- Front-end script for running sheepriver simulation
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

See the output of ``gabm.py --help`` for program usage
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
    import gabm
    gabm.GabmScript().run()

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

DEFAULT_REMOTE_BIN="ABM"

## custom application class
class GabmApplication(Application):
    """
    """
    application_name = 'gabm'

    def __init__(self, hunting, **extra_args):
        """
        Remote execution: ABM [value]
        """

        inputs = dict()
        outputs = dict()
        executables = list()

        if 'binary' in extra_args:
            remote_bin = os.path.basename(extra_args['binary'])
            arguments = "./%s " % remote_bin
            inputs[extra_args['binary']] = remote_bin
            executables.append(remote_bin)
        else:
            arguments = DEFAULT_REMOTE_BIN

        arguments += " %d " % hunting

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'gabm.log',
            join=True,
            executables = executables,
            **extra_args)

class GabmScript(SessionBasedScript):
    """

    The ``gabm`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gabm`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GabmApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GabmApplication,
            )

    def setup_args(self):

        self.add_param('range', type=str,
                       help="hunting pressures range. "
                       "Format: [int],[int]|[int]:[int]. E.g 1:432|3|6,9")

    def setup_options(self):
        self.add_param("-B", "--binary", metavar="STRING", type=str,
                       dest="binary",
                       default=None,
                       help="Location of statically linked binary to run")

    def parse_args(self):
        try:
            if self.params.binary:
                assert os.path.isfile(self.params.binary), \
                    "Input binary file %s not found" % self.params.sources

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
                            start,end = [ int(mrange) for mrange in \
                                          self.params.range.split(":") \
                                          if isinstance(int(mrange),int) ]
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
                        self.input_range = [ int(mrange) for mrange in \
                                             self.params.range.split(",") \
                                             if isinstance(int(self.params.range),int) ]
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
        For each input folder, create an instance of GabmApplication
        """
        tasks = []

        for hunting in self.input_range:

            # filename example: 0103645_anat.nii.gz
            # extract root folder name to be used as jobname
            extra_args = extra.copy()
            extra_args['jobname'] = 'abm-%s' % str(hunting)

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                        'run_%s' % hunting)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                        'run_%s' % hunting)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                        'run_%s' % hunting)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                        'run_%s' % hunting)

            if self.params.binary:
                extra_args['binary'] = os.path.abspath(self.params.binary)

            tasks.append(GabmApplication(
                hunting,
                **extra_args))

        return tasks
