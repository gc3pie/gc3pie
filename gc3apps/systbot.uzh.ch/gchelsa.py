#! /usr/bin/env python
#
#   gchelsa.py -- Front-end script for running chelsa application
#   driven by a custom made R script.
#
#   Copyright (C) 2016, 2019  University of Zurich. All rights reserved.
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

See the output of ``gchelsa.py --help`` for program usage
instructions.

Input argument consists of:
- Month range: integer range (e.g. 1:430)
...

Options:
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2016-02-109:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gchelsa
    gchelsa.GchelsaScript().run()

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

DEFAULT_CORES = 1
DEFAULT_MEMORY = Memory(62000,MB)

DEFAULT_REMOTE_TEMP="/tmp"
DEFAULT_REMOTE_INPUT="/data/input"
DEFAULT_REMOTE_OUTPUT="/data/output"
DEFAULT_REMOTE_OUTPUT_FILE="./output.txt"
DEFAULT_CHELSA_SCRIPT="/INPUT/CHELSA_LATLONG_HPC2.R"

## custom application class
class GchelsaApplication(Application):
    """
    """
    application_name = 'gchelsa'

    def __init__(self, month, **extra_args):
        """
        Remote command execution: R --vanilla --args "c(START,END) /tmp/ /mnt/s3it/out/ /mnt/" </mnt/CHELSA_LATLONG_HPC2.R >outfile.tx
        """
        inputs = dict()
        outputs = dict()

        if 'Rscript' in extra_args:
            chelsa_script = os.path.basename(extra_args['Rscript'])
            inputs[extra_args['Rscript']] = chelsa_script
        else:
            chelsa_script = DEFAULT_CHELSA_SCRIPT

        execution_script = """
#!/bin/sh

tempdir=`mktemp -d -p %s`
ulimit -a
# execute command
R --vanilla --args "gTIME=c(%s,%s) $tempdir/ %s %s" <%s>%s
RET=$?

echo Program terminated with exit code $RET
exit $RET
        """ % (extra_args['temp_data'],
               month,
               month,
               extra_args['output_data'],
               extra_args['input_data'],
               chelsa_script,
               DEFAULT_REMOTE_OUTPUT_FILE)

        try:
            # create script file
            (handle, self.tmp_filename) = tempfile.mkstemp(prefix='gchelsa-',
                                                           suffix=extra_args['jobname'])

            # XXX: use NamedTemporaryFile instead with 'delete' = False
            fd = open(self.tmp_filename,'w')
            fd.write(execution_script)
            fd.close()
            os.chmod(fd.name,0o777)
        except Exception, ex:
            gc3libs.log.debug("Error creating execution script" +
                              "Error type: %s." % type(ex) +
                              "Message: %s"  %ex.message)
            raise

        inputs[fd.name] = './gchelsa_wrapper.sh'

        extra_args['requested_memory'] = DEFAULT_MEMORY

        Application.__init__(
            self,
            arguments = ['./gchelsa_wrapper.sh'],
            inputs = inputs,
            outputs = [DEFAULT_REMOTE_OUTPUT_FILE],
            stdout = 'gchelsa.log',
            join=True,
            executables = ['gchelsa_wrapper.sh'],
            **extra_args)

class GchelsaScript(SessionBasedScript):
    """

    The ``gchelsa`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gchelsa`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GchelsaApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GchelsaApplication,
            )

    def setup_args(self):

        self.add_param('range', type=str,
                       help="Months range. "
                       "Format: [int]|[int]:[int]. E.g 1:432|3")

    def setup_options(self):
        self.add_param("-R", "--Rscript", metavar="STRING", type=str,
                       dest="Rscript",
                       default=None,
                       help="Location of master R script to drive the "
                       " execution of chelsa. Default: %(default)s")

        self.add_param("-I", "--input", metavar="PATH", type=str,
                       dest="input_data",
                       default=DEFAULT_REMOTE_INPUT,
                       help="Location of the input data folder. "
                       "Default: %(default)s")

        self.add_param("-O", "--output", metavar="PATH", type=str,
                       dest="output_data",
                       default=DEFAULT_REMOTE_OUTPUT,
                       help="Location of the output data folder. "
                       "Default: %(default)s")

        self.add_param("-T", "--temp", metavar="PATH", type=str,
                       dest="temp_data",
                       default=DEFAULT_REMOTE_TEMP,
                       help="Location of the temp data folder. "
                       "Default: %(default)s")

    def parse_args(self):
        try:
            if self.params.Rscript:
                assert os.path.isfile(self.params.Rscript), \
                    "R script file %s not found" % self.params.Rscript

            # Validate month range
            try:
                self.input_range = [ int(mrange) for mrange in \
                                     self.params.range.split(":") \
                                     if int(mrange) ]

                if len(self.input_range) == 1:
                    # Defined only single month
                    gc3libs.log.info("Defined single month to process: '%d'",
                                     self.input_range[0])
                elif len(self.input_range) == 2:
                    # Defined a range
                    self.input_range = range(self.input_range[0],
                                             self.input_range[1]+1)
                else:
                    # Anything else should fail
                    raise ValueError("No valid input range. "
                                         "Format: [int]|[int]:[int]. E.g 1:432|3")

            except ValueError as ex:
                gc3libs.log.debug(ex.message)
                raise AttributeError("No valid input range. "
                                     "Format: [int]|[int]:[int]. E.g 1:432|3")

        except AssertionError as ex:
            raise OSError(ex.message)

    def new_tasks(self, extra):
        """
        For each input folder, create an instance of GchelsaApplication
        """
        tasks = []

        for month in self.input_range:

            # filename example: 0103645_anat.nii.gz
            # extract root folder name to be used as jobname
            extra_args = extra.copy()
            extra_args['jobname'] = 'chelsa-%s' % str(month)

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                        'run_%s' % month)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                        'run_%s' % month)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                        'run_%s' % month)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                        'run_%s' % month)

            if self.params.Rscript:
                extra_args['Rscript'] = os.path.abspath(self.params.Rscript)

            extra_args['input_data'] = self.params.input_data
            extra_args['output_data'] = self.params.output_data
            extra_args['temp_data'] = self.params.temp_data

            tasks.append(GchelsaApplication(
                month,
                **extra_args))

        return tasks
