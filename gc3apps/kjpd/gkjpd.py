#! /usr/bin/env python
#
#   gkjpd.py -- Front-end script for running ParRecoveryFun Matlab
#   function with a given combination of reference models.
#
#   Copyright (C) 2014, 2015  University of Zurich. All rights reserved.
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

See the output of ``gkjpd.py --help`` for program usage
instructions.

Input parameters consists of:
@param_folder
@data_folder
...

Options:
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2014-12-12:
  * Added 'after_main_loop' to bring all results into 'store_results'
  2014-11-13:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gkjpd
    gkjpd.GkjpdScript().run()

import os
import sys
import re
import shutil

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

## custom application class
class GkjpdApplication(Application):
    """
    Custom class to wrap the execution of the Matlab script.
    """
    application_name = 'gkjpd'

    def __init__(self, subject, input_data_folder, **extra_args):

        self.output_dir = extra_args['output_dir']
        self.result_dir = extra_args['result_dir']

        inputs = dict()
        outputs = dict()
        executables = []


        # execution wrapper needs to be added anyway
        gkjpd_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gkjpd.sh")
        inputs[gkjpd_wrapper_sh] = os.path.basename(gkjpd_wrapper_sh)
        inputs[input_data_folder] = './input'

        _command = "./%s ./input %s " % (os.path.basename(gkjpd_wrapper_sh),
                                    subject)

        # arguments = "matlab -nodesktop -nosplash -nodisplay -nodesktop "\
        #             "-r \"addpath(\'/home/gc3-user/spm12\'); addpath(\'./input\'); preprocessing_s3it(\'./input\',\'%s\'); quit\""\
        #             % subject

        # Set output

        Application.__init__(
            self,
            arguments = _command,
            inputs = inputs,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'gkjpd.log',
            join=True,
            executables = "./%s" % os.path.basename(gkjpd_wrapper_sh),
            **extra_args)

class GkjpdScript(SessionBasedScript):
    """
    For each param file (with '.mat' extension) found in the 'param folder',
    GkjpdScript extracts the corresponding index (from filename) and searches for
    the associated file in 'data folder'. For each pair ('param_file','data_file'),
    GkjpdScript generates execution Tasks.

    The ``gkjpd`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gkjpd`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GkjpdApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GkjpdApplication,
            )

    def setup_args(self):

        self.add_param('subjects_file', type=str,
                       help="Path to the files containing a list of subjects. ")

        self.add_param('input', type=str,
                       help="Path to the data files.")

    def parse_args(self):
        """
        Check validity of input parameters and selected benchmark.
        """

        if not os.path.isdir(self.params.input):
            raise OSError("No such file or directory: %s ",
                          os.path.abspath(self.params.input))

        if not os.path.isfile(self.params.subjects_file):
            raise OSError("No such file or directory: %s ",
                          os.path.abspath(self.params.subjects_file))

        # Read subjects_file and create initial list
        with open(self.params.subjects_file) as fin:
            self.params.subjects = (fin.read().strip()).split(',')

    def new_tasks(self, extra):
        """
        For each of the network data and for each of the selected benchmarks,
        create a GkjpdApplication.

        First loop the input files, then loop the selected benchmarks
        """
        tasks = []


        for subject in self.params.subjects:
            jobname = "KJPD-%s" % (subject)

            extra_args = extra.copy()
            extra_args['jobname'] = jobname

            extra_args['result_dir'] = self.params.output
            extra_args['result_dir'] = extra_args['result_dir'].replace('NAME', self.params.session)

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', '%s' % (jobname))

            gc3libs.log.info("Creating GkjpdApplication for subject: %s",
                             subject)
            tasks.append(GkjpdApplication(
                subject,
                os.path.abspath(self.params.input),
                **extra_args))

        return tasks
