#! /usr/bin/env python
#
#   gcrashdetect.py -- Front-end script for evaluating Matlab functions
#   function over a large number of parameters.
#
#   Copyright (C) 2017, 2018  University of Zurich. All rights reserved.
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
Front-end script for submitting multiple Matlab jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gcrashdetect.py --help`` for program usage
instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2017-06-22:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'
__version__ = '1.0'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gcrashdetect
    gcrashdetect.GcrashdetectScript().run()

import os
import sys
import time
import tempfile

import tarfile
import shutil
import pandas

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file, existing_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

DEFAULT_ITERATIONS=1000
DEFAULT_CHUNKS=1
DEFAULT_REMOTE_OUTPUT_FOLDER="results"
MATLAB_CMD="matlab -nosplash -nodisplay -nodesktop -r \'{main_function}({iterations} {mask} {fn} {output});quit\'"
GCRASHDETECT_VALID_INPUT_FILE_EXTENSIONS = ['.m','.cpp','.mat']
GCRASHDETECT_INPUT_ARCHIVE = "gcrashdetect.tgz"

# utility methods
def _scan_and_tar(tarfile_folder, input_folder):
    """
    Create a tar file from all GCRASHDETECT_VALID_INPUT_FILE_EXTENSIONS
    found in input_folder
    """
    try:
        gc3libs.log.debug("Compressing input folder '%s'" % input_folder)
        cwd = os.getcwd()
        os.chdir(input_folder)

        tar = tarfile.open(os.path.join(tarfile_folder,
                                        GCRASHDETECT_INPUT_ARCHIVE),
                           "w:gz",
                           dereference=True)

        for f in [ elem for elem in os.listdir('.') if os.path.splitext(elem)[-1] in GCRASHDETECT_VALID_INPUT_FILE_EXTENSIONS or os.path.isdir(elem)]:
            tar.add(f)
        tar.close()
        os.chdir(cwd)
        return os.path.join(tarfile_folder,GCRASHDETECT_INPUT_ARCHIVE)
    except Exception as x:
        gc3libs.log.error("Failed creating input archive '%s': %s: %s",
                          os.path.join(tarfile_folder, GCRASHDETECT_INPUT_ARCHIVE),
                          x.__class__,x.message)
        raise

## custom application class
class GcrashdetectApplication(Application):
    """
    Execute Matlab main function passing the arguments extracted
    from the main input .csv file.
    """
    application_name = 'gcrashdetect'

    def __init__(self, matlab_function, parameter_list, tarfile=None, **extra_args):

        inputs = dict()
        outputs = dict()

        # execution wrapper needs to be added anyway
        wrapper = resource_filename(Requirement.parse("gc3pie"),
                                    "gc3libs/etc/gcrashdetect_wrapper.sh")

        inputs[wrapper] = "./wrapper.sh"

        arguments = "./wrapper.sh %s " % matlab_function

        for param in parameter_list:
            arguments += " %s " % param

        if tarfile:
            inputs[tarfile] = os.path.basename(tarfile)
            arguments += "-s %s " % inputs[tarfile]

        arguments += DEFAULT_REMOTE_OUTPUT_FOLDER

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = [DEFAULT_REMOTE_OUTPUT_FOLDER],
            stdout = 'gcrashdetect.log',
            join=True,
            executables = "./wrapper.sh",
            **extra_args)


class GcrashdetectScript(SessionBasedScript):
    """
    Parse the input .csv file. For each line in the input .csv file
    create a different Matlab execution.

    The ``gcrashdetect`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gcrashdetect`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gcrashdetect``
    aggregates them into a single larger output file located in
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            application = GcrashdetectApplication,
            stats_only_for = GcrashdetectApplication,
            )

    def setup_options(self):
        self.add_param("-d", "--mfuncs", metavar="PATH", type=str,
                       dest="matlab_source_folder", default=None,
                       help="Location of the Matlab scripts and "
                       "related Matlab functions. Default: %(default)s.")

    def setup_args(self):

        self.add_param('matlab_function', type=str,
                       help="Matlab function name")

        self.add_param('csv_input_file', type=existing_file,
                       help="Input .csv file with all parameters to be passed"
                       " to the Matlab function.")

    def parse_args(self):
        """
        Check if in source_folder there is a Matlab file named after
        the Matlab function passed as input argument.
        """
        assert os.path.isfile(os.path.join(self.params.matlab_source_folder,
                                           self.params.matlab_function+'.m')), \
                                           "Matlab function file '%s' not found." % self.params.matlab_function

    def new_tasks(self, extra):
        """
        For each line of the input .csv file generate
        an execution Task
        """
        tasks = []

        tarfile = None
        if self.params.matlab_source_folder:
            tarfile = _scan_and_tar(self.session.path, self.params.matlab_source_folder)


        for parameter in self._enumerate_csv(self.params.csv_input_file):
            parameter_str = '.'.join(str(x) for x in parameter)
            jobname = "gcrashdetect-%s" % parameter_str

            extra_args = extra.copy()

            extra_args['jobname'] = jobname

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

            extra_args['matlab_source_folder'] = self.params.matlab_source_folder
            self.log.debug("Creating Application for parameter : %s" %
                           (parameter_str))

            tasks.append(GcrashdetectApplication(
                self.params.matlab_function,
                parameter,
                tarfile,
                **extra_args))

        return tasks

    def _enumerate_csv(self, input_csv):
        """
        For each line of the input .csv file
        return list of parameters
        """
        parameters = pandas.read_csv(input_csv)
        for i,p in enumerate(parameters.values):
            yield p.tolist()
