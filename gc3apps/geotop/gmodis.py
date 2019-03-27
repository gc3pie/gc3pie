#! /usr/bin/env python
#
#   gmodis.py -- Front-end script for submitting multiple `MODIS` Matlab-based jobs.
#
#   Copyright (C) 2011, 2012  University of Zurich. All rights reserved.
#
#   This program is free software: you can redistribute it and/or modify
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
Front-end script for submitting multiple Matlab function jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gmodis.py --help`` for program usage instructions.

Input parameters consists of:
:param str input_dir: Path to the folder containing all the input data to be
                      processed.
       example: FSC10A1.A2000083_aT_fsc_stitch.mat

Option paramenters consist of:
:param str fsc_dir: Path to the folder containing all the FSC input files necessary
                    to process each of the input data.
                    Note: on a cloud based solution, the FSC input folder
                          will be re-deployed on the reference appliances.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2013-12-30:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gmodis
    gmodis.GmodisScript().run()

import os
import sys
import time
import tempfile

import shutil

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask


## custom application class
class GmodisApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'gmodis'

    def __init__(self, input_file, **extra_args):

        # setup output references
        self.output_dir = extra_args['output_dir']

        # setup input references

        inputs = dict()

        gmodis_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gmodis_wrapper.sh")
        inputs[gmodis_wrapper_sh] = os.path.basename(gmodis_wrapper_sh)

        _command = []

        _command.append("./%s" % os.path.basename(gmodis_wrapper_sh))

        # Add denug info
        _command.append("-d")

        if extra_args.has_key('fsc_dir'):
            inputs.update(dict((os.path.join(extra_args['fsc_dir'],v),
                            os.path.join(
                                os.path.basename(extra_args['fsc_dir']),
                                         v))
                           for v in os.listdir(extra_args['fsc_dir'])))




            _command.append("-f ./%s " % os.path.basename(extra_args['fsc_dir']))

        if extra_args.has_key('gmodis_funct'):
            # e.g. ('/home/data/matlab/gmodis','~/bin/gmodis')
            inputs[extra_args['gmodis_funct']] = os.path.basename(extra_args['gmodis_funct'])

            _command.append("-x ./%s " % os.path.basename(extra_args['gmodis_funct']))

        if extra_args.has_key('matlab_driver'):
            inputs[extra_args['matlab_driver']] = os.path.basename(extra_args['matlab_driver'])

            _command.append("-s ./%s " % os.path.basename(extra_args['matlab_driver']))

        inputs[input_file] = os.path.basename(input_file)
        _command.append(os.path.basename(input_file))

        outputs =  gc3libs.ANY_OUTPUT

        # Add memory requirement
        extra_args['requested_memory'] = 16*GB

        Application.__init__(
            self,
            arguments = _command,
            executables = "./%s" % os.path.basename(gmodis_wrapper_sh),
            inputs = inputs,
            outputs = outputs,
            stdout = 'gmodis.log',
            join=True,
            **extra_args)

class GmodisTask(RetryableTask):
    def __init__(self, input_file, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GmodisApplication(
                input_file,
                **extra_args),
            **extra_args
            )

    def retry(self):
        """
        Task will be retried iif the application crashed
        due to an error within the exeuction environment
        (e.g. VM crash or LRMS kill)
        """
        # XXX: check whether it is possible to distinguish
        # between the error conditions and set meaningfull exitcode
        return False


class GmodisScript(SessionBasedScript):
    """
Scan the specified INPUT directories recursively for simulation
directories and submit a job for each one found; job progress is
monitored and, when a job is done, its output files are retrieved back
into the simulation directory itself.

The ``gmodis`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``gmodis`` will delay submission of
newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GmodisApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GmodisApplication,
            )

    def setup_options(self):
        self.add_param("-f", "--fsc", metavar="PATH",
                       dest="fsc_dir", default=None,
                       help="Path to the FSC data folder.")

        self.add_param("-x", "--executable", metavar="PATH",
                       dest="gmodis_funct", default=None,
                       help="gmodis binary function.")

        self.add_param("-D", "--driver", metavar="PATH",
                       dest="matlab_driver", default=None,
                       help="Path to alternative Matlab driver "+
                       "script.")

    def setup_args(self):

        self.add_param('input_dir', type=str,
                       help="Path to input folder.")

    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """

        # check args:
        # XXX: make them position independent
        if not os.path.isdir(self.params.input_dir):
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid path to input folder: '%s'. Path not found"
                % self.params.input_dir)

        self.log.info("Input dir: %s" % self.params.input_dir)

        if self.params.fsc_dir:
            if not os.path.isdir(self.params.fsc_dir):
                raise gc3libs.exceptions.InvalidUsage(
                    "Input FSC folder '%s' does not exists"
                    % self.params.input_dir)
            self.log.info("Input data dir: [%s]" % self.params.fsc_dir)
        else:
            self.log.info("Input data dir: [use remote]")

        if self.params.gmodis_funct:
            if not os.path.isfile(self.params.gmodis_funct):
                raise gc3libs.exceptions.InvalidUsage(
                    "gmodis binary '%s' does not exists"
                    % self.params.gmodis_funct)
            self.log.info("gmodis binary: [%s]" % self.params.gmodis_funct)
        else:
            self.log.info("gmodis binary: [use remote]")


    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new GcgpsTask
        """


        tasks = []

        for input_file in os.listdir(os.path.abspath(self.params.input_dir)):
            try:
                # Take only .mat files
                if input_file.endswith('.mat'):

                    # Use first sequence in input file name as jobname
                    # e.g. FSC10A1.A2000083_aT_fsc_stitch.mat will get
                    # gmodis-FSC10A1.A2000083
                    jobname = "gmodis-%s" % input_file[:16]

                    extra_args = extra.copy()
                    extra_args['jobname'] = jobname
                    # FIXME: ignore SessionBasedScript feature of customizing
                    # output folder
                    extra_args['output_dir'] = self.params.output

                    extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
                    extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
                    extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
                    extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

                    self.log.info("Creating Task for input file: %s" % input_file)

                    if self.params.matlab_driver:
                        extra_args['matlab_driver'] = self.params.matlab_driver

                    if self.params.fsc_dir:
                        extra_args['fsc_dir'] = self.params.fsc_dir

                    if self.params.gmodis_funct:
                        extra_args['gmodis_funct'] = self.params.gmodis_funct

                    tasks.append(GmodisTask(
                        os.path.join(os.path.abspath(self.params.input_dir),input_file),
                        **extra_args))

            except Exception, ex:
                self.log.error("Unexpected error. Error type: %s, Message: %s" % (type(ex),str(ex)))

        return tasks
