#! /usr/bin/env python
#
#   gscr.py -- Front-end script for running ParRecoveryFun Matlab
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

See the output of ``gscr.py --help`` for program usage
instructions.

Input parameters consists of:
@param_folder
@data_folder
@sound_index
...

Options:
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2015-05-04:
  * Added -D and -L option
  -L [INT], --linkrange [INT]
                        Linkrange value. Default: 1.
  -D [INT], --displacement [INT]
                        Displacement value. Default: 10.

  2014-11-13:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gvideotrack
    gvideotrack.GVideoTrackingScript().run()

import os
import sys
import time
import tempfile
import re

import shutil
import random

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

TRAJECTORY_FILE_EXTENSION = ".ijout.txt"

## custom application class
class GVideoTrackingApplication(Application):
    """
    Custom class to wrap the execution of the Matlab script.
    """
    application_name = 'video_track'

    def __init__(self, video_file, **extra_args):

        self.output_dir = extra_args['output_dir']
        # self.result_dir = extra_args['result_dir']

        inputs = dict()
        outputs = dict()
        executables = []

        # Check if binary to be executed is provided as part of input arguments
        if 'R_master' in extra_args:
            inputs[os.path.abspath(extra_args["R_master"])] = "ParticleLinker.R"

        if 'jarfile' in extra_args:
            inputs[os.path.abspath(extra_args["jarfile"])] = "ParticleLinker.jar"

        if 'requested_memory' in extra_args:
            memory = int(extra_args['requested_memory'].amount(Memory.MB))
        else:
            gc3libs.log.warning("Requested memory not set. Using default 1MB")
            memory = 1000

        # Rscript --vanilla s3it_articleLinker.R trj_out/data00422.ijout.txt jar/ParticleLinker.jar result
        # arguments = "Rscript --vanilla ParticleLinker.R %s ParticleLinker.jar result %s" % (os.path.basename(video_file),memory)
        arguments = "Rscript --vanilla ParticleLinker.R %s ParticleLinker.jar result %s %s %s" % (
            os.path.basename(video_file),
            extra_args['linkrange'],
            extra_args['displacement'],
            memory
        )

        inputs[video_file] = os.path.basename(video_file)

        # Set output
        outputs['result/'] = 'result/'


        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gstrj.log',
            join=True,
            executables = executables,
            **extra_args)


class GVideoTrackingScript(SessionBasedScript):
    """
    For each param file (with '.mat' extension) found in the 'param folder',
    GscrScript extracts the corresponding index (from filename) and searches for
    the associated file in 'data folder'. For each pair ('param_file','data_file'),
    GscrScript generates execution Tasks.

    The ``gscr`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gscr`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GVideoTrackingApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GVideoTrackingApplication,
            )

    def setup_args(self):

        self.add_param('videos', type=str,
                       help="Path to the video trajectory files files.")

    def setup_options(self):
        self.add_param("-R", "--Rscript", metavar="[STRING]",
                       dest="R_master", default=None,
                       help="Location of the R script that implements the "
                       "'link_particles' function.")

        self.add_param("-j", "--jarfile", type=str, metavar="[STRING]",
                       dest="jarfile", default=None,
                       help="Location of the 'ParticleLinker.jar'.")

        self.add_param("-L", "--linkrange", type=int, metavar="[INT]",
                       dest="linkrange", default=1,
                       help="Linkrange value. Default: 1.")

        self.add_param("-D", "--displacement", type=int, metavar="[INT]",
                       dest="displacement", default=10,
                       help="Displacement value. Default: 10.")

    def parse_args(self):
        """
        Check validity of input parameters and selected benchmark.
        """

        if not os.path.isdir(self.params.videos):
            raise OSError("No such file or directory: %s ",
                          os.path.abspath(self.params.videos))

        if self.params.R_master:
            if not os.path.isfile(self.params.R_master):
                raise gc3libs.exceptions.InvalidUsage("link_particle function "
                                                      " file %s not found"
                                                      % self.params.R_master)

        if self.params.jarfile:
            if not os.path.isfile(self.params.jarfile):
                raise gc3libs.exceptions.InvalidUsage("ParticleLinker jar "
                                                      " file %s not found"
                                                      % self.params.jarfile)


        assert int(self.params.linkrange)
        assert int(self.params.displacement)

    def new_tasks(self, extra):
        """
        For each of the network data and for each of the selected benchmarks,
        create a GscrApplication.

        First loop the input files, then loop the selected benchmarks
        """
        tasks = []

        for video_file in os.listdir(self.params.videos):

            if not video_file.endswith(TRAJECTORY_FILE_EXTENSION):
                gc3libs.log.info("Ingoring input file %s. "
                                 "Not compliant with expected file extension."
                                 % video_file)
                continue

            # Extract filename
            jobname = video_file.split(TRAJECTORY_FILE_EXTENSION)[0].strip()

            extra_args = extra.copy()

            if self.params.R_master:
                extra_args['R_master'] = self.params.R_master

            if self.params.jarfile:
                extra_args['jarfile'] = self.params.jarfile

            extra_args['linkrange'] = self.params.linkrange
            extra_args['displacement'] = self.params.displacement

            extra_args['jobname'] = jobname

            tasks.append(GVideoTrackingApplication(
                os.path.join(self.params.videos,video_file),
                **extra_args))

        return tasks
