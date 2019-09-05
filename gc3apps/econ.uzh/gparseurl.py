#! /usr/bin/env python
#
#   gparseurl.py -- Front-end script for submitting multiple `GC-GPS` R-baseed jobs.
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
Front-end script for submitting multiple `R` jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gparseurl.py --help`` for program usage instructions.

Input parameters consists of:
:param str URL_file: Path to the file containing the list of the URL to be processed
   example:
https://archive.org/download/archiveteam-twitter-stream-2014-06/archiveteam-twitter-stream-2014-06.tar

Option:
:param str parser_script: Path to the parser script. Default: None.

"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2015-10-24:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gparseurl
    gparseurl.GParseURLScript().run()

import os
import sys
import time
import tempfile

import shutil

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask


## custom application class
class GParseURLApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'gparseurl'

    def __init__(self, url_to_read, **extra_args):


        inputs = dict()
        outputs = dict()

        if 'master_script' in extra_args:
            inputs[extra_args['master_script']] = "./master.py"

        command = "python ./master.py %s" % url_to_read

        outputs['result.json'] = "%s.json" % extra_args['jobname']

        Application.__init__(
            self,
            arguments = command,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gpaseurl.log',
            join=True,
            **extra_args)


class GParseURLScript(SessionBasedScript):
    """
Read URL_list file containing the list of URLs to be processed.
For each URL - or a chunk of them - create a GParseURLApplication
to run the parser script in a parallel manner. Job progress is
monitored and, when a job is done, its output files are retrieved back
into the simulation directory itself.

The ``gparseurl`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``gparseurl`` will delay submission of
newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GParseURLApplication,
            stats_only_for = GParseURLApplication,
            )

    def setup_options(self):
        self.add_param("-Z", "--master", metavar="PATH",
                       dest="master_script", default=None,
                       help="Path to the parser script. Default: None.")

    def setup_args(self):

        self.add_param('url_file', type=str,
                       help="Command file full path name")

    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to url_file should also be valid.
        """

        if not os.path.exists(self.params.url_file):
            raise gc3libs.exceptions.InvalidUsage(
                "gparseurl command file '%s' does not exist;"
                % self.params.url_file)
        gc3libs.utils.check_file_access(self.params.url_file, os.R_OK,
                                gc3libs.exceptions.InvalidUsage)

        if self.params.master_script and \
        not os.path.exists(self.params.master_script):
            raise gc3libs.exceptions.InvalidUsage(
                "Input folder '%s' does not exists"
                % self.params.master_script)

    def new_tasks(self, extra):
        """
        Read content of 'url_file'
        For each command line, generate a new GcgpsTask
        """

        tasks = []

        for url_to_read in open(self.params.url_file):
            jobname = os.path.splitext(os.path.basename(url_to_read))[0]
            extra_args = extra.copy()
            extra_args['jobname'] = jobname
            extra_args['output_dir'] = self.params.output

            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', os.path.join('.computation',jobname))
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', os.path.join('.computation',jobname))
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', os.path.join('.computation',jobname))
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', os.path.join('.computation',jobname))

            if self.params.master_script:
                extra_args['master_script'] = self.params.master_script

            self.log.debug("Creating Task for %s" % jobname)

            tasks.append(GParseURLApplication(
                url_to_read,
                **extra_args))

        return tasks
