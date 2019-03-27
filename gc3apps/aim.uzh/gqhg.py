#! /usr/bin/env python
#
#   gqhg.py -- Front-end script for running the QHGMain binary
#   over a list of intput population files .qdf
#
#   Copyright (C) 2016, 2017  University of Zurich. All rights reserved.
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

See the output of ``gqhg.py --help`` for program usage
instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2016-07-01:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'
__version__ = '1.0.0'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gqhg
    gqhg.GqhgScript().run()

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

DEFAULT_CORES = 1
DEFAULT_MEMORY = Memory(3000,MB)

DEFAULT_REMOTE_INPUT_FOLDER="./"
DEFAULT_REMOTE_OUTPUT_FOLDER="./output"
QDF_PATTERN = "qdf"
DEFAULT_ITERATIONS=10000
DEFAULT_RANDOM_LIMIT=1000000
DEFAULT_STEPS=1000
DEFAULT_EVENTS='write|geo|climate|veg@[1000],write|pop:Sapiens_ooa@[3000]+10000,write|stats@10000'
DEFAULT_QHMAIN_COMMAND="QHGMain --output-prefix=ooa "

## custom application class
class GqhgApplication(Application):
    """
    """
    application_name = 'gqhgi'

    def __init__(self, qdf_file, seed, steps, **extra_args):

        inputs = dict()
        outputs = dict()

        self.seed = seed
        output_logfile = "ooa_%d_08i" % seed
        qdf_filename = os.path.basename(qdf_file)

        inputs[qdf_file] = qdf_filename

        if 'grid' in extra_args:
            grid_filename = os.path.basename(extra_args['grid'])
            inputs[extra_args['grid']] = grid_filename

        for input_file in extra_args['event_files']:
            inputs[input_file]=os.path.basename(input_file)
            extra_args['event'] = extra_args['event'].replace(input_file,os.path.basename(input_file))

        arguments = DEFAULT_QHMAIN_COMMAND
        arguments += " --grid=%s " % grid_filename
        arguments += " --output-dir=%s " % DEFAULT_REMOTE_OUTPUT_FOLDER
        arguments += " --pops=%s " % qdf_filename
        arguments += " --shuffle=%d " %  seed
        arguments += " -n %d " % steps
        arguments += " --events=%s " % extra_args['event']
        arguments += " --log-file=%s.log > %s.out " % (output_logfile, output_logfile)

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = [DEFAULT_REMOTE_OUTPUT_FOLDER, output_logfile+".log",
                       output_logfile+".out"],
            stdout = 'gqhg.log',
            join=True,
            **extra_args)

class GqhgScript(SessionBasedScript):
    """

    The ``gqhg`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gqhg`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GqhgApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GqhgApplication,
            )

    def setup_options(self):

        self.add_param("-G", "--grid", metavar="[PATH]",
                       dest="grid", default=None,
                       help="Location of the Grid file")

        self.add_param("-I", "--iterations",
                       dest="iterations", default=DEFAULT_ITERATIONS,
                       help="Repeat simulation on same .qdf file. "
                       "Default: %(default)s.")

        self.add_param("-n", "--steps",
                       dest="steps", default=DEFAULT_STEPS,
                       help="Number of steps within single simulation "
                       "Default: %(default)s.")

        self.add_param("-M", "--master",
                       dest="master", default=None,
                       help="Use alternative binanry file. "
                       "Note: binary has to be statically compiled")

        self.add_param("-E", "--events",
                       dest="events", default=DEFAULT_EVENTS,
                       help="Use specific `events` directive. " \
                       "assumes: "
                       "-- each event file has suffix .qdf "
                       "-- 'env' type events only (e.g. not 'write')"
                       "-- each event is separated by a comma"
                       "-- everything between : and .qdf is the filename"
                       "Default: %(default)s.")

    def setup_args(self):

        self.add_param('input_data', type=str,
                       help="Root localtion of input data. "
                       "Note: walk through input folder and select "
                       " all files ending with .qdf")

    def parse_args(self):
        """
        Check valid input files in input folder.
        For each valid input file execute reference binary and repear
        `self.params.iterantions` time.
        """

        try:

            assert os.path.isdir(self.params.input_data), \
                "Input folder '%s' not found" % self.params.input_data

            if self.params.grid:
                assert os.path.isfile(self.params.grid), \
                    "Grid file %s not found" % self.params.grid

            self.qdf_files = [ os.path.abspath(os.path.join(self.params.input_data,qdf))
                               for qdf in os.listdir(self.params.input_data)
                               if qdf.endswith(QDF_PATTERN) ]
            assert len(self.qdf_files) > 0, \
                "No QDF files found in input folder %s" % self.params.input_data

        except AssertionError as ex:
            raise ValueError(ex.message)

    def new_tasks(self, extra):
        """
        For each input folder, create an instance of GqhgApplication
        """
        tasks = []

        list_input_qdf = parse_event_string(self.params.events)

        for qdf_file in self.qdf_files:

            for iteration in range(1,int(self.params.iterations)+1):

                seed = random.randint(1,DEFAULT_RANDOM_LIMIT)
                # extract root folder name to be used as jobname
                extra_args = extra.copy()
                jobname = "%s_%d" % (os.path.basename(qdf_file),iteration)
                extra_args['jobname'] = jobname

                extra_args['output_dir'] = self.params.output
                extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

                if self.params.grid:
                    extra_args['grid'] = os.path.abspath(self.params.grid)

                extra_args['event_files'] = list_input_qdf
                extra_args['event'] = self.params.events

                tasks.append(GqhgApplication(
                    qdf_file,
                    seed,
                    int(self.params.steps),
                    **extra_args))

        return tasks

def parse_event_string(eventstring):
    """Return a list of input event files given the command line event string."""
    """assumes: -- each event file has suffix .qdf
                -- 'env' type events only (e.g. not 'write')
                -- each event is separated by a comma
                -- events string begins with '-events='
                -- everything between : and .qdf is the filename
   """
    import string
    filesuffix = ".qdf"
    eventfilelist = []
    events = eventstring.split(",")
    for thisevent in events:
        if "=" in thisevent:   #   (e.g. '-events='.......)
            thisevent = thisevent.split("=")[1]
        first3letters = ""
        iletter = 0
        for thischar in thisevent: #   check 1st 3 letters for event 'tag' of env
            if thischar in string.ascii_letters:
                first3letters += thischar
                iletter += 1
            if iletter == 3:
                break
        if first3letters[:3] != "env":
            continue   # not the event we are looking for

        # filename starts after the ":"
        stringtmp = thisevent.split(":")[1].strip() # strip any space before filename
        stringtmp = stringtmp.split(filesuffix)[0]
        stringtmp += filesuffix
        eventfilelist.append(stringtmp)
    return eventfilelist
