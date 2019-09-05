#! /usr/bin/env python
#
#   gtopology.py -- Front-end script for running topology evaluations in
#   python over different initial parameter conditions.
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
Front-end script for submitting multiple topology jobs written in python.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gtopology.py --help`` for program usage
instructions.

Input parameters consists of:
10,10,2,1,10
10,10,3,1,10
10,10,4,1,10
...

XXX: To be clarified:
* dependency in igraph python library
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2016-05-10:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gtopology
    gtopology.GtopologyScript().run()

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
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

DEFAULT_MASTER_SCRIPT="run_topologies_cluster.py"

## custom application class
class GtopologyApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'gtopology'

    def __init__(self, param_file, source_folder, **extra_args):

        # setup output
        outputs = dict()

        # setup input
        inputs = dict()
        inputs[param_file] = "./input.csv"
        inputs[source_folder] = "./source/"

        if 'master_script' in extra_args:
            master_script = extra_args['master_script']
        else:
            master_script = DEFAULT_MASTER_SCRIPT

        arguments ="pwd ; date ; cd source ; python %s ../input.csv " % master_script

        # prepare execution script from command
        execution_script = """
#!/bin/sh

export PYTHONPATH=./source:$PYTHONPATH
cd source
# execute command
python %s ../input.csv
RET=$?

echo Program terminated with exit code $RET
exit $RET
        """ % (master_script)

        try:
            # create script file
            (handle, self.tmp_filename) = tempfile.mkstemp(prefix='gtopology-', suffix=extra_args['jobname'])

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

        inputs[fd.name] = "./master.sh"

        Application.__init__(
            self,
            arguments = ['./master.sh'],
            inputs = inputs,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'gtopology.log',
            join=True,
            executables = ['./master.sh'],
            **extra_args)

class GtopologyScript(SessionBasedScript):
    """
    Splits input .csv file into smaller chunks, each of them of size
    'self.params.chunk_size'.
    Then it submits one execution for each of the created chunked files.

    The ``gtopology`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gtopology`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gtopology``
    aggregates them into a single larger output file located in
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            application = GtopologyApplication,
            stats_only_for = GtopologyApplication,
            )

    def setup_options(self):
        self.add_param("-k", "--chunk", metavar="[NUM]",
                       dest="chunk_size", default="1000",
                       help="How to split the edges input data set. "
                       "Default: %(default)s")

        self.add_param("-M", "--master", metavar="[PATH]",
                       dest="master_script", default=DEFAULT_MASTER_SCRIPT,
                       help="Name of the main execution script. "
                       "Default: %(default)s")


    def setup_args(self):

        self.add_param('input', type=str,
                       help="Input csv.")

        self.add_param('source', type=str,
                       help="Location of the main script source folder.")


    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """

        try:
            assert os.path.isfile(self.params.input), \
                "Input csv file '%s' not found" % self.params.input

            assert os.path.isdir(self.params.source), \
                "Input source folder '%s' not found" % self.params.source

        except AssertionError as ex:
            raise OSError(ex.message)

        # Verify that 'self.params.chunk_size' is int
        int(self.params.chunk_size)
        self.params.chunk_size = int(self.params.chunk_size)


    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new GcgpsTask
        """
        tasks = []

        for (input_file, index_chunk) in self._generate_chunked_files_and_list(self.params.input,
                                                                              self.params.chunk_size):
            extra_args = extra.copy()

            jobname = "gtopology-%s" % (str(index_chunk))
            extra_args['jobname'] = jobname
            extra_args['index_chunk'] = str(index_chunk)

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                        os.path.join('.computation',
                                                                                     jobname))
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                        os.path.join('.computation',
                                                                                     jobname))
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                        os.path.join('.computation',
                                                                                     jobname))
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                        os.path.join('.computation',
                                                                                     jobname))

            extra_args['source'] = self.params.source
            extra_args['master_script'] = self.params.master_script

            self.log.debug("Creating Task for index : %d - %d" %
                           (index_chunk, (index_chunk + self.params.chunk_size)))

            tasks.append(GtopologyApplication(
                input_file,
                os.path.abspath(self.params.source),
                **extra_args))

        return tasks

    def _generate_chunked_files_and_list(self, file_to_chunk, chunk_size=1000):
        """
        Takes a file_name as input and a defined chunk size
        ( uses 1000 as default )
        returns a list of filenames 1 for each chunk created and a corresponding
        index reference
        e.g. ('/tmp/chunk2800.csv,2800) for the chunk segment that goes
        from 2800 to (2800 + chunk_size)
        """
        index = 0
        chunk = []
        failure = False
        chunk_files_dir = os.path.join(self.session.path,"tmp")

        # creating 'chunk_files_dir'
        if not(os.path.isdir(chunk_files_dir)):
            try:
                os.mkdir(chunk_files_dir)
            except OSError, osx:
                gc3libs.log.error("Failed while creating tmp folder %s. " % chunk_files_dir +
                                  "Error %s." % str(osx) +
                                  "Using default '/tmp'")
                chunk_files_dir = "/tmp"

        try:
            fd = open(file_to_chunk,'rb')

            fout = None

            for (i, line) in enumerate(fd):
                if i % chunk_size == 0:
                    if fout:
                        fout.close()
                    (handle, self.tmp_filename) = tempfile.mkstemp(dir=chunk_files_dir,
                                                                    prefix=
                                                                   'gtopology-',
                                                                   suffix=
                                                                   "%d.csv" % i)
                    fout = open(self.tmp_filename,'w')
                    chunk.append((fout.name,i))
                fout.write(line)
            fout.close()
        except OSError, osx:
            gc3libs.log.critical("Failed while creating chunk files." +
                                 "Error %s", (str(osx)))
            failure = True
        finally:
            if failure:
                # remove all tmp file created
                gc3libs.log.info("Could not generate full chunk list. "
                                 "Removing all existing tmp files... ")
                for (cfile, index) in chunk:
                    try:
                        os.remove(cfile)
                    except OSError, osx:
                        gc3libs.log.error("Failed while removing " +
                                          "tmp file %s. " +
                                          "Message %s" % osx.message)

        return chunk
