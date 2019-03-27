#! /usr/bin/env python
#
#   greduction.py -- Front-end script for evaluating 'explain_matrix.py'
#   function over a large dataset.
#
#   Copyright (C) 2011, 2012  University of Zurich. All rights reserved.
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
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2015-02-06:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import greduction
    greduction.GreductionScript().run()

import os
import sys
import time
import tempfile

import shutil
import json

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

## custom application class
class GreductionApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'greduction'

    def __init__(self, language_file, **extra_args):

        inputs = dict()
        outputs = dict()

        inputs[language_file] = "./input.json"
        outputs['results/'] = os.path.join(extra_args['output_dir'],'results/')

        arguments = "python explain_matrix.py input.json"

        # check the optional inputs

        if extra_args.has_key('explain_matrix'):
            inputs[extra_args['explain_matrix']] = "./explain_matrix.py"

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'greduction.log',
            join=True,
            **extra_args)

class GreductionScript(SessionBasedScript):
    """
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GreductionApplication,
            stats_only_for = GreductionApplication,
            )

    def setup_options(self):
        self.add_param("-k", "--chunk", metavar="[NUM]", #type=executable_file,
                       dest="chunk_size", default="1000",
                       help="How to split the edges input data set.")

        self.add_param("-M", "--master", metavar="[PATH]",
                       dest="explain_matrix", default=None,
                       help="Location of 'explain_matrix.py' file.")

    def setup_args(self):

        self.add_param('language_file', type=str,
                       help="Input language file (in JSON format).")

    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """

        # check args:
        # XXX: make them position independent
        if not os.path.isfile(self.params.language_file):
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid input language file: '%s'. File not found"
                % self.params.language_file)

        # Verify that 'self.params.chunk_size' is int
        int(self.params.chunk_size)
        self.params.chunk_size = int(self.params.chunk_size)

        if self.params.explain_matrix:
            assert os.path.isfile(self.params.explain_matrix)

    def new_tasks(self, extra):
        """
        """
        tasks = []

        for (input_file, index_chunk) in self._generate_chunked_files_and_list(self.params.language_file,
                                                                               self.params.chunk_size):


            jobname = "greduction-%s" % (str(index_chunk))

            extra_args = extra.copy()

            extra_args['index_chunk'] = str(index_chunk)

            extra_args['jobname'] = jobname

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


            if self.params.explain_matrix:
                extra_args['explain_matrix'] = self.params.explain_matrix

            self.log.debug("Creating Task for index : %d - %d" %
                           (index_chunk, (index_chunk + self.params.chunk_size)))

            tasks.append(GreductionApplication(
                    input_file,
                    **extra_args))

        return tasks

    def _generate_chunked_files_and_list(self, file_to_chunk, chunk_size):
        """
        Takes a file_name as input and a defined chunk size
        ( uses 1000 as default )
        returns a list of filenames 1 for each chunk created and a corresponding
        index reference
        """

        chunk = []
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

        with open(file_to_chunk) as f:
            data = json.load(f)

        for index in range(0,len(data),chunk_size):
            (handle, tmp_filename) = tempfile.mkstemp(dir=chunk_files_dir,
                                                      prefix='greduction-',
                                                      suffix="%d.json" % index)
            with open(tmp_filename,'w') as fout:
                json.dump(dict(data.items()[index:index+chunk_size-1]),fout)
            chunk.append((tmp_filename,index))
        return chunk
