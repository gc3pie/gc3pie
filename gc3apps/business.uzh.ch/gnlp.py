#! /usr/bin/env python
#
#   gnlp.py -- Front-end script for evaluating CoreNLP Sentiments
#   over a large dataset.
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
Front-end script for submitting multiple CoreNLP jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gnlp.py --help`` for program usage
instructions.

Input parameters consists of:
:param str input file: Path to an .xml file containing input data with structure
<ROWSET>
<ROW>
<FIELD1>
<PostId>
<ThreadID>
<UserID>
<TimeStamp>
<Upvotes>
<Downvotes>
<Flagged>
<Approved>
<Deleted>
<Replies>
<ReplyTo>
<Content>
</ROW>
<ROW>
...
</ROWSET>
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2014-09-05:
  * Added parsing of output file
  * Merging all results into single XML
  * Added <Sentiment> tag for results
  2014-08-14:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gnlp
    gnlp.GnlpScript().run()

import os
import sys
import time
import tempfile
import re

import shutil
from xml.etree import cElementTree as ET
from xml.etree.ElementTree import Element, SubElement, Comment, tostring

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask


XML_HEADER = """<?xml version="1.0"?>
<ROWSET>
"""
XML_FOOTER = "</ROWSET>"


## custom application class
class GnlpApplication(Application):
    """
    Custom class to wrap the execution of the CoreNLP java script.
    """
    application_name = 'corenlp'

    def __init__(self, input_data, **extra_args):

        # setup input references
        inputs = dict()

        inputs[input_data] = "./input.txt"

        # adding wrapper main script
        gnlp_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gnlp_wrapper.py")

        inputs[gnlp_wrapper_sh] = "./wrapper.py"

        arguments = "./wrapper.py ./input.txt ./output.txt"

        outputs = dict()
        outputs['./output.txt'] = extra_args['output_file']

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gnlp.log',
            stderr = 'gnlp.err',
            executables = "./wrapper.py",
            **extra_args)


class GnlpScript(SessionBasedScript):
    """
    Splits input .xml file into smaller chunks, each of them of size
    'self.params.chunk_size'.
    Then it submits one execution for each of the created chunked files.

    The ``gnlp`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gnlp`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gnlp``
    aggregates them into a single larger output file located in
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GnlpApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GnlpApplication,
            )

    def setup_options(self):
        self.add_param("-k", "--chunk", metavar="[NUM]",
                       dest="chunk_size", default="1000",
                       help="How to split the .XML input data set. "
                       "Default: 1000")

        self.add_param("--result-file", metavar="[STRING]",
                       dest="result_file", default='result.xml',
                       help="Name of the result file generated as the aggregation"
                       " of all results from each chunked execution. "
                       "Default: result.xml")

    def setup_args(self):

        self.add_param('input_data', type=str,
                       help="Input data full path name.")

    def parse_args(self):
        """
        Check presence of input file.
        """

        # check args:
        if not os.path.isfile(self.params.input_data):
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid path to input data: '%s'. File not found"
                % self.params.input_data)

        # Verify that 'self.params.chunk_size' is int
        int(self.params.chunk_size)
        self.params.chunk_size = int(self.params.chunk_size)

    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new GcgpsTask
        """
        tasks = []
        last_index = 0

        for (input_file, index_chunk) in self._generate_chunked_files_and_list(self.params.input_data,
                                                                              self.params.chunk_size):

            jobname = "gnlp-%d-%d" % (last_index,index_chunk)

            extra_args = extra.copy()

            extra_args['index_chunk'] = str(index_chunk)

            extra_args['jobname'] = jobname

            # extra_args['output_file'] = 'result.xml'
            extra_args['output_file'] = self.params.result_file

            extra_args['output_dir'] = os.path.abspath(self.params.output)
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

            self.log.debug("Creating Application for index : %d - %d" %
                           (last_index,index_chunk))

            tasks.append(GnlpApplication(
                    input_file,
                    **extra_args))

            last_index = index_chunk


        return tasks

    def after_main_loop(self):
        """
        Merge all result files together
        Then clean up tmp files.
        Format of output file:
        FiledID@<Sentiments>..</Sentiments>
        e.g.
        296@<Sentiments>Neutral</Sentiments>
        This means that the <Sentiments> tag should be added to the XML element
        corresponding to <Filed1>296</Field1>
        """

        # Open Session result file
        try:
            with open(self.params.result_file,'w+') as fd:
                # Write header
                fd.write(XML_HEADER)

                for task in self.session:
                    if isinstance(task,GnlpApplication) and task.execution.returncode == 0:
                        with open(os.path.join(task.output_dir,task.output_file),'r') as fin:
                            for line in fin:
                                if line.strip() in ['<?xml version="1.0"?>','<ROWSET>','</ROWSET>']:
                                    continue
                                fd.write(line)

                fd.write(XML_FOOTER)
        except OSError, osx:
            gc3libs.log.error("Failed while merging results. Error %s", str(osx))

    def _generate_chunked_files_and_list(self, file_to_chunk, chunk_size=1000):
        """
        Takes a file_name as input and a defined chunk size
        ( uses 1000 as default )
        returns a list of filenames 1 for each chunk created and a corresponding
        index reference
        e.g. ('/tmp/chunk2800.xml,2800) for the chunk segment that goes
        from 2800 to (2800 + chunk_size)
        """

        index = 0
        chunk = []
        fout = None
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

            with open(file_to_chunk,'r') as fin:
                for line in fin:
                    # Ignore header and ROWSET
                    if line.strip() in ['<?xml version="1.0"?>','<ROWSET>','']:
                        # Ignore and continue
                        continue

                    if line.strip() == "<row>":
                        if index % chunk_size == 0:
                            if fout:
                                # Close existing file and create a new one
                                fout.write(XML_FOOTER)
                                fout.close()
                                chunk.append((fout.name, index))
                            # Create new chunk file
                            fout = open(os.path.join(chunk_files_dir,"input-%d" % index),"w")
                            fout.write(XML_HEADER)
                        index += 1
                    if not fout:
                        fout = open(os.path.join(chunk_files_dir,"input-%d" % index),"w")
                    fout.write(line)

            # Just close the current fout file if needed
            fout.close()
            chunk.append((fout.name, index))

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
