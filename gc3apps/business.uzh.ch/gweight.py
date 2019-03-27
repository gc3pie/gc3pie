#! /usr/bin/env python
#
#   gweight.py -- Front-end script for evaluating R-based 'weight'
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
Front-end script for submitting multiple `R` jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gweight.py --help`` for program usage
instructions.

Input parameters consists of:
:param str edges file: Path to an .csv file containing input data in
the for of:
    X1   X2
1  id1  id2
2  id1  id3
3  id1  id4

...

XXX: To be clarified:
. When input files should be removed ?
. What happen if an error happen at merging time ?
. Should be possible to re-run a subset of the initial chunk list
without re-creating a new session ?
e.g. adding a new argument accepting chunk ranges (-R 3000:7500)
This would trigger the re-run of the whole workflow only
for lines between 3000 and 7500
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2013-07-03:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gweight
    gweight.GWeightScript().run()

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

## custom application class
class GWeightApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'gweight'

    def __init__(self, edges_data_filename, **extra_args):

        # setup output references
        # self.result_dir = result_dir
        self.output_dir = extra_args['output_dir']
        # self.output_filename = os.path.basename(edges_data_filename)
        self.output_filename = 'result-%s.csv' % extra_args['index_chunk']
        self.output_file = os.path.join(self.output_dir,self.output_filename)
        # outputs = [("./result.csv",self.output_filename)]
        outputs = [("./result.csv",self.output_filename)]
        # setup input references
        inputs = dict()

        inputs[edges_data_filename] = "./input.csv"

        arguments ="./wrapper.sh ./input.csv "

        # check the optional inputs

        if extra_args.has_key('weight_function'):
            inputs[extra_args['weight_function']] = "./bin/f_get_weight.r"
            arguments = arguments + "-w ./bin/f_get_weight.r "

        if extra_args.has_key('data'):
            inputs[extra_args['data']] = "./data/two_mode_network.rda"
            arguments += "-d ./data/two_mode_network.rda "

        if extra_args.has_key('driver_script'):
            inputs[extra_args['driver_script']] = "./bin/run.R"
            arguments +=  "-m ./bin/run.R "

        if extra_args.has_key('threads_posted_data'):
            inputs[extra_args['threads_posted_data']] = "./data/threads.nodes.posted.rda"
            arguments +=  "-t ./data/threads.nodes.posted.rda "


        # adding wrapper main script
        gweight_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gweight_wrap.sh")

        inputs[gweight_wrapper_sh] = 'wrapper.sh'

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gweight.log',
            join=True,
            executables = ['wrapper.sh'],
            **extra_args)

    def terminated(self):
        """
        Check whether output file has been properly created
        """
        # XXX: TBD, work on a more precise checking of the output file
        gc3libs.log.info("Application terminated with exit code %s" % self.execution.exitcode)
        if (not os.path.isfile(self.output_file)):
            gc3libs.log.error("Failed while checking outputfile %s." % self.output_file)
            # Retry
            self.execution.returncode = (0, 99)


class GWeightTask(RetryableTask):
    def __init__(self, edges_data_filename, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GWeightApplication(
                edges_data_filename,
                **extra_args),
            **extra_args
            )

    def retry(self):
        """
        Task will be retried iif the application crashed
        due to an error within the exeuction environment
        (e.g. VM crash or LRMS kill)
        """
        # XXX: check whether it is possible to distingish
        # between the error conditions and set meaningfull exitcode
        to_retry = RetryableTask.retry(self)
        gc3libs.log.debug("GWeightTask called with retry [%s]" % str(to_retry))
        return to_retry

        # if self.task.execution.exitcode != 0:
        #     # Candidate for retry
        #     # Let's check how many times it has been restarted yet without producing
        #     # any output
        #     if self.retried > self.max_retries:
        #         gc3libs.log.error("Maximum number of retries '%d' reached."
        #                           "Cloud not continue." % self.retried)
        #     else:
        #         return True
        # return False

class GWeightScript(SessionBasedScript):
    """
    Splits input .csv file into smaller chunks, each of them of size
    'self.params.chunk_size'.
    Then it submits one execution for each of the created chunked files.

    The ``gweight`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gweight`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gweight``
    aggregates them into a single larger output file located in
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GWeightTask,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GWeightTask,
            )

    def setup_options(self):
        self.add_param("-k", "--chunk", metavar="[NUM]", #type=executable_file,
                       dest="chunk_size", default="1000",
                       help="How to split the edges input data set.")

        self.add_param("-M", "--master", metavar="[PATH]",
                       dest="driver_script", default=None,
                       help="Location of master driver R script.")

        self.add_param("-D", "--data", metavar="[PATH]",
                       dest="data", default=None,
                       help="Location of the reference data in .rda format.")

        self.add_param("-F", "--weight", metavar="[PATH]",
                       dest="weight_function", default=None,
                       help="Location of the weight function R script.")

        self.add_param("-T", "--threads.posted", metavar="[PATH]",
                       dest="threads_posted_data", default=None,
                       help="Location of threads posted data in .rda format.")

    def setup_args(self):

        self.add_param('edges_data', type=str,
                       help="Input edges data full path name.")

    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """

        # check args:
        # XXX: make them position independent
        if not os.path.isfile(self.params.edges_data):
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid path to edges data: '%s'. File not found"
                % self.params.edges_data)

        self.edges_filename = os.path.basename(self.params.edges_data)

        # Verify that 'self.params.chunk_size' is int
        int(self.params.chunk_size)
        self.params.chunk_size = int(self.params.chunk_size)

    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new GcgpsTask
        """
        tasks = []

        for (input_file, index_chunk) in self._generate_chunked_files_and_list(self.params.edges_data,
                                                                              self.params.chunk_size):
            jobname = "gweight-%s" % (str(index_chunk))

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


            if self.params.driver_script:
                extra_args['driver_script'] = self.params.driver_script
            if self.params.data:
                extra_args['data'] = self.params.data
            if self.params.weight_function:
                extra_args['weight_function'] = self.params.weight_function
            if self.params.threads_posted_data:
                extra_args['threads_posted_data'] = self.params.threads_posted_data

            self.log.debug("Creating Task for index : %d - %d" %
                           (index_chunk, (index_chunk + self.params.chunk_size)))

            tasks.append(GWeightTask(
                    input_file,
                    **extra_args))

        return tasks

    def after_main_loop(self):
        """
        Merge all result files together
        Then clean up tmp files
        """
        # init result .csv file
        merged_csv = "result-%s" % os.path.basename(self.params.edges_data)

        try:
            fout=open(merged_csv,"w+")
            for task in self.session:
                if isinstance(task,GWeightTask) and task.execution.returncode == 0:
                    try:
                        for line in open(task.output_file):
                            fout.write(line)
                    except OSError, osx:
                        # Report failure and continue
                        # XXX: Check what would be the correct behaviour
                        gc3libs.log.error("Failed while merging result file %s." % task.output_file)
                        continue
            fout.close()
        except OSError, osx:
            gc3libs.log.critical("Failed while merging result files. " +
                                 "Error %s" % str(osx))
            raise
        finally:
            fout.close()

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
                                                                   'gweight-',
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
