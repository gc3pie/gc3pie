#! /usr/bin/env python
#
#   gweight.py -- Front-end script for evaluating R-based 'weight'
#   function over a large dataset.
#
#   Copyright (C) 2011, 2012 GC3, University of Zurich
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

"""

__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2013-07-03:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gweight
    gweight.GWeightScript().run()

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
class GWeightApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'gweight'
    
    def __init__(self, edges_data_filename, **extra_args):
        
        # setup output references
        # self.result_dir = result_dir
        self.output_dir = extra_args['output_dir']
        self.output_filename = os.path.basename(edges_data_filename)
        self.output_file = os.path.join(self.output_dir,self.output_filename)
        outputs = [("./result.csv",self.output_filename)]
        
        # setup input references
        # inputs = [ (edges_data_filename, "./input.csv") ]
        inputs = dict()

        inputs[edges_data_filename] = "./input.csv"

        # gweight_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
        #                                       "gc3libs/etc/gweight_wrap.sh")

        # inputs[gweight_wrapper_sh] = os.path.basename(gweight_wrapper_sh)

        Application.__init__(
            self,
            arguments = ['${HOME}/bin/wrapper.sh'],
            inputs = inputs,
            outputs = outputs,
            stdout = 'gweight.log',
            join=True,
            **extra_args)

    def terminated(self):
        """
        Check whether output file has been properly created
        """
        # XXX: TBD, work on a more precise checking of the output file
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
        if self.task.execution.exitcode != 100 or self.task.execution.exitcode != 0:
            # Candidate for retry
            # Let's check how many times it has been restarted yet without producing
            # any output
            if self.retried > self.max_retries:
                gc3libs.log.error("Maximum number of retries '%d' reached."
                                  "Cloud not continue." % self.retried)
            else:
                return True
        return False



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
            application = GWeightApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GWeightApplication,
            )

    def setup_options(self):
        self.add_param("-k", "--chunk", metavar="INT", #type=executable_file,
                       dest="chunk_size", default="1000",
                       help="How to split the edges input data set.")

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

        # XXX: how to avoid redo the whole chunking everytime 
        # SessionbasedScript is relaunched ?

        tasks = []

        for (input_file, index_chunk) in self._generate_chunked_files_and_list(self.params.edges_data, 
                                                                              self.params.chunk_size):
            
            jobname = "gweight-%s" % (str(index_chunk))

            extra_args = extra.copy()
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
                
            self.log.debug("Creating Task for index : %d - %d" %
                           (index_chunk, (index_chunk + self.params.chunk_size)))

            tasks.append(GWeightTask(
                    input_file,
                    **extra_args))

        return tasks

    def after_main_loop(self):
        """
        Merge all result files together
        """
        pass

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

        try:
            fd = open(file_to_chunk,'rb')

            fout = None

            for (i, line) in enumerate(fd):
                if i % chunk_size == 0:
                    if fout: 
                        fout.close()
                    (handle, self.tmp_filename) = tempfile.mkstemp(prefix=
                                                                   'gweight-', 
                                                                   suffix=
                                                                   "%d.csv" % i)
                    # XXX: use NamedTemporaryFile instead with 'delete' = False
                    fout = open(self.tmp_filename,'w')
                    chunk.append((fout.name,i))
                fout.write(line)
            fout.close()
        except OSError, osx:
            gc3libs.log.critical("Failed while creating chunk files." +
                                 "Error %s", (osx.message))
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
            

