#! /usr/bin/env python
#
#   gsinusrange.py -- Front-end script for running Matlab function
#   function over a large parameter range.
#
#   Copyright (c) 2016 S3IT, University of Zurich, http://www.s3it.uzh.ch/
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
Front-end script for running Matlab function
#   function over a large parameter range.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gsinusrange.py --help`` for program usage
instructions.

"""

# summary of user-visible changes
__changelog__ = """
  2016-09-19:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gsinusrange
    gsinusrange.GsinusrangeScript().run()

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
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

DEFAULT_REMOTE_OUTPUT_FOLDER = "./results"
DEFAULT_FUNCTION = "sinusrange"
TARFILE="source.tgz"
TEMP_FOLDER="/var/tmp"


## utility funtions

def _getchunk(file_to_chunk, chunk_size=2, chunk_files_dir='/var/tmp'):
    """
    Takes a file_name as input and a defined chunk size
    ( uses 1000 as default )
    returns a list of filenames 1 for each chunk created and a corresponding
    index reference
    e.g. ('/tmp/chunk2800.csv,2800) for the chunk segment that goes
    from 2800 to (2800 + chunk_size)
    """
    
    chunks = []
    
    # creating 'chunk_files_dir'
    if not(os.path.isdir(chunk_files_dir)):
        try:
            os.mkdir(chunk_files_dir)
        except OSError, osx:
            gc3libs.log.error("Failed while creating tmp folder %s. " % chunk_files_dir +
                              "Error %s." % str(osx) +
                              "Using default '/tmp'")
            chunk_files_dir = "/tmp"

    reader = pandas.read_csv(file_to_chunk, header=0, chunksize=chunk_size)
    
    index = 0
    for chunk in reader:
        index += 1     
        filename = "%s/chunk_%s.csv" % (chunk_files_dir,index)
        chunk.to_csv(filename, header=True, index=False)
        chunks.append((filename,index))
            
    return chunks


def _scanandtar(dir_to_scan, temp_folder=TEMP_FOLDER):
    try:
        gc3libs.log.debug("Compressing input folder '%s'" % dir_to_scan)
        cwd = os.getcwd()
        os.chdir(dir_to_scan)

        if not os.path.isdir(temp_folder):
            os.mkdir(temp_folder)
        
        with tarfile.open(os.path.join(temp_folder,TARFILE), "w:gz") as tar:

            tar.add(dir_to_scan, arcname=".")
            os.chdir(cwd)

            gc3libs.log.info("Created tar file '%s'" % TARFILE)
            return tar.name
        
    except Exception, x:
        gc3libs.log.error("Failed creating input archive '%s': %s %s",
                          os.path.join(dir_to_scan,),
                          type(x),x.message)
        raise

    
## custom application class

class GsinusrangeApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'gsinusrange'
    
    def __init__(self, input_file, mfunct, **extra_args):


        executables = []
        inputs = dict()
        outputs = dict()

        wrapper = resource_filename(Requirement.parse("gc3pie"),
                                    "gc3libs/etc/gthermostat.sh")
        inputs[wrapper] = "./wrapper.sh"

        inputs[input_file] = os.path.basename(input_file)

        arguments = "./wrapper.sh %s %s %s" % (mfunct,
                                               os.path.basename(input_file),
                                               DEFAULT_REMOTE_OUTPUT_FOLDER)

        if 'source' in extra_args:
            inputs[extra_args['source']] = os.path.basename(extra_args['source'])
            arguments += " -s %s" % os.path.basename(extra_args['source'])
            
        # Set output
        outputs[DEFAULT_REMOTE_OUTPUT_FOLDER] = DEFAULT_REMOTE_OUTPUT_FOLDER

        gc3libs.log.debug("Creating application for executing: %s",
                          arguments)
        
        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'gsinusrange.log',
            join=True,
            executables = executables,
            **extra_args)

class GsinusrangeScript(SessionBasedScript):
    """
    Takes 1 .csv input file containing the list of parameters to be passed
    to the `sinusrange` application.
    Each line of the input .csv file correspond to the parameter list to be
    passed to a single `ctx-linkdyn-ordprm-sirs.p4` execution. For each line
    of the input .csv file a GsinusrangeApplication needs to be generated (depends on
    chunk value passed as part of the input options).
    Splits input .csv file into smaller chunks, each of them of size 
    'self.params.chunk_size'.
    Then submits one execution for each of the created chunked files.

    The ``gsinusrange`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gsinusrange`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gsinusrange``
    aggregates them into a single larger output file located in 
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GsinusrangeApplication, 
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GsinusrangeApplication,
            )

    def setup_args(self):
        
        self.add_param('input_ranges', type=str,
                       help="folder containing input ranges .txt files")

    def setup_options(self):
        self.add_param("-R", "--src", metavar="[STRING]", 
                       dest="source", default=None,
                       help="Location of the Matlab functions.")

        self.add_param("-f", "--function", metavar="STRING",
                       dest="mfunct", default=DEFAULT_FUNCTION,
                       help="Name of the Matlab function to call."
                       " Default: %(default)s.")

        
    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """
        try:
            assert os.path.isdir(os.path.abspath(self.params.input_ranges)),  \
                "Input ranges folder not found: %s " \
                % os.path.abspath(self.params.input_ranges)

            if self.params.source:
                assert os.path.isdir(os.path.abspath(self.params.source)),  \
                    "Matlab source folder not found: %s " \
                    % os.path.abspath(self.params.source)

            assert os.path.isfile(os.path.join(self.params.source,self.params.mfunct + ".m")), \
                "Matlab funtion file %s/%s.m not found" % (self.params.source,
                                                           self.params.mfunct)
            
        except AssertionError as ex:
            raise ValueError(ex.message)            

    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new Application
        """
        tasks = []

        if self.params.source:
            tar_file = _scanandtar(os.path.abspath(self.params.source),
                                  temp_folder=os.path.join(self.session.path,"tmp"))

        for input_range_file in os.listdir(self.params.input_ranges):
            
            jobname = "gsinusrange-%s" % os.path.basename(input_range_file)

            extra_args = extra.copy()

            extra_args['jobname'] = jobname

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

            if self.params.source:
                extra_args['source'] = tar_file
            tasks.append(GsinusrangeApplication(
                os.path.join(self.params.input_ranges,input_range_file),
                self.params.mfunct,
                **extra_args))

        return tasks
