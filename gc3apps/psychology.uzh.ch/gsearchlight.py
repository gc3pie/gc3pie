#! /usr/bin/env python
#
#   gsearchlight.py -- Front-end script for evaluating cosmo searchlight.
#
#   Copyright (C) 2016, 2017 S3IT, University of Zurich
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
Front-end script for submitting multiple `Matlab` jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gsearchlight.py --help`` for program usage
instructions.
"""

__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2016-12-01:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gsearchlight
    gsearchlight.GsearchlightScript().run()

import os
import sys
import time
import tempfile

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file, positive_int, existing_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds

DEFAULT_ITERATIONS=1000
DEFAULT_REMOTE_OUTPUT_FILE="result"
MATLAB_CMD="matlab -nosplash -nodisplay -nodesktop -r \"{main_function} 1 {mask} {fn} {output};quit\""

## custom application class
class GsearchlightApplication(Application):
    """
    Custom class to wrap the execution of the Matlab function
    over a subset of the total number of events.
    """
    application_name = 'gsearchlight'
    
    def __init__(self, iterations, mask_file, fn_file, matlab_file, **extra_args):

        executables = []
        inputs = dict()
        outputs = dict()

        inputs[matlab_file] = os.path.basename(matlab_file)
        matlab_function = inputs[matlab_file].split('.')[0]
        inputs[mask_file] = os.path.basename(mask_file)
        inputs[fn_file] = os.path.basename(fn_file)
            
        arguments = MATLAB_CMD.format(main_function=matlab_function,
                                      mask=inputs[mask_file],
                                      fn=inputs[fn_file],
                                      output=DEFAULT_REMOTE_OUTPUT_FILE)
            
        # Set output
        outputs[DEFAULT_REMOTE_OUTPUT_FILE] = DEFAULT_REMOTE_OUTPUT_FILE

        gc3libs.log.debug("Creating application for executing: %s",
                          arguments)
        
        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gsearchlight.log',
            join=True,
            executables = executables,
            **extra_args)


class GsearchlightScript(SessionBasedScript):
    """
    Take total number of events and create a list of chunked events.
    For each chunk, run the provided Matlab function.

    The ``gsearchlight`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gsearchlight`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gsearchlight``
    aggregates them into a single larger output file located in 
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            application = GsearchlightApplication, 
            stats_only_for = GsearchlightApplication,
            )

    def setup_args(self):
        
        self.add_param('Mfunct',
                       type=existing_file,
                       help="Full path to Matlab function to execute.")

        self.add_param('mask',
                       type=existing_file,
                       help="Full path to Mask file.")

        self.add_param('fn',
                       type=existing_file,
                       help="Full path to Fn file.")
        
    def setup_options(self):
        self.add_param("-I", "--iterations", metavar="INT",
                       type=positive_int,
                       dest="niter", default=DEFAULT_ITERATIONS,
                       help="How to split the edges input data set. "
                       "Default: %(default)s.")

    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new Application
        """
        tasks = []

        for iteration in range(0,self.params.niter):
            extra_args = extra.copy()
            tasks.append(GsearchlightApplication(
                iteration,
                self.params.mask,
                self.params.fn,
                self.params.Mfunct,
                **extra_args))
                    
        return tasks
