#! /usr/bin/env python
#
#   gsceuafish.py -- Front-end script for evaluating R-based 'weight'
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

See the output of ``gsceuafish.py --help`` for program usage
instructions.

Input parameters consists of:
:param str edges file: Path to an .csv file containing input data in
the for of: 
    X1   X2
1  id1  id2
2  id1  id3
3  id1  id4

...
2015-09-29: aggregated result file should be named after the `-o` option


XXX: To be clarified:
. What happen if an error happen at merging time ?
. Should be possible to re-run a subset of the initial chunk list
without re-creating a new session ?
e.g. adding a new argument accepting chunk ranges (-R 3000:7500)
This would trigger the re-run of the whole workflow only 
for lines between 3000 and 7500
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
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gsceuafish
    gsceuafish.GsceuafishScript().run()

import os
import sys
import time
import tempfile

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

## custom application class
class GsceuafishApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'sceuafish'
    
    def __init__(self, parameter, **extra_args):

        inputs = dict()
        outputs = dict()

        # execution wrapper needs to be added anyway
        gscuafish_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gscuafish.sh")
        inputs[gscuafish_wrapper_sh] = os.path.basename(gscuafish_wrapper_sh)

        _command = "./%s %s " % (os.path.basename(gscuafish_wrapper_sh),
                                 ' '.join(str(x) for x in parameter))

        # command = "MainFunction %s; quit" % ' '.join(str(x) for x in parameter)

        if "main_loop_folder" in extra_args:
            inputs[extra_args['main_loop_folder']] = './data/'
            # _command += "-p ./data"
            # command = "addpath('./data/'); "+command

        # try:
        #     # create script file
        #     (handle, self.tmp_filename) = tempfile.mkstemp(prefix='gsceuafish', suffix=extra_args['jobname'])

        #     fd = open(self.tmp_filename,'w')
        #     fd.write(command)
        #     fd.close()
        # except Exception, ex:
        #     gc3libs.log.debug("Error creating execution script" +
        #                       "Error type: %s." % type(ex) +
        #                       "Message: %s"  %ex.message)
        #     raise

        # inputs[fd.name] = 'runme.m'

        # arguments = "matlab -nodesktop -nodisplay -nosplash < ./runme.m"
        
        Application.__init__(
            self,
            arguments = _command,
            inputs = inputs,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'gsceuafish.log',
            join=True,
            executables = "./%s" % os.path.basename(gscuafish_wrapper_sh),
            **extra_args)


class GsceuafishScript(SessionBasedScript):
    """
    Splits input .csv file into smaller chunks, each of them of size 
    'self.params.chunk_size'.
    Then it submits one execution for each of the created chunked files.
    
    The ``gsceuafish`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gsceuafish`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gsceuafish``
    aggregates them into a single larger output file located in 
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GsceuafishApplication, 
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GsceuafishApplication,
            )

    def setup_options(self):
        self.add_param("-d", "--data", metavar="PATH", type=str,
                       dest="main_loop", default=None,
                       help="Location of the Main_Loop.m script and "
                       "related MAtlab functions. Default: None")

    def setup_args(self):

        self.add_param('csv_input_file', type=str,
                       help="Input .csv file")

    def parse_args(self):
        """
        Check presence of input folder (should contains R scripts).
        path to command_file should also be valid.
        """
        assert os.path.isfile(self.params.csv_input_file), \
        "Input CSV file %s not found" % self.params.csv_input_file
        
        if self.params.main_loop:
            assert os.path.isdir(self.params.main_loop), \
            "Main_Loop.m location %s not found" % self.params.main_loop

        # try:
        #     assert os.path.isfile(self.params.csv_input_file)
        # except ValueError:
        #     raise gc3libs.exceptions.InvalidUsage(
        #         "Input CSV file %s not found" % self.params.csv_input_file)
        
        # if self.params.main_loop:
        #     if not os.path.isdir(self.params.main_loop):
        #         raise gc3libs.exceptions.InvalidUsage(
        #         "Main_Loop.m location %s not found" % self.params.main_loop)

    def new_tasks(self, extra):
        """
        For each line of the input .csv file generate
        an execution Task
        """
        tasks = []

        for parameter in self._enumerate_csv(self.params.csv_input_file):
            parameter_str = '.'.join(str(x) for x in parameter)
            jobname = "gsceuafish-%s" % parameter_str

            extra_args = extra.copy()

            extra_args['jobname'] = jobname
            
            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

            if self.params.main_loop:
                extra_args['main_loop_folder'] = self.params.main_loop
            
            self.log.debug("Creating Application for parameter : %s" %
                           (parameter_str))

            tasks.append(GsceuafishApplication(
                    parameter,
                    **extra_args))

        return tasks

    def _enumerate_csv(self, input_csv):
        """
        """
        parameters = pandas.read_csv(input_csv,header=None)
        for i,p in enumerate(parameters.values):
            yield p.tolist()


