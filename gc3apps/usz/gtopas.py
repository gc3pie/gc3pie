#! /usr/bin/env python
#
#   gtopas.py -- Front-end script for running BEAST jobs.
#   https://www.beast2.org/
#
#   Copyright (C) 2017 2018  University of Zurich. All rights reserved.
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
Front-end script for submitting multiple BEAST jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gtopas.py --help`` for program usage
instructions.

Input parameters consists of:
:param str BEAST XML file: Path to an .xml file containing BEAST settings 
and input data
"""

# summary of user-visible changes
__changelog__ = """
  2017-08-28:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'
__version__ = '1.0'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gtopas
    gtopas.GtopasScript().run()

import os
import sys
import time
import tempfile
import mimetypes
import random

import pandas
from xml.etree import cElementTree as ET
from xml.etree.ElementTree import Element, SubElement, Comment, tostring

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file, positive_int, existing_file, existing_directory
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

# Utility functions
def _get_valid_input(input_folder):
    """
    Return list of valid input file in .txt format
    Input file will be returned with ABS path
    """

    return [os.path.join(input_folder,input_file) for input_file in os.listdir(input_folder) if input_file.endswith('.txt') and if mimetypes.guess_type(input_file)[0] == 'text/plain']
    

## custom application class
class GtopasApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'gtopas'
    
    def __init__(self, input_file, **extra_args):

        executables = []
        inputs = dict()
        outputs = dict()
        
        Application.__init__(
            self,
            arguments = ['topas', os.path.basename(input_file_name)],
            inputs = [input_file],
            outputs = ['scores.csv'],
            # Note: aternatively, you could specify a folder name as 'output'. GC3Pie will retrieve the entire content of that folder
            # Example: outputs = ['./results/']
            stdout = 'gtopas.log',
            join=True,
            executables = executables,
            **extra_args)
    
class GtopasScript(SessionBasedScript):
    """
    The ``gtopas`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gtopas`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gtopas``
    aggregates them into a single larger output file located in 
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            application = GtopasApplication, 
            stats_only_for = GtopasApplication,
            )

    def setup_args(self):        
        self.add_param('input_folder',
                       type=existing_directory,
                       help="Path to input folder containing valid input .txt files.")        

    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new GcgpsTask
        """
        tasks = []
        
        for input_file in _get_valid_input(self.params.input_folder):
            
            jobname = extract_jobname_from_file(input_file)
                
            extra_args = extra.copy()
            extra_args['jobname']  = jobname
            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)
            
            tasks.append(GtopasApplication(
                input_file,
                **extra_args))

        return tasks

    def after_main_loop(self):
        """
        Merge all result files together
        Then clean up tmp files
        """
        for task in self.session:
            if isinstance(task, GsubbeastApplication) and task.execution.returncode == 0:
                result_log_file = os.path.join(task.output_dir, '{0}.log'.format(task.id_name))
                if os.path.isfile(result_log_file):
                    gc3libs.log.debug('Reading output file {0}'.format(result_log_file))
                    data = pandas.read_csv(result_log_file, sep='\t', comment="#")
                    for key in df_dict.keys():
                        cols = [col for col in data.columns if key in col]
                        # In case multiple entries, take the first occurrence                    
                        # column_to_search = "{0}:{1}".format(key,task.id_name)
                        gc3libs.log.debug("Column [{0}] found {1} occurances. Should be 1.".format(key,len(cols)))
                        if len(cols) > 0:
                            df_dict[key][task.id_name] = data[cols[0]]
                        else:
                            gc3libs.log.error("Skipping column {0} as no occourrences have been found".format(key))

                else:
                    gc3libs.log.error('Output file {0} for task {1} not found'.format(result_log_file, task.id_name))
                    continue
            else:
                gc3libs.log.warning('Task {0} not completed or failed during execution. Ignoring'.format(task.id_name))

        gc3libs.log.info('Writing aggregated .csv results in {0}'.format(self.params.result_csv))
        for key in df_dict.keys():
            df = df_dict[key]
            df.to_csv(os.path.join(self.params.result_csv, '{0}.csv'.format(key)))

        return

    
