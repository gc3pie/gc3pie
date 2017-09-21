#! /usr/bin/env python
#
#   gsubbeast.py -- Front-end script for running BEAST jobs.
#   https://www.beast2.org/
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
Front-end script for submitting multiple BEAST jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gsubbeast.py --help`` for program usage
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
    import gsubbeast
    gsubbeast.GsubbeastScript().run()

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

DEFAULT_REMOTE_OUTPUT_FOLDER = "./results"
DEFAULT_SEED_RANGE=37444887175646646
DEFAULT_JAR_LOCATION="/app/beast/lib/beast.jar"
BEAST_COMMAND="java -jar {jar} -working -overwrite -beagle -seed {seed} {resume} {input_xml}"

# Utility functions
def _get_id_from_inputfile(input_file):
    """
    by convention, input XML files will have an 'id' tag in the 'data' item
    Return 'id' tag name
    """
    id_name = filename = os.path.splitext(os.path.basename(input_file))[0]
    tree = ET.parse(input_file)
    root = tree.getroot()
    data = root.find('data')
    if data.get('id'):
        id_name = data.get('id')
    else:
        gc3libs.log.warning("Failed to extract 'id' from input file {0}. Using filename instead".format(filename))
    gc3libs.log.info("Using id '{0}' for filename {1}".format(id_name, filename))
    return id_name

def _get_valid_input(input_folder, resume):
    """
        [ input_xml for input_xml in os.listdir(self.params.input_folder) if mimetypes.guess_type(input_xml)[0] == 'application/xml' ]:
            for rep in range(0,self.params.repeat):
    """

    state_file = None
    for input_file in [ input_xml for input_xml in os.listdir(input_folder) if mimetypes.guess_type(input_xml)[0] == 'application/xml' ]:
        if resume and os.path.isfile(os.path.join(input_folder,input_file+'.state')):
            state_file = os.path.join(input_folder,input_file+'.state')
        yield (os.path.join(input_folder,input_file),state_file)
                                     
    
def _check_exit_condition(log, output_dir):
    """
    Inspect output folder.
    Check for termiantion condition AND .state file
    If termination condition not met, return list of .state files
    If termiantion condition is met, return an empty list.
    """
    TERMIANTION_PATTERN = "End likelihood"
    with open(log) as fd:
        for line in fd:
            if TERMIANTION_PATTERN in line:
                # Job completed. 
                # Return an empty list
                return (None,None)

    # Somehow job was not completed.
    # Search for .state file and return it
    results = os.listdir(output_dir)
    for item in results:
        if item.endswith(".state"):
            return (os.path.join(output_dir,item),results)

    return (None,None)

## custom application class
class GsubbeastApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'gsubbeast'
    
    def __init__(self, input_file, state_file, id_name, seed, jar=None, **extra_args):

        executables = []
        inputs = dict()
        outputs = dict()
        
        inputs[input_file] = os.path.basename(input_file)

        if state_file:
            inputs[state_file] = os.path.basename(state_file)
            resume_option=" -t {state_file}".format(state_file=state_file)
        else:
            resume_option=""

        if jar:
            inputs[jar] = os.path.basename(jar)
            jar_cmd = inputs[jar]
        else:
            jar_cmd = DEFAULT_JAR_LOCATION

        arguments = BEAST_COMMAND.format(jar=jar_cmd,
                                         seed=seed,
                                         resume=resume_option,
                                         input_xml=inputs[input_file])

        gc3libs.log.debug("Creating application for executing: %s", arguments)

        self.id_name = id_name
        self.seed = seed
        self.jar_cmd = jar_cmd
        
        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'gsubbeast.log',
            join=True,
            executables = executables,
            **extra_args)


    def terminated(self):
        """
        Check whehter expected output log files have been generated.
        """
        result_log_file = os.path.join(self.output_dir, '{0}.log'.format(self.id_name))
        gc3libs.log.info('Application terminated with exit code %s' % self.execution.exitcode)
        if not os.path.isfile(result_log_file):
            gc3libs.log.error('Failed while checking outputfile %s.' % result_log_file)
            self.execution.returncode = (0, 99)


class GsubbeastRetryableTask(RetryableTask):
    def __init__(self, input_file, state_file, id_name, seed=None, jar=None, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GsubbeastApplication(
                input_file,
                state_file,
                id_name,
                seed=seed,
                jar=jar,
                **extra_args),
            **extra_args
            )

    def retry(self):
        """ 
        Task will be retried iif the application crashed
        due to an error within the exeuction environment
        (e.g. VM crash or LRMS kill)
        """
        # XXX: check whether termination condition could not be met.
        # Then include .state file and resume simulation.

        (state_file,input_list) = _check_exit_condition(os.path.join(self.output_dir,
                                                                     self.task.stdout),
                                                        self.output_dir)
        if state_file:
            resume_option=" -t {state_file}".format(state_file=self.state_file)
            self.inputs = []
            for item in input_list:
                # get the whole output folder as input and re-submit
                self.inputs[os.path.join(self.output_dir,item)] = item

            self.arguments = "./wrapper.sh {jar} -s {seed} {resume} {input_xml}".format(seed=self.seed,
                                                                                        jar=self.jar_options,
                                                                                        resume=resume_option,
                                                                                        input_xml=self.inputs[input_file])
            return True
        else:
            return False
    
    
class GsubbeastScript(SessionBasedScript):
    """
    The ``gsubbeast`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gsubbeast`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gsubbeast``
    aggregates them into a single larger output file located in 
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            application = GsubbeastApplication, 
            stats_only_for = GsubbeastApplication,
            )

    def setup_args(self):        
        self.add_param('input_folder',
                       type=existing_directory,
                       help="Path to input folder containing valid input .xml files.")

    def setup_options(self):

        self.add_param("-U", "--resume",
                       action="store_true",
                       dest="resume",
                       default=False,
                       help="Use existing '.state' files to "
                       "resume interrupted BEAST execution. "
                       "Default: %(default)s.")

        self.add_param("-R", "--repeat", metavar="[INT]",
                       type=positive_int,
                       dest="repeat",
                       default=1,
                       help="Repeat analysis. Default: %(default)s.")


        self.add_param("-B", "--jar", metavar="[PATH]",
                       type=existing_file,
                       dest="jar",
                       default=None,
                       help="Path to Beast.jar file.")

        self.add_param("-F", "--follow",
                       dest="follow",
                       action="store_true",
                       default=False,
                       help="Periodically fetch job's output folder and copy locally.")

        self.add_param('-M', '--merge-anyway',
                       dest='merge_anyway',
                       action='store_true',
                       default=False,
                       help="Merge results only when all jobs have completed " \
                       " successfully. Default: %(default)s.")

        self.add_param('-O', '--store-aggregate-csv',
                       dest='result_csv',
                       default='.',
                       help="Location of aggregated .csv results. Default: '%(default)s'.")

        self.add_param('-P', '--extract-columns',
                       dest='columns',
                       default=['TreeHeight.t'],
                       help='Comma separated list of columns name to extract from " \
                       "output log file. Default: %(default)s.')

    def parse_args(self):
        self.params.columns = self.params.columns.split(',')
        
        
    def before_main_loop(self):
        # XXX: should this be done with `make_controller` instead?
        self._controller.retrieve_running = self.params.follow
        self._controller.retrieve_overwrites = self.params.follow
        self._controller.retrieve_changed_only = self.params.follow

    def new_tasks(self, extra):
        """
        Read content of 'command_file'
        For each command line, generate a new GcgpsTask
        """
        tasks = []
        
        for (input_file, stat_file) in _get_valid_input(self.params.input_folder,
                                           self.params.resume):
            id_name = _get_id_from_inputfile(input_file)
            for rep in range(0,self.params.repeat):

                jobname = '{0}-{1}'.format(id_name, rep)                

                extra_args = extra.copy()
                extra_args['jobname'] = jobname            
                extra_args['output_dir'] = self.params.output
                extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

                self.log.debug("Creating Application for file '%s'" % jobname)

                tasks.append(GsubbeastRetryableTask(
                    input_file,
                    stat_file,
                    id_name,
                    seed=random.randrange(DEFAULT_SEED_RANGE),
                    jar=self.params.jar,
                    **extra_args))

        return tasks

    def after_main_loop(self):
        """
        Merge all result files together
        Then clean up tmp files
        """
        if not self.params.merge_anyway:
            for task in self.session:
                if task.execution.state != Run.State.TERMINATED:
                    gc3libs.log.warning('Could not perform aggregation task as not all jobs have terminated.')
                    return
                if task.execution.returncode is not None and task.execution.returncode != 0:
                    gc3libs.log.warning('Could not perform aggregation task as not all jobs have completed successfully.')
                    return

        df_dict = dict()
        for df_name in self.params.columns:
            df_dict[df_name] = pandas.DataFrame()

        for task in self.session:
            if isinstance(task, GsubbeastRetryableTask) and task.execution.returncode == 0:
                result_log_file = os.path.join(task.output_dir, '{0}.log'.format(task.id_name))
                if os.path.isfile(result_log_file):
                    gc3libs.log.debug('Reading output file {0}'.format(result_log_file))
                    data = pandas.read_csv(result_log_file, sep='\t', comment="#")
                    for key in df_dict.keys():
                        column_to_search = "{0}:{1}".format(key,task.id_name)
                        gc3libs.log.debug("Column '{0}' counts {1} elements".format(key,
                                                                                    len(data[column_to_search])))
                        df_dict[key][task.id_name] = data[column_to_search]

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

