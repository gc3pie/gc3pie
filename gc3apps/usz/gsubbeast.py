#! /usr/bin/env python
#
#   gsubbeast.py -- Front-end script for evaluating R-based 'weight'
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

See the output of ``gsubbeast.py --help`` for program usage
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

# summary of user-visible changes
__changelog__ = """
  2013-07-03:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
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

# Utility functions
def _get_valid_input(input_folder, restart):
    """
        [ input_xml for input_xml in os.listdir(self.params.input_folder) if mimetypes.guess_type(input_xml)[0] == 'application/xml' ]:
            for rep in range(0,self.params.repeat):
    """

    state_file = None
    for input_file in [ input_xml for input_xml in os.listdir(input_folder) if mimetypes.guess_type(input_xml)[0] == 'application/xml' ]:
        if restart and os.path.isfile(os.path.join(input_folder,input_file+'.state')):
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
    
    def __init__(self, input_file, state_file, seed, jar=None, **extra_args):

        executables = []
        inputs = dict()
        outputs = dict()
        
        inputs[input_file] = os.path.basename(input_file)
        gsubbeast_wrapper = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gsubbeast_wrapper.sh")

        inputs[gsubbeast_wrapper] = "./wrapper.sh"
        executables.append(inputs[gsubbeast_wrapper])

        if state_file:
            inputs[state_file] = os.path.basename(state_file)
            resume_option=" -t {state_file}".format(state_file=state_file)
        else:
            resume_option=""

        if jar:
            inputs[jar] = os.path.basename(jar)
            jar_option = "-j "+inputs[jar]
        else:
            jar_option = ""

        arguments ="./wrapper.sh {jar} -s {seed} {resume} {input_xml}".format(seed=seed,
                                                                                  jar=jar_option,
                                                                                  resume=resume_option,
                                                                                  input_xml=inputs[input_file])

        gc3libs.log.debug("Creating application for executing: %s", arguments)

        self.seed = seed
        self.jar_option = jar_option
        
        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'gsubbeast.log',
            join=True,
            executables = executables,
            **extra_args)


class GsubbeastRetryableTask(RetryableTask):
    def __init__(self, input_file, state_file, seed, jar=None, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GsubbeastApplication(
                input_file,
                state_file,
                seed,
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
        # Then include .state file and restart simulation.

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

        self.add_param("-U", "--restart",
                       action="store_true",
                       dest="restart",
                       default=False,
                       help="Use existing '.state' files to "
                       "restart interrupted BEAST execution. "
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
        
        for (input_file,stat_file) in _get_valid_input(self.params.input_folder,
                                           self.params.restart):
            for rep in range(1,self.params.repeat):

                jobname = "{xml}-{rep}".format(xml=os.path.splitext(os.path.basename(input_file))[0],
                                               rep=rep)
                
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
                    seed=random.randrange(DEFAULT_SEED_RANGE),
                    jar=self.params.jar,
                    **extra_args))

        return tasks
