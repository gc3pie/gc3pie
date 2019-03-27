#! /usr/bin/env python
#
"""
Front-end script for making a session out of a generic command-line.
The tool has been requested from the System Botanik and should enable
various type of executables to be run on a VM by passing the arguments
in a csv control file.
"""

from __future__ import absolute_import, print_function

# Copyright (C) 2011-2014  University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__author__ = 'Tyanko Aleksiev <tyanko.alexiev@gmail.com>'

# summary of user-visible changes
__changelog__ = """
  2014-04-17:
    * Initial draft version.
"""
__docformat__ = 'reStructuredText'


if __name__ == "__main__":
    import ggeneric
    ggeneric.GGeneric().run()


## stdlib imports
import itertools
import os
import os.path
import string
import sys

## GC3Libs imports
import gc3libs
from gc3libs import Application, Run, Task
import gc3libs.exceptions
from gc3libs.cmdline import SessionBasedScript, executable_file, existing_file, nonnegative_int
import gc3libs.utils
from gc3libs.workflow import RetryableTask


## helper class

class GGenericApplication(Application):


    def __init__(self, executable,  **extra_args):
        # setup for finding actual files

        """
        The wrapper script is being used for start the simulation.
        """
        files_to_send = []

        ggeneric_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/ggeneric_wrapper.sh")

        basename_executable_file = os.path.basename(executable)
        files_to_send.append((ggeneric_wrapper_sh,os.path.basename(ggeneric_wrapper_sh)))
        files_to_send.append((executable,basename_executable_file))


        cmd = "./ggeneric_wrapper.sh"

        if 'tarfile' in extra_args:
            cmd += " -F %s " % extra_args['tarfile']
            files_to_send.append(extra_args['tarfile'],'tarfile')

        cmd += " %s " % extra_args['executable']

        cmd += " %s " % extra_args['options']

        self.output_dir = basename_input_file + self.params.suffix
        extra_args['output_dir'] = self.output_dir

        Application.__init__(
            self,
            # arguments should mimic the command line interfaca of the command to be
            # executed on the remote end
            arguments = cmd,
            inputs = files_to_send,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'ggeneric.log',
            join=True,
            **extra_args)

class GGenericTask(RetryableTask, gc3libs.utils.Struct):
    """
    Run ``ggeneric`` with the given executable and options.
    """
    def __init__(self, executable, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GGenericApplication(executable, **extra_args),
            # keyword arguments
            **extra_args)




## the script itself
class GGeneric(SessionBasedScript):
    """
Manages a task session for all the lines in the given control file.

The script takes only two positional argument which arethe executable
to be run and the control file.

Further options that could be specified are for:
 - REPS_PER_PROCESS - this will set the number of repetitions of each run.
 - FILE_NAME - specify the path and file name of the executable/script or an archive (.tar.gz) containing it along with
   other files needed to run the job (i.e. it could be a bash script and other binaries). If it is .gz or .tar.gz then
   ggeneric would unpack it appropriately before starting the executable.
   (if not specify, default to 1,2,3, per line of the control file)
 - OUTPUT_SUFFIX - suffix for output dir name so all output (all files generated in the current directory of the job that was run)
   would be collected to [NAME_OF_OUTPUT_COLUMN][OUTPUT_SUFFIX][.[REPETITION]]/

    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            )


    def setup_args(self):
        self.add_param('executable',type=executable_file, help="Path to the executable command.")
        self.add_param('control_file', type=existing_file, help="Control file containing executable options")


    def setup_options(self):
        self.add_param("-R", "--repeats", metavar="INT", dest="repeats", default=1,
                       help="Number of repetitions for every run.")

        self.add_param("-F", "--tarfile", metavar="PATH", dest="tarfile", default=None,
                       help="Point to an additional archive which contains files" +
                            "or executables needed.")

        self.add_param("-O", "--output_suffix", metavar="str", dest="suffix", default=None,
                       help="Suffix to be added at the end of the output directory")


    def parse_args(self):

        if self.params.executable:
            self.params.executable = os.path.abspath(self.params.executable)

        if self.parsms.tarfile:
            self.params.tarfile = os.path.abspath(self.params.tarfile)

    def new_tasks(self, extra):

        tasks = []
        extra_args = extra.copy()

        # copy options and aguments to the extra arguments
        if self.params.repeats:
            extra_args['repeats'] = self.params.repeats

        if self.params.tarfile:
            extra_args['tarfile'] = self.params.tarfile

        if self.params.suffix:
            extra_args['suffix'] = self.params.suffix

        if self.params.executable:
            extra_args['executable'] = self.params.executable

        if self.params.control_file.endswith('.csv'):
                try:
                    inputfile = open(self.params.control_file, 'r')
                except (OSError, IOError), ex:
                    self.log.warning("Cannot open input file '%s': %s: %s",
                                     path, ex.__class__.__name__, str(ex))
                for row in csv.reader(inputfile):
                    # create a string containing the parameters
                    # to be used for calling the executable
                    options=""
                    for i in range(0, length(row)):
                        options=options+args[i] + " "
                    extra_args['options'] = options

                    # create multiple tasks of the same type
                    # if we have to execute multiple runs with the
                    # same input.
                    for i in range(1, self.params.repeats):
                        tasks.append(GGenericTask(
                            self.params.executable,
                            **extra_args
                            ))

        return tasks
