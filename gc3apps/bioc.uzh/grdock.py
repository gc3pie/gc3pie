#! /usr/bin/env python
#
#   grdock.py -- Front-end script for running the docking program rDock
#   over a list of ligand files.
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

It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``grdock.py --help`` for program usage
instructions.

Input parameters consists of:

...

Options:
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2015-02-17:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import grdock
    grdock.GrdockScript().run()

import os
import sys
import time
import tempfile
import re

import shutil
# import csv

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

DEFAULT_CORES = 1
DEFAULT_MEMORY = Memory(1500,MB)
DEFAULT_WALLTIME = Duration(300,hours)

## custom application class
class GrdockApplication(Application):
    """
    Custom class to wrap the execution of rdock.
    The wrapper script that will be executed on the remote end is
    organised in two steps:
    step 1: cavity creattion (will use rbcavity)
    step 2: docking (will use rbdock)

    Application will take the input ligand file and a ligand index
    ligand index is used to create an output file that maintains the
    same ligand index as a suffix:
    Es: input ligand: Docking1.sd -> output: Docked1.sd
    """
    application_name = 'grbdock'

    def __init__(self, docking_file, docking_index, **extra_args):

        self.output_dir = extra_args['output_dir']
        self.docking_index = docking_index

        inputs = dict()
        outputs = dict()

        grdock_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/grdock_wrapper.sh")

        inputs[grdock_wrapper_sh] = os.path.basename(grdock_wrapper_sh)

        inputs[docking_file] = os.path.basename(docking_file)

        if extra_args['data_folder']:
            for element in os.listdir(extra_args['data_folder']):
                inputs[os.path.abspath(os.path.join(extra_args['data_folder'],
                                                   element))] = os.path.basename(element)

        arguments = "./%s -n %s -o Docked%s %s results" % (inputs[grdock_wrapper_sh],
                                                           extra_args['rbdock_iterations'],
                                                           self.docking_index,
                                                           os.path.basename(docking_file))

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = ["results/"],
            stdout = 'grdock.log',
            join=True,
            **extra_args)


class GrdockScript(SessionBasedScript):
    """
    The script takes as input either a comma separated list of ligand files
    (with `.sd` extension) or a comma separated list of folders were to find the
    input ligand files.
    For each input ligand file, `grdocking` creates an instance of
    GrdockApplication.

    The ``grdock`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``grdock`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GrdockApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GrdockApplication,
            )

    def setup_options(self):
        self.add_param("-i", "--iterations", metavar="[NUM]",
                       dest="rbdock_iterations", default="20",
                       help="Number of iterations for rbdock. "
                       "Default: 20")

        self.add_param("-d", "--data", metavar="[STRING]",
                       dest="data_folder", default=None,
                       help="Path to data folder (e.g. where "
                       "crebbp-without-water-Tripos.mol2, "
                       "Pseudo-Ligand-in-pose.sd, Water-in-3P1C.pdb "
                       "and/or Docking.sd, could be retrieved")

    def parse_args(self):
        """
        Check that each element in args is at least a alid folder
        """

        self.input_dockings = []

        for argument in self.params.args:
            if os.path.isfile(argument) and argument.endswith(".sd"):
                self.input_dockings.append(os.path.abspath(argument))
            if os.path.isdir(argument):
                # walk through it and recursively search for valid .sd files
                for root,dirs,files in os.walk(argument):
                    for ff in files:
                        if ff.endswith(".sd"):
                            self.input_dockings.append(os.path.join(root,ff))

        if self.params.data_folder:
            assert os.path.isdir(self.params.data_folder)


    def new_tasks(self, extra):
        """
        For each input folder, create an instance of GrdockApplication
        """
        tasks = []

        for docking_file in self.input_dockings:

            # extract root folder name to be used as jobname
            jobname = os.path.basename(docking_file)
            res = re.findall('\d+.', docking_file, flags=re.IGNORECASE)
            if res:
                docking_index = res[-1]
            else:
                gc3libs.log.warning("Failed to extract index number from input "
                                    "dockig file %s." % docking_file)
                docking_index = "none"

            extra_args = extra.copy()

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                        'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                        'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                        'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                        'run_%s' % jobname)

            extra_args['rbdock_iterations'] = self.params.rbdock_iterations
            extra_args['data_folder'] = self.params.data_folder

            tasks.append(GrdockApplication(
                docking_file,
                docking_index,
                **extra_args))

        return tasks
