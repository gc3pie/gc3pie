#! /usr/bin/env python
#
#   grosetta.py -- Front-end script for submitting ROSETTA jobs to SMSCG.
#
#   Copyright (C) 2010, 2011, 2012  University of Zurich. All rights reserved.
#
#   This program is free software: you can redistribute it and/or modify
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
Front-end script for submitting ROSETTA jobs to SMSCG.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``grosetta --help`` for program usage instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2012-02-28:
    * Use option ``-R`` to request a specific release of Rosetta.
  2011-03-28:
    * No longer require any input file on the command line: e.g., ``grosetta -l``
      will just list the status of jobs in the current session.  On the other hand,
      if the FLAGS file is given, then given must be also at least one input file.
  2010-12-20:
    * Initial release, forking off the old `grosetta`/`gdocking` sources.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'



# workaround Issue 95, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == '__main__':
    import grosetta2015
    grosetta2015.GRosettaScript().run()


# stdlib imports
import os
import os.path
import sys

from pkg_resources import Requirement, resource_filename

# interface to Gc3libs
import gc3libs
from gc3libs.application.rosetta import RosettaApplication
from gc3libs.cmdline import SessionBasedScript, positive_int
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

INPUT_PDB_EXTENSION="pdb"
ROSETTA_OPTION_FILE="options"
INPUT_LIST_PATTERNS = [INPUT_PDB_EXTENSION,ROSETTA_OPTION_FILE]

## the application class
## custom application class
class Rosetta2015Application(Application):
    """
    """
    application_name = 'grosetta2015'

    def __init__(self, input_folder, command_to_run, **extra_args):

        inputs = dict()
        outputs = dict()

        # grosetta2015_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
        #                                       "gc3libs/etc/rosetta2015.sh")
        # inputs[grosetta2015_wrapper_sh] = os.path.basename(grosetta2015_wrapper_sh)

        for input in os.listdir(input_folder):
            inputs[os.path.join(input_folder,input)] = os.path.basename(input)

        # arguments = "./%s --no-tar @options" % (inputs[grosetta2015_wrapper_sh])

        Application.__init__(
            self,
            arguments = command_to_run,
            inputs = inputs,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'grosetta2015.log',
            join=True,
            **extra_args)

## the script class
class GRosettaScript(SessionBasedScript):
    """
Run MiniRosetta on the specified INPUT files and fetch OUTPUT files
back from the execution machine; if OUTPUT is omitted, all '*.pdb',
'*.sc' and '*.fasc' files are retrieved.  Several instances can be run in
parallel, depending on the '-P' and '-p' options.

The `grosetta` command keeps a record of jobs (submitted, executed and
pending) in a session file (set name with the '-s' option); at each
invocation of the command, the status of all recorded jobs is updated,
output from finished jobs is collected, and a summary table of all
known jobs is printed.  New jobs are added to the session if the number
of wanted decoys (option '-P') is raised.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; `grosetta` will delay submission
of newly-created jobs so that this limit is never exceeded.

Note: the list of INPUT and OUTPUT files must be separated by ':'
(on the shell command line, put a space before and after).
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            usage = "%(prog)s [options] FLAGSFILE INPUT ... [: OUTPUT ...]",
            version = __version__, # module version == script version
            application = RosettaApplication,
            )

    def setup_args(self):

        self.add_param('input_pdb', type=str,
                       help="Root localtion of input .pdb pais.")

        self.add_param('command_to_run', type=str,
                       help="Rosetta related command to be executed. "
                       "Note: no control over the consistency nor the availability "
                       " of the command will be made.")

    def parse_args(self):

        self.input_pdbs = dict()

        self.input_folders = [ os.path.join(self.params.input_pdb,folder) for folder in os.listdir(self.params.input_pdb) ]

        # for r,d,f in os.walk(self.params.input_pdb):
        #     input_pdb = []
        #     for infile in f:
        #         if any(infile.endswith(pattern) for pattern in INPUT_LIST_PATTERNS):
        #             input_pdb.append(os.path.join(r,infile))
        #     # XXX: Validation of input files to be done
        #     if input_pdb:
        #         self.input_pdbs[r] = input_pdb

        self.log.debug("Found '%d' valid input folders", len(self.input_folders))

    def new_tasks(self, extra):
        tasks = []

        for input_folder in self.input_folders:
            # extract root folder name to be used as jobname
            pairname = input_folder

            extra_args = extra.copy()
            extra_args['jobname'] = pairname

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                        'run_%s' % pairname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                        'run_%s' % pairname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                        'run_%s' % pairname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                        'run_%s' % pairname)

            gc3libs.log.debug("Adding task for folder %s" % input_folder)

            tasks.append(Rosetta2015Application(
                input_folder,
                self.params.command_to_run,
                **extra_args))
        return tasks
