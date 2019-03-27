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
    import grosetta
    grosetta.GRosettaScript().run()


# stdlib imports
import os
import os.path
import sys

# interface to Gc3libs
import gc3libs
from gc3libs.application.rosetta import RosettaApplication
from gc3libs.cmdline import SessionBasedScript, positive_int



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

    def setup_options(self):
        self.add_param("-P", "--total-decoys", dest="total_decoys",
                       type=positive_int, default=1,
                       metavar="NUM",
                       help="Compute NUM decoys per input file (default: %(default)s)."
                       )
        self.add_param("-p", "--decoys-per-job", dest="decoys_per_job",
                       type=positive_int, default=1,
                       metavar="NUM",
                       help="Compute NUM decoys in a single job (default: %(default)s)."
                       " This parameter should be tuned so that the running time"
                       " of a single job does not exceed the maximum wall-clock time."
                       )
        self.add_param("-x", "--protocol", dest="protocol",
                       type=str, default="minirosetta.static",
                       metavar="PROTOCOL",
                       help="Run the specified Rosetta protocol/application; default: %(default)s")
        self.add_param("-R", "--release",
                       type=str, dest="rosetta_release", default="3.1",
                       metavar="NAME",
                       help="Numerical suffix to identify which version of Rosetta should be requested."
                       " For example: '-e 20110622' will run rosetta-svn20110622."
                       " (default: %(default)s)"
                       )

    def parse_args(self):
        self.instances_per_file = self.params.total_decoys
        self.instances_per_job = self.params.decoys_per_job
        self.extra['number_of_decoys_to_create'] = self.params.decoys_per_job

        # parse positional arguments
        args = self.params.args
        if len(args) == 0:
            # no args given, so no new jobs added to the session
            self.flags_file = 'PLEASE GIVE FLAGS FILE ON THE COMMAND LINE'
            inputs = [ ]
            self.outputs = [ ]
        else:
            # if FLAGS is given, then we want at least one input file
            if len(args) < 2:
                raise RuntimeError("Incorrect usage; please run '%s --help' to read instructions." % self.name)
            try:
                self.flags_file = args[0]
                del args[0]
                if ':' in args:
                    separator = args.index(':')
                    inputs = args[:separator]
                    self.outputs = args[(separator+1):]
                else:
                    inputs = args
                    self.outputs = [ ]
            except:
                raise RuntimeError("Incorrect usage; please run '%s --help' to read instructions." % self.name)

            # make flags file path absolute
            if not os.path.isabs(self.flags_file):
                self.flags_file = os.path.abspath(self.flags_file)
            if not os.path.exists(self.flags_file):
                raise RuntimeError("Flags file '%s' does not exist." % self.flags_file)
            self.log.info("Using flags file '%s'", self.flags_file)

        # massage input file list to have only absolute paths
        inputs_ = [ ]
        for path in inputs:
            if not os.path.exists(path):
                self.log.error("Cannot access input path '%s' - aborting.", path)
                raise RuntimeError("Cannot access input path '%s' - aborting.", path)
            else:
                # make paths absolute
                if not os.path.isabs(path):
                    path = os.path.abspath(path)
                inputs_.append(path)
        self.inputs = inputs_
        #self.log.debug("Gathered input files: '%s'" % str.join("', '", inputs))

    def new_tasks(self, extra):
        ## compute number of decoys already being computed in this session
        decoys = 0
        for task in self.session:
            start, end = task.jobname.split('--')
            decoys += int(end) - int(start)
        self.log.debug("Total no. of decoys already scheduled for computation: %d", decoys)

        # add jobs to the session, until we are computing the specified number of decoys
        # XXX: if the number of requested decoys is lowered, we should cancel jobs!
        if decoys < self.params.total_decoys:
            if decoys > 0:
                self.log.info("Already computing %d decoys, requested %d more.",
                              decoys, self.params.total_decoys - decoys)
            # create new jobs and add them to session
            for nr in range(decoys, self.params.total_decoys, self.params.decoys_per_job):
                jobname = ("%d--%d"
                           % (nr, min(self.params.total_decoys,
                                      nr + self.params.decoys_per_job - 1)))
                # yield new job to construct to `self._main()`
                yield (
                    jobname, RosettaApplication,
                    # args
                    (self.params.protocol, self.params.rosetta_release, self.inputs, self.outputs),
                    # kwargs
                    {'arguments':[ '-out:nstruct', str(self.params.decoys_per_job) ],
                     'flags_file':self.flags_file,
                     },
                    )
