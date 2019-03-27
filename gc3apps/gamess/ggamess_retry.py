#! /usr/bin/env python
#
#   ggamess_retry.py -- Front-end script for submitting multiple GAMESS jobs to SMSCG.
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
Front-end script for submitting multiple GAMESS jobs to SMSCG.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``ggamess --help`` for program usage instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2011-08-03:
    * Experiment with retry policy.
  2010-12-20:
    * Initial release, forked off the ``grosetta`` sources.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


# workaround for Issue 95,
# see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import ggamess_retry


import os


import gc3libs
from gc3libs.application.gamess import GamessApplication
from gc3libs.cmdline import SessionBasedScript
from gc3libs.workflow import RetryableTask


## retry policy

class GamessRetryPolicy(RetryableTask):

    def __init__(self, inp_file_path, *other_input_files, **extra_args):
        """Constructor. Interface compatible with `GamessApplication`:class:"""
        if extra_args.has_key('tags'):
            extra_args['tags'].append('ENV/CPU/OPTERON-2350')
        else:
            extra_args['tags'] = [ 'ENV/CPU/OPTERON-2350' ]
        task = GamessApplication(inp_file_path, *other_input_files, **extra_args)
        RetryableTask.__init__(self, task, max_retries=3, **extra_args)


    def retry(self):
        # return True or False depending whether the application
        # should be re-submitted or not.
        gamess = self.task
        gamess_out = os.path.join(gamess.output_dir, gamess.stdout)
        if not os.path.exists(gamess_out):
            # no output, try again and hope for the best
            return True
        gamess_outfile = open(gamess_out, 'r')
        for line in gamess_outfile:
            if "gracefully" in line:
                # all OK
                return False
            if "ddikick.x: Timed out" in line:
                # try again
                return True
            if "Failed creating" in line:
                # try again
                return True
            if "I/O ERROR" in line:
                # try spreading over more cores and resubmit
                gamess.requested_cores *= 2
                return True



## the main script

class GGamessScript(SessionBasedScript):
    """
Scan the specified INPUT directories recursively for '.inp' files,
and submit a GAMESS job for each input file found; job progress is
monitored and, when a job is done, its '.out' and '.dat' file are
retrieved back into the same directory where the '.inp' file is (this
can be overridden with the '-o' option).

The `ggamess` command keeps a record of jobs (submitted, executed and
pending) in a session file (set name with the '-s' option); at each
invocation of the command, the status of all recorded jobs is updated,
output from finished jobs is collected, and a summary table of all
known jobs is printed.  New jobs are added to the session if new input
files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; `ggamess` will delay submission
of newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = ggamess_retry.GamessRetryPolicy,
            input_filename_pattern = '*.inp',
            # `GamessRetryPolicy` is the top-level object now,
            # so only print information about it.
            stats_only_for = ggamess_retry.GamessRetryPolicy,
            )

# run it
if __name__ == '__main__':
    GGamessScript().run()
