#! /usr/bin/env python
#
#   gcodeml.py -- Front-end script for submitting multiple CODEML jobs to SMSCG.
#
#   Copyright (C) 2010, 2011 GC3, University of Zurich
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
Front-end script for submitting multiple CODEML jobs to SMSCG.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gcodeml --help`` for program usage instructions.
"""
__version__ = '1.0rc3 (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2011-02-08:
    * Initial release, forked off the ``ggamess`` sources.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


import fnmatch
import logging
import os
import os.path
import re

import gc3libs
from gc3libs.application.codeml import CodemlApplication
from gc3libs.cmdline import SessionBasedScript


## the script itself

class GCodemlScript(SessionBasedScript):
    """
    Scan the specified INPUTDIR directories recursively for '.ctl' files,
    and submit a CODEML job for each input file found; job progress is
    monitored and, when a job is done, its '.mlc' file is retrieved back
    into the same directory where the '.ctl' file is (this can be
    overridden with the '-o' option).
    
    The `gcodeml` command keeps a record of jobs (submitted, executed and
    pending) in a session file (set name with the '-s' option); at each
    invocation of the command, the status of all recorded jobs is updated,
    output from finished jobs is collected, and a summary table of all
    known jobs is printed.  New jobs are added to the session if new input
    files are added to the command line.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; `gcodeml` will delay submission
    of newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = CodemlApplication,
            input_filename_pattern = '*.ctl'
            )

# run it
if __name__ == '__main__':
    GCodemlScript().run()
