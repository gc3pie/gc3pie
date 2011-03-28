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
__version__ = '1.0rc6 (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2011-03-22:
    * New option ``-x`` to set the path to the ``codeml`` executable.
  2011-03-21:
    * Changed argument processing logic: no longer submit each
      ``.ctl`` file as a separate jobs, instead assume all the
      files in a single directory are related and bundle them into
      a single job.
    * New option ``-O`` to upload output files to a GridFTP
      server, instead of downloading them to a local destination.
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
import gc3libs.exceptions


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
            )


    def setup_options(self):
        # add some options to the std ones
        self.add_param("-O", "--output-url", action="store",
                       dest="output_base_url", default="",
                       help="Upload output files to this URL,"
                       "which must be a protocol that ARC supports."
                       "(e.g., 'gsiftp://...')")
        self.add_param("-x", "--codeml-executable", action="store",
                       dest="codeml", default="codeml", metavar="PATH",
                       help="Filesystem path to the CODEML executable.")
        # change default for the "-o"/"--output" option
        self.actions['output'].default = 'SESSION'


    def process_args(self, extra):
        """Implement the argument -> jobs mapping."""
        ## process additional options
        if not os.path.isabs(self.params.codeml):
            self.params.codeml = os.path.join(os.getcwd(), self.params.codeml)
        gc3libs.utils.test_file(self.params.codeml, os.R_OK|os.X_OK)

        ## do the argument -> job mapping, really
        inputs = set()

        def contain_ctl_files(paths):
            for path in paths:
                if path.endswith('.ctl'):
                    return True
            return False

        for path in self.params.args:
            self.log.debug("Now processing input argument '%s' ..." % path)
            if not os.path.isdir(path):
                raise gc3libs.exceptions.InvalidUsage(
                    "Argument '%s' is not a directory path." % path)
            
            # recursively scan for input files
            for dirpath, dirnames, filenames in os.walk(path):
                if contain_ctl_files(filenames):
                    inputs.add(os.path.realpath(dirpath))

        self.log.debug("Gathered input directories: '%s'"
                       % str.join("', '", inputs))

        for dirpath in inputs:
            # gather control files; other input files are
            # automatically pulled in by CodemlApplication by parsing
            # the '.ctl'
            ctl_files = [ os.path.join(dirpath, filename)
                          for filename in os.listdir(dirpath)
                          if filename.endswith('.ctl') ]
            # set optional arguments (path to 'codeml' binary, output URL, etc.)
            kwargs = extra.copy()
            kwargs.setdefault('codeml', self.params.codeml)
            if self.params.output_base_url != "":
                kwargs['output_base_url'] = self.params.output_base_url
            # yield new job
            yield ((os.path.basename(dirpath) or dirpath) + '.out',
                   CodemlApplication, ctl_files, kwargs)


# run it
if __name__ == '__main__':
    GCodemlScript().run()
