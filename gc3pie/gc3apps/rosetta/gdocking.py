#! /usr/bin/env python
#
#   gdocking.py -- Front-end script for submitting ROSETTA `docking_protocol` jobs to SMSCG.
#
#   Copyright (C) 2010-2011 GC3, University of Zurich
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

See the output of ``gdocking --help`` for program usage instructions.
"""
__version__ = '1.0rc6 (SVN $Revision$)'
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
# summary of user-visible changes
__changelog__ = """
  2011-02-10:
    * Renamed option '-t'/'--tarfile' to '-T'/'--collect', 
      in order not to conflict with `SessionBasedScript` 
      option '-t'/'--table'.
    * Removed the '-b' option.
  2010-12-20:
    * Renamed to ``gdocking``; default session file is not ``gdocking.csv``.
  2010-09-21:
    * Do not collect output FASC and PDB files into a single tar file. 
      (Get old behavior back with the '-t' option)
    * Do not compress PDB files in Rosetta output. 
      (Get the old behavior back with the '-z' option)
  2010-08-09:
    * Exitcode tracks job status; use the "-b" option to get the old behavior back.
      The new exitcode is a bitfield; the 4 least-significant bits have the following
      meaning:
         ===    ============================================================
         Bit    Meaning
         ===    ============================================================
         0      Set if a fatal error occurred: `grosetta` could not complete
         1      Set if there are jobs in `FAILED` state
         2      Set if there are jobs in `RUNNING` or `SUBMITTED` state
         3      Set if there are jobs in `NEW` state
         ===    ============================================================
      This boils down to the following rules:
         * exitcode == 0: all jobs are `DONE`, no further `grosetta` action
         * exitcode == 1: an error interrupted `grosetta` execution
         * exitcode == 2: all jobs finished, but some are in `FAILED` state
         * exitcode > 3: run `grosetta` again to progress jobs
    * when all jobs are finished, exit `grosetta` even if the "-C" option is given
    * Print only summary of job statuses; use the "-l" option to get the long listing
  2010-07-26:
    * Default output directory is now './' (should be less surprising to users).
    * FASC and PDBs are now collected in the output directory.
  2010-07-15:
    * After successful retrieval of job information, reorder output files so that:
      - for each sumitted job, there is a corresponding ``input.N--M.fasc`` file,
        in the same directory as the input ".pdb" file;
      - all decoys belonging to the same input ".pdb" file are collected into 
        a single ``input.decoys.tar`` file (in the same dir as the input ".pdb" file);
      - output from grid jobs is kept untouched in the "job.XXX/" directories.
    * Compress PDB files by default, and prefix them with a "source filename + N--M" prefix
    * Number of computed decoys can now be increased from the command line:
      if `grosetta` is called with different '-P' and '-p' options, it will
      add new jobs to the list so that the total number of decoys per input file
      (including already-submitted ones) is up to the new total.
    * New '-N' command-line option to discard old session contents and start a new session afresh.
  2010-07-14:
    * Default session file is now './grosetta.csv', so it's not hidden to users.
"""
__docformat__ = 'reStructuredText'


import grp
import os
import os.path
import pwd
import sys
import tarfile


## interface to Gc3libs

import gc3libs
from gc3libs.application.rosetta import GDockingApplication
from gc3libs.cmdline import SessionBasedScript


## the script class

class GDockingScript(SessionBasedScript):
    """
Compute decoys of specified '.pdb' files by running several
Rosetta 'docking_protocol' instances in parallel.

The `gdocking` command keeps a record of jobs (submitted, executed and
pending) in a session file; at each invocation of the command, the
status of all recorded jobs is updated, output from finished jobs is
collected, and a summary table of all known jobs is printed.

If any INPUT argument is specified on the command line, `gdocking`
appends new jobs to the session file, up to the quantity needed
to compute the requested number of decoys.  Each of the INPUT
parameters can be either a single '.pdb' file, or a directory, which
is recursively scanned for '.pdb' files.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; `gdocking` will delay submission
of newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GDockingApplication,
            input_filename_pattern = '*.pdb'
            )

    def setup_options(self):
        self.add_param("-f", "--flags-file", dest="flags_file", 
                       default=os.path.join(gc3libs.Default.RCDIR, 'docking_protocol.flags'),
                       metavar="PATH",
                       help="Pass the specified flags file to Rosetta 'docking_protocol'"
                       " Default: '%(default)s'"
                       )
        self.add_param("-P", "--decoys-per-file", type=int, dest="decoys_per_file", 
                       default=1,
                       metavar="NUM",
                       help="Compute NUM decoys per input file (default: %(default)s)."
                       )
        self.add_param("-p", "--decoys-per-job", type=int, dest="decoys_per_job", 
                       default=1,
                       metavar="NUM",
                       help="Compute NUM decoys in a single job (default: %(default)s)."
                       " This parameter should be tuned so that the running time"
                       " of a single job does not exceed the maximum wall-clock time."
                       )
        self.add_param("-T", "--collect", dest="collect", default=False, action="store_true",
                       help="Collect all output PDB and FASC/SC files into a single '.tar' file,"
                       " located in the output directory (see the '-o' option)."
                       )
        self.add_param("-z", "--compress-pdb", dest="compress", default=False, action="store_true",
                       help="Compress '.pdb' output files with `gzip`."
                       )

    def parse_args(self):
        if self.params.decoys_per_file < 1:
            raise RuntimeError("Argument to option -P/--decoys-per-file must be a positive integer.")
        self.instances_per_file = self.params.decoys_per_file

        if self.params.decoys_per_job < 1:
            raise RuntimeError("Argument to option -p/--decoys-per-job must be a positive integer.")
        self.instances_per_job = self.params.decoys_per_job

        self.extra['number_of_decoys_to_create'] = self.params.decoys_per_job
        self.extra['collect'] = self.params.collect

        if not os.path.isabs(self.params.flags_file):
            self.params.flags_file = os.path.join(os.getcwd(), self.params.flags_file)
        if not os.path.exists(self.params.flags_file):
            raise RuntimeError("Flags file '%s' does not exist." % self.params.flags_file)
        self.extra['flags_file'] = self.params.flags_file
        self.log.info("Using flags file '%s'", self.params.flags_file)


## run it
if __name__ == '__main__':
    GDockingScript().run()
