#! /usr/bin/env python
#
#   gdocking.py -- Front-end script for submitting ROSETTA `docking_protocol` jobs to SMSCG.
#
#   Copyright (C) 2010-2012  University of Zurich. All rights reserved.
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

from __future__ import absolute_import, print_function

__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
# summary of user-visible changes
__changelog__ = """
  2012-02-28:
    * Use option ``-R`` to request a specific release of Rosetta.
  2011-05-06:
    * Workaround for Issue 95: now we have complete interoperability
      with GC3Utils.
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


# workaround for Issue 95,
# see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gdocking
    gdocking.GDockingScript().run()


# stdlib imports
import grp
import logging
import os
import os.path
import pwd
import sys
import tarfile
import time

# interface to Gc3libs
import gc3libs
from gc3libs.application.rosetta import RosettaDockingApplication
from gc3libs.cmdline import SessionBasedScript, existing_file, positive_int


## The GDocking application

class GDockingApplication(RosettaDockingApplication):
    """
    Augment a `RosettaDockingApplication` with state transition
    methods that implement job status reporting for the UI, and data
    post-processing.
    """
    def __init__(self, pdb_file_path, native_file_path=None,
                 number_of_decoys_to_create=1, flags_file=None,
                 collect=False, **extra_args):
        RosettaDockingApplication.__init__(
            self, pdb_file_path, native_file_path,
            number_of_decoys_to_create, flags_file,
            **extra_args)
        # save pdb_file_path for later processing
        self.pdb_file_path = pdb_file_path
        # define additional attributes
        self.collect = collect, # whether to collect result PDBs into a tarfile
        self.computed = 0 # number of decoys actually computed by this job

    def terminated(self):
        output_dir = self.output_dir
        # work directory is the parent of the download directory
        work_dir = os.path.dirname(output_dir)
        # move around output files so they're easier to preprocess:
        #   1. All '.fasc' files land in the same directory as the input '.pdb' file
        #   2. All generated '.pdb'/'.pdb.gz' files are collected in a '.decoys.tar'
        #   3. Anything else is left as-is
        input_name = os.path.basename(self.pdb_file_path)
        input_name_sans = os.path.splitext(input_name)[0]
        output_tar_filename = os.path.join(output_dir, 'docking_protocol.tar.gz')
        # count: 'protocols.jobdist.main: Finished 1brs.0--1.1brs_0002 in 149 seconds.'
        if os.path.exists(output_tar_filename):
            output_tar = tarfile.open(output_tar_filename, 'r:gz')
            # single tar file holding all decoy .PDB files
            pdbs_tarfile_path = os.path.join(work_dir, input_name_sans) + '.decoys.tar'
            if self.collect:
                if not os.path.exists(pdbs_tarfile_path):
                    pdbs = tarfile.open(pdbs_tarfile_path, 'w')
                else:
                    pdbs = tarfile.open(pdbs_tarfile_path, 'a')
            for entry in output_tar:
                if (entry.name.endswith('.fasc') or entry.name.endswith('.sc')):
                    filename, extension = os.path.splitext(entry.name)
                    scoring_file_name = (os.path.join(work_dir, input_name_sans)
                                         + '.' + self.jobname + extension)
                    src = output_tar.extractfile(entry)
                    dst = open(scoring_file_name, 'wb')
                    dst.write(src.read())
                    dst.close()
                    src.close()
                elif (self.collect and
                      (entry.name.endswith('.pdb.gz') or entry.name.endswith('.pdb'))):
                    src = output_tar.extractfile(entry)
                    dst = tarfile.TarInfo(entry.name)
                    dst.size = entry.size
                    dst.type = entry.type
                    dst.mode = entry.mode
                    dst.mtime = entry.mtime
                    dst.uid = os.getuid()
                    dst.gid = os.getgid()
                    dst.uname = pwd.getpwuid(os.getuid()).pw_name
                    dst.gname = grp.getgrgid(os.getgid()).gr_name
                    if hasattr(entry, 'pax_headers'):
                        dst.pax_headers = entry.pax_headers
                    pdbs.addfile(dst, src)
                    src.close()
            if self.collect:
                pdbs.close()
        else: # no `docking_protocol.tar.gz` file
            self.info = ("No 'docking_protocol.tar.gz' file found.")


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
            # Use fully-qualified name for class,
            # to allow GC3Utils to unpickle job instances
            # see: https://github.com/uzh/gc3pie/issues/95
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
        self.add_param("-P", "--decoys-per-file", dest="decoys_per_file",
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
        self.add_param("-R", "--release",
                       type=str, dest="rosetta_release", default="3.1",
                       metavar="NAME",
                       help="Numerical suffix to identify which version of Rosetta should be requested."
                       " For example: '-e 20110622' will run rosetta-svn20110622."
                       " (default: %(default)s)"
                       )
        self.add_param("-T", "--collect", dest="collect", default=False, action="store_true",
                       help="Collect all output PDB and FASC/SC files into a single '.tar' file,"
                       " located in the output directory (see the '-o' option)."
                       )
        self.add_param("-z", "--compress-pdb", dest="compress", default=False, action="store_true",
                       help="Compress '.pdb' output files with `gzip`."
                       )

    def parse_args(self):
        self.instances_per_file = self.params.decoys_per_file
        self.instances_per_job = self.params.decoys_per_job

        self.extra['application_release'] = self.params.rosetta_release
        self.extra['number_of_decoys_to_create'] = self.params.decoys_per_job
        self.extra['collect'] = self.params.collect

        try:
            existing_file(self.params.flags_file)
        except Exception, ex:
            gc3libs.log.error("Invalid flags file `%s`: please supply a valid flags file by adding the option `-f PATH`" % self.params.flags_file)
            raise ex

        self.extra['flags_file'] = os.path.abspath(self.params.flags_file)
        self.log.info("Using flags file '%s'", self.extra['flags_file'])
