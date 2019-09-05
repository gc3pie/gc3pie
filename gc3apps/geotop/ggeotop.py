#! /usr/bin/env python
#
#   ggeotop.py -- Front-end script for submitting multiple `GEOtop` jobs to SMSCG.
#
#   Copyright (C) 2012, 2013  University of Zurich. All rights reserved.
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
Front-end script for submitting multiple `GEOtop` jobs to SMSCG.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``ggeotop --help`` for program usage instructions.

Contents of a typical input folder::

    svf.asc
    asp.asc
    slp.asc
    dem.asc
    geotop.inpts
    rad/
    in/
    rec/
    maps/
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2013-11-05:
  * Support for local filesystem

  2012-01-19:
  * Added mechanism to validate input folder on more detailed pattern
  * compression of input done only on those folder/files known (as
    opposite as before that was done with exclusion)

  2011-11-07:
  * Initial release, forked off the ``gmhc_coev`` sources.
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import ggeotop
    ggeotop.GGeotopScript().run()


# std module imports
import csv
import glob
import math
import os
import posix
import re
import shutil
import sys
import time

import tarfile

from pkg_resources import Requirement, resource_filename

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask


GEOTOP_INPUT_ARCHIVE = "input.tgz"
GEOTOP_OUTPUT_ARCHIVE = "output.tgz"

## custom application class

class GeotopApplication(Application):
    """
    Custom class to wrap the execution of the ``GEOtop` program.

    For more information about GEOtop, see <http://www.geotop.org/>
    """
    # From GEOtop's "read me" file:
    #
    # RUNNING
    # Run this simulation by calling the executable (GEOtop_1.223_static)
    # and giving the simulation directory as an argument.
    #
    # EXAMPLE
    # ls2:/group/geotop/sim/tmp/000001>./GEOtop_1.223_static ./
    #
    # TERMINATION OF SIMULATION BY GEOTOP
    # When GEOtop terminates due to an internal error, it mostly reports this
    # by writing a corresponding file (_FAILED_RUN or _FAILED_RUN.old) in the
    # simulation directory. When is terminates sucessfully this file is
    # named (_SUCCESSFUL_RUN or _SUCCESSFUL_RUN.old).
    #
    # RESTARTING SIMULATIONS THAT WERE TERMINATED BY THE SERVER
    # When a simulation is started again with the same arguments as described
    # above (RUNNING), then it continues from the last saving point. If
    # GEOtop finds a file indicating a successful/failed run, it terminates.

    application_name = 'geotop'

    def _scan_and_tar(self, simulation_dir):
        try:
            gc3libs.log.debug("Compressing input folder '%s'" % simulation_dir)
            cwd = os.getcwd()
            os.chdir(simulation_dir)
            # check if input archive already present. If so, remove it
            # XXX: improvement: add to the current archive only newer files from 'rec' folder
            if os.path.isfile(GEOTOP_INPUT_ARCHIVE):
                try:
                    os.remove(GEOTOP_INPUT_ARCHIVE)
                except OSError, x:
                    gc3libs.log.error("Failed removing '%s': %s: %s",
                                      GEOTOP_INPUT_ARCHIVE, x.__class__, x.message)
                    pass

            tar = tarfile.open(GEOTOP_INPUT_ARCHIVE, "w:gz", dereference=True)
            tar.add('./geotop.inpts')
            tar.add('./in')
            tar.add('./out')
            # tar.add('./maps')
            # tar.add('./rec')
            # tar.add('./rad')
            # tar.add('./svf.asc')
            # tar.add('./slp.asc')
            # tar.add('./asp.asc')
            # tar.add('./dem.asc')
            tar.close()
            os.chdir(cwd)
            yield (tar.name, GEOTOP_INPUT_ARCHIVE)
        except Exception, x:
            gc3libs.log.error("Failed creating input archive '%s': %s: %s",
                              os.path.join(simulation_dir, GEOTOP_INPUT_ARCHIVE),
                              x.__class__,x.message)
            raise

    def __init__(self, simulation_dir, executable=None, **extra_args):
        # remember for later
        self.simulation_dir = simulation_dir
        self.shared_FS = extra_args['shared_FS']

        inputs = dict()

        # execution wrapper needs to be added anyway
        geotop_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/geotop_wrap.sh")
        inputs[geotop_wrapper_sh] = os.path.basename(geotop_wrapper_sh)

        _command = "./%s " % os.path.basename(geotop_wrapper_sh)

        # If shared_FS, no inputs are defined
        # as they are already available on the computational nodes
        if not self.shared_FS:
            # compress input folder
            inputs.update(dict(self._scan_and_tar(simulation_dir)))

            # set ANY_OUTPUT for output
            outputs = gc3libs.ANY_OUTPUT

            # Set executable name and include in input list
            if executable is not None:
                # use the specified executable
                # include executable within input list
                executable_name = './' + os.path.basename(executable)
                inputs[executable] = os.path.basename(executable)

            # use '-l' flag for wrapper script for non-shared FS
            _command += "input.tgz "
        else:
            # sharedFS: everything is local
            executable_name = os.path.abspath(executable)
            _command += " %s " % os.path.abspath(self.simulation_dir)
            outputs = []

        _command += "%s" % executable_name

        # set some execution defaults...
        extra_args.setdefault('requested_cores', 1)
        extra_args.setdefault('requested_architecture', Run.Arch.X86_64)
        extra_args.setdefault('requested_walltime', Duration(8, hours))
        # ...and remove excess ones
        extra_args.pop('output_dir', None)
        Application.__init__(
            self,
            # GEOtop requires only one argument: the simulation directory
            # In our case, since all input files are staged to the
            # execution directory, the only argument is fixed to ``.``
            # arguments = ['./'+os.path.basename(geotop_wrapper_sh), 'input.tgz', executable_name ],
            arguments = _command,
            inputs = inputs,
            # outputs = gc3libs.ANY_OUTPUT,
            outputs = outputs,
            output_dir = os.path.join(simulation_dir, 'tmp'),
            stdout = 'ggeotop.log',
            join=True,
            tags = [ 'APPS/EARTH/GEOTOP-1.224' ],
            **extra_args)


    def terminated(self):
        """
        Analyze the retrieved output and decide whether to submit
        another run or not, depending on whether tag files named
        ``_SUCCESSFUL_RUN`` or ``_FAILED_RUN`` are found.
        """
        # provisionally set exit code to 99 (resubmit), will override
        # later if the tag files ``_SUCCESSFUL_RUN`` or
        # ``_FAILED_RUN`` are found.
        self.execution.returncode = (0, 99)


        if not self.shared_FS:
            full_tarname = os.path.join(self.output_dir, GEOTOP_OUTPUT_ARCHIVE)

            # check and unpack output archive
            if os.path.isfile(full_tarname):
                # execution somehow terminated.
                # untar archive
                gc3libs.log.info("Expected output archive found in file '%s'",
                                 full_tarname)
                try:
                    tar = tarfile.open(full_tarname)
                    gc3libs.log.debug("Output tarfile '%s' contains: %s",
                                      full_tarname, str.join(', ', tar.getnames()))
                    tar.extractall(path=self.simulation_dir)
                    tar.close()
                    os.remove(full_tarname)
                except Exception, ex:
                    gc3libs.log.error("Error opening output archive '%s': %s: %s",
                                      full_tarname, ex.__class__, ex.message)
                    pass

            tmp_output_dir = self.output_dir
            exclude = [
                os.path.basename(self.arguments[0]),
                self.stdout,
                self.stderr,
                GEOTOP_OUTPUT_ARCHIVE,
                ]

            if not os.path.isdir(tmp_output_dir):
                # output folder not available
                # log failure and stop here
                gc3libs.log.warning("Output folder '%s' not found. Cannot process any further" % tmp_output_dir)
                # use exit code 100 to indicate total failure
                # XXX: When this happens ?
                self.execution.returncode = (0, 100)
            else:
                # move files one level up, except the ones listed in `exclude`
                for entry in os.listdir(tmp_output_dir):
                    src_entry = os.path.join(tmp_output_dir, entry)
                    gc3libs.log.debug("Considering entry '%s' ...", src_entry)
                    # concatenate all output files together
                    if entry == self.stdout:
                        gc3libs.utils.cat(src_entry, output=os.path.join(self.simulation_dir, entry), append=True)
                        # try remove it
                        os.remove(src_entry)
                        gc3libs.log.debug("  ... appended to '%s'", os.path.join(self.simulation_dir, entry))
                        continue
                    if entry == self.stderr:
                        gc3libs.utils.cat(src_entry, output=os.path.join(self.simulation_dir, entry), append=True)
                        # try remove it
                        os.remove(src_entry)
                        gc3libs.log.debug("  ... appended to '%s'", os.path.join(self.simulation_dir, entry))
                        continue
                    if entry in exclude or (entry.startswith('script.') and entry.endswith('.sh')):
                        # delete entry and continue with next one
                        os.remove(src_entry)
                        gc3libs.log.debug("  ... it's a GC3Pie auxiliary file, ignore it!",)
                        continue

                    # now really move file one level up
                    dest_entry = os.path.join(self.simulation_dir, entry)
                    if os.path.exists(dest_entry):
                        # backup with numerical suffix
                        # gc3libs.utils.backup(dest_entry)
                        shutil.rmtree(dest_entry, ignore_errors=True)
                    os.rename(os.path.join(tmp_output_dir, entry), dest_entry)
                    gc3libs.log.debug("  ... moved to '%s'", os.path.join(dest_entry))
                # os.removedirs(tmp_output_dir)
                shutil.rmtree(tmp_output_dir, ignore_errors=True)

        # search for termination files
        if (os.path.exists(os.path.join(self.simulation_dir, '_SUCCESSFUL_RUN'))
            or os.path.exists(os.path.join(self.simulation_dir, 'out', '_SUCCESSFUL_RUN'))):
            # XXX: why are we looking for '.old' files??
            # or os.path.exists(os.path.join(self.simulation_dir, '_SUCCESSFUL_RUN.old'))):
            # or os.path.exists(os.path.join(self.simulation_dir,'out', '_SUCCESSFUL_RUN.old'))):
            self.execution.returncode = (0, posix.EX_OK)
        elif (os.path.exists(os.path.join(self.simulation_dir, '_FAILED_RUN'))
              or os.path.exists(os.path.join(self.simulation_dir, '_FAILED_RUN'))):
            # XXX: why are we looking for '.old' files??
            # or os.path.exists(os.path.join(self.simulation_dir, 'out', '_FAILED_RUN.old'))
            # or os.path.exists(os.path.join(self.simulation_dir, 'out', '_FAILED_RUN.old'))):
            # use exit code 100 to indicate total failure
            self.execution.returncode = (0, 100)
        else:
            # should be resubmitted
            # call _scan_and_tar to create new archive
            # XXX: To consider a better way of handling this
            # at the moment the entire input archive is recreated


            # XXX: How to distinguish a failed geotop execution from one terminated
            # by the LRMS ?

            gc3libs.log.warning("Simulation did *not* produce any output marker"
                                "[_SUCCESSFUL_RUN,_FAILED_RUN].")
            self.execution.returncode = (0,98)
            if not self.shared_FS:
                gc3libs.log.info("Updating tar archive for resubmission.")
                inputs = dict(self._scan_and_tar(self.simulation_dir))
            # self._scan_and_tar(self.simulation_dir)


class GeotopTask(RetryableTask, gc3libs.utils.Struct):
    """
    Run ``geotop`` on a given simulation directory until completion.
    """
    def __init__(self, simulation_dir, executable=None, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GeotopApplication(simulation_dir, executable, **extra_args),
            # keyword arguments
            **extra_args)

    # def retry(self):
    #     """
    #     Resubmit a GEOtop application instance iff it exited with code 99.

    #     *Note:* There is currently no upper limit on the number of
    #     resubmissions!
    #     """
    #     if self.task.execution.exitcode != 100 or self.task.execution.exitcode != 0:
    #         # Candidate for retry
    #         # Let's check how many times it has been restarted yet without producing
    #         # any output
    #         if self.retried > self.max_retries:
    #             gc3libs.log.error("Maximum number of retries '%d' reached."
    #                               "Cloud not continue." % self.retried)
    #         else:
    #             return True
    #     return False


## main script class

class GGeotopScript(SessionBasedScript):
    """
Scan the specified INPUT directories recursively for simulation
directories and submit a job for each one found; job progress is
monitored and, when a job is done, its output files are retrieved back
into the simulation directory itself.

A simulation directory is defined as a directory containing a
``geotop.inpts`` file.

The ``ggeotop`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``ggeotop`` will delay submission of
newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GeotopTask,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GeotopTask,
            )

    def setup_options(self):
        self.add_param("-x", "--executable", metavar="PATH", #type=executable_file,
                       dest="executable", default=None,
                       help="Path to the GEOtop executable file.")

        # self.add_param("-q", "--summary", metavar="TIME", dest="summary_period",
        #                default=1800,
        #                help="Creates a summary of the current execution.")

        self.add_param("-S", "--sharedfs", dest="shared_FS",
                       action="store_true", default=False,
                       help="Whether the destination resource should assume shared filesystem where Input/Output data will be made available. Data transfer will happen through lcoal filesystem. Default: False.")

    def parse_args(self):
        """
        Check validity and consistency of command-line options.
        """
        if self.params.executable is None:
            raise gc3libs.exceptions.InvalidUsage(
                "Use the '-x' option to specify a valid path to the GEOtop executable.")
        if not os.path.exists(self.params.executable):
            raise gc3libs.exceptions.InvalidUsage(
                "Path '%s' to the GEOtop executable does not exist;"
                " use the '-x' option to specify a valid one."
                % self.params.executable)
        gc3libs.utils.check_file_access(self.params.executable, os.R_OK|os.X_OK,
                                gc3libs.exceptions.InvalidUsage)


    def new_tasks(self, extra):
        # input_files = self._search_for_input_files(self.params.args, 'geotop.inpts')

        # the real input to GEOtop are the directories containing `geotop.inpts`
        # as well as 'in' and 'out' fodlers
        for path in self._validate_input_folders(self.params.args):
            # construct GEOtop job

            args = extra.copy()
            args['shared_FS'] = self.params.shared_FS

            yield GeotopTask(
                path,                   # path to the directory containing input files
                os.path.abspath(self.params.executable), # path to the GEOtop executable
                # job name
                jobname=gc3libs.utils.basename_sans(path),
                # extra keyword arguments passed to the constructor,
                # see `GeotopTask.__init__`
                **args
                )

    def _validate_input_folders(self, paths):
        """
        Recursively scan each location in list `paths` for files
        matching a glob pattern, and return the set of path names to
        such files.

        By default, the value of `self.input_filename_pattern` is used
        as the glob pattern to match file names against, but this can
        be overridden by specifying an explicit argument `pattern`.
        """
        for path in paths:
            self.log.debug("Now processing input path '%s' ..." % path)
            if os.path.isdir(path):
                # recursively scan for input files
                for dirpath, dirnames, filenames in os.walk(path):
                    if ("geotop.inpts" in filenames
                        and "in" in dirnames
                        and "out" in dirnames
                        and not dirpath.endswith("/tmp")
                        and not dirpath.endswith("~")
                        and not os.path.isfile(os.path.join(dirpath, 'out', '_SUCCESSFUL_RUN'))
                        and not os.path.isfile(os.path.join(dirpath, 'out', '_FAILED_RUN'))
                        ):
                        # Return absolute path
                        yield os.path.abspath(dirpath)
                    else:
                        gc3libs.log.warning(
                            "Ignoring path '%s':"
                            " will not be included in the simulation input bundle.",
                            dirpath)
            else:
                gc3libs.log.warning("Ignoring input path '%s': not a directory.",
                                    path)
