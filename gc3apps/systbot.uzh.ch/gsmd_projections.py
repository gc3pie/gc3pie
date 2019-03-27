#! /usr/bin/env python
#
#   smd_projections.py -- Front-end script for submitting multiple
#   `smd_projections` jobs.
#
#   Copyright (C) 2013, 2014  University of Zurich. All rights reserved.
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
Front-end script for submitting multiple `GSMD_projections` jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``smd_projections --help`` for program usage instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """

  2014-02-27:
  * Initial release, forked off the ``gstructure`` sources.
"""
__author__ = 'Tyanko Aleksiev <tyanko.aleksiev@chem.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gsmd_projections
    gsmd_projections.GSMD_ProjectionsScript().run()

from pkg_resources import Requirement, resource_filename
import os
import posix
import tarfile

# gc3 library imports
import gc3libs.utils
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

## custom application class

class GSMD_ProjectionsApplication(Application):

    application_name = 'gsmd_projections'

    def __init__(self, **extra_args):
        """
        The wrapper script is being used for start the simulation.
        """
        files_to_send = []

        smd_projections_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/smd_projections_wrapper.sh")

        basename_input_tar = os.path.basename(extra_args['input_tar'])
        files_to_send.append((smd_projections_wrapper_sh,os.path.basename(smd_projections_wrapper_sh)))
        files_to_send.append((extra_args['input_tar'],basename_input_tar))


        cmd = "./smd_projections_wrapper.sh -d"

        if 'calibration' in extra_args:
            cmd += " -b "
            files_to_send.append((extra_args['calibration'],'calibration.tar'))

        cmd += " %s " % basename_input_tar

        cmd += " %s " % basename_input_tar.split('.')[0]

        extra_args['requested_memory'] = 6*GB

        self.output_dir = basename_input_tar.split('.')[0] + "_output"
        extra_args['output_dir'] = self.output_dir

        Application.__init__(
            self,
            # arguments should mimic the command line interfaca of the command to be
            # executed on the remote end
            arguments = cmd,
            inputs = files_to_send,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'smd_projections.log',
            join=True,
            **extra_args)

class GSMD_ProjectionsTask(RetryableTask, gc3libs.utils.Struct):
    """
    Run ``smd_projections`` on a given simulation directory until completion.
    """
    def __init__(self, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GSMD_ProjectionsApplication(**extra_args),
            # keyword arguments
            **extra_args)

## main script class

class GSMD_ProjectionsScript(SessionBasedScript):
    """
Scan the specified INPUT directories recursively for simulation
directories and submit a job for each one found; job progress is
monitored and, when a job is done, its output files are retrieved back
into the simulation directory itself.

The ``smd_projections`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``smd_projections`` will delay submission of
newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GSMD_ProjectionsTask,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GSMD_ProjectionsTask,
            )


    def setup_options(self):

        self.add_param("-b", "--calibration", metavar="CALIBRATION_DIR",
                       dest="calibration_dir", help="Use a different calibration directory.")

    def setup_args(self):

        self.add_param('input_source', type=str,
                       help="Projections input directory")

    def parse_args(self):

        self.params.input_source = os.path.abspath(self.params.input_source)


    def new_tasks(self, extra):

        tasks = []
        input_tars = []
        extra_args = extra.copy()

        cwd = os.getcwd()
        os.chdir(self.params.input_source)

        for projection_dir in self._list_local_folder(self.params.input_source):

            # Check if tar exists
            input_tar_file = projection_dir + ".tar"
            if os.path.isfile(input_tar_file):
                try:
                    os.remove(input_tar_file)
                except OSError, x:
                    gc3libs.log.error("Failed removing '%s': %s: %s",
                                      input_tar_file, x.__class__, x.message)
                    pass
            tar = tarfile.open(input_tar_file, "w:gz", dereference=True)
            tar.add(projection_dir)
            tar.close()
            input_tars.append(tar)

        os.chdir(cwd)


        # Create an archive of the calibration directory
        if os.path.isdir(self.params.calibration_dir):

            calibration_tar_file = "calibration.tar"

            if os.path.isfile(calibration_tar_file):
                try:
                    os.remove(calibration_tar_file)
                except OSError, x:
                    gc3libs.log.error("Failed removing '%s': %s: %s",
                                      calibration_tar_file, x.__class__, x.message)
                    pass

            tar = tarfile.open(calibration_tar_file, "w:gz", dereference=True)
            tar.add(self.params.calibration_dir)
            tar.close()

            extra_args['calibration'] = calibration_tar_file

        for input_tar in input_tars:

            jobname = "%s" % input_tar

            extra_args['jobname'] = jobname

            extra_args['input_tar'] = self.params.input_source + "/" + os.path.basename(input_tar.name)

            self.log.info("Creating Task for input file: %s" % input_tar.name)

            tasks.append(GSMD_ProjectionsTask(
                **extra_args
                ))

        return tasks

    def _list_local_folder(self, input_folder):
        """
        Return a list of all the directories in the input folder.
        """

        return [ infile for infile in os.listdir(input_folder) if os.path.isdir(os.path.join(input_folder,infile)) ]
