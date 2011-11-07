#! /usr/bin/env python
#
#   ggeotop.py -- Front-end script for submitting multiple `GEOtop` jobs to SMSCG.
#
#   Copyright (C) 2011 GC3, University of Zurich
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
"""
__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2011-11-07:
    * Initial release, forked off the ``gmhc_coev`` sources.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


# ugly workaround for Issue 95,
# see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import ggeotop


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

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task, RetryableTask
from gc3libs.cmdline import SessionBasedScript
from gc3libs.compat.collections import defaultdict
from gc3libs.dag import SequentialTaskCollection


## custom application class

class GeotopApplication(Application):
    """
    Custom class to wrap the execution of the ``GEOtop` program.

    For more information about GEOtop, see <http://www.goetop.org/>
    """
    def __init__(self, simulation_dir, executable=None, **kw):
        # remember for later
        self.simulation_dir = simulation_dir
        # stage all (non-hidden) files in the simulation directory for input
        inputs = dict(entry for entry in os.listdir(simulation_dir)
                      if not entry.startswith('.'))
        if executable is not None:
            # use the specified executable
            executable_name = './' + os.path.basename(executable)
            inputs[executable] = os.path.basename(executable)
        else:
            raise NotImplementedError("No RTE for GEOtop defined; please specify an executable!")
            # use the default one provided by the RTE
            executable_name = '/$GEOTOP'
        # set some execution defaults
        kw.setdefault('requested_cores', 1)
        kw.setdefault('requested_architecture', Run.Arch.X86_64)
        Application.__init__(
            self,
            executable = executable_name,
            # GEOtop requires only one argument: the simulation directory
            # In our case, since all input files are staged to the
            # execution directory, the only argument is fixed to ``.``
            arguments = [ '.' ],
            inputs = inputs,
            outputs = gc3libs.ANY_OUTPUT,
            output_dir = os.path.join(simulation_dir, 'tmp'),
            stdout = 'ggeotop.log',
            join=True,
            #tags = [ 'APPS/GEOTOP-1.223' ],
            **kw)


    def terminated(self):
        """
        Analyze the retrieved output and decide whether to submit
        another run or not, depending on whether tag files named
        ``_SUCCESSFUL_RUN`` or ``_FAILED_RUN`` are found.
        """
        tmp_output_dir = self.output_dir
        exclude = [
            os.path.basename(self.executable),
            self.stdout,
            self.stderr,
            ]
        # provisionally set exit code to 99 (resubmit), will override
        # later if the tag files ``_SUCCESSFUL_RUN`` or
        # ``_FAILED_RUN`` are found.
        self.execution.returncode = 99
        # move files one level up, except the ones listed in `exclude`
        for entry in os.listdir(tmp_output_dir):
            src_entry = os.path.join(tmp_output_dir, entry)
            # concatenate all output files together
            if entry == self.stdout:
                gc3libs.utils.cat(src_entry, output=os.path.join(self.simulation_dir, entry), append=True)
                continue
            if entry == self.stderr:
                gc3libs.utils.cat(src_entry, output=os.path.join(self.simulation_dir, entry), append=True)
                continue
            if entry in exclude or (entry.startswith('script.') and entry.endswith('.sh')):
                # delete entry and continue with next one
                os.remove(src_entry)
                continue
            # special files indicate successful or unsuccessful completion
            if entry in [ '_SUCCESSFUL_RUN', '_SUCCESSFUL_RUN.old' ]:
                self.execution.returncode = posix.EX_OK
            elif entry in [ '_FAILED_RUN', '_FAILED_RUN.old' ]:
                # use exit code 100 to indicate total failure
                self.execution.returncode = 100
            # now really move file one level up
            dest_entry = os.path.join(self.simulation_dir, entry)
            if os.path.exists(dest_entry):
                # backup with numerical suffix
                gc3libs.utils.backup(dest_entry)
            os.rename(os.path.join(tmp_output_dir, entry), dest_entry)
        os.removedirs(tmp_output_dir)


class GeotopTask(RetryableTask):

    def __init__(self, simulation_dir, executable=None, **kw):
        RetryableTask.__init__(
            self,
            # task name
            os.path.basename(simulation_dir),
            # actual computational job
            GeotopApplication(simulation_dir, executable, **kw),
            # keyword arguments
            **kw)

    def retry(self):
        """
        Resubmit a GEOtop application instance iff it exited with code 99.

        *Note:* There is currently no upper limit on the number of
        resubmissions!
        """
        if self.task.execution.exitcode == 99:
            return True
        else:
            return False


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
            application = ggeotop.GeotopTask,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = ggeotop.GeotopTask,
            )

    def setup_options(self):
        self.add_param("-x", "--executable", metavar="PATH",
                       dest="executable", default=None,
                       help="Path to the GEOtop executable file.")
        # change default for the "-o"/"--output" option
        #self.actions['output'].default = 'NPOPSIZE/PARAMS/ITERATION'


    @staticmethod
    def _valid_simulation_dir(path):
        """Return ``True`` if `path` is a valid GEOtop simulation directory."""
        return (os.path.isdir(path)
                and os.path.exists(os.path.join(path, 'geotop.inpts')))
    
    def new_tasks(self, extra):
        inputs = self._search_for_input_files(self.params.args,
                                              matches=self._valid_simulation_dir)
        for path in inputs:
            # construct GEOtop job
            yield (
                # job name
                gc3libs.utils.basename_sans(path),
                # application class
                GeotopTask,
                # parameters to `cls` constructor, see `GeotopTask.__init__`
                [ path, self.params.executable ],
                # keyword arguments, see `GeotopTask.__init__`
                extra.copy())

        

# run it
if __name__ == '__main__':
    GGeotopScript().run()
