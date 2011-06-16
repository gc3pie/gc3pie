#! /usr/bin/env python
#
#   gmhc_coev.py -- Front-end script for submitting multiple `MHC_coev` jobs to SMSCG.
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
Front-end script for submitting multiple `MHC_coev` jobs to SMSCG.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gmhc_coev --help`` for program usage instructions.
"""
__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2011-06-15:
    * Initial release, forked off the ``ggamess`` sources.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


# ugly workaround for Issue 95,
# see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import gmhc_coev


# std module imports
import glob
import os
import shutil
import sys
import time

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript
from gc3libs.dag import SequentialTaskCollection


## custom application class

class GMhcCoevApplication(Application):
    """
    Custom class to wrap the execution of a single step of the
    ``MHC_coev_*` program by T. Wilson.
    """
    def __init__(self, executable, output_dir, latest_work=None, **kw):
        kw.setdefault('requested_memory', 1)
        kw.setdefault('requested_cores', 1)
        kw.setdefault('requested_architecture', Run.Arch.X86_64)
        bin = os.path.basename(executable)
        inputs = { executable:bin }
        if latest_work is not None:
            inputs[latest_work] = 'latest_work.mat'
        Application.__init__(self,
                             executable = '/usr/bin/env',
                             arguments = [
                                 'LD_LIBRARY_PATH=/opt/MATLAB/MATLAB_Compiler_Runtime/v713/runtime/glnxa64:$LD_LIBRARY_PATH',
                                 './' + bin
                                 ],
                             inputs = inputs,
                             outputs = gc3libs.ANY_OUTPUT,
                             output_dir = output_dir,
                             executables = [ bin ],
                             **kw)


class GMhcCoevTask(SequentialTaskCollection):
    """
    Custom class to wrap the execution of the ``MHC_coev_*` program by
    T. Wilson.  Execution continues in stepf of predefined duration
    until no ``latest_work.mat`` files are produced.  All the output
    files ever produced are collected in the path specified by the
    `output_dir` parameter to `__init__`.
    """

    def __init__(self, executable, single_run_duration, output_dir,
                 grid=None, **kw):

        """
        Create a new task running an ``MHC_coev`` binary.

        Each binary is expected to run for `single_run_duration`
        minutes and dump its state in file ``latest_work.mat`` if it's
        not finished. This task will continue re-submitting the same
        executable together with the saved workspace until no file
        ``latest_work.mat`` is created.

        :param str executable: Path to the ``MHC_coev`` executable binary.

        :param int single_run_duration: Duration of a single step in minutes.

        :param str output_dir: Path to a directory where output files
        from all runs should be collected.

        :param grid: See `TaskCollection`.
        """
        # remember values for later use
        self.executable = executable
        self.output_dir = output_dir
        self.single_run_duration = single_run_duration
        self.extra = kw

        self.jobname = kw.get('jobname',
                              os.path.basename(self.executable))

        # create initial task and register it
        initial_task = GMhcCoevApplication(executable,
                                           output_dir = os.path.join(output_dir, 'tmp'),
                                           required_walltime = single_run_duration + 1,
                                           **kw)
        SequentialTaskCollection.__init__(self, self.jobname, [initial_task], grid)

        
    def next(self, done):
        """
        Analyze the retrieved output and decide whether to submit
        another run or not, depending on whether there is a
        ``latest_work.mat`` file.
        """
        task_output_dir = self.tasks[done].output_dir
        # move files one level up
        for entry in os.listdir(task_output_dir):
            dest_entry = os.path.join(self.output_dir, entry)
            if os.path.exists(dest_entry):
                # backup with numerical suffix
                gc3libs.utils.backup(dest_entry)
            os.rename(os.path.join(task_output_dir, entry), dest_entry)
        os.removedirs(task_output_dir)
        # if a `latest_work.mat` file exists, then we need
        # more time to compute the required number of generations
        latest_work = os.path.join(self.output_dir, 'latest_work.mat')
        if os.path.exists(latest_work):
            self.add(
                GMhcCoevApplication(self.executable,
                                    output_dir = os.path.join(self.output_dir, 'tmp'),
                                    latest_work = latest_work,
                                    required_walltime = self.single_run_duration + 1,
                                    **self.extra))
            return Run.State.RUNNING
        else:
            self.execution.returncode = self.tasks[done].execution.returncode
            return Run.State.TERMINATED


## main script class
            
class GMhcCoevScript(SessionBasedScript):
    """
Scan the specified INPUT directories recursively for executable files
whose name starts with the string ``MHC_coev``, and submit a job for
each file found; job progress is monitored and, when a job is done,
its ``.sav`` output files are retrieved back into the same directory
where the executable file is (this can be overridden with the ``-o``
option).

The ``gmhc_coev`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``gmhc_coev`` will delay submission of
newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = gmhc_coev.GMhcCoevTask,
            input_filename_pattern = 'MHC_coev_*'
            )

    def new_tasks(self, extra):
        inputs = self._search_for_input_files(self.params.args)

        for path in inputs:
            kw = extra.copy()
            kw['output_dir'] = os.path.dirname(path)
            yield (gc3libs.utils.basename_sans(path),
                   self.application,
                   [path, self.params.walltime*60],
                   extra.copy())
        
# run it
if __name__ == '__main__':
    GMhcCoevScript().run()
