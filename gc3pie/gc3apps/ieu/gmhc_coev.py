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
import re
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
    def __init__(self, executable,
                 N, p_mut_coeff, choose_or_rand, sick_or_not, off_v_last,
                 output_dir, latest_work=None, **kw):
        kw.setdefault('requested_memory', 1)
        kw.setdefault('requested_cores', 1)
        kw.setdefault('requested_architecture', Run.Arch.X86_64)
        self.executable_name = os.path.basename(executable)
        self.p_mut_coeff = p_mut_coeff,
        self.N = N,
        self.choose_or_rand = choose_or_rand,
        self.sick_or_not = sick_or_not,
        self.off_v_last = off_v_last,
        inputs = { executable:self.executable_name }
        if latest_work is not None:
            inputs[latest_work] = 'latest_work.mat'
        Application.__init__(self,
                             executable = '/usr/bin/env',
                             arguments = [
                                 'LD_LIBRARY_PATH=/opt/MATLAB/MATLAB_Compiler_Runtime/v713/runtime/glnxa64:$LD_LIBRARY_PATH',
                                 './' + self.executable_name,
                                 kw.get('requested_walltime')*60, # == single_run_time == ses_t
                                 N, p_mut_coeff, choose_or_rand, sick_or_not, off_v_last,
                                 ],
                             inputs = inputs,
                             outputs = gc3libs.ANY_OUTPUT,
                             output_dir = output_dir,
                             executables = [ self.executable_name ],
                             stdout = 'matlab.log',
                             stderr = 'matlab.err',
                             **kw)


class GMhcCoevTask(SequentialTaskCollection):
    """
    Custom class to wrap the execution of the ``MHC_coev_*` program by
    T. Wilson.  Execution continues in stepf of predefined duration
    until no ``latest_work.mat`` files are produced.  All the output
    files ever produced are collected in the path specified by the
    `output_dir` parameter to `__init__`.
    """

    def __init__(self, executable, single_run_duration, generations_to_do,
                 p_mut_coeff, N, choose_or_rand, sick_or_not, off_v_last,
                 output_dir, 
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
        self.generations_to_do = generations_to_do
        self.p_mut_coeff = p_mut_coeff,
        self.N = N,
        self.choose_or_rand = choose_or_rand,
        self.sick_or_not = sick_or_not,
        self.off_v_last = off_v_last,
        self.extra = kw

        self.generations_done = 0

        self.jobname = kw.get('jobname',
                              os.path.basename(self.executable))

        # create initial task and register it
        initial_task = GMhcCoevApplication(executable,
                                           N, p_mut_coeff, choose_or_rand, sick_or_not, off_v_last,
                                           output_dir = os.path.join(output_dir, 'tmp'),
                                           required_walltime = single_run_duration + 1,
                                           **kw)
        SequentialTaskCollection.__init__(self, self.jobname, [initial_task], grid)

    # regular expression for extracting the generation no. from an output file name
    GENERATIONS_FILENAME_RE = re.compile(r'(?P<generation_no>[0-9]+)gen\.mat$')
        
    def next(self, done):
        """
        Analyze the retrieved output and decide whether to submit
        another run or not, depending on whether there is a
        ``latest_work.mat`` file.
        """
        task_output_dir = self.tasks[done].output_dir
        exclude = [
            os.path.basename(self.tasks[done].executable),
            os.path.basename(self.tasks[done].executable_name),
            self.tasks[done].stdout,
            self.tasks[done].stderr,
            ]
        # move files one level up, except the ones listed in `exclude`
        for entry in os.listdir(task_output_dir):
            src_entry = os.path.join(task_output_dir, entry)
            # concatenate all output files together
            if entry == self.tasks[done].stdout:
                gc3libs.utils.cat(src_entry, output=os.path.join(self.output_dir, entry), append=True)
            if entry == self.tasks[done].stderr:
                gc3libs.utils.cat(src_entry, output=os.path.join(self.output_dir, entry), append=True)
            if entry in exclude or (entry.startswith('script.') and entry.endswith('.sh')):
                # delete entry and continue with next one
                os.remove(src_entry)
                continue
            # if `entry` is a generation output file, get the
            # generation no. and update the generation count
            match = GMhcCoevTask.GENERATIONS_FILENAME_RE.match(entry)
            if match:
                generation_no = match.group('generation_no')
                self.generations_done = max(self.generations_done, generation_no)
            # now really move file one level up
            dest_entry = os.path.join(self.output_dir, entry)
            if os.path.exists(dest_entry):
                # backup with numerical suffix
                gc3libs.utils.backup(dest_entry)
            os.rename(os.path.join(task_output_dir, entry), dest_entry)
        os.removedirs(task_output_dir)
        # if a `latest_work.mat` file exists, then we need
        # more time to compute the required number of generations
        latest_work = os.path.join(self.output_dir, 'latest_work.mat')
        if self.generations_done < self.generations_to_do:
            self.add(
                GMhcCoevApplication(self.executable,
                                    self.p_mut_coeff, self.N, self.choose_or_rand, self.sick_or_not, self.off_v_last,
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

    def setup_options(self):
        self.add_param("-G", "--generations", metavar="NUM",
                       dest="generations", type=int, default=3000,
                       help="Compute NUM generations (default: 3000).")

    def new_tasks(self, extra):
        inputs = self._search_for_input_files(self.params.args)

        P_MUT_COEFF_RE = re.compile(r'((?P<num>[0-9]+)x)?10min(?P<exp>[0-9]+)')
        N_RE = re.compile(r'N(?P<N>[0-9]+)')

        for path in inputs:
            kw = extra.copy()

            # parameter values are embedded into the directory name
            name = os.path.basename(os.path.dirname(path))
            p_mut_coeff, N, choose_or_rand, sick_or_not, off_v_last = name.split('__')
            match = P_MUT_COEFF_RE.match(p_mut_coeff)
            if not match:
                self.log.warning("Cannot parse P_MUT_COEFF expression '%s'"
                                 " - ignoring directory '%s'" % (p_mut_coeff, path))
                continue
            p_mut_coeff = float(match.group('num')) * 10.0**(-int(match.group('exp')))

            match = N_RE.match(N)
            if not match:
                self.log.warning("Cannot parse N expression '%s'"
                                 " - ignoring directory '%s'" % (N, path))
                continue
            N = int(match.group('N'))

            if choose_or_rand == "RM":
                choose_or_rand = 1
            elif choose_or_rand == "DMAM":
                choose_or_rand = 2
            elif choose_or_rand == "DMSSGD":
                choose_or_rand = 3
            else:
                self.log.warning("Cannot parse CHOOSE_OR_RAND expression '%s'"
                                 " - ignoring directory '%s'" % (choose_or_rand, path))
                continue

            if sick_or_not == "pat_on":
                sick_or_not = 1
            elif sick_or_not == "pat_off":
                sick_or_not = 0
            else:
                self.log.warning("Cannot parse SICK_OR_NOT expression '%s'"
                                 " - ignoring directory '%s'" % (sick_or_not, path))
                continue

            if not off_v_last.startswith("offval_"):
                self.log.warning("Cannot parse OFF_V_LAST expression '%s'"
                                 " - ignoring directory '%s'" % (off_v_last, path))
                continue
            off_v_last = off_v_last[7:]
            if off_v_last.startswith("0"):
                off_v_last = float("." + off_v_last[1:])
            elif off_v_last.startswith("1"):
                off_v_last = 1.0
            else:
                self.log.warning("Cannot parse OFF_V_LAST expression '%s'"
                                 " - ignoring directory '%s'" % (off_v_last, path))
                continue

            yield ('MHC_coev_' + name,
                   self.application,
                   [path,                    # executable
                    self.params.walltime*60, # single_run_duration
                    self.params.generations,
                    p_mut_coeff,
                    N,
                    choose_or_rand,
                    sick_or_not,
                    off_v_last,
                    #os.path.dirname(path),  # output_dir
                    ],
                   extra.copy())
        
# run it
if __name__ == '__main__':
    GMhcCoevScript().run()
