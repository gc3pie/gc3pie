#! /usr/bin/env python
#
#   gcelljunction.py -- GC3Pie front-end for running the
#   "tricellular_junction" code by T. Aegerter
#
#   Copyright (C) 2014, 2019  University of Zurich. All rights reserved.
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
Front-end script for running multiple `tricellular_junction` instances.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gcelljunction --help`` for program usage instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
* 2014-08-15: Add option to restart jobs from saved data.
* 2014-07-17: Snapshot output of RUNNING jobs at every cycle.
* 2014-03-10: Report on task progress in the "info" line.
* 2014-03-03: Initial release, forked off the ``gmhc_coev`` sources.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == '__main__':
    import gcelljunction
    gcelljunction.GCellJunctionScript().run()


# std module imports
import csv
import glob
import os
import re
import sys
import time
from pkg_resources import Requirement, resource_filename

# gc3 library imports
import gc3libs
import gc3libs.defaults
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript
from collections import defaultdict
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.utils import string_to_boolean
from gc3libs.workflow import RetryableTask


## custom application class

class GCellJunctionApplication(Application):
    """
    Custom class to wrap the execution of the ``tricellular_junction``
    program by T. Aegerter.
    """

    application_name = 'tricellular_junction'

    def __init__(self, sim_no, executable=None, restart=None, **extra_args):
        self.sim_no = sim_no
        wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                       "gc3libs/etc/gcelljunction_wrapper.sh")
        inputs = { wrapper_sh:os.path.basename(wrapper_sh) }
        extra_args.setdefault('requested_cores',        1)
        extra_args.setdefault('requested_memory',       3*GB)
        extra_args.setdefault('requested_architecture', Run.Arch.X86_64)
        extra_args.setdefault('requested_walltime',     60*Duration.days)
        # command-line parameters to pass to the tricellular_junction_* program
        self.sim_no = sim_no
        program_opts = [ sim_no ]
        if executable is not None:
            # use the specified executable
            exename = os.path.basename(executable)
            executable_name = './' + exename
            inputs[executable] = exename
            exe_opts = ['-x', executable_name]
        else:
            # assume one is installed in the VM
            executable_name = 'tricellular_junctions'
            exe_opts = [ ]
        self.restart = restart
        if self.restart:
            assert len(self.restart) == 3
            program_opts += [ 1, self.restart[0] ]
            inputs[self.restart[1]] = 'restart_data.mat'
            inputs[self.restart[2]] = 'restart_data4.mat'
        else:
            program_opts += [ 0, 0 ]
        Application.__init__(
            self,
            arguments=['./' + os.path.basename(wrapper_sh)] + exe_opts + [ '--' ] + program_opts,
            inputs = inputs,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'tricellular_junctions.log',
            join=True,
            **extra_args)

    def terminated(self):
        restart = self.find_restart_data(self.output_dir)
        if restart:
            self.restart = restart

    @staticmethod
    def find_restart_data(output_dir):
        data_dir = os.path.join(output_dir, 'data')
        if os.path.isdir(data_dir):
            # find the latest output file
            entries = [ name
                        for name in os.listdir(data_dir)
                        if name.startswith('junction_') and name.endswith('.mat') ]
            latest = None
            latest_idx = 0
            for name in entries:
                idx = int(name[len('junction_'):-len('.mat')])
                if (latest is None) or (idx > latest_idx):
                    latest = name
                    latest_idx = idx
            assert latest is not None
            restart_data_file = os.path.join(data_dir, latest)
            restart_data_seqno = latest_idx
        else:
            restart_data_file = None
            restart_data_seqno = None

        data4_dir = os.path.join(output_dir, 'data4')
        if os.path.isdir(data4_dir):
            restart_data4_file = os.path.join(data4_dir, 'junctions.mat')
            if not os.path.exists(restart_data4_file):
                restart_data4_file = None

        if restart_data_file and restart_data4_file:
            return (restart_data_seqno, restart_data_file, restart_data4_file)
        else:
            return None


class GCellJunctionTask(RetryableTask, gc3libs.utils.Struct):
    """
    Retry execution of a `GCellJunctionApplication` if it fails.
    """
    def __init__(self, sim_no, executable=None, restart=None, **extra_args):
        self.sim_no = sim_no
        RetryableTask.__init__(
            self,
            # actual computational job
            GCellJunctionApplication(sim_no, executable, restart, **extra_args),
            # keyword arguments
            **extra_args)

    def retry(self):
        """Never resubmit a task."""
        return False

    _CHECK_LINES = 5
    def update_state(self, **extra_args):
        super(GCellJunctionTask, self).update_state(**extra_args)
        if self.execution.state == Run.State.RUNNING:
            try:
                estimated_size = gc3libs.defaults.PEEK_FILE_SIZE * self._CHECK_LINES
                with self.task.peek('stdout', offset=-estimated_size, size=estimated_size) as fd:
                    # drop first and last lines, as they may be partial
                    lines = fd.readlines()[1:-1]
                    # gc3libs.log.debug(
                    #     "Lines read from remote output: %s",
                    #     str.join(' ', [("<<%s>>" % ln) for ln in lines]))
                    for line in reversed(lines):
                        line = line.strip()
                        if line != '':
                            self.execution.info = line
                            break
            except Exception, err:
                gc3libs.log.warning(
                    "Ignored error while updating state of Task %s: %s: %s",
                    self, err.__class__.__name__, err)


## main script class

class GCellJunctionScript(SessionBasedScript):
    """
Read the specified INPUT ``.csv`` files and submit jobs according
to the content of those files.  Job progress is monitored and, when a
job is done, its ``data/`` and ``data4/`` output directories are
retrieved back into the same directory where the executable file is
(this can be overridden with the ``-o`` option).

The ``gcelljunction`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``gcelljunction`` will delay submission of
newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            input_filename_pattern = '*.csv',
            application = GCellJunctionApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GCellJunctionTask,
            )


    def setup_options(self):
        self.add_param("-x", "--executable", metavar="PATH",
                       dest="executable", default=None,
                       help="Path to the `tricellular_junctions` executable file.")
        # change default for the memory/walltime options
        self.actions['memory_per_core'].default = 3*Memory.GB
        self.actions['wctime'].default = '60 days'


    def make_directory_path(self, pathspec, jobname):
        # XXX: Work around SessionBasedScript.process_args() that
        # apppends the string ``NAME`` to the directory path.
        # This is really ugly, but the whole `output_dir` thing needs to
        # be re-thought from the beginning...
        if pathspec.endswith('/NAME'):
            return pathspec[:-len('/NAME')]
        else:
            return pathspec


    def before_main_loop(self):
        # XXX: should this be done with `make_controller` instead?
        self._controller.retrieve_running = True
        self._controller.retrieve_overwrites = True
        self._controller.retrieve_changed_only = True


    def new_tasks(self, extra):
        # how many iterations are we already computing (per parameter set)?
        iters = defaultdict(int)
        for task in self.session:
            name, instance = task.jobname.split('#')
            iters[name] = max(iters[name], int(instance))

        for path in self.params.args:
            if path.endswith('.csv'):
                try:
                    inputfile = open(path, 'r')
                except (OSError, IOError), ex:
                    self.log.warning("Cannot open input file '%s': %s: %s",
                                     path, ex.__class__.__name__, str(ex))
                    continue
                for lineno, line in enumerate(inputfile):
                    line = line.strip()
                    # ignore blank and comment lines (those that start with '#')
                    if len(line) == 0 or line.startswith('#'):
                        continue
                    try:
                        parts = re.split("\s*[ ,]\s*", line)
                        sim_no = int(parts[0])
                        if len(parts) == 2:
                            restart = string_to_boolean(parts[1])
                        else:
                            restart = False
                    except ValueError:
                        self.log.error("Wrong format in line %d of file '%s':"
                                       " need 1 integer value (`SimNo`),"
                                       " and -optionally- a flag to restart,"
                                       " but actually got '%s'."
                                       " Ignoring input line, fix it and re-run.",
                                       lineno+1, path, line)
                        continue # with next `row`
                    # extract parameter values
                    basename = ('tricellular_junction_%d' % (sim_no,))

                    # prepare job(s) to submit
                    already = len([ task for task in self.session if task.sim_no == sim_no ])
                    kwargs = extra.copy()
                    base_output_dir = kwargs.pop('output_dir', self.params.output)
                    jobname = ('%s#%d' % (basename, already+1))
                    output_dir = os.path.join(base_output_dir, jobname)
                    if restart and os.path.isdir(output_dir):
                        self.log.debug("Looking for restart files in directory '%s' ...", output_dir)
                        restart = GCellJunctionApplication.find_restart_data(output_dir)
                    else:
                        restart = None
                    yield GCellJunctionTask(
                        sim_no,
                        executable=self.params.executable,
                        restart=restart,
                        jobname=jobname,
                        output_dir=output_dir,
                        **kwargs)

            else:
                self.log.error("Ignoring input file '%s': not a CSV file.", path)
