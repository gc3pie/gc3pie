#! /usr/bin/env python
#
#   gfdiv.py -- GC3Pie front-end for running the
#   "functional diversity" code by F. D. Schneider
#
#   Copyright (C) 2016 S3IT, Zentrale Informatik, University of Zurich
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
Front-end script for parallelizing execution of the "functional
diversity" calculations.

It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gcelljunction --help`` for program usage instructions.
"""
__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
* 2016-01-25: Initial release.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == '__main__':
    import gfdiv
    gfdiv.GfdivScript().run()


# std module imports
from argparse import ArgumentError
import os
from os.path import basename, exists, join, realpath
import sys
from pkg_resources import Requirement, resource_filename

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, existing_file
from gc3libs.quantity import Memory, kB, MB, GB, Duration, days, hours, minutes, seconds
from gc3libs.utils import basename_sans, fgrep, irange, occurs, parse_range
from gc3libs.workflow import RetryableTask


## custom application class

class MatlabRetryOnOutOfMemory(RetryableTask):
    """
    Run a task and retry it if failed with an out-of-memory condition.

    Memory requirements will be increased each time by a configurable
    amount until a set maximum is reached; when that happens, we just
    give up retrying and mark the task as failed.

    Proper detection of the out-of-memory condition relies on MATLAB's
    error message ``Out of memory.`` being detected in the task's
    STDOUT stream.
    """

    def __init__(self, task, increment=1*GB, maximum=31*GB, **extra_args):
        self.increment = increment
        self.maximum = maximum
        RetryableTask.__init__(self, task, **extra_args)

    def retry(self):
        last_run = self.task.execution
        if last_run.returncode == 0:
            gc3libs.log.debug(
                "%s: Task finished successfully,"
                " *not* resubmitting it again.", self.task)
            return False
        else:
            # task errored out, find out why
            try:
                requested_memory = self.task.requested_memory or last_run.max_used_memory
            except AttributeError:
                requested_memory = last_run.max_used_memory
            generic_memory_error = (last_run.max_used_memory > requested_memory)
            task_stderr = os.path.join(self.task.output_dir,
                                       self.task.stdout if self.task.join else self.task.stderr)
            matlab_memory_error = (
                occurs('Out of memory.', task_stderr, fgrep)
                or occurs('MATLAB:nomem.', task_stderr, fgrep))
            if generic_memory_error or matlab_memory_error:
                new_requested_memory = requested_memory + self.increment
                if new_requested_memory >= self.maximum:
                    gc3libs.log.info(
                        "%s: Possible out-of-memory condition detected,"
                        " but increasing memory requirements would"
                        " exceed set maximum of %s.  Aborting task.",
                        self.task, self.maximum)
                    return False
                else:
                    self.task.requested_memory = new_requested_memory
                    gc3libs.log.info(
                        "%s: Possible out-of-memory condition detected,"
                        " will request %s for next run.",
                        self.task, self.task.requested_memory)
                    return True
            else:
                gc3libs.log.info(
                    "%s: Task failed for non-memory-related reasons,"
                    " *not* resubmitting it again.", self.task)
                return False


class FunctionalDiversityApplication(Application):
    """
    Custom class to wrap the execution of the "functional diversity"
    MATLAB code by Fabian Daniel Schneider.
    """

    application_name = 'functional_diversity'

    # pattern for the MATLAB commands to run
    matlab_cmd = (
        "load('{inputname}.mat');"
        "outputData={funcname}(inputData,{radius});"
        "save('{outputfile}','outputData');"
    )

    def __init__(self, funcfile, radius, inputfile, outputfile=None,
                 **extra_args):
        funcname = basename_sans(funcfile)
        self.funcname = funcname
        self.radius = radius
        # map args to file system names
        inputname = basename_sans(inputfile)
        if outputfile is None:
            outputfile = ('output_{inputname}_{radius}.mat'.format(**locals()))
        # default execution params
        extra_args.setdefault('requested_cores',        1)
        extra_args.setdefault('requested_memory',       3*GB)
        extra_args.setdefault('requested_architecture', Run.Arch.X86_64)
        extra_args.setdefault('requested_walltime',     30*days)
        # actual app initialization
        Application.__init__(
            self,
            arguments=[
                'matlab', '-nodisplay', '-nojvm', #'-singleCompThread',
                '-r', (self.matlab_cmd.format(**locals()))
            ],
            inputs = [funcfile, inputfile],
            outputs = [outputfile],
            stdout = 'matlab.log',
            join=True,
            **extra_args)


## main script class

class GfdivScript(SessionBasedScript):
    """
Run the specified MATLAB function on all the given input files,
for all neighborhood radius parameter in the given range.

The ``gfdiv`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files or an extended range is specified in the command line.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = FunctionalDiversityApplication,
        )


    def setup_options(self):
        # change default for the memory/walltime options
        self.actions['memory_per_core'].default = 3*Memory.GB
        self.actions['wctime'].default = '60 days'


    def setup_args(self):
        self.add_param('funcname', help=(
            "Name of the MATLAB function to apply to the input data."
            " A corresponding `.m` file must exist in the current directory."))
        self.add_param('range', help=(
            "Range for the neighborhood radius parameter."
            " Specify as MIN:MAX (e.g., 1:170) to compute all radii"
            " from MIN to MAX (inclusive).  Optionally,"
            " the MIN:MAX:STEP form can be used to further specify"
            " an increment; e.g., 1:170:10 would run computations"
            " with radius 1,11,21,...,161."))
        self.add_param('inputfile', nargs='+', type=existing_file,
                       help=("Input data file(s)."))


    def make_directory_path(self, pathspec, jobname):
        # XXX: Work around SessionBasedScript.process_args() that
        # apppends the string ``NAME`` to the directory path.
        # This is really ugly, but the whole `output_dir` thing needs to
        # be re-thought from the beginning...
        if pathspec.endswith('/NAME'):
            return pathspec[:-len('/NAME')]
        else:
            return pathspec


    # def before_main_loop(self):
    #     # XXX: should this be done with `make_controller` instead?
    #     self._controller.retrieve_overwrites = True


    def get_function_name_and_file(self, funcname=None):
        if funcname is None:
            funcname = self.params.funcname
        if funcname.endswith('.m'):
            funcname_m = funcname
            funcname = funcname[:-len('.m')]
        else:
            funcname_m = funcname + '.m'
        funcfile = join(os.getcwd(), funcname_m)
        if not exists(funcfile):
            raise ArgumentError(
                self.actions['funcname'],
                ("Cannot read file '{funcfile}'"
                 " providing MATLAB function '{funcname}'.")
                .format(**locals()))
        return funcname, funcfile

    def new_tasks(self, extra):
        # find MATLAB function to run
        funcname, funcfile = self.get_function_name_and_file()
        # get range for neighborhood radius
        low, high, step = parse_range(self.params.range)
        for radius in irange(low, high+1, step):
            for inputfile in self.params.inputfile:
                kwargs = extra.copy()
                base_output_dir = kwargs.pop('output_dir', self.params.output)
                inputfile = realpath(inputfile)
                inputname = basename_sans(inputfile)
                jobname = ('{funcname}_{inputname}_{radius}'.format(**locals()))
                outputfile = ('output_{inputname}_{radius}.mat'.format(**locals()))
                output_dir = join(base_output_dir, jobname)
                yield MatlabRetryOnOutOfMemory(
                    FunctionalDiversityApplication(
                        funcfile,
                        radius,
                        inputfile,
                        jobname=jobname,
                        outputfile=outputfile,
                        output_dir=output_dir,
                        **kwargs),
                    increment=4*GB,
                )
