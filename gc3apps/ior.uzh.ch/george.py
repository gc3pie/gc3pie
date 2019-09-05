#! /usr/bin/env python
#
"""
Driver script for running 'Value Function Iteration' programs
on the SMSCG infrastructure.
"""
# Copyright (C) 2011-2012  University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
# summary of user-visible changes
__changelog__ = """
  2011-05-06:
    * Workaround for Issue 95: now we have complete interoperability
      with GC3Utils.
"""
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == '__main__':
from __future__ import absolute_import, print_function
    import george
    george.GeorgeScript().run()


# stdlib imports
import ConfigParser
import csv
import math
import os
import os.path
import shutil
import sys
from prettytable import PrettyTable
import types

# interface to Gc3libs
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file, nonnegative_int, positive_int
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

IN_VALUES_FILE = 'ValuesIn.txt'
OUT_VALUES_FILE = 'ValuesOut.txt'
TMPDIR = '/tmp'


## support classes

# For each input file, a new computation is started, totally
# `self.params.iterations` passes, each pass corresponding to
# an application of the `self.params.executable` function.
#
# This is the crucial point:

class ValueFunctionIteration(SequentialTaskCollection):
    """
    Perform a (predefined) number of iterations of a certain function
    over a set of values.  The function to be iterated is implemented
    in the form of an executable program, that takes a single input
    file IN_VALUES_FILE (a list of values) and creates a single output
    file OUT_VALUES_FILE.

    The several-passes computation is implemented as a sequential task
    collection (totalling `total_iterations` steps); each step of
    which is a parallel collection of tasks, each of which is a
    single-core task executing the given program.
    """

    def __init__(self, executable, initial_values_file,
                 total_iterations, slice_size=0,
                 output_dir=TMPDIR, **extra_args):
        """
        Create a new task that runs `executable` over a set of values
        (initially given by `initial_values_file`, then the output of
        a run is fed as input to the next one), iterating the process
        `total_iterations` times.

        If `slice_size` is a positive integer, then chop the input into
        chunks of -at most- the given size and compute them as separate
        independent jobs.

        Extra keyword arguments are saved and passed down to construct
        the `ValueFunctionIterationApplication`.
        """
        assert slice_size >= 0, \
               "Argument `slice_size` to ValueFunctionIteration.__init__" \
               " must be a non-negative integer."

        # remember values for later use
        self.executable = executable
        self.initial_values = initial_values_file
        self.total_iterations = total_iterations - 1
        self.slice_size = slice_size
        self.datadir = output_dir
        self.extra = extra_args

        # count initial values (i.e. number of lines in file)
        self.num_input_values = 0
        if os.path.exists(initial_values_file):
            with open(initial_values_file, 'r') as fo:
                self.num_input_values = sum(1 for i in fo)

        # this little piece of black magic is to ensure intermediate
        # filenames appear numerically sorted in `ls -l` output
        self.values_filename_fmt = ('values.%%0%dd.txt'
                                    % (1 + int(math.log10(total_iterations))))

        self.jobname = extra_args.get('jobname',
                              gc3libs.utils.basename_sans(initial_values_file))

        # create initial task and register it
        initial_task = ValueFunctionIterationPass(executable, initial_values_file,
                                                  0, total_iterations, slice_size,
                                                  self.datadir, self.extra,
                                                  parent=self.jobname)
        SequentialTaskCollection.__init__(self, self.jobname, [initial_task])


    def __str__(self):
        return self.jobname


    def next(self, iteration):
        """
        If there are more iterations to go, enqueue the corresponding jobs.

        See: `SequentialTaskCollection.next`:meth: for a description
        of the contract that this method must implement.
        """
        if iteration == self.total_iterations:
            return Run.State.TERMINATED
        else:
            last = self.tasks[iteration]
            ## write output values to a 'values.<iteration>.txt' file
            last_values_filename = os.path.join(self.datadir,
                                                self.values_filename_fmt % iteration)
            last_values_file = open(last_values_filename, 'w+')
            for value in last.output_values:
                last_values_file.write('%f\n' % value)
            last_values_file.close()
            ## now create new task for computing the next pass
            self.add(
                ValueFunctionIterationPass(
                    self.executable,
                    last_values_filename,
                    iteration+1,
                    self.total_iterations,
                    self.slice_size,
                    extra=self.extra,
                    parent=self.jobname,
                    )
                )
            return Run.State.RUNNING


    def terminated(self):
        """
        Collect the output of all iterations into a single '.csv'
        file.
        """
        # determine output file name
        output_filename_base = gc3libs.utils.basename_sans(self.initial_values)
        if output_filename_base.endswith('.input'):
            output_filename_base = output_filename_base[:-6]
        output_filename = output_filename_base  + '.output.csv'
        gc3libs.log.debug("SequentialTaskCollection %s done,"
                          " now processing results into file '%s'..."
                          % (self, output_filename))
        output_file = open(output_filename, 'w+') # write+truncate
        output_csv = csv.writer(output_file)
        output_csv.writerow(['i'] + [ ('n=%d' % n)
                                      for n in range(1, len(self.tasks)+1) ])
        for i, values in enumerate(zip(*[ task.output_values
                                          for task in self.tasks ])):
            output_csv.writerow([i] + list(values))
        output_file.close()
        # same stuff, albeit in a different output format for SimpleDP compatibility
        output_filename = output_filename_base + '.output.txt'
        shutil.copyfile(self.initial_values, output_filename)
        output_file = open(output_filename, 'a') # write, no truncate
        for task in self.tasks:
            for value in task.output_values:
                output_file.write("%s\n" % value)
        output_file.close()
        gc3libs.log.debug("  ...done.")


class ValueFunctionIterationPass(ParallelTaskCollection):
    """
    Compute the values taken by a certain function over a set of
    inputs.  The function to be iterated is implemented in the form of
    an executable program, that takes a single input file `IN_VALUES_FILE`
    (a list of values) and creates a single output file `OUT_VALUES_FILE`.

    The computation will be split into separate independent processes,
    each working over a fraction of the input values (determined by
    the `slice_size` argument to `__init__`).

    This is meant to be a single step in the value function iteration.
    """

    def __init__(self, executable, input_values_file,
                 iteration, total_iterations,
                 slice_size=0, datadir=TMPDIR, extra={ },
                 parent=None):
        """
        Create a new tasks that runs `executable` over the set of
        values contained in file `input_values_file` (one
        floating-point number per line).

        If `slice_size` is a positive integer, then chop the input into
        chunks of -at most- the given size and compute them as separate
        independent jobs.

        Any other argument is passed unchanged to the
        `ParallelTaskCollection` ctor.
        """
        assert slice_size >= 0, \
               "Argument `slice_size` to ValueFunctionIterationPass.__init__" \
               " must be a non-negative integer."
        assert isinstance(extra, dict), \
               "Argument `extra` to ValueFunctionIterationPass.__init__" \
               " must be a dictionary instance."

        self.input_values = input_values_file
        self.output_values = None

        total_input_values = _count_input_values(input_values_file)

        if slice_size < 1:
            # trick to make the for-loop below work in the case of one
            # slice only
            slice_size = total_input_values

        # pad numbers with correct amount of zeros, so they look
        # sorted in plain `ls -l` output
        fmt = '%%0%dd' % (1 + int(math.log10(float(total_iterations))))
        self.jobname = ("%s.%s"
                        % ((parent or gc3libs.utils.basename_sans(input_values_file)),
                           (fmt % iteration)))

        # create data sub-directory
        datasubdir = os.path.join(datadir, self.jobname)
        if not os.path.exists(datasubdir):
            os.makedirs(datasubdir)

        # build list of tasks
        tasks = [ ]
        for start in range(0, total_input_values, slice_size):
            # create new job to handle this slice of values
            extra_args = extra.copy()
            extra_args['parent'] = self.jobname
            tasks.append(
                ValueFunctionIterationApplication(
                    executable,
                    input_values_file,
                    iteration,
                    total_iterations,
                    # each task computes values with i in range
                    # `start..end` (inclusive), and `end` is
                    # generally `slice_size` elements after `start`
                    start,
                    end=min(start + slice_size - 1, total_input_values),
                    output_dir = datasubdir,
                    **extra_args
                    )
                )

        # actually init jobs
        ParallelTaskCollection.__init__(self, self.jobname, tasks)


    def __str__(self):
        return self.jobname


    def terminated(self):
        """
        Collect all results from sub-tasks into `self.output_values`.
        """
        gc3libs.log.debug(
            "%s terminated, now collecting return values from sub-tasks ..."
            % self)
        self.output_values = [ ]
        for task in self.tasks:
            if task.execution.exitcode != 0:
                self.execution.exitcode = 1
                gc3libs.log.error(
                    "%s: sub-task %s failed with exit code %d. Aborting."
                    % (self, task, task.execution.exitcode))
                return
            self.output_values.extend(task.output_values)
        gc3libs.log.debug("  ...done.")


class ValueFunctionIterationApplication(Application):
    """
    A computational job to reckon the values taken by a certain
    function over a set of inputs.  The function to be iterated is
    implemented in the form of an executable program, that takes a
    single input file IN_VALUES_FILE (a list of values) and creates a
    single output file OUT_VALUES_FILE.

    Optional arguments `start` and `end` constrain the application of
    the function to a subset of the input values (those with positions
    in the range `start..end`, inclusive of endpoints).

    When the job turns to TERMINATED state and the output is fetched
    back, the list of output values is made available in the
    `self.output_values` attribute.
    """

    application_name = 'vfi'

    def __init__(self, executable, input_values_file, iteration, total_iterations,
                 start=0, end=None, parent=None, **extra_args):
        count = _count_input_values(input_values_file)
        if end is None:
            end = count-1 # last allowed position
        # make a readable jobname, indicating what part of the computation this is
        extra_args.setdefault('jobname',
                      '%s.%d--%d' % (
                          parent or gc3libs.utils.basename_sans(input_values_file),
                          start, end))
        Application.__init__(
            self,
            os.path.basename(executable),
            # `start` and `end` arguments to `executable` follow the
            # FORTRAN convention of being 1-based
            arguments = [ extra_args['discount_factor'], iteration, total_iterations,
                          count, start+1, end+1 ],
            inputs = { executable:os.path.basename(executable),
                       input_values_file:IN_VALUES_FILE },
            outputs = { OUT_VALUES_FILE:OUT_VALUES_FILE },
            join = True,
            stdout = 'job.log', # stdout + stderr
            **extra_args)


    def __str__(self):
        return self.jobname


    def terminated(self):
        gc3libs.log.debug("%s: TERMINATED with return code %d ..." % (self.persistent_id, self.execution.returncode))
        if self.execution.returncode == 0:
            gc3libs.log.debug("%s: terminated correctly, now post-processing results ..." % self.persistent_id)
            # everything ok, try to post-process results
            results = [ ]
            output_dir = self.output_dir
            output_filename = os.path.join(output_dir, OUT_VALUES_FILE)
            try:
                result_file = open(output_filename, 'r')
                for lineno, line in enumerate(result_file):
                    # convert line to floating-point number
                    results.append(float(line))
                result_file.close()
                self.output_values = results
                # if we got to this point, parsing went fine
                # so we can remove the output directory altogether
                shutil.rmtree(output_dir, ignore_errors=True)
            except ValueError, ex:
                # some line cannot be parsed as a floating-point number
                msg = ("Invalid content in file '%s' at line %d: %s"
                       % (output_filename, lineno, str(ex)))
                gc3libs.log.error("%s: %s" % (self.persistent_id, msg))
                self.info = msg
                self.exitcode = 65 # EX_DATAERR in /usr/include/sysexits.h
            except IOError, ex:
                # error opening or reading file
                msg = ("I/O error processing output file '%s': %s"
                       % (output_filename, str(ex)))
                gc3libs.log.error("%s: %s" % (self.persistent_id, msg))
                self.info = msg
                self.exitcode = 74 # EX_IOERR in /usr/include/sysexits.h
            except Exception, ex:
                msg = ("Error processing result file '%s': %s"
                       % (output_filename, str(ex)))
                gc3libs.log.error("%s: %s" % (self.persistent_id, msg))
                self.info = msg
                self.exitcode = 70 # EX_SOFTWARE in /usr/include/sysexits.h


## auxiliary functions

def _count_input_values(input_values):
    """
    Return number of input values.  Argument `input_values` can be
    either a file name (in which case the count of lines in the file
    is returned, assuming that the file has one input value per line),
    or a Python sequence of values.
    """
    if isinstance(input_values, types.StringTypes):
        # `input_values` is the name of a file, count number of lines
        input_values_file = open(input_values, 'r')
        count = len(input_values_file.readlines())
        input_values_file.close()
        return count
    else:
        # assume `input_values` is a sequence of some type and return
        # its length
        return len(input_values)


## main

class GeorgeScript(SessionBasedScript):
    """
    Execute P iterations of the specified value function on the input
    data.  For each input file, all iterations are collected back into
    a single '.csv' output file: column K in the output file contains
    the results of the K-th iteration of the value function.

    Computation of the value function on a set of values is performed
    by a separate program ``vfi.exe``.  You can set an alternate path
    to the compute program using the ``-x`` command-line option.

    The number P of iterations can be set with the ``-P`` command-line
    option.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.3',
            # only '.txt' files are considered as valid input
            input_filename_pattern = '*.ini',
            )

    def setup_args(self):
        super(GeorgeScript, self).setup_args()

        self.add_param('-P', '--iterations', metavar='NUM',
                       dest='iterations', type=positive_int, default=1,
                       help="Compute NUM iterations per each output file"
                       " (default: %(default)s).")
        self.add_param('-p', '--slice-size', metavar='NUM',
                       dest='slice_size', type=nonnegative_int, default=0,
                       help="Process at most NUM states in a single"
                       " computational job.  Each input file is chopped"
                       " into files that contain at most NUM elements,"
                       " each of which is processed separately as a single"
                       " computational jobs.  The higher NUM is, the less"
                       " jobs will be created, but the longer each job will"
                       " last; this value and the DURATION argument to"
                       " option `-w` are directly proportional."
                       " If 0 (default), no chopping takes place and one job"
                       " only is used for each pass of the computation.")
        self.add_param("-x", "--execute", dest="execute",
                       type=executable_file, default="vfi.exe",
                       metavar="EXECUTABLE",
                       help="Run the specified EXECUTABLE program"
                       " to compute one step of values;"
                       " default: %(default)s.")


    def parse_args(self):
        if not os.path.isabs(self.params.execute):
            self.params.execute = os.path.abspath(self.params.execute)


    def new_tasks(self, extra):
        inis = self._search_for_input_files(self.params.args)

        p = ConfigParser.SafeConfigParser()
        successfully_read = p.read(inis)
        for filename in inis:
            if filename not in successfully_read:
                self.log.error("Could not read/parse input file '%s'. Ignoring it."
                               % filename)

        for name in p.sections():
            # path to the initial values file
            if not p.has_option(name, 'initial_values_file'):
                self.log.error("Required parameter 'initial_values_file' missing"
                               " in input '%s'.  Ignoring."
                               % name)
            path = p.get(name, 'initial_values_file')
            if not os.path.isabs(path):
                path = os.path.abspath(path)
            if not os.path.exists(path):
                self.log.error("Input values file '%s' does not exist."
                               " Ignoring task '%s', which depends on it."
                               % (path, name))

            # import running parameters from cfg file
            extra_args = extra.copy()
            extra_args['jobname'] = name
            try:
                extra_args.setdefault('discount_factor',
                              p.getfloat(name, 'discount_factor'))
            except (KeyError, ConfigParser.Error, ValueError), ex:
                self.log.error("Could not read required parameter 'discount_factor'"
                               " in input '%s': %s"
                               % (name, str(ex)))

            yield (name, george.ValueFunctionIteration, [
                self.params.execute, path,
                self.params.iterations,
                self.params.slice_size
                ], extra_args)


    def print_summary_table(self, output, stats):
        table = PrettyTable(['Input', 'Iteration', 'Tasks Generated/Total', 'Progress'])
        table.align = 'c'
        table.align['Input'] = 'l'
        def compute_stats(collection):
            result = collection.stats()
            def add_stats(s1, s2):
                for k in s2.iterkeys():
                    s1[k] += s2[k]
                return s1
            for task in collection.tasks:
                if hasattr(task, 'stats'):
                    add_stats(result, task.stats())
            return result
        for toplevel in self.session:
            current_iteration = toplevel._current_task
            total_iterations = toplevel.total_iterations
            if toplevel.slice_size == 0:
                mult = 2
            else:
                mult = 1 + (toplevel.num_input_values / toplevel.slice_size)
            generated_tasks = 1 + mult * (1 + current_iteration)
            total_tasks = 1 + mult * (1 + total_iterations)
            progresses = [ ]
            for state in [ Run.State.NEW,
                           Run.State.SUBMITTED,
                           Run.State.RUNNING,
                           Run.State.STOPPED,
                           Run.State.TERMINATING,
                           Run.State.TERMINATED,
                           Run.State.UNKNOWN
                           ]:
                stats = compute_stats(toplevel)
                count = stats[state]
                if count > 0:
                    progresses.append("%.1f%% %s"
                                      % (100.0 * count / (total_tasks-1), state))
                    if state == 'TERMINATED':
                        progresses[-1] += (" (%.1f%% ok, %.1f%% failed)"
                                           % (100.0 * stats['ok'] / count,
                                              100.0 * stats['failed'] / count))
            table.add_row([
                toplevel.jobname,
                "%d/%d" % (1+current_iteration, 1+total_iterations),
                "%d/%d" % (generated_tasks, total_tasks),
                str.join(", ", progresses)
                ])
        output.write(str(table))
        output.write("\n")
