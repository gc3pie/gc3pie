#! /usr/bin/env python
#
"""
Driver script for running 'Value Function Iteration' programs
on the SMSCG infrastructure.
"""
# Copyright (C) 2011 GC3, University of Zurich. All rights reserved.
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
__docformat__ = 'reStructuredText'
__version__ = '$Revision$'


import csv
import math
import os
import os.path
import shutil
import sys
import types

## interface to Gc3libs

import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript
from gc3libs.dag import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

IN_VALUES_FILE = 'Values.txt'
OUT_VALUES_FILE = 'SolVal.txt'
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
    
    def __init__(self, executable, initial_values_file, total_iterations,
                 slice_size=0, output_dir=TMPDIR, grid=None, **kw):
        """
        Create a new tasks that runs `executable` over a set of values
        (initially given by `initial_values_file`, then the output of
        a run is fed as input to the next one), riterating the process
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
        self.extra = kw

        # this little piece of black magic is to ensure intermediate
        # filenames appear numerically sorted in `ls -l` output
        self.values_filename_fmt = 'values.%%0%dd.txt' % (1 + int(math.log10(total_iterations)))

        # create initial task and register it
        initial_task = ValueFunctionIterationPass(executable, initial_values_file,
                                                  0, total_iterations, slice_size,
                                                  self.datadir, self.extra, grid)
        jobname = gc3libs.utils.basename_sans(initial_values_file)
        SequentialTaskCollection.__init__(self, jobname, [initial_task], grid)


    def next(self, iteration):
        """
        If there are more iterations to go, enqueue the corresponding jobs.

        See: `SequentialTaskCollection.next`:meth: for a description
        of the contract that this method must implement.
        """
        gc3libs.log.debug("ValueFunctionIteration.next(%s, %d)" % (self, iteration))
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
            self.tasks.append(
                ValueFunctionIterationPass(
                    self.executable,
                    last_values_filename,
                    iteration+1,
                    self.total_iterations,
                    self.slice_size,
                    extra=self.extra,
                    grid=self._grid,
                    )
                )
            return Run.State.RUNNING


    def terminated(self):
        """
        Collect the output of all iterations into a single '.csv'
        file.
        """
        # determine output file name
        output_filename = gc3libs.utils.basename_sans(self.initial_values) + '.csv'
        gc3libs.log.debug("SequentialTaskCollection %s done,"
                          " now processing results into file '%s'..."
                          % (self, output_filename))
        output_file = open(output_filename, 'w+')
        output_csv = csv.writer(output_file)
        output_csv.writerow(['I'] + [ ('Pass %d' % n)
                                          for n in range(1, len(self.tasks)+1) ])
        for n, values in enumerate(zip(*[ task.output_values
                                          for task in self.tasks ])):
            output_csv.writerow([n] + list(values))
        output_file.close()
        gc3libs.log.debug("  ...done.")


class ValueFunctionIterationPass(ParallelTaskCollection):
    """
    Compute the values taken by a certain function over a set of
    inputs.  The function to be iterated is implemented in the form of
    an executable program, that takes a single input file IN_VALUES_FILE
    (a list of values) and creates a single output file OUT_VALUES_FILE.

    The computation will be split into separate independent processes,
    each working over a fraction of the input values (determined by
    the `slice_size` argument to `__init__`).

    This is meant to be a single step in the value function iteration.
    """

    def __init__(self, executable, input_values_file,
                 iteration, total_iterations,
                 slice_size=0, datadir=TMPDIR, extra={ },
                 grid=None):
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
        
        # create data sub-directory
        datasubdir = os.path.join(datadir, "pass." + (fmt % iteration))
        if not os.path.exists(datasubdir):
            os.makedirs(datasubdir)
            
        # build list of tasks
        tasks = [ ]
        for start in range(0, total_input_values, slice_size):
            # create new job to handle this slice of values
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
                    **(extra.copy())
                    )
                )

        # actually init jobs
        jobname = gc3libs.utils.basename_sans(input_values_file)
        ParallelTaskCollection.__init__(self, jobname, tasks, grid)


    def terminated(self):
        """
        Collect all results from sub-tasks into `self.output_values`.
        """
        gc3libs.log.debug("Pass %s terminated, now collecting return values from sub-tasks ..." % self)
        self.output_values = [ ]
        for task in self.tasks:
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
    
    def __init__(self, executable, input_values_file, iteration, total_iterations,
                 start=0, end=None, **kw):
        """
        """
        count = _count_input_values(input_values_file)
        if end is None:
            end = count-1 # last allowed position
        kw.setdefault('jobname', gc3libs.utils.basename_sans(input_values_file))
        Application.__init__(
            self,
            executable,
            # `start` and `end` arguments to `executable` follow the
            # FORTRAN convention of being 1-based
            arguments = [ count, iteration, total_iterations, start+1, end+1 ],
            inputs = { input_values_file:IN_VALUES_FILE },
            outputs = { OUT_VALUES_FILE:OUT_VALUES_FILE },
            join = True,
            stdout = 'output.txt', # stdout + stderr
            **kw)


    def postprocess(self, output_dir):
        if self.execution.returncode == 0:
            # everything ok, try to post-process results
            results = [ ]
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
                self.info = ("Invalid content in file '%s' at line %d: %s"
                             % (output_filename, lineno, str(ex)))
                self.exitcode = 65 # EX_DATAERR in /usr/include/sysexits.h
            except IOError, ex:
                # error opening or reading file
                self.info = ("I/O error processing output file '%s': %s"
                             % (output_filename, str(ex)))
                self.exitcode = 74 # EX_IOERR in /usr/include/sysexits.h
            except Exception, ex:
                self.info = ("Error processing result file '%s': %s" 
                             % (output_filename, str(ex)))
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
    by a separate program ``vfi.exe``.  You can set an alternate
    program using the ``-x`` command-line option, but any program must
    accept exactly three command-line arguments, namely: input file
    path, start index in the range to compute, end index in the range,
    and produce the set of output values on standard output.

    The number P of iterations can be set with the ``-P`` command-line
    option.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.1',
            # only '.txt' files are considered as valid input
            input_filename_pattern = '*.txt',
            )

    def setup_args(self):
        super(GeorgeScript, self).setup_args()
        
        self.add_param('-P', '--iterations', dest='iterations',
                       type=int, default=1,
                       metavar='NUM',
                       help="Compute NUM iterations per each output file"
                       " (default: %(default)s).")
        self.add_param('-p', '--slice-size', dest='slice_size',
                       type=int, default=0,
                       metavar='NUM',
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
                       default="vfi.exe",
                       metavar="EXECUTABLE",
                       help="Run the specified EXECUTABLE program"
                       " to compute one step of values;"
                       " default: %(default)s.")


    def parse_args(self):
        if self.params.iterations < 1:
            raise RuntimeError("Argument to option -P/--iterations must be a positive integer.")

        if self.params.slice_size < 0:
            raise RuntimeError("Argument to option -p/--slice-size must be a non-negative integer.")

        if not os.path.exists(self.params.execute):
            raise RuntimeError("Cannot find executable '%s'; use option `-x` to specify an alternate path."
                               % self.params.execute)
        if not os.path.isfile(self.params.execute):
            raise RuntimeError("Given executable '%s' is not a file."
                               % self.params.execute)
        if not os.access(self.params.execute, os.R_OK):
            raise RuntimeError("Cannot read executable file '%s'."
                               % self.params.execute)
        if not os.access(self.params.execute, os.X_OK):
            raise RuntimeError("File '%s' lacks execute permissions."
                               % self.params.execute)


    def new_tasks(self, extra):
        inputs = self._search_for_input_files(self.params.args)

        for path in inputs:
            yield (gc3libs.utils.basename_sans(path),
                   ValueFunctionIteration,
                   [self.params.execute,
                    path,
                    self.params.iterations,
                    self.params.slice_size,],
                   extra.copy())



# run script
if __name__ == '__main__':
    GeorgeScript().run()
