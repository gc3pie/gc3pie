#! /usr/bin/env python
#
#   gbenchmark.py -- Front-end script for benchmarking statistical
#   Business software on a common set of network data.
#
#   Copyright (C) 2011, 2012  University of Zurich. All rights reserved.
#
#   This program is free software: you can redistribute it and/or
#   modify
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
Front-end script for running selected benchmark statistical business
software.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gbenchmark.py --help`` for program usage
instructions.

Input parameters consists of:
:param str network map files: Path to folder containing all network files
(in .dat format) in the for of:

...

Options:
:benchmark str benchmark suite: String identifying the benchmark suite to be
used. Available benchmarks are:
 . Infomap

Important: All benchmark have to be executed on the same hardware in order to
provide consistent benchmark results.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2014-03-14:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gbenchmarkallinone
    gbenchmarkallinone.GbenchmarkScript().run()

import os
import sys
import time
import tempfile

import shutil
# import csv

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

BENCHMARKS=['infomap', 'lprop', 'leadingeigenvector', 'fastgreedy', 'multilevel', 'walktrap']
DEFAULT_BENCHMARKS = "infomap, lprop, leadingeigenvector, fastgreedy, multilevel, walktrap"

"""
* Run all benchmarks for each network file on the same VM
* There will be more network files
* Put all network files in single folder
* Run all benchmakrs in sequence
* Make sure each benchmark is executed on an exclusive core
* Run 6 VMs (2 cores each: 1 for OS 1 for R execution)
  * Each VM runs all network files against single benchmark
* Space not an issur: copy all selected network files in the destination VM
* Copy results back
* Use same image file with iGraph already installed
* Use same flavor as before (to guarantee that each VM runs of separate hardware)
* Results are collected only at the end when all network files have been processed by a ginven algorithm
"""

## custom application class
class GbenchmarkApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'benchmark'

    def __init__(self, network_data_path, benchmark_name, benchmark_file, **extra_args):

        self.output_dir = extra_args['output_dir']

        inputs = dict()

        inputs[network_data_path] = './network_data/'
        inputs[benchmark_file] = os.path.basename(benchmark_file)

        # adding wrapper main script
        gbenchmark_wrapper = resource_filename(Requirement.parse("gc3pie"),
                                                  "gc3libs/etc/gbenchmark_wrapper_allinone.py")

        inputs[gbenchmark_wrapper] = "gbenchmark_wrapper.py"

        arguments = "python gbenchmark_wrapper.py %s %s " % (inputs[benchmark_file], inputs[network_data_path])

        # Take full node from 'benchmark' flavor
        extra_args['requested_cores'] = 8

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = ["./results"],
            stdout = 'gbenchmark.log',
            join=True,
            executables = ['gbenchmark_wrapper.py', inputs[benchmark_file]],
            **extra_args)

class GbenchmarkScript(SessionBasedScript):
    """
    Fro each network file (with '.dat' extension) found in the 'input folder',
    GbenchmarkScript generates as many Tasks as 'benchmarks' defined.

    The ``gbenchmark`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gbenchmark`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GbenchmarkApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GbenchmarkApplication,
            )

    def setup_options(self):
        self.add_param("-b", "--benchmark", metavar="[STRING]",
                       dest="benchmarks", default=DEFAULT_BENCHMARKS,
                       help="Comma separated list of benchmarks that " \
                       " should be executed. " \
                       "Default: %s." % DEFAULT_BENCHMARKS)

        self.add_param("-D", "--benchmark_scripts_location", metavar="[STRING]",
                       dest="benchmarks_location", default=None,
                       help="Execution scripts for the benchmarks")

    def setup_args(self):

        self.add_param('network_path', type=str,
                       help="Path to the folder containing network files.")

    def parse_args(self):
        """
        Check validity of input parameters and selected benchmark.
        """

        # check args:
        if not os.path.isdir(self.params.network_path):
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid path to network data: '%s'."
                % self.params.network_path)

        if not os.path.isdir(self.params.benchmarks_location):
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid path to benchmarks scripts location: '%s'."
                % self.params.benchmarks_location)

        # Clear benchmarks list
        self.params.benchmarks = [ bench.strip() for bench in self.params.benchmarks.split(",") ]

        self.benchmarks = dict()

        for test in os.listdir(self.params.benchmarks_location):
            benchmark_name = test.strip().split(".")[0]
            if benchmark_name in self.params.benchmarks:
                gc3libs.log.info("Adding benchmark [%s] with execution file [%s]" % (benchmark_name,
                                                                                     os.path.join(self.params.benchmarks_location,
                                                                                                  test)))
                self.benchmarks[benchmark_name] = os.path.join(self.params.benchmarks_location,test)
                self.params.benchmarks.remove(benchmark_name)

    def new_tasks(self, extra):
        """
        For each of the network data and for each of the selected benchmarks,
        create a GbenchmarkApplication.

        First loop the input files, then loop the selected benchmarks
        """
        tasks = []

        for (benchmark_name,benchmark_file) in self.benchmarks.items():

            # XXX: need to find a more compact name
            jobname = "gbenchmark-%s" % (benchmark_name)

            extra_args = extra.copy()

            extra_args['jobname'] = jobname

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', benchmark_name)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', benchmark_name)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', benchmark_name)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', benchmark_name)

            tasks.append(GbenchmarkApplication(
                os.path.abspath(self.params.network_path),
                benchmark_name,
                benchmark_file,
                **extra_args))

        return tasks
