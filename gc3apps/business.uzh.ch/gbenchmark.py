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
    import gbenchmark
    gbenchmark.GbenchmarkScript().run()

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

BENCHMARKS=['infomap-python', 'infomap-cpp', 'infomap-r', 'lprop-python', 'lprop-r', 'lprop-cpp', 'leadingeigenvector-python', 'leadingeigenvector-r', 'leadingeigenvector-cpp', 'edgebetweenness-r', 'fastgreedy-r', 'multilevel-r', 'optimalmodularity-r', 'spinglass-r', 'walktrap-r', 'edgebetweenness-python', 'fastgreedy-python', 'multilevel-python', 'optimalmodularity-python', 'spinglass-python', 'walktrap-python', 'edgebetweenness-cpp', 'fastgreedy-cpp', 'multilevel-cpp', 'optimalmodularity-cpp', 'spinglass-cpp', 'walktrap-cpp']

## custom application class
class GbenchmarkApplication(Application):
    """
    Custom class to wrap the execution of the R scripts passed in src_dir.
    """
    application_name = 'benchmark'

    def __init__(self, network_data_file, run_script, **extra_args):

        self.output_dir = extra_args['output_dir']

        inputs = dict()

        network_data_filename = os.path.basename(network_data_file)
        inputs[network_data_file] = network_data_filename
        inputs[run_script] = os.path.basename(run_script)

        # adding wrapper main script
        gbenchmark_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                                  "gc3libs/etc/gbenchmark_wrapper.sh")

        inputs[gbenchmark_wrapper_sh] = "gbenchmark_wrapper.sh"

        arguments = "./gbenchmark_wrapper.sh -d -b %s -r %s ./%s " % (extra_args["benchmark_type"], os.path.basename(run_script), network_data_filename)

        extra_args['requested_cores'] = 8

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = ["./results"],
            stdout = 'gbenchmark.log',
            join=True,
            executables = ['wrapper.sh'],
            **extra_args)

class GbenchmarkTask(RetryableTask):
    def __init__(self, network_data_file, run_script, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GbenchmarkApplication(
                network_data_file,
                run_script,
                **extra_args),
            **extra_args
            )

    def retry(self):
        """
        Task will be retried iif the application crashed
        due to an error within the exeuction environment
        (e.g. VM crash or LRMS kill)
        """
        #XXX: for the moment do not retry failed tasks
        return False

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
            application = GbenchmarkTask,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GbenchmarkTask,
            )

    def setup_options(self):
        self.add_param("-b", "--benchmark", metavar="[STRING]",
                       dest="benchmarks", default="Infomap",
                       help="Comma separated list of benchmarks that " \
                       " should be executed. " \
                       "Available benchmarks: [infomap] [lprop].")

        self.add_param("-R", "--run_script", metavar="[STRING]",
                       dest="run_script", default=None,
                       help="Execution script for the given benchmark")

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
                "Invalid path to network data: '%s'. File not found"
                % self.params.network_path)

        # Verify execution script
        if not os.path.isfile(self.params.run_script):
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid path to execution script: '%s'. File not found"
                % self.params.run_script)

        # Verify the selected benchmark
        _benchmarks = [benchmark.strip().lower() for benchmark in
                           self.params.benchmarks.split(',')]

        self.benchmarks = []

        compare = lambda(x):[b for b in BENCHMARKS if ((b.split("-")[0] == x) or (b == x)) and b not in self.benchmarks]
        for bench in _benchmarks:
            self.benchmarks.extend(compare(bench))

        gc3libs.log.info("Benchmark retained: %s", str(self.benchmarks))

    def new_tasks(self, extra):
        """
        For each of the network data and for each of the selected benchmarks,
        create a GbenchmarkApplication.

        First loop the input files, then loop the selected benchmarks
        """
        tasks = []

        for input_file_name in os.listdir(self.params.network_path):

            # Rule-out files without '.dat' extension
            if not input_file_name.endswith(".dat"):
                continue

            input_file = os.path.join(self.params.network_path, input_file_name)

            for benchmark in self.benchmarks:
                # XXX: need to find a more compact name
                jobname = "gbenchmark-%s-%s" % (benchmark,(os.path.basename(input_file)))

                (benchmark_name,benchmark_type) = benchmark.split("-")

                extra_args = extra.copy()

                extra_args['jobname'] = jobname
                extra_args['benchmark_name'] = benchmark_name
                extra_args['benchmark_type'] = benchmark_type

                extra_args['output_dir'] = self.params.output
                extra_args['output_dir'] = extra_args['output_dir'].replace('NAME',
                                                                            os.path.join(benchmark_name,
                                                                                         benchmark_type,
                                                                                         input_file_name))
                extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION',
                                                                            os.path.join(benchmark_name,
                                                                                         benchmark_type,
                                                                                         input_file_name))
                extra_args['output_dir'] = extra_args['output_dir'].replace('DATE',
                                                                            os.path.join(benchmark_name,
                                                                                         benchmark_type,
                                                                                         input_file_name))
                extra_args['output_dir'] = extra_args['output_dir'].replace('TIME',
                                                                            os.path.join(benchmark_name,
                                                                                         benchmark_type,
                                                                                         input_file_name))

                tasks.append(GbenchmarkTask(
                    input_file,
                    os.path.abspath(self.params.run_script),
                    **extra_args))

        return tasks
