#!/bin/env python
#
# gbenchmark_wrapper_allinone.py -- wrapper script for executing igraph R script
#
# Authors: Sergio Maffioletti <sergio.maffioletti@uzh.ch>
#
# Copyright (C) 2015  University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#

from __future__ import absolute_import, print_function, unicode_literals
import sys
import os
import subprocess

DEFAULT_RESULT_FOLDER = "./results"

def usage():
    print("""
    Usage:
    gbenchmark_allinone.py <benchmark_file> <network files folder>
    """)

def RunBenchmarks(benchmark_file, network_files_folder):

    # create result fodler
    os.makedirs(DEFAULT_RESULT_FOLDER)
    returncode = 0

    # scan through network_files_folder and run benchmark for each network file
    for network in os.listdir(network_files_folder):
        os.makedirs(os.path.join(DEFAULT_RESULT_FOLDER,network))
        command="Rscript --vanilla %s %s %s" % (benchmark_file,
                                                os.path.join(network_files_folder,
                                                             network),
                                                os.path.join(DEFAULT_RESULT_FOLDER,
                                                             network))
        proc = subprocess.Popen(
            [command],
            shell=True,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE)

        print("Running command %s" % command)
        (stdout, stderr) = proc.communicate()

        if proc.returncode != 0:
            print("Execution failed with exit code: %d" % proc.returncode)

            print(stdout)
            print(stderr)

            returncode = proc.returncode

if __name__ == '__main__':
    if len(sys.argv[1:]) != 2:
           usage()
           sys.exit(1)
    sys.exit(RunBenchmarks(sys.argv[1],sys.argv[2]))
