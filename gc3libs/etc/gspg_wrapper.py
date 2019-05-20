#!/bin/env python

"""
# gspg_wrapper.py -- Wrapper script to execute CTX analysis
#
# Authors: Sergio Maffioletti <sergio.maffioletti@uzh.ch>
#
# Copyright (C) 2015-2016  University of Zurich. All rights reserved.
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
#-----------------------------------------------------------------------
"""

from __future__ import absolute_import, print_function, unicode_literals
from builtins import next
from builtins import range
import os
import sys
import subprocess
import argparse
import time
import csv

DEFAULT_BINARY="ctx-linkdyn-ordprm-sirs.p4"
RESULT_FILE="results.csv"

def runctx(args):

    results = dict()

    parser = argparse.ArgumentParser(description='Run CTX simulation.')
    parser.add_argument('inputcsv', metavar='I',
                        help='location of input .csv file.')
    parser.add_argument('--ctx', dest='ctx', action='store', default=None,
                        help='Alterantive path to ctx binay (default: None)')

    arguments = parser.parse_args(args)

    command = ""

    if arguments.ctx:
        command = "%s " % arguments.ctx
    else:
        command =  DEFAULT_BINARY
    command += " -i ./input.dat"

    # use csv package only
    with open(arguments.inputcsv,'rb') as rd:
        reader = csv.reader(rd)
        columns = next(reader)
        # open destination file
        for line in reader:
            index_of_dat = line[-1]
            with open("./input.dat",'wb') as fd:
                for index in range(0,len(line)-1):
                    fd.write("%s\t%s\n" % (columns[index],line[index]))

            _process = subprocess.Popen(command,stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        close_fds=True, shell=True)

            stime = time.time()
            (out,err) = _process.communicate()
            ftime = time.time()
            print("Index %s processed in %d" % (index_of_dat,(ftime-stime)))
            exitcode = _process.returncode

            if exitcode == 0:
                results[index_of_dat] = out.strip().split('\t')
            else:
                print("ERROR %d. message: %s" % (exitcode,err))

    # collect all results into a single .csv file
    print("Aggregating results")
    with open(RESULT_FILE,'wb') as rd:
        for idx,line in list(results.items()):
            rd.write(idx + "," + ",".join(x for x in line) + "\n")
    print("Done")

if __name__ == '__main__':
    if len(sys.argv[1:]) < 1:
           sys.exit(1)
    sys.exit(runctx(sys.argv[1:]))
