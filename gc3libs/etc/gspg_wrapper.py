#!/bin/env python

"""
# gspg_wrapper.py -- Wrapper script to execute CTX analysis 
# 
# Authors: Sergio Maffioletti <sergio.maffioletti@uzh.ch>
#
# Copyright (c) 2015-2016 S3IT, University of Zurich, http://www.s3it.uzh.ch
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

import os
import sys
import subprocess
import argparse

import pandas
import time


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
        command =  "ctx-linkdyn-ordprm-sirs.p4 "
    command += " -i ./input.dat"

    # Prepare input file from inputcsv
    reader = pandas.read_csv(arguments.inputcsv, header=0)
    for index in range(0,len(reader)-1):
        indata = reader.ix[index]
        index_of_dat = indata.pop('id')
        indata.to_csv("./input.dat",header=False,index=True,sep="\t")
        _process = subprocess.Popen(command,stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   close_fds=True, shell=True)
        stime = time.time()
        (out,err) = _process.communicate()
        ftime = time.time()
        print "Index %d processed in %d" % (index,(ftime-stime))
        exitcode = _process.returncode

        if exitcode == 0:
            results[index_of_dat] = out.strip().split('\t')

    # collect all results into a single .csv file
    print("Aggregating results")
    # s = pandas.Series(results.values(), results.keys())
    s = pandas.DataFrame.from_dict(results,orient='index')
    s.to_csv('results.csv', header=False,index=True)
    print "Done"
    
if __name__ == '__main__':
    if len(sys.argv[1:]) < 1:
           sys.exit(1)
    sys.exit(runctx(sys.argv[1:]))
