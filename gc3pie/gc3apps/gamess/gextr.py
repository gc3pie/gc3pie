#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2009-2011 GC3, University of Zurich. All rights reserved.
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
import os
import re
import sys
import fnmatch


regexps = {
#		'title':re.compile(r'INPUT CARD>(?P<title>[A-Z]?)'),		# how to read in title INPUT CARD>C4H10
    'nbasis':re.compile(r'NUMBER OF CARTESIAN GAUSSIAN BASIS FUNCTIONS *= *(?P<nbasis>[0-9]+)'),
#    'parall':re.compile(r'PARALL *= *(?P<parall>[TF])'),
    'wctime':re.compile(r'TOTAL WALL CLOCK TIME *= *(?P<wctime>[0-9]+\.[0-9]+) *SECONDS'),
#    'itermem':re.compile(r'MEMORY REQUIRED FOR UHF/ROHF ITERS *= *(?P<itermem>[0-9]+) *WORDS\.'),
    'totmem':re.compile(r'(?P<totmem>[0-9]+) *WORDS OF DYNAMIC MEMORY USED'),
    # input params are used to group output files into homogeneous groups
    'scftyp':re.compile(r'SCFTYP=(?P<scftyp>[A-Z]+)', re.I),
    'gbasis':re.compile(r'GBASIS=(?P<gbasis>[A-Z0-9-]+)', re.I),
    'bytes_per_integral':re.compile(r'(?P<bytes_per_integral>[0-9]+) BYTES/INTEGRAL'),
    'num_integrals':re.compile(r'NUMBER \s* OF \s* NONZERO \s* TWO-ELECTRON \s* INTEGRALS \s* = \s*(?P<num_integrals>[0-9]+)', re.X)
#    'itol':re.compile(r'ITOL=(?P<itol>[0-9]+)', re.I),
}

failed_job_re = re.compile(r'EXECUTION OF GAMESS TERMINATED -ABNORMALLY-')
invalid_basis_re = re.compile(r'ILLEGAL \s+ (EXTENDED|GENERAL)? \s* BASIS \s+ FUNCTION \s+ REQUESTED.', re.X)

def extract_data(file):
    line_is_title = False
    result = { 'file':os.path.splitext(os.path.basename(file.name))[0] }
    for line in file:
        if failed_job_re.search(line):
            raise RuntimeError("Failed GAMESS job.")
        for name, rx in regexps.items():
            match = rx.search(line)
            if match:
                result[name] = match.group(name)
        # molecule name is first line after the $DATA in input file
        if line_is_title:
            result['title'] = line.strip()[11:] # remove 'INPUT CARD>' prefix
            line_is_title = False
        if line.upper().startswith(r' INPUT CARD> $DATA'):
            line_is_title=True
    return result


## main: run tests

dirname = sys.argv[1]
dirpath = os.path.abspath(dirname)
#print 'dirpath=',dirpath
dirlist = os.listdir(dirpath)
#print 'dirlist=',dirlist
filelist = fnmatch.filter(dirlist, '*.out')
#print 'pathlist =',pathlist
os.chdir(dirpath)

data = [ ]
for filename in filelist: #sys.argv[1:]:
    file = open(filename, 'r')
    try:
        data.append(extract_data(file))
    except RuntimeError:
        sys.stderr.write("Ignoring failed GAMESS job output file '%s'.\n" % filename)
    file.close()

fields = ['file', 'title'] + list(sorted(regexps.keys()))

output = csv.DictWriter(sys.stdout, fields, dialect='excel')
output.writerow(dict((f,f) for f in fields))
for row in data:
    output.writerow(row)

