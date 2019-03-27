#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2009-2011  University of Zurich. All rights reserved.
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
# TAKES 1 HOUR FOR EXTRACTING ~1670 OUTPUT FILES

__docformat__ = 'reStructuredText'
__changelog__ = """
  2011-08-17:
    * reading file as one string once before inspection
    * inspection of total string ("re.search")
    * added multiline regexp. for title section
    * added leading explicit char.s to "byte_int" and "totmem" => much faster
    * added search function for folder containing the files
"""


from __future__ import absolute_import, print_function
import csv
import os
import re
import sys
import fnmatch


regexps = {
    'title':re.compile(r'INPUT CARD\> \$DATA *\n INPUT CARD\>(?P<title>.{0,10})'),
    'nbasis':re.compile(r'NUMBER OF CARTESIAN GAUSSIAN BASIS FUNCTIONS *= *(?P<nbasis>[0-9]+)'),
    'ngauss':re.compile(r'NGAUSS=(?P<ngauss>[0-9])'),
    'npfunc':re.compile(r'NPFUNC=(?P<npfunc>[0-9])'),
    'ndfunc':re.compile(r'NDFUNC=(?P<ndfunc>[0-9])'),
    'nffunc':re.compile(r'NFFUNC=(?P<nffunc>[0-9])'),
#    'parall':re.compile(r'PARALL *= *(?P<parall>[TF])'),
    'wctime':re.compile(r'TOTAL WALL CLOCK TIME *= *(?P<wctime>[0-9]+\.[0-9]+) *SECONDS'),
#    'itermem':re.compile(r'MEMORY REQUIRED FOR UHF/ROHF ITERS *= *(?P<itermem>[0-9]+) *WORDS\.'),
    'totmem':re.compile(r'\n *(?P<totmem>[0-9]+) *WORDS OF DYNAMIC MEMORY USED'),
    'scftyp':re.compile(r'SCFTYP=(?P<scftyp>[A-Z]+)', re.I),
    'gbasis':re.compile(r'GBASIS=(?P<gbasis>[A-Z0-9-]+)', re.I),
    'byte_int':re.compile(r'USING (?P<byte_int>[0-9]+) BYTES/INTEGRAL'),
    'no_int':re.compile(r'NUMBER \s* OF \s* NONZERO \s* TWO-ELECTRON \s* INTEGRALS \s* = \s*(?P<no_int>[0-9]+)', re.X),
#    'itol':re.compile(r'ITOL=(?P<itol>[0-9]+)', re.I)
}


def CLEANLIST(itmlst, delstr) :
	delpos = []; p = 0					# list of deleting position, position
	for itm in itmlst :
		if re.match(delstr, itm) :		
			delpos.append(p)
		p += 1
	ndp = 0								# no. of deleted positions
	for p in delpos :			
		p -= ndp				# convert. pos. in old dirlist to pos. in current one
		itmlst.pop(p)
#		del dirlist(p)			# gives error "can't delete function call"
		ndp += 1
	return itmlst


def search_for_input_directories(root, name):
    """
    Return list of directories under `root` (at any nesting level)
    whose with user specified input dir. `name`.
    """
    result = []
    for rootpath, dirs, files in os.walk(root):
        matching_dirs = []
        # removing hidden directories from directory list
        """
        if os.path.basename(rootpath).startswith(r'.'):
            print "DEBUG: current path:\n", rootpath
        if rootpath == root: 
            print "DEBUG: rootpath:\n", rootpath
            print "DEBUG: raw dirs:\n", dirs
        """
        dirs = CLEANLIST(dirs, '\.')
#        if rootpath == root: 
#            print "DEBUG: cleaned dirs:\n", dirs
        for dirname in dirs:
#            if dirname in names:  # support of several input folders undesirable
            if dirname == name:
                result.append(os.path.join(rootpath, dirname))
                matching_dirs.append(dirname)
        # removing assigned directories from directory list
        for dirname in matching_dirs:
            dirs.remove(dirname)
#    print "DEBUG: dirs matching", name, ":\n", result    
    return result


def search_for_input_files(root, ext):
    """
    Return list of files under `dir` (or any if its subdirectories)
    that match extension `ext`.
    """
    result = [ ]
    for rootpath, dirs, files in os.walk(root):
        # removing hidden directories from directory list
#        dirs = [ d for d in dirs if not d.startswith('.') ]  see above
        dirs = CLEANLIST(dirs, '\.')
        for filename in files:
            if filename.endswith(ext):
                result.append(os.path.join(rootpath, filename))
    return result


## main: run tests

dirname = sys.argv[1]
dirpath = search_for_input_directories(os.getcwd(), dirname)
print 'dirpath=', dirpath
filelist = search_for_input_files(dirpath[0], 'out')
#print 'file list=', filelist

data = [ ]
for filename in filelist:
    file = open(filename, 'r')
    result = { 'file':os.path.splitext(os.path.basename(file.name))[0] }
    content = file.read()
#    print 'content:\n', [content]
    file.close()
    for name, rx in regexps.items():
        match = rx.search(content)
        if match:
            result[name] = match.group(name)
    data.append(result)

fields = ['file'] + list(sorted(regexps.keys()))

output = csv.DictWriter(sys.stdout, fields, dialect='excel')
output.writerow(dict((f,f) for f in fields))
for row in data:
    output.writerow(row)

