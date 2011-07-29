#! /usr/bin/env python
#
#   sepjobs.py -- Classify GAMESS output files according to keyword matches
#
#   Copyright (C) 2010, 2011 GC3, University of Zurich
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
Classify GAMESS output files according to keyword matches.
"""
__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2011-07-27:
    * Initial release.
"""
__author__ = 'Timm Reumann <timm.reumann@uzh.ch>, Riccardo Murri <riccardo.murri@gmail.com>'
__docformat__ = 'reStructuredText'



# TAKES 106 MIN. FOR SEPARATING ~2100 OUTPUT FILES

import os
import re
import sys
import fnmatch
import shutil

# pyCLI
#import cli.app
import cli.log
#import pstats

def search_for_input_directories(root, names):
    """
    Return list of directories under `root` (at any nesting level)
    whose name is contained in the list `names`.
    """
    result = [ ]
    for rootpath, dirs, files in os.walk(root):
        found_dirname = [ ]
        # removing hidden directories from directory list
        dirs = [ d for d in dirs if not d.startswith(r'.') ]
        for dirname in dirs:
            if dirname in names:
                result.append(os.path.join(rootpath, dirname))
                found_dirname.append(dirname)
        # removing assigned directories from directory list
        for dirname in found_dirname:
            dirs.remove(dirname)	
    return result


def search_for_input_files(root, ext):
    """
    Return list of files under `dir` (or any if its subdirectories)
    that match extension `ext`.
    """
    result = [ ]
    for rootpath, dirs, files in os.walk(root):
        # removing hidden directories from directory list
        dirs = [ d for d in dirs if not d.startswith('.') ]
        for filename in files:
            if filename.endswith(ext):
                result.append(os.path.join(rootpath, filename))
    return result


def parse_kwfile(filename):
    """
    Read `filename` and return a dictionary mapping a folder name into
    a corresponding regular expression to search for.
    """
    input = open(filename, 'r')
    result = { }
    for lineno, line in enumerate(input):
        line = line.strip()
        # ignore comments and empty lines
        if line == '' or line.startswith('#'):
            continue
        parts = line.split(':')
        if len(parts) < 3:
            log.error("Incorrect line %d in file '%s', ignoring.", lineno, filename)
            continue
        foldername, searchtype, what_to_search = parts[0:3]
        print "parts:", foldername, searchtype, what_to_search
        if len(parts) == 4:
            start = float(parts[3]) / 100.0
        else:
            start = 0.0
        if len(parts) == 5:
            end = float(parts[4]) / 100.0
        else:
            end = 1.0
        if searchtype in ('regexp', 're'):
            result[foldername] = (re.compile(what_to_search), start, end)
        else:
            # literal string search
            result[foldername] = (what_to_search, start, end)
    return result


## main function

@cli.log.LoggingApp
def sepjobs(cmdline):
    # `cmdline` is pyCLI's interface object
    cmdline.log.debug("Starting.")

    # parse classification file
    searches = parse_kwfile(cmdline.params.kwfile)
    cmdline.log.debug("Searching keywords: %s", searches)

    # create indexes
    index = { }
    for foldername in searches.iterkeys():
        index[foldername] = set()
    cmdline.log.debug("index: %s", index)
    # look for directory names under the search root (if given)
    dirs = search_for_input_directories(cmdline.params.search_root,
                                            cmdline.params.inpdir)
    cmdline.log.debug("Searching directories: %s", dirs)

    # look for input files
    files = [ ]
    for dirpath in dirs:
        files.extend(search_for_input_files(dirpath, cmdline.params.ext))
#    cmdline.log.debug("List of input files: %s", files)

    # let's rock!
    failrt = 0
    hitrt = 0
    unknown = set(files)
    
    for filepath in files:
#        cmdline.log.info("Now processing file '%s' ...", filepath)
        inputfile = open(filepath, 'r')

        for foldername, spec in searches.iteritems():
            regexp, start, end = spec

            # compute range
            flsize = os.path.getsize(filepath)
            btpos = int(flsize * start)
            btrng = int(flsize * (end - start)) + 1

            # read content
            inputfile.seek(btpos, 0)         # going to byte position in file
            content = inputfile.read(btrng)  # reading byte range from byte pos. file  

            def act_on_file(filepath):
                """Actions to perform when a file matches a classification keyword."""
                index[foldername].add(filepath) 
                unknown.remove(filepath)
                if cmdline.params.move:
                    if not os.path.exists(foldername):
                        os.makedirs(foldername)
                    os.rename(filepath,
                        os.path.join(foldername, os.path.basename(filepath)))
                
            match = regexp.search(content)
            if match :
                hitrt += 1
                act_on_file(filepath)
                break
            else:
                # retry with whole file
                content = inputfile.read()
                match = regexp.search(content)
                if match:
                    failrt += 1
                    act_on_file(filepath)
                    break
                    
    # print summary
    for foldername, filenames in index.iteritems():
        print "== %s ==" % foldername, "no. of files:", len(index[foldername])
        for filename in sorted(filenames):
            print ("    " + filename)
    print "== unknown ==", "no. of files:", len(unknown)
    for filename in sorted(unknown):
        print ("    " + filename)
    cmdline.log.info("failure rate = %d, hit rate = %d", failrt, hitrt)


## command-line parameters

sepjobs.add_param('kwfile',  help="Path to the file containing foldername to string search mappings.")
sepjobs.add_param('inpdir',  nargs='+',
                  help="Directory where files to analyze are located.")
sepjobs.add_param('-e', '--extension', '--ext',
                  dest='ext', metavar='EXT', default='out',
                  help="Restrict search to file with this extension."
                  " (default: %(default)s)")
sepjobs.add_param('-m', '--move',
                  dest='move', action='store_true', default=False,
                  help="Move files into folders named after their classification keyword.")
sepjobs.add_param('-S', '--search-root',
                  dest='search_root', default=os.path.expanduser('~'), metavar='DIR',
                  help="Search for input directories under the directory tree rooted at DIR. Must be a COMPLETE PATH e.g. ~/Desktop/Project"
                  "  (Default: '%(default)s')")


if __name__ == '__main__':
        sepjobs.run()
