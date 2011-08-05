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
  2011-08-04:
    * Removed csv-stuff, cannot deal with ""I/O", ERROR"
    * Now using ":" to separate searchstring from rest and "," for residual fields
    * Found problem with list comprehension and os.walk in "search_for_input_directories"
    * Added "done"-flag to make script ingoring already processed lines in config. file
  2011-08-03:
    * Use `.csv` file for configuration.
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


def CLEANLIST(itmlst, delstr) :
	delpos = []; p = 0					# list of deleting position, position
	for itm in itmlst :
		if re.match(delstr, itm) :		# "\" to suppress interpret. of "." by python
			delpos.append(p)
		p += 1
	ndp = 0								# no. of deleted positions
	for p in delpos :			
		p -= ndp				# convert. pos. in old dirlist to pos. in current one
		itmlst.pop(p)
#		del dirlist(p)			# gives error "can't delete function call"
		ndp += 1
	return itmlst


def search_for_input_directories(root, names):
    """
    Return list of directories under `root` (at any nesting level)
    whose name is contained in the list `names`.
    """
    result = []
    for rootpath, dirs, files in os.walk(root):
        matching_dirs = []
        # removing hidden directories from directory list
        if os.path.basename(rootpath).startswith(r'.'):
            print "DEBUG: current path:\n", rootpath
        if rootpath == root: 
            print "DEBUG: rootpath:\n", rootpath
            print "DEBUG: raw dirs:\n", dirs
        """
        dirs = [ d for d in dirs if not d.startswith(r'.') ]
        This produces the correct output according to print, but os.walk
        somehow ignores it - still goes into .dirname
        """
        dirs = CLEANLIST(dirs, '\.')
        if rootpath == root: 
            print "DEBUG: cleaned dirs:\n", dirs
        for dirname in dirs:
            if dirname in names:
                result.append(os.path.join(rootpath, dirname))
                matching_dirs.append(dirname)
        # removing assigned directories from directory list
        for dirname in matching_dirs:
            dirs.remove(dirname)
    print "DEBUG: matching dirs:\n", result    
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


class StringAdaptor(str):
    """
    Augment a `str` object with `search` and `match` methods with
    interface compatible with that exposed by the `re` objects.
    """
    def search(self, container):
        return (self in container)
    def match(self, container):
        return container.startswith(self)


def parse_kwfile(filename):
    """
    Read `filename` and return a dictionary mapping a folder name into
    a corresponding regular expression to search for.
    """
    number = re.compile(r'[0-9]+\.?[0-9]*')
    done = re.compile(r'done[ ,]*')
    file = open(filename, 'rw')
    result = { } ; newlns = []
    for line in file:
       
    	newlns.append(line)
        if line == '\n' or line.startswith('#') or line.isspace():
            print "DEBUG: ignoring line %r"% line
            continue        			# skip empty or comment lines
        if done.search(line.rsplit(':', 1)[1]):   #line.endswith('done'):
            print "DEBUG: ignoring line %r"% line
            continue            # skip lines already used for separation
            
        print "DEBUG: processing line %r"% line
        newlns[len(newlns)-1] = newlns[len(newlns)-1].rstrip(' \n') + ', done\n'
        print "DEBUG: new lines %r"% newlns[len(newlns)-1]
        what_to_search, rest = line.rsplit(':', 1)
        print "DEBUG: search string:", str(what_to_search)
        rest = rest.replace(' ', '').rstrip('\n') + ','
        searchtype, foldername, rest = rest.split(',', 2)
        print "DEBUG: searchtype, foldername:", searchtype, foldername
        start = 0.0 ; end = 1.0
        if number.match(rest):
        	startstr, rest = rest.split(',', 1)
        	start = float(startstr) / 100.0
        if number.match(rest):
        	endstr, rest = rest.split(',', 1)
        	end = float(endstr) / 100.0
        print "DEBUG: start, end:", start, end
        if searchtype in ('regexp', 're'):
            result[foldername] = (re.compile(what_to_search), start, end)
        else:
            # literal string search
            result[foldername] = (StringAdaptor(what_to_search), start, end)
        
#    print "DEBUG: parse_kwfile result is '%s'" % result
#    file.writelines(newlns)
    file.close()
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
#        for filename in sorted(filenames):
#            print ("    " + filename)
    print "== unknown ==", "no. of files:", len(unknown)
#    for filename in sorted(unknown):
#        print ("    " + filename)
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
        
""" 
config file for sepjobs.py:

# sample classifier file for sepjobs.py
#
# Each line has this form:
#    
#   searchstring: literal|regexp, foldername, [start, end], ['done']
#
               
gracefully: regexp, gracefully, 90.0, done         

I/O ERROR: literal, io_error, 80, 90, done, ,      
ddikick.x: Timed out: literal, timed_out, 90, 99 , , ,
Failed creating: literal, failed_creat, 20,,,,,,,
"""
