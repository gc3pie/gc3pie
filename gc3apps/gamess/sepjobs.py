#! /usr/bin/env python
#
#   sepjobs.py -- Classify GAMESS output files according to keyword matches
#
#   Copyright (C) 2010, 2011  University of Zurich. All rights reserved.
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
# summary of user-visible changes
__changelog__ = """
  2011-08-18:
    * update of config.file now after file inspection and job assignment
    * worked up movement of unassigned jobs to "none-found"
  2011-08-16:
    * added profiling
  2011-08-15:
    * witing new lines to config.file now seems to work (although I don't know why)
    * now moving folder of each job instead of moving files if each inspected file resides in its own folder, autom. determing common path part of file paths
    * added profiling
  2011-08-09:
    * moving files into subfolders for key strings now works
    * automatic distinct. between new and old search by subfolder "none_found" now works
    * added working dir. and source dir. to handle new and old search
    * still problem with writing to config.file
  2011-08-08:
    * removed support for several input dir.s
    * default search rootpath now is current path of script
    * opening of file in loop for search item now conditioned by search range of item
    * search in entire file now outside loop for search items
    * worked up parse_kwfile
    * still problem with file moving
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


from __future__ import absolute_import, print_function
import os
import re
import sys
import fnmatch
import shutil
import cli.log


from gc3libs.cmdline import existing_file, existing_directory


sys.setcheckinterval(1000)    # interpret. check interval for threads and signals

def CLEANLIST(itmlst, delstr) :
        delpos = []; p = 0                                      # list of deleting position, position
        for itm in itmlst :
                if re.match(delstr, itm) :              # "\" to suppress interpret. of "." by python
                        delpos.append(p)
                p += 1
        ndp = 0                                                         # no. of deleted positions
        for p in delpos :                       
                p -= ndp                                # convert. pos. in old dirlist to pos. in current one
                itmlst.pop(p)
#               del dirlist(p)                  # gives error "can't delete function call"
                ndp += 1
        return itmlst


def search_for_input_directories(root, name):
    """
    Return list of directories under `root` (at any nesting level)
    whose with user specified input dir. `name`.
    """
    name = name.strip('/')
    print 'DEBUG: stripped name of input folder:', name
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
        
        dirs = [ d for d in dirs if not d.startswith(r'.') ]
        This produces the correct output according to print, but os.walk
        somehow ignores it - still goes into .dirname
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


def search_for_none_found(root):
    old_search = False
    if 'none_found' in os.listdir(root):
        old_search = True
        root = os.path.join(root, 'none_found')
#    print 'DEBUG: dirlist for search of none_found:\n', os.listdir(root)
    return root, old_search


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


def parse_kwfile(filename, old_search):
    """
    Read `filename` and return a dictionary mapping a folder name into
    a corresponding regular expression to search for.
    """
    number = re.compile(r'[0-9]+\.?[0-9]*')
    done = re.compile(r'done[ ,]*')
    folder_exists = False
 #   configfile = open(filename, 'r')
    result = [] ; newlns = []
    with open(filename, 'r') as configfile:
        for line in configfile: 
            newlns.append(line)
            if line == '\n' or line.startswith('#') or line.isspace():
#                print "DEBUG: ignoring line %r"% line
                continue                        # skip empty or comment lines
            if done.search(line.rsplit(':', 1)[1]):
                if old_search:
#                    print "DEBUG: ignoring line %r"% line
                    continue            # skip lines already used for separation
            else:
                newlns[len(newlns)-1] = newlns[len(newlns)-1].rstrip(' \n') + ', done\n'
#            print "DEBUG: processing line %r"% line
#            print "DEBUG: new lines %r"% newlns[len(newlns)-1]
            what_to_search, rest = line.rsplit(':', 1)
#            print "DEBUG: search string:", str(what_to_search)
            start = 0.0 ; end = 1.0
            rest = rest.replace(' ', '').rstrip('\n') + ',,,'
            field = rest.split(',')
            searchtype = field[0]
            foldername = field[1]
            if number.match(field[2]):
                start = float(field[2]) / 100.0
            if number.match(field[3]):
                end = float(field[3]) / 100.0
            print "DEBUG: search string, searchtype, foldername, start, end:\n", '"', what_to_search, '"', searchtype, foldername, start, end
            if searchtype in ('regexp', 're'):
                result.append([foldername, re.compile(what_to_search), start, end, 
                               folder_exists])
            else:                                   # literal string search
                result.append([foldername, re.compile(what_to_search), start, end, 
                               folder_exists])
        configfile.close()
    return result, newlns


## main function

@cli.log.LoggingApp
def sepjobs(cmdline):
    # `cmdline` is pyCLI's interface object
    cmdline.log.debug("Starting.")
    
    # look for directory name under the search root (if given)
    dirs = search_for_input_directories(cmdline.params.search_root,
                                            cmdline.params.inpdir)
    wrkdir = dirs[0]
    cmdline.log.debug("Working directory: %s", wrkdir)
    srcdir, old_search = search_for_none_found(wrkdir)
    none_found_exists = old_search
    # look for input files and common path of all files
    files = search_for_input_files(srcdir, cmdline.params.file)
#    cmdline.log.debug("List of input files: %s", files)
        # conditioning by cmdln argument "-m" + number later
#    if cmdline.params.dirlvl == -1:
    commonpath = os.path.commonprefix(files).rsplit('/',1)[0]
#    else if cmdline.params.dirlvl == 0:
        
#    else:
#        commonpath = os.path.commonprefix(files).rsplit('/',cmdline.params.dirlvl)[0]
    cmdline.log.debug("common prefix input file paths:\n %s", commonpath)
    # parse classification file
    srchkey, newlns = parse_kwfile(cmdline.params.kwfile, old_search)
    cmdline.log.debug("Searching keywords: %s", srchkey)
    # create indices
    index = { }
    for i in xrange(0, len(srchkey)):
        index[srchkey[i][0]] = set()
    cmdline.log.debug("index: %s", index)
    index['none_found'] = set(files)
    
    def act_on_file(filepath, foldername, assigned, folder_exists, old_search): 
        """Actions to perform when a file matches a classification keyword."""
        if assigned:
            index[foldername].add(filepath) 
            index['none_found'].remove(filepath)
        """
        dirpth0 = os.path.dirname(filepath)
        print dirpth0.replace(commonpath, 
            os.path.join(wrkdir, foldername), 1)
        """
        if cmdline.params.move:
#              cmdline.log.info("Going to move file")
            if not folder_exists: 
                os.mkdir(os.path.join(wrkdir, foldername))
                folder_exists = True
#              cmdline.log.info("Moving file")
            dirpath0 = os.path.dirname(filepath)
            if dirpath0 != commonpath:
                dirpath1 = dirpath0.replace(commonpath, 
                           os.path.join(wrkdir, foldername), 1)
                try:
                    os.makedirs(dirpath1)
                except OSError:
                    print '\r'
                os.rename(dirpath0, dirpath1)
            else:
                os.rename(filepath, os.path.join(wrkdir, foldername, 
                    os.path.basename(filepath)))
        return assigned, folder_exists    
    
    # let's rock!
    failrt = 0
    hitrt = 0
    Nsrchkeys = len(srchkey)
    for filepath in files:
#        cmdline.log.info("Now processing file '%s' ...", filepath)
        inputfile = open(filepath, 'r')
        start0 = 1.0 ; end0 = 0.0
        assigned = False
        for i in xrange(0, Nsrchkeys):
#            cmdline.log.info("Now looking for '%s' ...", srchkey[i][4])
            if srchkey[i][2] < start0 or srchkey[i][3] > end0: 
                # compute range
                flsize = os.path.getsize(filepath)
                btpos = int(flsize * srchkey[i][2])
                btrng = int(flsize * (srchkey[i][3] - srchkey[i][2])) + 1
                # read content
                inputfile.seek(btpos, 0)         # going to byte position in file
                content = inputfile.read(btrng)  # reading byte range from byte pos.
                start0 = srchkey[i][2]
                end0 = srchkey[i][3]
            match = srchkey[i][1].search(content)       
            if match:
#                cmdline.log.info("Found match, folder_exists='%s'", srchkey[i][4])
                hitrt += 1
                assigned, srchkey[i][4] = act_on_file(filepath, srchkey[i][0], True, 
                                                      srchkey[i][4], old_search)
                break
        # retry search with whole file
        if not assigned:               
            content = inputfile.read()
            for i in xrange(0, Nsrchkeys):
                match = srchkey[i][1].search(content)
                if match:
                    failrt += 1
                    assigned, srchkey[i][4] = act_on_file(filepath, srchkey[i][0], True, 
                                                          srchkey[i][4], old_search)
                    # how to avoid copying regexp back to srchkey ?  
                    break
        if not assigned:
            assigned, none_found_exists = act_on_file(filepath, 'none_found', False, 
                                                      none_found_exists, old_search)
        inputfile.close()
    with open(cmdline.params.kwfile, 'w') as configfile:
        print "DEBUG: writing new lines to config.file"
        configfile.writelines(newlns)
        configfile.close()
    # print summary
    for foldername, filenames in index.iteritems():
        print "== %s ==" % foldername, "no. of files:", len(index[foldername])
#        for filename in sorted(filenames):
#            print ("    " + filename)
    cmdline.log.info("failure rate = %d, hit rate = %d", failrt, hitrt)


## command-line parameters

sepjobs.add_param('kwfile', type=existing_file,
                  help="Path to the file containing foldername to string search mappings.")
sepjobs.add_param('inpdir', type=existing_directory, # no support of several input dir.s
                  help="Directory where files to analyze are located.")
sepjobs.add_param('-f', '--file', '--fl',
                  dest='file', metavar='EXT', default='.out',
                  help="Restrict search to file with this extension."
                  " (default: %(default)s)")
# sepjobs.add_param('-m', '--move',
#                   dest='dirlvl', default=-1,
#                   help="Move files with their folders accord. to classification. Optionally with directory level (0 for pure file moving, 3 for moving file with folders up to 3rd level")
sepjobs.add_param('-m', '--move',
                  dest='move', action='store_true', default=False,
                  help="Move files into folders named after their classification keyword.")
                  
sepjobs.add_param('-S', '--search-root', metavar='DIR',
                  dest='search_root',
                  type=existing_directory, default=os.getcwd(), 
                  help="Search for input directories under the directory tree rooted at DIR. Must be a COMPLETE PATH e.g. ~/Desktop/Project"
                  "  (Default: '%(default)s')")

import cProfile
import profile
import pstats
if __name__ == '__main__':
    sepjobs.run()
    """
    # determining overhead of cProfiler
    pr = profile.Profile()
    cumovrhd = 0.0
    for i in range(5):
        ovrhd = pr.calibrate(10000)
        print 'overhead', i, '=', ovrhd 
        cumovrhd += ovrhd
    print 'average overhead =', cumovrhd / 5.0
    profile.Profile.bias = cumovrhd / 5.0
    cProfile.run('sepjobs.run()', 'sepjobs.prfl')
    p = pstats.Stats('sepjobs.prfl')
    p.sort_stats('time', 'calls').print_stats(10)
    """
       
""" 
config file for sepjobs.py:

# sample classifier file for sepjobs.py
#
# Each line has this form:
#    
#   searchstring: literal|regexp, foldername, [start, end], ['done']
#
               
gracefully: regexp, gracefully, 90.0, , done         

#I/O ERROR: literal, io_error, 80,, done, ,      
#ddikick.x: Timed out: literal, timed_out,, , , ,
#Failed creating: literal, failed_creat, 90,,,,,,,
#ILLEGAL GENERAL  BASIS FUNCTION REQUESTED: literal, ILLEGAL_BASISFUNC, 80,    
#ILLEGAL EXTENDED BASIS FUNCTION REQUESTED.: literal, ILLEGAL_EXTBASISFUNC, 80  , 
#JACOBI DIAGONALIZATION FAILS: literal, JACOBI_DIAG_FAILS, 80  , done
#ILLEGAL          BASIS FUNCTION REQUESTED: literal, ILLEGAL_____BASISFUNC, 80  
"""
