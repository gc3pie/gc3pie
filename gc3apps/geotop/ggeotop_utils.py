#! /usr/bin/env python

from __future__ import absolute_import, print_function
import sys
import fnmatch
import os
from optparse import OptionParser
import shutil

"""
1. new simulation: just generated
2. was already running: mixed with some results
3. pooluted by ggeotop

take *.asc as valid inputs also for job submission
folder pattern:
ggeotop.inpts
in/
out/

retrieve only out/
in/ remain unchaged

"""

dirs = ["in", "maps" "rec", "rad"]
# files = ["svf.asc","slp.asc", "asp.asc", "dem.asc"]

def search_and_list(input_folder):
    # agreed vlid input folder format:
    # ggeotop.inpts
    # in/
    # out/
    #
    for r,d,f in os.walk(input_folder):
        if "geotop.inpts" in f and "in" in d and "out" in d:
            print(r)

def main():
    """
    walk through the folder and remove
    N0*.asc
    out/
    *~
    """
    parser = OptionParser(usage="%prog [clean|list] [INPUT FOLDERs]")
    parser.add_option("-s", "--simulate", action="store_true", dest="simulate",
                      metavar="LEVEL",
                      help="Execute scrit in simulation mode"
                      " (default is FALSE).")

    parser.add_option("-f", "--full", action="store_true", dest="rec_also",
                      metavar="LEVEL",
                      help="Force removing also 'rec' folder")

    (options, args) = parser.parse_args(sys.argv[1:])

    if len(args) < 2:
        print(parser.usage)
        return 1

    command = args[0]
    input_folders = args[1:]
    
    if command == "list":
        for folder in input_folders:
            search_and_list(folder)
    elif command == "clean":
        for folder in input_folders:
            clean_folder(folder, simulate=options.simulate, rec_also=options.rec_also)
    else:
        print(parser.usage)

def clean_folder(input_folder, simulate, rec_also):
    
    filename_patterns = ["*~", "*.tgz"]
    dirname_patterns = ["tmp","*~"]

    if rec_also:
        dirname_patterns.append("rec")

    for r,d,f in os.walk(input_folder):
        
        skip = False
        
        for directory in dirs:
            if r.endswith(directory):
                skip = True

        if skip:
            continue

        # previous approach. keep for the record
        # folders_to_remove = [ folder for folder in d if folder in dirname_patterns ]
        # files_to_remove = [ fn for fn in f if fn in filename_patterns ]

        # start remove folders
        for folder in d:
            for p in dirname_patterns:
                if fnmatch.fnmatch(folder,p):
                    try:
                        # XXX: is this too strong assumption ?
                        # remove a folder 'out'
                        if simulate:
                            print("Removing folder %s" % os.path.join(r,folder))
                        else:
                            shutil.rmtree(os.path.join(r,folder), ignore_errors=True)
                            if folder == "rec":
                                # create an empty new one
                                os.mkdir(os.path.join(r,folder))
                    except Exception as x:
                        print("Failed deleting folder %s. Error %s. Message %s" % (folder,x.__class__,x.message))
                    break

        for fn in f:
            # try remove file matching pattern
            for p in filename_patterns:
                if fnmatch.fnmatch(fn,p):
                    try:
                        if simulate:
                            print("Removing file %s" % os.path.join(r,fn))
                        else:
                            os.remove(os.path.join(r,fn))
                    except Exception as x:
                        print("Failed deleting file %s. Error %s. Message %s" % (fn,x.__class__,x.message))
                    break


if __name__ == "__main__":
    sys.exit(main())
