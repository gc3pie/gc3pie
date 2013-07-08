#! /usr/bin/env python
#
"""
Exercise D

build a ProcessFilesInParallel class that takes
* a directory
* a filename pattern
* a command (Application class)

and runs one job per each file in directory that matches the given
pattern

"""
# Copyright (C) 2012, GC3, University of Zurich. All rights reserved.
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


import gc3libs
import gc3libs.cmdline


class ProcessFilesInParallel(gc3libs.cmdline.SessionBasedScript):
    """
    Process files in parallel.

    This script will require 3 command line options:

    --command   COMMAND    the command to execute.
    --directory DIRECTORY  the directory containing input files.
    --pattern   PATTERN    pattern input file must match.

    and it will execute command `COMMAND` for each file matching
    `PATTERN` into the directory `DIRECTORY`.
    
    """
    version = '0.1'

    def setup_options(self):
        self.add_param('--command', required=True,
                       help="Command to execute on each input file.")
        self.add_param('--directory', required=True,
                       help="Directory containing the input files.")
        self.add_param('--pattern', required=True,
                       help="Pattern that input files inside `DIRECTORY` must match.")

    def new_tasks(self, extra):
        input_files = self._search_for_input_files(
            self.params.directory,
            pattern=self.params.pattern)

        if not input_files:
            print "No input files matching `%s` in directory `%s`" % (
                self.params.pattern, self.params.directory)
            return 

        for ifile in input_files:
            kw = extra.copy()
            kw['stdout'] = 'stdout.txt'
            yield (
                "File:%s" % ifile.replace('/', '_'),
                gc3libs.Application,
                [[self.params.command, ifile],
                 [ifile],
                 [],
                 ],
                kw)
                
        
## main: run tests

if "__main__" == __name__:
    from exercise_D import ProcessFilesInParallel
    ProcessFilesInParallel().run()
