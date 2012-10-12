#! /usr/bin/env python
#
"""
Exercise C (2)

* explain _search_for_input_files(): modify the script so that it does
  md5sums of files found in directories given on the command line: in
  bash terms this is 'find DIRS -name PATTERN | xargs md5sum'

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


class GDemoSimpleApp(gc3libs.Application):
    """
    This simple application will run /bin/hostname on the remove host,
    and retrive the output in a file named `stdout.txt` into the
    output directory
    """
    def __init__(self, input_files, **extra):
        # output_dir is automatically passed to the __init__()
        # constructor by the `SessionBasedScript` class, in case no
        # reasonable default was given, therefore, this line of code
        # will never be executed.
        if 'output_dir' not in extra:
            extra['output_dir'] = "./mygc3job"
        gc3libs.Application.__init__(
            self,
            arguments = ['/usr/bin/md5sum'] + input_files, # mandatory
            inputs = input_files,                  # mandatory
            outputs = [],                 # mandatory
            stdout = "stdout.txt",
            stderr = "stderr.txt", **extra)

class GDemoScript(gc3libs.cmdline.SessionBasedScript):
    """
    GDemo script
    """
    version = '0.1'

    def setup_options(self):
        self.add_param('-n', '--copies', default=10, type=int,
                       help="Number of copies of the default application to run."
                       )

    def new_tasks(self, extra):
        # GC3Pie will delay submission of jobs if these exceed the
        # maximum number of jobs for a resource. Please note that in
        # `exercise_A` you have to do it by yourself.

        # Search for the input files.
        input_files = list(self._search_for_input_files(self.params.args, pattern='*'))

        if not input_files:
            return
        # compute how many input files we are going to assign to each
        # application
        max_per_app = len(input_files)/self.params.copies

        for i in range(self.params.copies):
            # Get the correct "slice" of input files for this app.
            ifiles = input_files[i*max_per_app : (i+1)*max_per_app]
            yield ('GDemoApp',
                   GDemoSimpleApp,
                   [ifiles],
                   extra.copy())

if __name__ == "__main__":
    from exercise_C_2 import GDemoScript
    GDemoScript().run()
