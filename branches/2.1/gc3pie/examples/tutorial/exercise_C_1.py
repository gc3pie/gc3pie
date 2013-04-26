#! /usr/bin/env python
#
"""
Exercise C (1)

* have the number of jobs in list settable via command-line argument

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
    def __init__(self, **extra):
        # output_dir is automatically passed to the __init__()
        # constructor by the `SessionBasedScript` class, in case no
        # reasonable default was given, therefore, this line of code
        # will never be executed.
        if 'output_dir' not in extra:
            extra['output_dir'] = "./mygc3job"
        gc3libs.Application.__init__(
            self,
            arguments = ['/bin/hostname'], # mandatory
            inputs = [],                  # mandatory
            outputs = [],                 # mandatory
            stdout = "stdout.txt", **extra)

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
        for i in range(self.params.copies):
            yield ('GDemoApp',
                   GDemoSimpleApp,
                   [], extra)

if __name__ == "__main__":
    from exercise_C import GDemoScript
    GDemoScript().run()
