#! /usr/bin/env python
#
"""
Exercise B (1)

* insert the gdemo_simple application into the minimal session-based
  script, write a anew_tasks() that inserts a single hard-coded job
  into the session

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

    def new_tasks(self, extra):
        return [
            ('GDemoApp',
             GDemoSimpleApp,
             [],
             extra,),
            ]

if __name__ == "__main__":
    from exercise_B_1 import GDemoScript
    GDemoScript().run()
