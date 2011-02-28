#! /usr/bin/env python
#
"""
Specialized support for computational jobs running simple demo.
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
__version__ = '1.0rc2 (SVN $Revision$)'


import gc3libs
import gc3libs.application
from gc3libs.exceptions import *
import os
import os.path
from pkg_resources import Requirement, resource_filename


class Square(gc3libs.Application):
    """
    Square class, takes a filename containing a list of integer to be squared.
    writes an output containing the square of each of them
    """
    def __init__(self, x):
        # src_square_sh = resource_filename(Requirement.parse("gc3utils"),
        #                                   "gc3libs/etc/square.sh")

        _inputs = []
        # _inputs.append((input_file, os.path.basename(input_file)))
        # _inputs.append((src_square_sh, 'square.sh'))

        _outputs = []

        # kw.setdefault('stdout', 'stdout.txt')
        # kw.setdefault('stderr', 'stderr.txt')
        
        #  gc3libs.Application.__init__(self, executable, arguments, inputs, outputs, output_dir, **kw):


        gc3libs.Application.__init__(self,
                                     executable = "/usr/bin/expr",
                                     arguments = [str(x), "*", str(x)],
                                     inputs = [],
                                     outputs = [],
                                     output_dir = None,
                                     stdout = "stdout.txt",
                                     stderr = "stderr.txt",
                                     # set computational requirements. XXX this is mandatory, thus probably should become part of the Application's signature
                                     requested_memory = 1,
                                     requested_cores = 1,
                                     requested_walltime = 1
                                     )



gc3libs.application.register(Square, 'square')

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="square",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
