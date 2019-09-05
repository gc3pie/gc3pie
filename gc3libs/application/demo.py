#! /usr/bin/env python

"""
Specialized support for computational jobs running simple demo.
"""

# Copyright (C) 2009-2012  University of Zurich. All rights reserved.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
__docformat__ = 'reStructuredText'


import os
from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.application
from gc3libs.exceptions import *
from gc3libs.quantity import GB, hours


class Square(gc3libs.Application):

    """
    Square class, takes a filename containing a list of integer to be squared.
    writes an output containing the square of each of them
    """

    application_name = 'demo'

    def __init__(self, x):
        # src_square_sh = resource_filename(Requirement.parse("gc3utils"),
        #                                   "gc3libs/etc/square.sh")

        _inputs = []
        # _inputs.append((input_file, os.path.basename(input_file)))
        # _inputs.append((src_square_sh, 'square.sh'))

        _outputs = []

        # extra_args.setdefault('stdout', 'stdout.txt')
        # extra_args.setdefault('stderr', 'stderr.txt')

        gc3libs.Application.__init__(self,
                                     arguments=[
                                         "/usr/bin/expr",
                                         str(x),
                                         "*",
                                         str(x)],
                                     inputs=[],
                                     outputs=[],
                                     output_dir=None,
                                     stdout="stdout.txt",
                                     stderr="stderr.txt",
                                     # set computational requirements. XXX this
                                     # is mandatory, thus probably should
                                     # become part of the Application's
                                     # signature
                                     requested_memory=1 * GB,
                                     requested_cores=1,
                                     requested_walltime=1 * hours,
                                     )


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="square",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
