#! /usr/bin/env python
#
"""
Python implementation of rosenbrock.cpp for tests.
"""
# Copyright (C) 2011, 2012, 2013,  University of Zurich. All rights reserved.
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
from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
__docformat__ = 'reStructuredText'

import os
import sys
import logging
import tempfile
import shutil
import logging


if __name__ == "__main__":
    print("Compute Rosenbrock function")

    # log = open('/home/benjamin/.gc3/debug.log', 'a')
    # log.write("Running in directory: '%s'\n" % os.getcwd())
    # log.write("Contents of working directory: %s\n" % os.listdir(os.getcwd()))
    # log.close()

    para_file = open("parameters.in")
    x = float(para_file.readline().split()[1])
    y = float(para_file.readline().split()[1])

    fun = 100.0 * (y - x ** 2) ** 2 + (1 - x) ** 2

    out_file = open("rosenbrock.out", 'w')
    out_file.write(str(fun) + '\n')

    print("done")
