#! /usr/bin/env python
"""
GC3Libs - A python package for controlling the life-cycle of a Grid or batch computational job 

GC3Libs provides services for submitting computational jobs to Grids
and batch systems and controlling their execution, persisting job
information, and retrieving the final output.

GC3Libs takes an application-oriented approach to batch computing. A
generic `Application` class provides the basic operations for
controlling remote computations, but different `Application` subclasses
can expose adapted interfaces, focusing on the most relevant aspects
of the application being represented. 
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
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


# NG's default packages install arclib into /opt/nordugrid/lib/pythonX.Y/site-packages;
# add this anyway in case users did not set their PYTHONPATH correctly
import sys
sys.path.append('/opt/nordugrid/lib/python%d.%d/site-packages' 
                % sys.version_info[:2])

import logging
log = logging.getLogger("gc3libs")



## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
