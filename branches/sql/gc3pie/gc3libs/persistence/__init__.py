#! /usr/bin/env python
#
"""
Facade to store and retrieve Job information from permanent storage.
"""
# Copyright (C) 2009-2012 GC3, University of Zurich. All rights reserved.
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
__version__ = 'development version (SVN $Revision$)'


# Export the "public API" towards other modules, so that
# one can do ``import gc3libs.persistence`` and load whatever
# should be normally needed and supported.  Other modules in
# this package should be considered "internal use only".
from filesystem import FilesystemStore
from idfactory import IdFactory, JobIdFactory
from store import make_store, Persistable


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="persistence",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
