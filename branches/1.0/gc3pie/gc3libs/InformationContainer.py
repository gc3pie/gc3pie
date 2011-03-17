#! /usr/bin/env python
"""
A specialized `dict` class.
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
__version__ = '1.0rc3 (SVN $Revision$)'


import gc3libs.exceptions
from utils import Struct


class InformationContainer(Struct):

    def __init__(self, initializer=None, **keywd):
        Struct.__init__(self, initializer, **keywd)
        if not self.is_valid():
            raise gc3libs.exceptions.InvalidInformationContainerError('Object `%s` of class `%s` failed validity check.' % (self, self.__class__.__name__))

    def is_valid(self):
        raise NotImplementedError("Abstract method `is_valid()` called - this should have been defined in a derived class.")
