#! /usr/bin/env python
"""
A specialized dictionary for representing computational resource characteristics.
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

import gc3libs
from gc3libs.utils import Struct

# DEBUG-print of data members accesses...
#import sys
#from utils import Struct
#import gc3libs # for the logging mechanism


# -----------------------------------------------------
# Resource
#
class Resource(Struct):
    '''
    `Resource` objects are dictionaries, comprised of the following keys.

    Statically provided, i.e., specified at construction time and changed never after:

      arc_ldap             string
      architecture         list of string *
      auth                 string  *
      frontend             string  *
      gamess_location      string
      max_cores_per_job    int     *
      max_memory_per_core  int     *
      max_walltime         int     *
      name                 string  *
      ncores               int
      type                 int     *

    Starred attributes are required for object construction.

    Dynamically provided (i.e., defined by the `get_resource_status()` method or similar):
      free_slots          int
      user_run            int
      user_queued         int
      queued              int
    '''

    def __init__(self, **keywd):

        self.isValid = False

        mandatory = [
            'architecture',
            'auth',
            'max_cores_per_job',
            'max_memory_per_core',
            'max_walltime',
            'name',
            'max_cores',
            'type',
            ]
        for param in mandatory:
            if param not in keywd:
                raise TypeError("Missing mandatory attribute '%s'" % param)

        Struct.__init__(self, **keywd)

        gc3libs.log.info("Resource %s init [ok]" % self.name)
        self.isValid = True

    def is_valid(self):
        return self.isValid
