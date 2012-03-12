#! /usr/bin/env python
"""
A specialized dictionary for representing computational resource characteristics.
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

    def __init__(self, name=None, type=None, auth=None, architecture=None, max_cores_per_job=None, max_memory_per_core=None, max_walltime=None, **keywd):
        
        self.isValid = False

        if not name:
            raise TypeError("Missing mandatory attribute 'name'")
        else:
            self.name = str(name)

        if not type:
            raise TypeError("Missing mandatory attribute 'type'")
        else:
            self.type = str(type)

        if not architecture:
            raise TypeError("Missing mandatory attribute 'architecture'")
        else:
            self.architecture = str(architecture)

        if not auth:
            raise TypeError("Missing mandatory attribute 'auth'")
        else:
            self.auth = str(auth)

        if not max_cores_per_job:
            raise TypeError("Missing mandatory attribute 'max_cores_per_job'")
        else:
            # try to type it to int
            try:
                self.max_cores_per_job = int(max_cores_per_job)
            except ValueError:
                # not a pure int
                raise TypeError("Mandatory attribute 'max_cores_per_job' should be <int>")

        if not max_memory_per_core:
            raise TypeError("Missing mandatory attribute 'max_memory_per_core'")
        else:
            # try to type it to int
            try:
                self.max_memory_per_core = int(max_memory_per_core)
            except ValueError:
                # not a pure int
                raise TypeError("Mandatory attribute 'max_memory_per_core' should be <int>")

        if not max_walltime:
            raise TypeError("Missing mandatory attribute 'max_walltime'")
        else:
            # try to type it to int
            try:
                self.max_walltime = int(max_walltime)
            except ValueError:
                # not a pure int
                raise TypeError("Mandatory attribute 'max_walltime' should be <int>")

        Struct.__init__(self, **keywd)

        gc3libs.log.info("Resource %s init [ok]" % self.name)
        self.isValid = True

    def is_valid(self):
        return self.isValid

