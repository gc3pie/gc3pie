#! /usr/bin/env python
"""
A specialized dictionary for representing computational resource characteristics.
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
__version__ = '1.0rc1 (SVN $Revision$)'


from InformationContainer import InformationContainer

# DEBUG-print of data members accesses...
#import sys
#from utils import Struct
#import gc3libs # for the logging mechanism


# -----------------------------------------------------
# Resource
#
class Resource(InformationContainer):
    '''
    `Resource` objects are dictionaries, comprised of the following keys.
    
    Statically provided, i.e., specified at construction time and changed never after:

      arc_ldap             string   
      auth                 string
      frontend             string
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

    def is_valid(self):
        if (self.has_key('max_cores_per_job') 
            and self.has_key('max_memory_per_core') 
            and self.has_key('type') 
            and self.has_key('name') 
            and self.has_key('max_walltime') 
            ):
            return True
        else:
            return False

    # def __getattr__(self, key):
    #     name = Struct.__getattr__(self, 'name')
    #     gc3libs.log.debug("Resource '%s': query for attribute '%s'" % (name, key))
    #     return Struct.__getattr__(self, key)
    # def __getitem__(self, key):
    #     name = Struct.__getitem__(self, 'name')
    #     gc3libs.log.debug("Resource '%s': query for attribute '%s'" % (name, key))
    #     return dict.__getitem__(self, key)
