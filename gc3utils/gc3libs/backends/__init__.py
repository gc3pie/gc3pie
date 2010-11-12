#! /usr/bin/env python
"""
Interface to different resource management systems for the GC3Libs.
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
#
# Includes parts adapted from the ``bzr`` code, which is
# copyright (C) 2005, 2006, 2007, 2008, 2009 Canonical Ltd
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
from gc3libs.Exceptions import *


class LRMS:
    """Base class for interfacing with a computing resource."""

    def __init__(self, resource): 
        raise NotImplementedError("Abstract method `LRMS()` called - this should have been defined in a derived class.")
    
    def submit_job(self, application):
        """
        Submit a single job.
        Return a Job object.
        """
        raise NotImplementedError("Abstract method `LRMS.submit_job()` called - this should have been defined in a derived class.")

    def check_status(self, job):
        """
        Check the status of a single job.
        Return a Job object.
        """
        raise NotImplementedError("Abstract method `LRMS.check_status()` called - this should have been defined in a derived class.")
    
    def get_results(self, job):
        """
        Retrieve results from a single job.
        Return a Job object.
        """
        raise NotImplementedError("Abstract method `LRMS.get_results()` called - this should have been defined in a derived class.")
    
    def cancel_job(self, job):
        """
        Cancel a single running job.
        Return a Job object.
        """
        raise NotImplementedError("Abstract method `LRMS.cancel_job()` called - this should have been defined in a derived class.")
    
    def get_resource_status(self):
        """
        Get the status of a single resource.
        Return a Resource object.
        """
        raise NotImplementedError("Abstract method `LRMS.get_resource_status()` called - this should have been defined in a derived class.")
    
    def tail(self, job, remote_filename, **kw):
        """
        Gets the output of a running job, similar to ngcat.
        Return open File handler to local copy of the file
        
        examples:
        h = gcli.tail(job,'stdout')
        for line in h:
            print line

        h = gcli.tail(job,'stdout', {'offset':1024,'buffer_size':2048})
        ...
        
        Copy a remote file belonging to the job sandbox and return a file handler to the local copy of the file.
        Additional parameters could be:
           offset: int
           buffer_size: int
        Primarly conceived for stdout and stderr.
        Any exception raised by operations will be passed through.
        @param job: the job object
        @type job: gc3utils.Job
        @param remote_filename: the remote file to copy
        @type remote_filename: string
        @since: 0.2
        """
        raise NotImplementedError("Abstract method `LRMS.tail()` called - this should have been defined in a derived class.")
    
    def is_valid(self):
        """
        Determine if a provided LRMS instance is valid.
        Returns True or False.
        """
        raise NotImplementedError("Abstract method `LRMS.is_valid()` called - this should have been defined in a derived class.")



## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
