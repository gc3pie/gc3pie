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


class LRMS(object):
    """Base class for interfacing with a computing resource."""

    def cancel_job(self, job):
        """
        Cancel a running job.  If `job` has a queued or running remote
        instance, tell the execution middleware to cancel it.
        """
        raise NotImplementedError("Abstract method `LRMS.cancel_job()` called - this should have been defined in a derived class.")
    
    def get_resource_status(self):
        """
        Update the status of the resource associated with this `LRMS`
        instance in-place.  Return updated `Resource` object.
        """
        raise NotImplementedError("Abstract method `LRMS.get_resource_status()` called - this should have been defined in a derived class.")
    
    def get_results(self, job, download_dir):
        """
        Retrieve job output files into local directory `download_dir`.
        """
        raise NotImplementedError("Abstract method `LRMS.get_results()` called - this should have been defined in a derived class.")
    
    def get_state(self, job):
        """
        Query the state of the remote job associated with `job` and
        return the corresponding `Job.State`.
        
        See `Job.State` for more details.
        """
        raise NotImplementedError("Abstract method `LRMS.update_state()` called - this should have been defined in a derived class.")
    
    def is_valid(self):
        """
        Determine if a provided LRMS instance is valid.
        Returns True or False.
        """
        raise NotImplementedError("Abstract method `LRMS.is_valid()` called - this should have been defined in a derived class.")

    def submit_job(self, application, job):
        """
        Submit an `Application` instance to the configured
        computational resource; return a `gc3libs.Job` instance for
        controlling the submitted job.

        This method only returns if the job is successfully submitted;
        upon any failure, an exception is raised.

        *Note:* 

          1. `job.state` is *not* altered; it is the caller's
          responsibility to update it.

          2. the `job` object may be updated with any information that
          is necessary for this LRMS to perform further operations on
          it.
        """
        raise NotImplementedError("Abstract method `LRMS.submit_job()` called - this should have been defined in a derived class.")

    def tail(self, job, remote_filename, local_file, offset=0, size=None):
        """
        Download `size` bytes (at offset `offset` from the start) from
        remote file `remote_filename` and write them into
        `local_file`.  If `size` is `None` (default), then snarf the
        contents of remote file from `offset` unto the end.

        Argument `local_file` is either a local path name (string), or
        a file-like object supporting a `.write()` method.  If
        `local_file` is a path name, it is created if not existent,
        otherwise overwritten.

        Argument `remote_filename` is the name of a file in the remote job
        "sandbox".
        
        Any exception raised by operations will be passed through.
        """
        raise NotImplementedError("Abstract method `LRMS.tail()` called - this should have been defined in a derived class.")
    


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
