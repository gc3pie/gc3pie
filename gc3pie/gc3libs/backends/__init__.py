#! /usr/bin/env python
"""
Interface to different resource management systems for the GC3Libs.
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
import gc3libs.exceptions


class LRMS(object):
    """Base class for interfacing with a computing resource.

    The following attributes are statically provided, i.e., they are
    specified at construction time and changed never after:

      ===================  ============== =========
      Attribute name       Type           Required?
      ===================  ============== =========
      arc_ldap             string
      architecture         set of string  yes
      auth                 string         yes
      frontend             string         yes
      gamess_location      string
      max_cores_per_job    int            yes
      max_memory_per_core  int            yes
      max_walltime         int            yes
      name                 string         yes
      ncores               int
      type                 string         yes
      ===================  ============== =========

    The following attributes are dynamically provided (i.e., defined
    by the `get_resource_status()` method or similar):

      ===================  ============== =========
      Attribute name       Type           Required?
      ===================  ============== =========
      free_slots           int
      user_run             int
      user_queued          int
      queued               int
      ===================  ============== =========

    """
    def __init__(self, name, type, auth, architecture,
                 max_cores_per_job, max_memory_per_core, max_walltime, **kw):

        self.name = str(name)
        self.type = str(type)
        self.auth = str(auth)

        if len(architecture) == 0:
            raise gc3libs.exceptions.InvalidType(
                "Empty value list for mandatory attribute 'architecture'")
        self.architecture = architecture

        try:
            self.max_cores_per_job = int(max_cores_per_job)
        except ValueError:
            raise InvalidValue(
                "Mandatory attribute 'max_cores_per_job' should be integer, got '%s' instead."
                % max_cores_per_job)

        try:
            self.max_memory_per_core = int(max_memory_per_core)
        except ValueError:
            raise InvalidValue(
                "Mandatory attribute 'max_memory_per_core' should be integer, got '%s' instead."
                % max_cores_per_job)

        try:
            self.max_walltime = int(max_walltime)
        except ValueError:
            raise InvalidValue(
                "Mandatory attribute 'max_walltime' should be integer, got '%s' instead."
                % max_cores_per_job)

        # additional keyword args set attributes
        for k,v in kw.iteritems():
            setattr(self, k, v)

        gc3libs.log.info("Resource '%s' initialized successfully.", self.name)


    def cancel_job(self, app):
        """
        Cancel a running job.  If `app` is associated to a queued or
        running remote job, tell the execution middleware to cancel
        it.
        """
        raise NotImplementedError("Abstract method `LRMS.cancel_job()` called - this should have been defined in a derived class.")

    def free(self, app):
        """
        Free up any remote resources used for the execution of `app`.
        In particular, this should delete any remote directories and
        files.

        Call this method when `app.execution.state` is anything other
        than `TERMINATED` results in undefined behavior and will
        likely be the cause of errors later on.  Be cautious.
        """
        raise NotImplementedError("Abstract method `LRMS.free()` called - this should have been defined in a derived class.")

    def get_resource_status(self):
        """
        Update the status of the resource associated with this `LRMS`
        instance in-place.  Return updated `Resource` object.
        """
        raise NotImplementedError("Abstract method `LRMS.get_resource_status()` called - this should have been defined in a derived class.")

    def get_results(self, job, download_dir, overwrite=False):
        """
        Retrieve job output files into local directory `download_dir`
        (which must already exists).  Will not overwrite existing
        files, unless the optional argument `overwrite` is `True`.
        """
        raise NotImplementedError("Abstract method `LRMS.get_results()` called - this should have been defined in a derived class.")

    def update_job_state(self, app):
        """
        Query the state of the remote job associated with `app` and
        update `app.execution.state` accordingly.  Return the
        corresponding `Run.State`; see `Run.State` for more details.
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
             is necessary for this LRMS to perform further operations on it.
        """
        raise NotImplementedError("Abstract method `LRMS.submit_job()` called - this should have been defined in a derived class.")

    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        """
        Download `size` bytes (at offset `offset` from the start) from
        remote file `remote_filename` and write them into
        `local_file`.  If `size` is `None` (default), then snarf
        contents of remote file from `offset` unto the end.

        Argument `local_file` is either a local path name (string), or
        a file-like object supporting a `.write()` method.  If
        `local_file` is a path name, it is created if not existent,
        otherwise overwritten.

        Argument `remote_filename` is the name of a file in the remote job
        "sandbox".

        Any exception raised by operations will be passed through.
        """
        raise NotImplementedError("Abstract method `LRMS.peek()` called - this should have been defined in a derived class.")

    def validate_data(self, data_file_list=None):
        """
        Return True if the list of files is expressed in one of the file transfer protocols the LRMS supports.
        Return False otherwise
        """
        raise NotImplementedError("Abstract method 'LRMS.validate_data()' called - this should have been defined in a derived class.")

    def close(self):
        """
        Implement gracefully close on LRMS dependent resources
        e.g. transport
        """
        raise NotImplementedError("Abstract method 'LRMS.close()' called - this should have been defined in a derived class.")


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
