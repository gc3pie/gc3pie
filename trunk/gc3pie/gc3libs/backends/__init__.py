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
from gc3libs.compat.functools import wraps
import gc3libs.exceptions
import gc3libs.utils


class LRMS(gc3libs.utils.Struct):
    """Base class for interfacing with a computing resource.

    The following construction parameters are also set as instance
    attributes.  All of them are mandatory, except `auth`.

    +=====================+==============+===============================+
    |Attribute name       |Expected Type |Meaning                        |
    +=====================+==============+===============================+
    |`name`               |string        |A unique identifier for this   |
    |                     |              |resource, used for generating  |
    |                     |              |error message.                 |
    +---------------------+--------------+-------------------------------+
    |`architecture`       |set of        |Should contain one entry per   |
    |                     |`Run.Arch`    |each architecture              |
    |                     |values        |supported. Valid architecture  |
    |                     |              |values are constants in the    |
    |                     |              |`gc3libs.Run.Arch` class.      |
    +---------------------+--------------+-------------------------------+
    |`auth`               |string        |A `gc3libs.authentication.Auth`|
    |                     |              |instance that will be used to  |
    |                     |              |access the computational       |
    |                     |              |resource associated with this  |
    |                     |              |backend.  The default value    |
    |                     |              |`None` is used to mean that no |
    |                     |              |authentication credentials are |
    |                     |              |needed (e.g., access to the    |
    |                     |              |resource has been              |
    |                     |              |pre-authenticated) or is       |
    |                     |              |managed outside of GC3Pie).    |
    +---------------------+--------------+-------------------------------+
    |`max_cores`          |int           |Maximum number of CPU cores    |
    |                     |              |that GC3Pie can allocate on    |
    |                     |              |this resource.                 |
    +---------------------+--------------+-------------------------------+
    |`max_cores_per_job`  |int           |Maximum number of CPU cores    |
    |                     |              |that GC3Pie can allocate on    |
    |                     |              |this resource *for a single    |
    |                     |              |job*.                          |
    +---------------------+--------------+-------------------------------+
    |`max_memory_per_core`|int           |Maximum memory (in GBs) that   |
    |                     |              |GC3Pie can allocate to jobs on |
    |                     |              |this resource.  The value is   |
    |                     |              |*per core*, so the actual      |
    |                     |              |amount allocated to a single   |
    |                     |              |job is the value of this entry |
    |                     |              |multiplied by the number of    |
    |                     |              |cores requested by the job.    |
    +---------------------+--------------+-------------------------------+
    |`max_walltime`       |int           |Maximum wall-clock time (in    |
    |                     |              |seconds) that can be allotted  |
    |                     |              |to a single job running on this|
    |                     |              |resource.                      |
    +---------------------+--------------+-------------------------------+

    The above should be considered *immutable* attributes: they are
    specified at construction time and changed never after.

    The following attributes are instead dynamically provided (i.e.,
    defined by the `get_resource_status()` method or similar), thus
    can change over the lifetime of the object:

    ===================  =====
    Attribute name       Type
    ===================  =====
    free_slots           int
    user_run             int
    user_queued          int
    queued               int
    ===================  =====

    """
    def __init__(self, name,
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime, auth=None):
        self.name = str(name)
        self.updated = False

        if len(architecture) == 0:
            raise gc3libs.exceptions.InvalidType(
                "Empty value list for mandatory attribute 'architecture'")
        self.architecture = architecture

        self.max_cores = int(max_cores)
        self.max_cores_per_job = int(max_cores_per_job)
        self.max_memory_per_core = int(max_memory_per_core)
        self.max_walltime = int(max_walltime)

        # see `authenticated` below
        self._auth_fn = auth

        gc3libs.log.info(
            "Computational resource '%s' initialized successfully.", self.name)


    @staticmethod
    def authenticated(fn):
        """
        Decorator: mark a function as requiring authentication.

        Each invocation of the decorated function causes a call to the
        `get` method of the authentication object (configured with the
        `auth` parameter to the class constructor).
        """
        @wraps(fn)
        def wrapper(self, *args, **kwargs):
            if self._auth_fn is not None:
                self._auth_fn()
            return fn(self, *args, **kwargs)
        return wrapper


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
