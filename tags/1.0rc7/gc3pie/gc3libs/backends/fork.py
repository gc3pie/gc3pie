#! /usr/bin/env python
"""
Interface to different resource management systems for the GC3Libs.
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
__version__ = '$Revision: 1165 $'

import subprocess
import os

import gc3libs
import gc3libs.exceptions
from gc3libs import log, Run
from gc3libs.utils import same_docstring_as
from gc3libs.backends import LRMS
import gc3libs.backends.transport as transport


class ForkLrms(object):
    """Base class for interfacing with a computing resource."""

    procloc = "/proc"
    _stdout = None
    _stderr = None

    def __init__(self, resource, auths):
        assert resource.type == gc3libs.Default.FORK_LRMS, \
            "ForkLrms.__init__(): Failed. Resource type expected 'fork'. Received '%s'" \
            % resource.type

        # checking mandatory resource attributes
        resource.name
        resource.frontend
        resource.transport

        self._resource = resource

        # XXX: do we need this at all ?
        # auth = auths.get(resource.auth)

        # We only support fork with 'local' transport
        if resource.transport == 'local':
            self.transport = transport.LocalTransport()
        elif resource.transport == 'ssh':
            raise gc3libs.exceptions.TransportError("Unsuported transport '%s'. fork only supports transport of type: 'local'", resource.transport)
        
        self.isValid = 1

    def _get_proc_state(self, pid):
        """
        Getting process state.
        params: pid - process id
        
        return: (see manpage of proc, "RSDZTWX")
        R (runnig)
        S (sleeping)
        D (disk sleep)
        Z (zombie)
        T (tracing stop)
        W (paging)
        X (dead) or no process associated with pid
        
        raise: IOError if status file cannot be accessed
               ForkError if any other exception is raised
      """
        
        statfile = os.path.join(self.procloc, str(pid), "stat")
        
        try:
            if not os.path.exists(statfile):
                return 'X'
            fd = open(statfile,'r')
            status = fd.readline().split(" ")[2]
            fd.close()
            gc3libs.log.debug('Reading process status %s' % status)
            return status

        except IOError:
            raise
        except Exception, ex:
            log.error('Error while trying to read status file. Error type %s. message %s' % (ex.__class__, ex.message))
            raise gc3libs.exceptions.ForkError(x.message)


    def cancel_job(self, app):
        """
        Cancel a running job.  If `app` is associated to a queued or
        running remote job, tell the execution middleware to cancel
        it.
        """
        job = app.execution
        try:
            self.transport.connect()

            _command = 'kill '+job.lrms_jobid
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code != 0:
                # It is possible that 'kill' fails because job has been already completed
                # thus the cancel_job behaviour should be to 
                log.error('Failed executing remote command: %s. exit status %d' % (_command,exit_code))
                log.debug('Command returned stdout: %s' % stdout)
                log.debug('Command returned stderr: %s' % stderr)
                if exit_code == 127:
                    # failed executing remote command
                    raise LRMSError('Failed executing command')

            self.transport.close()
            return job
        except:
            self.transport.close()
            log.critical('Failure in checking status')
            raise


    def free(self, app):
        """
        Free up any remote resources used for the execution of `app`.
        In particular, this should delete any remote directories and
        files.

        Call this method when `app.execution.state` is anything other
        than `TERMINATED` results in undefined behavior and will
        likely be the cause of errors later on.  Be cautious.
        """
        # raise NotImplementedError("Abstract method `LRMS.free()` called - this should have been defined in a derived class.")
        pass
    

    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        """
        Update the status of the resource associated with this `LRMS`
        instance in-place.  Return updated `Resource` object.
        """
        self._resource.free_slots = 0
        self._resource.user_run = 0      
        self._resource.user_queued = 0
        self._resource.queued = 0
        return self._resource


    def get_results(self, app, download_dir, overwrite=False):
        """
        Retrieve job output files into local directory `download_dir`
        (which must already exists).  Will not overwrite existing
        files, unless the optional argument `overwrite` is `True`.
        """
        try:
            fd = open(os.path.join(download_dir,app.stdout),'wb')
            fd.write(self._stdout)
            fd.close()
            fd = open(os.path.join(download_dir,app.stderr),'wb')
            fd.write(self._stderr)
            fd.close()
        except Exception, ex:
            raise gc3libs.exceptions.DataStagingError("Failed downloading results: %s" % str(ex))

        return
    
    def update_job_state(self, app):
        """
        Query the state of the remote job associated with `app` and
        update `app.execution.state` accordingly.  Return the
        corresponding `Run.State`; see `Run.State` for more details.
        """

        job = app.execution

        try:
            pid = app.execution.lrms_jobid
            pid_status = self._get_proc_state(pid)

            if 'R' in pid_status:
                state = Run.State.RUNNING
            elif pid_status in 'XZ':
                state = Run.State.TERMINATED
            else:
                # XXX: are we sure ?
                state = Run.State.STOPPED

            job.returncode = 0
            app.execution.state = state
            return state
        except Exception, ex:
            log.error('Failed while updating job status. Error type %s. message %s' % (ex.__class__, ex.message))
            raise
    
    @same_docstring_as(LRMS.is_valid)
    def is_valid(self):
        """
        Determine if a provided LRMS instance is valid.
        Returns True or False.
        """
        return self.isValid

    @same_docstring_as(LRMS.submit_job)
    def submit_job(self, app):
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
        job = app.execution
        try:
            self.transport.connect()

            _command = app.cmdline(self._resource)
            (exitcode, self._stdout, self._stderr) = self.transport.execute_command(_command)

            self.transport.close()


            pid = self.transport.get_pid()
            if pid == -1:
                gc3libs.log.error('Failed getting pid from running process. Assigning Default ')
                pid = "Temp_Default"
            job.lrms_jobid = pid
            job.name = pid
    
            job.stdout_filename = app.stdout
            job.stderr_filename = app.stderr
            return job

        except:
            self.transport.close()
            log.critical("Failure submitting job to resource '%s' - see log file for errors"
                         % self._resource.name)
            raise


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
    


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
