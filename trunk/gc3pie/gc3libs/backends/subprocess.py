#! /usr/bin/env python
"""
Run applications as local processes.
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

import os
import os.path
import posix
import shutil
import sys
import tempfile

import gc3libs
import gc3libs.exceptions
from gc3libs import log, Run
from gc3libs.utils import same_docstring_as, same_file, copy_recursively
from gc3libs.backends import LRMS



class SubprocessLrms(LRMS):
    """
    Execute an application as a local process.
    """

    def __init__(self, resource, auths):
        assert resource.type == gc3libs.Default.SUBPROCESS_LRMS, \
            "SubprocessLrms.__init__():" \
            " Expected resource type 'subprocess', got '%s' instead" \
            % resource.type

        # checking mandatory resource attributes
        resource.name
        resource.ncores

        # ok, save resource parameters
        self._resource = resource

        # use `ncores` as the max number of processes to allow
        self._resource.free_slots = int(resource.ncores)
        self._resource.user_run = 0      
        self._resource.user_queued = 0
        self._resource.queued = 0

        if not hasattr(self._resource, 'spooldir'):
            # default is to use $TMPDIR or '/tmp' (see `tempfile.mkftemp`)
            self._resource.spooldir = None

        self.isValid = 1


    def is_valid(self):
        return self.isValid


    def cancel_job(self, app):
        """
        Cancel a running job.  If `app` is associated to a queued or
        running remote job, tell the execution middleware to cancel
        it.
        """
        try:
            pid = int(app.execution.lrms_jobid)
            posix.kill(pid, 15)
            # XXX: should we check that the process actually died?
            self._resource.free_slots += 1
        except OSError, ex:
            if ex.errno == 10:
                raise gc3libs.exceptions.InvalidArgument(
                    "Job '%s' refers to non-existent local process %s"
                    % (app, app.execution.lrms_jobid))
            else:
                raise
        except ValueError, ex:
            raise gc3libs.exceptions.InvalidArgument(
                "Invalid field `lrms_jobid` in Job '%s':"
                " should be a number, is '%s' instead"
                % (app, app.execution.lrms_jobid))


    def free(self, app):
        """
        Delete the temporary directory where a child process has run,
        with all its content, recursively.
        """
        shutil.rmtree(app.execution.lrms_execdir)
    

    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        # if we have been doing our own book-keeping well, then
        # there's no resource status to update
        return self._resource


    @same_docstring_as(LRMS.get_results)
    def get_results(self, app, download_dir, overwrite=False):
        if app.output_base_url is not None:
            raise gc3libs.exceptions.DataStagingError(
                "Retrieval of output files to non-local destinations"
                " is not supported in the SubProcess backend.")
        for r, l in app.outputs.items():
            copy_recursively(os.path.join(app.execution.lrms_execdir, r),
                             os.path.join(download_dir, l.path))

    
    def update_job_state(self, app):
        """
        Query the running status of the local process whose PID is
        stored into `app.execution.lrms_jobid`, and map the POSIX
        process status to GC3Libs `Run.State`.
        """
        try:
            pid = int(app.execution.lrms_jobid)
            (pid_, status) = posix.waitpid(pid, posix.WNOHANG)
            if pid_ == 0:
                gc3libs.log.debug("Child process %d not yet done." % pid)
                return Run.State.RUNNING
            gc3libs.log.debug("Got status %d for child process PID %d" % (status, pid))
        except OSError, ex:
            if ex.errno == 10:
                # XXX: is `InvalidArgument` the correct exception here?
                raise gc3libs.exceptions.InvalidArgument(
                    "Job '%s' refers to non-existent local process %s"
                    % (app, app.execution.lrms_jobid))
            else:
                raise
        except ValueError, ex:
            # XXX: is `InvalidArgument` the correct exception here?
            raise gc3libs.exceptions.InvalidArgument(
                "Invalid field `lrms_jobid` in Job '%s':"
                " should be a number, is '%s' instead"
                % (app, app.execution.lrms_jobid))

        # map POSIX status to GC3Libs `State`
        if posix.WIFSTOPPED(status):
            app.execution.state = Run.State.STOPPED
            app.execution.returncode = status
        elif posix.WIFSIGNALED(status) or posix.WIFEXITED(status):
            app.execution.state = Run.State.TERMINATING
            app.execution.returncode = status
            # book-keeping
            self._resource.free_slots += 1
            self._resource.user_run -= 1
        else:
            # no changes
            pass

        return app.execution.state
    
    
    def submit_job(self, app):
        """
        Run an `Application` instance as a local process.

        :see: `LRMS.submit_job`
        """
        # We cannot use `subprocess` or other front-end modules that
        # hide the differences between UNIX and Windows, exactly
        # because we need to get the PID of the submitted process.
        # So, let's use `posix.fork` as if we were programming C ...

        ## determine execution directory
        execdir = tempfile.mkdtemp(prefix='gc3libs.', suffix='.tmp.d',
                                   dir=self._resource.spooldir)

        ## generate child process
        pid = posix.fork()

        if pid: # parent process
            app.execution.lrms_jobid = pid
            app.execution.lrms_execdir = execdir
            app.execution.state = Run.State.RUNNING
            # book-keeping
            self._resource.free_slots -= 1
            self._resource.user_run += 1
    
        else: # child process
            try:
                ## stage inputs files into execution directory
                for l, r in app.inputs.items():
                    copy_recursively(l.path, os.path.join(execdir, r))

                ## change to execution directory
                os.chdir(execdir)

                # try to ensure that a local executable really has
                # execute permissions, but ignore failures (might be a
                # link to a file we do not own)
                if app.executable.startswith('./'):
                    try:
                        os.chmod(app.executable, 0755)
                    except OSError:
                        pass

                ## set up redirection
                if app.stdin is not None:
                    stdin = open(app.stdin, 'r')
                else:
                    stdin = open(os.devnull, 'r')
                posix.dup2(stdin.fileno(), 0)

                if app.stdout is not None:
                    stdout = open(app.stdout, 'w')
                else:
                    stdout = open(os.devnull, 'w')
                posix.dup2(stdout.fileno(), 1)

                if app.join:
                        stderr = stdout
                else:
                    if app.stderr is not None:
                        stderr = open(app.stderr, 'w')
                    else:
                        stderr = open(os.devnull, 'w')
                posix.dup2(stderr.fileno(), 2)

                # close extra fd's after duplication
                os.close(stdin.fileno())
                os.close(stdout.fileno())
                if app.stderr is not None and stderr is not stdout:
                    posix.close(stderr.fileno())

                ## set up environment
                for k,v in app.environment:
                    os.environ[k] = v

                ## finally.. exec()
                if os.path.exists(app.executable) and not os.path.isabs(app.executable):
                    # local file
                    os.execl('./' + app.executable, app.executable, *app.arguments)
                else:
                    # search in path
                    os.execlp(app.executable, app.executable, *app.arguments)
                
            except Exception, ex:
                sys.excepthook(* sys.exc_info())
                gc3libs.log.error("Failed starting local process '%s': %s"
                                  % (app.executable, str(ex)))
                # simulate what the shell does in case of failed exec
                sys.exit(127)


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
        raise NotImplementedError("Method `SubprocessLRMS.peek()` is not yet implemented.")
    
    def validate_data(self, data_file_list=None):
        """
        Supported protocols: file
        """
        for url in data_file_list:
            if not url.scheme in ['file']:
                return False
        return True


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
