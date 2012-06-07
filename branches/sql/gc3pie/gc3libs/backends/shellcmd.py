#! /usr/bin/env python
"""
Run applications as local processes.
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
__version__ = '$Revision: 1165 $'


# stdlib imports
import os
import os.path
import posix
import shutil
import sys
import tempfile

# GC3Pie imports
import gc3libs
import gc3libs.exceptions
from gc3libs import log, Run
from gc3libs.utils import same_docstring_as, samefile, copy_recursively
from gc3libs.backends import LRMS



class ShellcmdLrms(LRMS):
    """
    Execute an `Application`:class: instance as a local process.
    """

    def __init__(self, resource, auths):
        assert resource.type in [gc3libs.Default.SHELLCMD_LRMS,
                                 gc3libs.Default.SUBPROCESS_LRMS], \
            "ShellcmdLrms.__init__():" \
            " Expected resource type 'shellcmd', got '%s' instead" \
            % resource.type

        # checking mandatory resource attributes
        resource.name
        resource.max_cores

        # ok, save resource parameters
        self._resource = resource

        # use `max_cores` as the max number of processes to allow
        self._resource.free_slots = int(resource.max_cores)
        self._resource.user_run = 0
        self._resource.user_queued = 0
        self._resource.queued = 0

        if not hasattr(self._resource, 'spooldir'):
            # default is to use $TMPDIR or '/tmp' (see `tempfile.mkftemp`)
            self._resource.spooldir = None

        self.isValid = True

    def is_valid(self):
        return self.isValid


    def cancel_job(self, app):
        """
        Cancel a running job.

        If `app` is associated to a queued or running remote job, tell
        the execution middleware to cancel it.
        """
        try:
            pid = int(app.execution.lrms_jobid)
            posix.kill(pid, 15)
            # XXX: should we check that the process actually died?
            self._resource.free_slots += app.requested_cores
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
                " expected a number, got '%s' (%s) instead"
                % (app, app.execution.lrms_jobid, type(app.execution.lrms_jobid)))


    @same_docstring_as(LRMS.close)
    def close(self):
        # XXX: free any resources in use?
        pass


    def free(self, app):
        """
        Delete the temporary directory where a child process has run.

        The temporary directory is removed with all its content,
        recursively.
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
                " is not supported in the Shellcmd backend.")
        for r, l in app.outputs.items():
            relative_dest_path = l.path
            if r == gc3libs.ANY_OUTPUT:
                r = ''
                relative_dest_path = ''
            copy_recursively(os.path.join(app.execution.lrms_execdir, r),
                             os.path.join(download_dir, relative_dest_path))


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
                " expected a PID number, got '%s' (%s) instead"
                % (app, app.execution.lrms_jobid, type(app.execution.lrms_jobid)))

        # map POSIX status to GC3Libs `State`
        if posix.WIFSTOPPED(status):
            app.execution.state = Run.State.STOPPED
            app.execution.returncode = status
        elif posix.WIFSIGNALED(status) or posix.WIFEXITED(status):
            app.execution.state = Run.State.TERMINATING
            app.execution.returncode = status
            # book-keeping
            self._resource.free_slots += app.requested_cores
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
        if self._resource.free_slots == 0:
            raise gc3libs.exceptions.LRMSSubmitError(
                "Resource %s already running maximum allowed number of jobs"
                " (increase 'max_cores' to raise)." % self._resource.name)

        gc3libs.log.debug("Executing local command '%s %s' ..."
                          % (app.executable, str.join(" ", app.arguments)))

        # We cannot use `exec` or other front-end modules that
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
            self._resource.free_slots -= app.requested_cores
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

                # close extra fd's after duplication; for this we need
                # to determine highest-numbered fd in use (which could
                # have been opened by user-level code).  Apparently
                # there's no portable way of doing it, see:
                # http://stackoverflow.com/questions/899038/getting-the-highest-allocated-file-descriptor
                maxfd = 3
                try:
                    for entry in os.listdir('/proc/%s/fd' % os.getpid()):
                        try:
                            maxfd = max(maxfd, int(entry))
                        except ValueError:
                            pass
                except OSError:
                    # /proc not mounted? fall-back to the maximum theoretical fd
                    maxfd = posix.sysconf(posix.sysconf_names['SC_OPEN_MAX'])
                # XXX: since Python 2.6, there's `os.closerange()` for this
                for fd in xrange(3, maxfd):
                    try:
                        posix.close(fd)
                    except OSError:
                        pass

                ## set up environment
                for k,v in app.environment:
                    os.environ[k] = v

                ## finally.. exec()
                if not os.path.isabs(app.executable) and os.path.exists(app.executable):
                    # local file
                    os.execl(os.path.join(os.getcwd(), app.executable),
                             app.executable, *app.arguments)
                else:
                    # search in path
                    os.execlp(app.executable, app.executable, *app.arguments)

            except Exception, ex:
                sys.excepthook(* sys.exc_info())
                gc3libs.log.error("Failed starting local process '%s': %s"
                                  % (app.executable, str(ex)))
                # simulate what the shell does in case of failed exec
                sys.exit(127)


    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        rfh = open(remote_filename, 'r')
        rfh.seek(offset)
        data = rfh.read(size)
        rfh.close()

        try:
            local_file.write(data)
        except (TypeError, AttributeError):
            output_file = open(local_file, 'w+b')
            output_file.write(data)
            output_file.close()


    def validate_data(self, data_file_list=[]):
        """
        Return `False` if any of the URLs in `data_file_list` cannot
        be handled by this backend.

        The `shellcmd`:module: backend can only handle ``file`` URLs.
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
