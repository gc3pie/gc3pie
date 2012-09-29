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
import psutil

# GC3Pie imports
import gc3libs
import gc3libs.exceptions
from gc3libs import log, Run
from gc3libs.utils import same_docstring_as, samefile, copy_recursively, Struct
from gc3libs.backends import LRMS


class ShellcmdLrms(LRMS):
    """
    Execute an `Application`:class: instance as a local process.

    Construction of an instance of `ShellcmdLrms` takes the following
    optional parameters (in addition to any parameters taken by the
    base class `LRMS`:class:):

    :param str time_cmd:
      Path to the GNU ``time`` command.  Default is
      `/usr/bin/time`:file: which is correct on all known Linux
      distributions.

      This backend uses many of the
      extended features of GNU ``time``, so the shell-builtins or the
      BSD ``time`` will not work.

    :param str spooldir:
      Path to a filesystem location where to create
      temporary working directories for processes executed through
      this backend. The default value `None` means to use ``$TMPDIR``
      or `/tmp`:file: (see `tempfile.mkftemp` for details).
    """

    # this matches what the ARC grid-manager does
    TIMEFMT = "WallTime=%es\nKernelTime=%Ss\nUserTime=%Us\nCPUUsage=%P\nMaxResidentMemory=%MkB\nAverageResidentMemory=%tkB\nAverageTotalMemory=%KkB\nAverageUnsharedMemory=%DkB\nAverageUnsharedStack=%pkB\nAverageSharedMemory=%XkB\nPageSize=%ZB\nMajorPageFaults=%F\nMinorPageFaults=%R\nSwaps=%W\nForcedSwitches=%c\nWaitSwitches=%w\nInputs=%I\nOutputs=%O\nSocketReceived=%r\nSocketSent=%s\nSignals=%k\nReturnCode=%x\n"
    WRAPPER_DIR = '.gc3pie_shellcmd'
    WRAPPER_OUTPUT_FILENAME = 'resource_usage.txt'
    WRAPPER_PID = 'wrapper.pid'

    def __init__(self, name,
                 # these parameters are inherited from the `LRMS` class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime, auth=None,
                 # these are specific to `ShellcmdLrms`
                 time_cmd='/usr/bin/time',
                 spooldir=None,
                 **extra_args):

        # init base class
        LRMS.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth)

        # use `max_cores` as the max number of processes to allow
        self.free_slots = int(max_cores)
        self.user_run = 0
        self.user_queued = 0
        self.queued = 0

        # GNU time is needed
        self.time_cmd = time_cmd

        # default is to use $TMPDIR or '/tmp' (see `tempfile.mkftemp`)
        self.spooldir = spooldir


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
            self.free_slots += app.requested_cores
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
        return self


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
            try:
                copy_recursively(os.path.join(app.execution.lrms_execdir, r),
                                 os.path.join(download_dir, relative_dest_path))
            except IOError, ex:
                gc3libs.log.warning(
                        "Ignoring missing file %s" % (r))
                continue


    def update_job_state(self, app):
        """
        Query the running status of the local process whose PID is
        stored into `app.execution.lrms_jobid`, and map the POSIX
        process status to GC3Libs `Run.State`.
        """
        wrapper_dir = os.path.join(
            app.execution.lrms_execdir,
            ShellcmdLrms.WRAPPER_DIR)
        pidfile = open(os.path.join(wrapper_dir,
                                    ShellcmdLrms.WRAPPER_PID))
        pid = int(pidfile.read().strip())
        pidfile.close()

        # I'm using try... except... instead of pid_exists() to avoid
        # race conditions
        try:
            process = psutil.Process(pid)
            gc3libs.log.debug("Child process %d not yet done." % pid)
            if process.status in [ psutil.STATUS_STOPPED,
                                   psutil.STATUS_TRACING_STOP]:
                app.execution.state = Run.State.STOPPED
            elif process.status in [ psutil.STATUS_RUNNING,
                                     psutil.STATUS_SLEEPING,
                                     psutil.STATUS_DISK_SLEEP ]:
                app.execution.state = Run.State.RUNNING
            else:
                # This can probably happen only with zombies!
                raise psutil.NoSuchProcess("Process %s in zombie status" % pid)

        except psutil.NoSuchProcess:
            # process is probably terminated. Check the wrapper
            # output file.

            # XXX: Free resources. A bit optimistic?
            self.free_slots += app.requested_cores
            self.user_run -= 1

            wrapper_filename = os.path.join(
                app.execution.lrms_execdir,
                ShellcmdLrms.WRAPPER_DIR,
                ShellcmdLrms.WRAPPER_OUTPUT_FILENAME)

            if os.path.isfile(wrapper_filename):
                outcoming = self._parse_wrapper_output(wrapper_filename)
                app.execution.state = Run.State.TERMINATING
                app.execution.returncode = int(outcoming.ReturnCode)
            else:
                raise gc3libs.exceptions.InvalidArgument(
                    "Job '%s' refers to process wrapper %s which ended unexpectedly"
                    % (app, app.execution.lrms_jobid))

        return app.execution.state


    def submit_job(self, app):
        """
        Run an `Application` instance as a local process.

        :see: `LRMS.submit_job`
        """
        if self.free_slots == 0:
            raise gc3libs.exceptions.LRMSSubmitError(
                "Resource %s already running maximum allowed number of jobs"
                " (increase 'max_cores' to raise)." % self.name)

        gc3libs.log.debug("Executing local command '%s' ..."
                          % (str.join(" ", app.arguments[1:])))
        # We cannot use `exec` or other front-end modules that
        # hide the differences between UNIX and Windows, exactly
        # because we need to get the PID of the submitted process.
        # So, let's use `posix.fork` as if we were programming C ...

        ## determine execution directory
        execdir = tempfile.mkdtemp(prefix='gc3libs.', suffix='.tmp.d',
                                   dir=self.spooldir)

        ## generate child process
        pid = posix.fork()

        if pid: # parent process
            app.execution.lrms_jobid = pid
            app.execution.lrms_execdir = execdir
            app.execution.state = Run.State.RUNNING
            # book-keeping
            self.free_slots -= app.requested_cores
            self.user_run += 1

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
                if app.arguments[0].startswith('./'):
                    try:
                        os.chmod(app.arguments[0], 0755)
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
                cmd = app.arguments[0]
                if not os.path.isabs(cmd) and os.path.exists(cmd):
                    # local file
                    cmd = os.path.join(os.getcwd(), app.arguments[0])

                # Create the directory in which the pid and the output
                # from the wrapper script will be stored
                wrapper_dir = os.path.join(
                    execdir,
                    ShellcmdLrms.WRAPPER_DIR)

                if not os.path.isdir(wrapper_dir):
                    os.mkdir(wrapper_dir)

                if posix.fork(): # parent process, exits to avoid zombies
                    # The bug is cause by the fact that in order to
                    # avoid creation of zombie processes we have to
                    # *daemonize*, which basically means that we have
                    # to do something like:
                    #
                    # if not posix.fork()
                    #     # the child
                    #     if not posix.fork():
                    #         # the nephew
                    #         os.execlp(<the program we want to run>)
                    #     else: # still the child
                    #         # exits, so that the nephew will not
                    #         # become a zombie
                    #         sys.exit(0)
                    #
                    # However, sys.exit() basically raises a
                    # SystemExit exception, which is catched (and then
                    # "ignored") by nose, so we would end up with a
                    # lot of clones of the nosetests programs running
                    # at the same time.
                    #
                    # To avoid this we call os.execlp('/bin/true')
                    # instead, which will overwrite the current
                    # instance of nosetests with /bin/true, which will
                    # exit without problem.
                    os.execlp('/bin/true', '/bin/true')

                    # In case os.execlp() will fail we still call
                    # sys.exit(0), which should be just fine if not
                    # called from within nose.
                    sys.exit(0)

                pidfile = open(os.path.join(wrapper_dir,
                                            ShellcmdLrms.WRAPPER_PID), 'w')
                pidfile.write(str(os.getpid()))
                pidfile.write('\n')
                pidfile.close()
                os.execlp(self.time_cmd, self.time_cmd,
                          "-o", os.path.join(
                              wrapper_dir,
                              ShellcmdLrms.WRAPPER_OUTPUT_FILENAME),
                          "-f", ShellcmdLrms.TIMEFMT,
                          cmd, *app.arguments[1:])

            except Exception, ex:
                sys.excepthook(* sys.exc_info())
                gc3libs.log.error("Failed starting local process '%s': %s"
                                  % (app.arguments[0], str(ex)))
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

        The `shellcmd`:mod: backend can only handle ``file`` URLs.
        """
        for url in data_file_list:
            if not url.scheme in ['file']:
                return False
        return True

    def _parse_wrapper_output(self, wrapper_filename):
        """
        Parse the file saved by the wrapper in
        `ShellcmdLrms.WRAPPER_OUTPUT_FILENAME` inside the WRAPPER_DIR
        in the job's execution directory and return a `Struct`:class:
        containing the values found on the file.
        """
        wrapper_file = open(wrapper_filename)
        wrapper_output = Struct()
        for line in wrapper_file:
            if '=' not in line: continue
            k,v = line.strip().split('=', 1)
            wrapper_output[k] = v

        return wrapper_output

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
