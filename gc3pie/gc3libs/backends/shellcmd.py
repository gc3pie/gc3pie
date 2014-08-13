#! /usr/bin/env python
"""
Run applications as local processes.
"""
# Copyright (C) 2009-2014 GC3, University of Zurich. All rights reserved.
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
import cPickle as pickle
from getpass import getuser
import os
import os.path
import posix
import posixpath
import time

# GC3Pie imports
import gc3libs
import gc3libs.exceptions
import gc3libs.backends.transport
from gc3libs import log, Run
from gc3libs.utils import same_docstring_as, samefile, copy_recursively
from gc3libs.utils import Struct, sh_quote_unsafe, defproperty
from gc3libs.backends import LRMS
from gc3libs.quantity import Memory, kB, MB, GB

def _make_remote_and_local_path_pair(transport, job, remote_relpath,
                                     local_root_dir, local_relpath):
    """
    Return list of (remote_path, local_path) pairs corresponding to
    """
    # see https://github.com/fabric/fabric/issues/306 about why it is
    # correct to use `posixpath.join` for remote paths (instead of
    # `os.path.join`)
    remote_path = posixpath.join(job.execution.lrms_execdir, remote_relpath)
    local_path = os.path.join(local_root_dir, local_relpath)
    if transport.isdir(remote_path):
        # recurse, accumulating results
        result = list()
        for entry in transport.listdir(remote_path):
            result += _make_remote_and_local_path_pair(
                transport, job,
                posixpath.join(remote_relpath, entry),
                local_path, entry)
        return result
    else:
        return [(remote_path, local_path)]


class ShellcmdLrms(LRMS):
    """Execute an `Application`:class: instance as a local process.

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
      or `/var/tmp`:file: (see `tempfile.mkftemp` for details).

    :param str resourcedir:

      Path to a filesystem location where to create a temporary
      directory that will contain information on the jobs running on
      the machine. The default value `None` means to use
      ``$HOME/.gc3/shellcmd.d``.

    :param str transport:
      Transport to use to connecet to the resource. Valid values are
      `ssh` or `local`.

    :param str frontend:

      If `transport` is `ssh`, then `frontend` is the hostname of the
      remote machine where the jobs will be executed.

    :param bool ignore_ssh_host_key:

      When connecting to a remote resource using `ssh` the server ssh
      public key is usually checked against a database of known hosts,
      and if the key is found but it does not match with the one saved
      in the database the connection will fail. Setting
      `ignore_ssh_host_key` to `True` will disable this check, thus
      introducing a potential security issue, but allowing connection
      even though the database contain old/invalid keys (the use case
      is when connecting to VM on a cloud, since the IP is usually
      reused and therefore the ssh key is recreated).

    :param bool override:

      `ShellcmdLrms` by default will try to gather information on the
      machine the resource is running on, including the number of
      cores and the available memory. These values may be different
      from the values stored in the configuration file. If `override`
      is True, then the values automatically discovered will be used
      instead of the ones in the configuration file. If `override` is
      False, instead, the values in the configuration file will be
      used.

    :param int ssh_timeout:

      If `transport` is `ssh`, this value will be used as timeout (in
      seconds) for the TCP connect.

    """

    # this matches what the ARC grid-manager does
    TIMEFMT = """WallTime=%es
KernelTime=%Ss
UserTime=%Us
CPUUsage=%P
MaxResidentMemory=%MkB
AverageResidentMemory=%tkB
AverageTotalMemory=%KkB
AverageUnsharedMemory=%DkB
AverageUnsharedStack=%pkB
AverageSharedMemory=%XkB
PageSize=%ZB
MajorPageFaults=%F
MinorPageFaults=%R
Swaps=%W
ForcedSwitches=%c
WaitSwitches=%w
Inputs=%I
Outputs=%O
SocketReceived=%r
SocketSent=%s
Signals=%k
ReturnCode=%x"""
    WRAPPER_DIR = '.gc3pie_shellcmd'
    WRAPPER_SCRIPT = 'wrapper_script.sh'
    WRAPPER_OUTPUT_FILENAME = 'resource_usage.txt'
    WRAPPER_PID = 'wrapper.pid'

    RESOURCE_DIR = '$HOME/.gc3/shellcmd.d'

    def __init__(self, name,
                 # these parameters are inherited from the `LRMS` class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime,
                 auth=None,
                 # these are specific to `ShellcmdLrms`
                 # ignored if `transport` is 'local'
                 frontend='localhost', transport='local', time_cmd=None,
                 override='False', keyfile=None, ignore_ssh_host_keys=False,
                 spooldir=None, resourcedir=None,
                 ssh_timeout=gc3libs.Default.SSH_CONNECT_TIMEOUT,
                 **extra_args):

        # init base class
        LRMS.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth, **extra_args)

        # GNU time is needed
        self.time_cmd = time_cmd

        # default is to use $TMPDIR or '/var/tmp' (see
        # `tempfile.mkftemp`), but we delay the determination of the
        # correct dir to the submit_job, so that we don't have to use
        # `transport` right now.
        self.spooldir = spooldir

        # Resource dir is expanded in `_gether_machine_specs()` and
        # only after that the `resource_dir` property will be
        # set. This is done in order to delay a possible connection to
        # a remote machine, and avoiding such a remote connection when
        # it's not needed.
        self.cfg_resourcedir = resourcedir or ShellcmdLrms.RESOURCE_DIR

        # Configure transport
        if transport == 'local':
            self.transport = gc3libs.backends.transport.LocalTransport()
            self._username = getuser()
        elif transport == 'ssh':
            auth = self._auth_fn()
            self._username = auth.username
            self.transport = gc3libs.backends.transport.SshTransport(
                frontend, username=self._username, keyfile=keyfile,
                ignore_ssh_host_keys=ignore_ssh_host_keys, timeout=ssh_timeout)
        else:
            raise gc3libs.exceptions.TransportError(
                "Unknown transport '%s'" % transport)
        self.frontend = frontend

        # use `max_cores` as the max number of processes to allow
        self.user_queued = 0
        self.queued = 0
        self.job_infos = {}
        self.total_memory = max_memory_per_core
        self.available_memory = self.total_memory
        self.override = gc3libs.utils.string_to_boolean(override)

    @defproperty
    def resource_dir():
        def fget(self):
            try:
                return self._resource_dir
            except:
                # Since RESOURCE_DIR contains the `$HOME` variable, we
                # have to expand it by connecting to the remote
                # host. However, we don't want to do that during
                # initialization, so we do it the first time this is
                # actually needed.
                self._gather_machine_specs()
                return self._resource_dir

        def fset(self, value):
            self._resource_dir = value
        return locals()

    @defproperty
    def user_run():
        def fget(self):
            return len([i for i in self.job_infos.values()
                        if not i['terminated']])
        return locals()

    @staticmethod
    def _filter_cores(job):
        if job['terminated']:
            return 0
        else:
            return job['requested_cores']

    def _compute_used_cores(self, jobs):
        """
        Accepts a dictionary of job informations and returns the
        sum of the `requested_cores` attributes.
        """
        return sum(map(self._filter_cores, jobs.values()))

    @defproperty
    def free_slots():
        """Returns the number of cores free"""
        def fget(self):
            """
            Sums the number of corse requested by jobs not in TERM*
            state and returns the difference from the number of cores
            of the resource.
            """
            return self.max_cores - self._compute_used_cores(self.job_infos)
        return locals()

    @same_docstring_as(LRMS.cancel_job)
    def cancel_job(self, app):
        try:
            pid = int(app.execution.lrms_jobid)
        except ValueError, ex:
            raise gc3libs.exceptions.InvalidArgument(
                "Invalid field `lrms_jobid` in Job '%s':"
                " expected a number, got '%s' (%s) instead"
                % (app, app.execution.lrms_jobid,
                   type(app.execution.lrms_jobid)))

        self.transport.connect()
        exit_code, stdout, stderr = self.transport.execute_command(
            'kill %d' % pid)
        # XXX: should we check that the process actually died?
        if exit_code != 0:
            # Error killing the process. It may not exists or we don't
            # have permission to kill it.
            exit_code, stdout, stderr = self.transport.execute_command(
                "ps ax | grep -E '^ *%d '" % pid)
            if exit_code == 0:
                # The PID refers to an existing process, but we
                # couldn't kill it.
                log.error("Could not kill job '%s': %s", pid, stderr)
            else:
                # The PID refers to a non-existing process.
                log.error(
                    "Could not kill job '%s'. It refers to non-existent"
                    " local process %s.", app, app.execution.lrms_jobid)
        self._delete_job_resource_file(pid)

    @same_docstring_as(LRMS.close)
    def close(self):
        # XXX: free any resources in use?
        pass

    def free(self, app):
        """
        Delete the temporary directory where a child process has run.
        The temporary directory is removed with all its content,
        recursively.

        If the deletion is successful, the `lrms_execdir` attribute in
        `app.execution` is reset to `None`; subsequent invocations of
        this method on the same applications do nothing.
        """
        try:
            if app.execution.lrms_execdir is not None:
                self.transport.connect()
                self.transport.remove_tree(app.execution.lrms_execdir)
                app.execution.lrms_execdir = None
        except Exception, ex:
            log.warning("Could not remove directory '%s': %s: %s",
                        app.execution.lrms_execdir, ex.__class__.__name__, ex)

        try:
            pid = app.execution.lrms_jobid
            self._delete_job_resource_file(pid)
        except AttributeError:
            # lrms_jobid not yet assigned
            # probabaly submit process failed before
            # ingnore and continue
            pass

    @defproperty
    def frontend():
        def fget(self):
            return self._frontend
        def fset(self, value):
            self._frontend = value
            self.transport.remote_frontend = value
        return locals()

    def _gather_machine_specs(self):
        """
        Gather information about this machine and, if `self.override`
        is true, also update the value of `max_cores` and
        `max_memory_per_jobs` attributes.

        This method works with both Linux and MacOSX.
        """
        self.transport.connect()

        # This is supposed to spit out the ful path on the remote end
        exit_code, stdout, stderr = self.transport.execute_command(
            "echo %s" % sh_quote_unsafe(self.cfg_resourcedir))

        self.resource_dir = stdout.strip()
        # XXX: it is actually necessary to create the folder
        # as a separate step
        log.info('Creating resource file directory: %s ...', self.resource_dir)
        try:
            self.transport.makedirs(self.resource_dir)
        except Exception, ex:
            log.error("Failed creating resource directory: %s. Error "
                      "type: %s. Message: %s", resource_dir, type(ex), str(ex))
            # cannot continue
            raise

        exit_code, stdout, stderr = self.transport.execute_command('uname -m')
        arch = gc3libs.config._parse_architecture(stdout)
        if arch != self.architecture:
            raise gc3libs.exceptions.ConfigurationError(
                "Invalid architecture: configuration file says `%s` but "
                "it actually is `%s`" % (str.join(', ', self.architecture),
                                         str.join(', ', arch)))

        exit_code, stdout, stderr = self.transport.execute_command('uname -s')
        self.running_kernel = stdout.strip()

        # Fix time_cmd variable
        if not self.time_cmd:
            # Check if gnu time is installed. We use `command` in
            # order to execute the binary and not the shell builtin,
            # if present. Cfr. POSIX standard.  Please, also note that
            # the wrapper script will execute `exec time_cmd` in order
            # to replace the current shell, but `exec` will never run
            # the builtin.
            time_cmd = "time"
            exit_code, stdout, stderr = self.transport.execute_command(
                'command %s --version 2>&1 | grep GNU' % time_cmd)
            if exit_code == 0:
                # Default `time` command is GNU! Good!
                self.time_cmd = time_cmd
            else:
                # This could be a MacOSX system. Check if GNU time is
                # installed as `gtime` via homebrew or MacPorts.
                time_cmd = "gtime"
                exit_code, stdout, stderr = self.transport.execute_command(
                    "command %s --version 2>&1 | grep GNU" % time_cmd)
                if exit_code == 0:
                    self.time_cmd = time_cmd

        if not self.time_cmd:
            raise gc3libs.exceptions.ConfigurationError(
                "Unable to find GNU `time` installed on your system."
                " Please, install GNU time and set `time_cmd`"
                " configuration option in gc3pie.conf.")

        if not self.override:
            # Ignore other values.
            return

        if self.running_kernel == 'Linux':
            exit_code, stdout, stderr = self.transport.execute_command('nproc')
            max_cores = int(stdout)

            # get the amount of total memory from /proc/meminfo
            with self.transport.open('/proc/meminfo', 'r') as fd:
                for line in fd:
                    if line.startswith('MemTotal'):
                        self.total_memory = int(line.split()[1]) * Memory.KiB
                        break

        elif self.running_kernel == 'Darwin':
            exit_code, stdout, stderr = self.transport.execute_command(
                'sysctl hw.ncpu')
            max_cores = int(stdout.split(':')[-1])

            exit_code, stdout, stderr = self.transport.execute_command(
                'sysctl hw.memsize')
            self.total_memory = int(stdout.split(':')[1]) * Memory.B

        if max_cores != self.max_cores:
            log.info(
                "Mismatch of value `max_cores` on resource '%s': configuration"
                " file says `max_cores=%d` while it's actually `%d`."
                " Updating current value.", self.name, self.max_cores, max_cores)
            self.max_cores = max_cores

        if self.total_memory != self.max_memory_per_core:
            log.info(
                "Mismatch of value `max_memory_per_core` on resource %s:"
                " configuration file says `max_memory_per_core=%s` while it's actually `%s`."
                " Updating current value.",
                self.name,
                self.max_memory_per_core,
                self.total_memory.to_str('%g%s', unit=Memory.MB))
            self.max_memory_per_core = self.total_memory

        self.available_memory = self.total_memory

    def _get_persisted_resource_state(self):
        """
        Get information on total resources from the files stored in
        `self.resource_dir`. It then returns a dictionary {PID: {key:
        values}} with informations for each job which is associated to
        a running process.
        """
        self.transport.connect()
        pidfiles = self.transport.listdir(self.resource_dir)
        log.debug("Checking status of the following PIDs: %s",
                  str.join(", ", pidfiles))
        job_infos = {}
        for pid in pidfiles:
            job = self._read_job_resource_file(pid)
            if job:
                job_infos[pid] = job
            else:
                # Process not found, ignore it
                continue
        return job_infos

    def _read_job_resource_file(self, pid):
        """
        Get resource information on job with pid `pid`, if it
        exists. Returns None if it does not exist.
        """
        self.transport.connect()
        log.debug("Reading resource file for pid %s", pid)
        jobinfo = None
        fname = posixpath.join(self.resource_dir, str(pid))
        fp = self.transport.open(fname, 'rb')
        try:
            jobinfo = pickle.load(fp)
        except Exception, ex:
            log.error("Unable to read remote resource file %s: %s",
                      fname, ex)
            raise
        fp.close()
        return jobinfo

    def _update_job_resource_file(self, pid, resources):
        """
        Update file in `self.resource_dir/PID` with `resources`.
        """
        self.transport.connect()
        # XXX: We should check for exceptions!
        log.debug("Updating resource file for pid %s", pid)
        fp = self.transport.open(
            posixpath.join(self.resource_dir, str(pid)), 'wb')
        pickle.dump(resources, fp, -1)
        fp.close()

    def _delete_job_resource_file(self, pid):
        """
        Delete `self.resource_dir/PID` file
        """
        self.transport.connect()
        # XXX: We should check for exceptions!
        log.debug("Deleting resource file for pid %s", pid)
        pidfile = posixpath.join(self.resource_dir, str(pid))
        try:
            self.transport.remove(pidfile)
        except Exception, ex:
            log.error("Ignoring error while deleting file %s", pidfile)

    @staticmethod
    def _filter_memory(job):
        if job['requested_memory'] is None or job['terminated']:
            return 0*MB
        else:
            return job['requested_memory']

    def _compute_used_memory(self, jobs):
        """
        Accepts a dictionary of job informations and returns the
        sum of the `requested_memory` attributes.
        """
        used_memory = sum(map(self._filter_memory, jobs.values()))
        # in case `jobs.values()` is the empty list, the `sum()`
        # built-in returns (built-in) integer `0`, which is why we can
        # use the `is` operator for this comparison ;-)
        if used_memory is 0:
            return 0*MB
        else:
            return used_memory

    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        # if we have been doing our own book-keeping well, then
        # there's no resource status to update
        self.updated = False
        if not hasattr(self, 'running_kernel'):
            self._gather_machine_specs()

        self.job_infos = self._get_persisted_resource_state()
        used_memory = self._compute_used_memory(self.job_infos)
        self.available_memory = self.total_memory - used_memory
        self.updated = True
        log.debug("Recovered resource information from files in %s:"
                  " available memory: %s, memory used by jobs: %s",
                  self.resource_dir,
                  self.available_memory.to_str('%g%s', unit=Memory.MB, conv=float),
                  used_memory.to_str('%g%s', unit=Memory.MB, conv=float))
        return self

    @same_docstring_as(LRMS.get_results)
    def get_results(self, app, download_dir,
                    overwrite=False, changed_only=True):
        if app.output_base_url is not None:
            raise gc3libs.exceptions.DataStagingError(
                "Retrieval of output files to non-local destinations"
                " is not supported in the ShellCmd backend.")

        self.transport.connect()
        # Make list of files to copy, in the form of (remote_path,
        # local_path) pairs.  This entails walking the
        # `Application.outputs` list to expand wildcards and
        # directory references.
        stageout = list()
        for remote_relpath, local_url in app.outputs.iteritems():
            local_relpath = local_url.path
            if remote_relpath == gc3libs.ANY_OUTPUT:
                remote_relpath = ''
                local_relpath = ''
            stageout += _make_remote_and_local_path_pair(
                self.transport, app, remote_relpath,
                download_dir, local_relpath)

        # copy back all files, renaming them to adhere to the
        # ArcLRMS convention
        log.debug("Downloading job output into '%s' ...", download_dir)
        for remote_path, local_path in stageout:
            # ignore missing files (this is what ARC does too)
            self.transport.get(remote_path, local_path,
                               ignore_nonexisting=True,
                               overwrite=overwrite,
                               changed_only=changed_only)
        return

    def update_job_state(self, app):
        """
        Query the running status of the local process whose PID is
        stored into `app.execution.lrms_jobid`, and map the POSIX
        process status to GC3Libs `Run.State`.
        """
        self.transport.connect()
        pid = app.execution.lrms_jobid
        exit_code, stdout, stderr = self.transport.execute_command(
            "ps ax | grep -E '^ *%d '" % pid)
        if exit_code == 0:
            log.debug("Process with pid %s found. Checking its status", pid)
            # Process exists. Check the status
            status = stdout.split()[2]
            if status[0] == 'T':
                # Job stopped
                app.execution.state = Run.State.STOPPED
            elif status[0] in ['R', 'I', 'U', 'S', 'D', 'W']:
                # Job is running. Check manpage of ps both on linux
                # and BSD to know the meaning of these statuses.
                app.execution.state = Run.State.RUNNING
        else:
            log.debug(
                "Process with PID %d not found. Checking wrapper file", pid)
            # pid does not exists in process table. Check wrapper file
            # contents
            app.execution.state = Run.State.TERMINATING
            if pid in self.job_infos:
                self.job_infos[pid]['terminated'] = True
            if app.requested_memory:
                self.available_memory += app.requested_memory
            wrapper_filename = posixpath.join(
                app.execution.lrms_execdir,
                ShellcmdLrms.WRAPPER_DIR,
                ShellcmdLrms.WRAPPER_OUTPUT_FILENAME)
            try:
                wrapper_file = self.transport.open(wrapper_filename, 'r')
            except Exception, ex:
                self._delete_job_resource_file(pid)
                log.error("Opening wrapper file %s raised an exception: %s",
                          wrapper_filename, str(ex))
                raise gc3libs.exceptions.InvalidArgument(
                    "Job '%s' refers to process wrapper %s which"
                    " ended unexpectedly"
                    % (app, app.execution.lrms_jobid))
            try:
                outcome = self._parse_wrapper_output(wrapper_file)
                app.execution.returncode = int(outcome.ReturnCode)
                self._delete_job_resource_file(pid)
            except:
                wrapper_file.close()

        self._get_persisted_resource_state()
        return app.execution.state

    def submit_job(self, app):
        """
        Run an `Application` instance as a local process.

        :see: `LRMS.submit_job`
        """
        # Update current resource usage to check how many jobs are
        # running in there.  Please note that for consistency with
        # other backends, these updated information are not kept!
        try:
            self.transport.connect()
        except gc3libs.exceptions.TransportError, ex:
            raise gc3libs.exceptions.LRMSSubmitError(
                "Unable to access shellcmd resource at %s: %s" %
                (self.frontend, str(ex)))

        job_infos = self._get_persisted_resource_state()
        free_slots = self.max_cores - self._compute_used_cores(job_infos)
        available_memory = self.total_memory - \
            self._compute_used_memory(job_infos)

        if self.free_slots == 0 or free_slots == 0:
            # XXX: We shouldn't check for self.free_slots !
            raise gc3libs.exceptions.LRMSSubmitError(
                "Resource %s already running maximum allowed number of jobs"
                " (%s). Increase 'max_cores' to raise." %
                (self.name, self.max_cores))

        if app.requested_memory and \
                (available_memory < app.requested_memory or
                 self.available_memory < app.requested_memory):
            raise gc3libs.exceptions.LRMSSubmitError(
                "Resource %s does not have enough available memory:"
                " %s requested, but only %s available."
                % (self.name,
                   app.requested_memory.to_str('%g%s', unit=Memory.MB),
                   available_memory.to_str('%g%s', unit=Memory.MB),
               ))

        log.debug("Executing local command '%s' ...",
                  str.join(" ", app.arguments))

        # Check if spooldir is a valid directory
        if not self.spooldir:
            ex, stdout, stderr = self.transport.execute_command(
                "echo $TMPDIR")
            if ex != 0 or not stdout.strip() or not stdout[0] == '/':
                log.debug(
                    "Unable to recover a valid absolute path for spooldir. "
                    "Using `/var/tmp`")
                self.spooldir = '/var/tmp'
            else:
                self.spooldir = stdout.strip()

        ## determine execution directory
        exit_code, stdout, stderr = self.transport.execute_command(
            "mktemp -d %s " % posixpath.join(
                self.spooldir, 'gc3libs.XXXXXX'))
        if exit_code != 0:
            log.error(
                "Error creating temporary directory on host %s: %s",
                self.frontend, stderr)
            log.debug('Freeing resources used by failed application')
            self.free(app)
            raise gc3libs.exceptions.LRMSSubmitError(
                "Error creating temporary directory on host %s: %s",
                self.frontend, stderr)

        execdir = stdout.strip()
        app.execution.lrms_execdir = execdir

        # Copy input files to remote dir
        for local_path, remote_path in app.inputs.items():
            remote_path = posixpath.join(execdir, remote_path)
            remote_parent = os.path.dirname(remote_path)
            try:
                if remote_parent not in ['', '.']:
                    log.debug("Making remote directory '%s'", remote_parent)
                    self.transport.makedirs(remote_parent)
                log.debug("Transferring file '%s' to '%s'",
                          local_path.path, remote_path)
                self.transport.put(local_path.path, remote_path)
                # preserve execute permission on input files
                if os.access(local_path.path, os.X_OK):
                    self.transport.chmod(remote_path, 0755)
            except:
                log.critical(
                    "Copying input file '%s' to remote host '%s' failed",
                    local_path.path, self.frontend)
                log.debug('Cleaning up failed application')
                self.free(app)
                raise

        # try to ensure that a local executable really has
        # execute permissions, but ignore failures (might be a
        # link to a file we do not own)
        if app.arguments[0].startswith('./'):
            try:
                self.transport.chmod(posixpath.join(execdir, app.arguments[0][2:]), 0755)
                # os.chmod(app.arguments[0], 0755)
            except:
                log.error(
                    "Failed setting execution flag on remote file '%s'",
                    posixpath.join(execdir, app.arguments[0]))

        ## set up redirection
        redirection_arguments = ''
        if app.stdin is not None:
            stdin = open(app.stdin, 'r')
            redirection_arguments += " < %s" % app.stdin

        if app.stdout is not None:
            redirection_arguments += " > %s" % app.stdout

        if app.join:
            redirection_arguments += " 2>&1"
        else:
            if app.stderr is not None:
                redirection_arguments += " 2> %s" % app.stderr

        ## set up environment
        env_arguments = ''
        for k, v in app.environment.iteritems():
            env_arguments += "%s=%s; " % (k, v)
        arguments = str.join(' ', (sh_quote_unsafe(i) for i in app.arguments))

        # Create the directory in which pid, output and wrapper script
        # files will be stored
        wrapper_dir = posixpath.join(
            execdir,
            ShellcmdLrms.WRAPPER_DIR)

        if not self.transport.isdir(wrapper_dir):
            try:
                self.transport.makedirs(wrapper_dir)
            except:
                log.error("Failed while creating remote folder "+
                          "%s" % wrapper_dir)
                self.free(app)
                raise

        # Build
        pidfilename = posixpath.join(wrapper_dir,
                                     ShellcmdLrms.WRAPPER_PID)
        wrapper_output_filename = posixpath.join(
            wrapper_dir,
            ShellcmdLrms.WRAPPER_OUTPUT_FILENAME)
        wrapper_script_fname = posixpath.join(
            wrapper_dir,
            ShellcmdLrms.WRAPPER_SCRIPT)

        try:
            # Create the wrapper script
            wrapper_script = self.transport.open(
                wrapper_script_fname, 'w')
            wrapper_script.write("""#!/bin/sh
echo $$ > %s
cd %s
exec %s -o %s -f '%s' /bin/sh %s -c '%s %s'
""" % (pidfilename, execdir, self.time_cmd,
       wrapper_output_filename,
       ShellcmdLrms.TIMEFMT, redirection_arguments,
       env_arguments, arguments))
            wrapper_script.close()
        except gc3libs.exceptions.TransportError:
            log.error("Freeing resources used by failed application")
            self.free(app)
            raise

        try:
            self.transport.chmod(wrapper_script_fname, 0755)

            # Execute the script in background
            self.transport.execute_command(wrapper_script_fname, detach=True)
        except gc3libs.exceptions.TransportError:
            log.error("Freeing resources used by failed application")
            self.free(app)
            raise

        # Just after the script has been started the pidfile should be
        # filled in with the correct pid.
        #
        # However, the script can have not been able to write the
        # pidfile yet, so we have to wait a little bit for it...
        pidfile = None
        for retry in gc3libs.utils.ExponentialBackoff():
            try:
                pidfile = self.transport.open(pidfilename, 'r')
                break
            except gc3libs.exceptions.TransportError, ex:
                if '[Errno 2]' in str(ex):  # no such file or directory
                    time.sleep(retry)
                    continue
                else:
                    raise
        if pidfile is None:
            #XXX: probably self.free(app) should go here as well
            raise gc3libs.exceptions.LRMSSubmitError(
                "Unable to get PID file of submitted process from"
                " execution directory `%s`: %s"
                % (execdir, pidfilename))
        pid = pidfile.read().strip()
        try:
            pid = int(pid)
        except ValueError:
            #XXX: probably self.free(app) should go here as well
            pidfile.close()
            raise gc3libs.exceptions.LRMSSubmitError(
                "Invalid pid `%s` in pidfile %s." % (pid, pidfilename))
        pidfile.close()

        # Update application and current resources
        app.execution.lrms_jobid = pid
        # We don't need to update free_slots since its value is
        # checked at runtime.
        if app.requested_memory:
            self.available_memory -= app.requested_memory
        self.job_infos[pid] = {
            'requested_cores': app.requested_cores,
            'requested_memory': app.requested_memory,
            'execution_dir': execdir,
            'terminated': False,
        }
        self._update_job_resource_file(pid, self.job_infos[pid])
        return app

    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        # `remote_filename` must be relative to the execution directory
        assert not os.path.isabs(remote_filename)
        # see https://github.com/fabric/fabric/issues/306 about why it
        # is correct to use `posixpath.join` for remote paths (instead
        # of `os.path.join`)
        remote_filename = posixpath.join(
            app.execution.lrms_execdir, remote_filename)
        statinfo = self.transport.stat(remote_filename)
        # seeking backwards past the beginning of the file makes the
        # read() fail in SFTP, so be sure that we do not rewind too
        # much...
        if offset < 0 and -offset > statinfo.st_size:
            offset = 0
        gc3libs.log.debug(
            "Reading %s bytes starting at offset %s from remote file '%s' ...",
            size, offset, remote_filename)
        with self.transport.open(remote_filename, 'r') as remotely:
            if offset >= 0:
                # seek from start of file
                remotely.seek(offset, os.SEEK_SET)
            else:
                # seek from end of file
                remotely.seek(offset, os.SEEK_END)
            data = remotely.read(size)
            try:
                # is `local_file` a file-like object?
                local_file.write(data)
            except (TypeError, AttributeError):
                # no, then treat it as a file name
                with open(local_file, 'w+b') as locally:
                    locally.write(data)

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

    def _parse_wrapper_output(self, wrapper_file):
        """
        Parse the file saved by the wrapper in
        `ShellcmdLrms.WRAPPER_OUTPUT_FILENAME` inside the WRAPPER_DIR
        in the job's execution directory and return a `Struct`:class:
        containing the values found on the file.

        `wrapper_file` is an opened file. This method will rewind the
        file before reading.
        """
        wrapper_file.seek(0)
        wrapper_output = Struct()
        for line in wrapper_file:
            if '=' not in line:
                continue
            k, v = line.strip().split('=', 1)
            wrapper_output[k] = v

        return wrapper_output

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
