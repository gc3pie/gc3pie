#! /usr/bin/env python
"""
Run applications as local processes.
"""
# Copyright (C) 2009-2016 S3IT, Zentrale Informatik, University of Zurich. All rights reserved.
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


# stdlib imports
import cPickle as pickle
from getpass import getuser
import os
import os.path
import posixpath
import time
from pkg_resources import Requirement, resource_filename

# GC3Pie imports
import gc3libs
import gc3libs.exceptions
import gc3libs.backends.transport
from gc3libs import log, Run
from gc3libs.utils import same_docstring_as
from gc3libs.utils import Struct, sh_quote_safe, sh_quote_unsafe, defproperty
from gc3libs.backends import LRMS
from gc3libs.quantity import Duration, Memory, MB


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


def _parse_time_duration(val):
    """
    Convert the output of common Linux/UNIX system utilities into a GC3Pie `Duration` object.

    Any of the time formats *DD-HH:MM:SS* (days, hours, minutes, seconds),
    *HH:MM:SS* (hours, minutes, seconds), or *MM:SS* (minutes, seconds), or
    even just the number of seconds are acceptable::

      >>> _parse_time_duration('25-00:31:05') == Duration('25d') + Duration('31m') + Duration('5s')
      True

      >>> _parse_time_duration('1:02:03') == Duration('1h') + Duration('2m') + Duration('3s')
      True

      >>> _parse_time_duration('01:02') == Duration('1m') + Duration('2s')
      True

      >>> _parse_time_duration('42') == Duration(42, unit=Duration.s)
      True

    The *seconds* portion of the time string can be followed by
    decimal digits for greater precision::

      >>> _parse_time_duration('0:00.00') == Duration(0, unit=Duration.s)
      True

      >>> _parse_time_duration('4.20') == Duration(4.20, unit=Duration.s)
      True

    When only the number of seconds is given, an optional trailing
    unit specified `s` is allowed::

      >>> _parse_time_duration('4.20s') == Duration(4.20, unit=Duration.s)
      True

    Among the programs whose output can be parsed by this function, there are:

    - GNU time's `%e` format specifier;
    - output of `ps -o etime=` (on both GNU/Linux and MacOSX)
    """
    n = val.count(':')
    if 2 == n:
        if '-' in val:
            days, timespan = val.split('-')
            return (Duration(days + 'd') + Duration(timespan))
        else:
            # Duration's ctor can natively parse this
            return Duration(val)
    elif 1 == n:
        # AA:BB is rejected as ambiguous by `Duration`'s built-in
        # parser; work around it
        mm, ss = val.split(':')
        return (Duration(int(mm, 10), unit=Duration.m)
                + Duration(float(ss), unit=Duration.s))
    elif 0 == n:
        # remove final unit spec, if present
        if val.endswith('s'):
            val = val[:-1]
        # number of seconds with up to 2 decimal precision
        return Duration(float(val), unit=Duration.s)
    else:
        raise ValueError(
            "Expecting duration in the form HH:MM:SS, MM:SS,"
            " or just number of seconds,"
            " got {val} instead".format(val=val))


def _parse_percentage(val):
    """
    Convert a percentage string into a Python float.
    The percent sign at the end is optional::

    >>> _parse_percentage('10')
    10.0
    >>> _parse_percentage('10%')
    10.0
    >>> _parse_percentage('0.25%')
    0.25
    """
    return float(val[:-1]) if val.endswith('%') else float(val)


def _parse_returncode_string(val):
    return Run.shellexit_to_returncode(int(val))


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

    # how to translate GNU time output into GC3Pie execution metrics
    TIMEFMT_CONV = {
        # GNU time output key    .execution attr                     converter function
        # |                        |                                  |
        # v                        v                                  v
        'WallTime':              ('duration',                         _parse_time_duration),
        'KernelTime':            ('shellcmd_kernel_time',             Duration),
        'UserTime':              ('shellcmd_user_time',               Duration),
        'CPUUsage':              ('shellcmd_cpu_usage',               _parse_percentage),
        'MaxResidentMemory':     ('max_used_memory',                  Memory),
        'AverageResidentMemory': ('shellcmd_average_resident_memory', Memory),
        'AverageTotalMemory':    ('shellcmd_average_total_memory',    Memory),
        'AverageUnsharedMemory': ('shellcmd_average_unshared_memory', Memory),
        'AverageUnsharedStack':  ('shellcmd_average_unshared_stack',  Memory),
        'AverageSharedMemory':   ('shellcmd_average_shared_memory',   Memory),
        'PageSize':              ('shellcmd_page_size',               Memory),
        'MajorPageFaults':       ('shellcmd_major_page_faults',       int),
        'MinorPageFaults':       ('shellcmd_minor_page_faults',       int),
        'Swaps':                 ('shellcmd_swapped',                 int),
        'ForcedSwitches':        ('shellcmd_involuntary_context_switches', int),
        'WaitSwitches':          ('shellcmd_voluntary_context_switches',   int),
        'Inputs':                ('shellcmd_filesystem_inputs',       int),
        'Outputs':               ('shellcmd_filesystem_outputs',      int),
        'SocketReceived':        ('shellcmd_socket_received',         int),
        'SocketSent':            ('shellcmd_socket_sent',             int),
        'Signals':               ('shellcmd_signals_delivered',       int),
        'ReturnCode':            ('returncode',                       _parse_returncode_string),
    }

    WRAPPER_DIR = '.gc3pie_shellcmd'
    WRAPPER_SCRIPT = 'wrapper_script.sh'
    WRAPPER_DOWNLOADER = 'downloader.py'
    WRAPPER_OUTPUT_FILENAME = 'resource_usage.txt'
    WRAPPER_PID = 'wrapper.pid'

    RESOURCE_DIR = '$HOME/.gc3/shellcmd.d'


    def __init__(self, name,
                 # these parameters are inherited from the `LRMS` class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime,
                 auth=None,
                 # these are specific to `ShellcmdLrms`
                 frontend='localhost', transport='local',
                 time_cmd=None,
                 override='False',
                 spooldir=None,
                 resourcedir=None,
                 # SSH-related options; ignored if `transport` is 'local'
                 ssh_config=None,
                 keyfile=None,
                 ignore_ssh_host_keys=False,
                 ssh_timeout=None,
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

        # Resource dir is expanded in `_gather_machine_specs()` and
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
                frontend,
                ignore_ssh_host_keys=ignore_ssh_host_keys,
                ssh_config=(ssh_config or auth.ssh_config),
                username=self._username,
                port=auth.port,
                keyfile=(keyfile or auth.keyfile),
                timeout=(ssh_timeout or auth.timeout),
            )
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
        except ValueError:
            raise gc3libs.exceptions.InvalidArgument(
                "Invalid field `lrms_jobid` in Job '%s':"
                " expected a number, got '%s' (%s) instead"
                % (app, app.execution.lrms_jobid,
                   type(app.execution.lrms_jobid)))

        self.transport.connect()
        # Kill all the processes belonging to the same session as the
        # pid we actually started.

        # On linux, kill '$(ps -o pid= -g $(ps -o sess= -p %d))' would
        # be enough, but on MacOSX it doesn't work.
        exit_code, stdout, stderr = self.transport.execute_command(
            "ps -p %d  -o sess=" % pid)
        if exit_code != 0 or not stdout.strip():
            # No PID found. We cannot recover the session group of the
            # process, so we cannot kill any remaining orphan process.
            log.error("Unable to find job '%s': no pid found." % pid)
        else:
            exit_code, stdout, stderr = self.transport.execute_command(
                'kill $(ps -ax -o sess=,pid= | egrep "^[ \t]*%s[ \t]")' % stdout.strip())
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
        except Exception as ex:
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

        # expand env variables in the `resource_dir` setting
        exit_code, stdout, stderr = self.transport.execute_command(
            'echo %s' % sh_quote_unsafe(self.cfg_resourcedir))
        self.resource_dir = stdout.strip()

        # XXX: it is actually necessary to create the folder
        # as a separate step
        if not self.transport.exists(self.resource_dir):
            try:
                log.info("Creating resource file directory: '%s' ...",
                         self.resource_dir)
                self.transport.makedirs(self.resource_dir)
            except Exception as ex:
                log.error("Failed creating resource directory '%s':"
                          " %s: %s", self.resource_dir, type(ex), str(ex))
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

        # ensure `time_cmd` points to a valid value
        self.time_cmd = self._locate_gnu_time()
        if not self.time_cmd:
            raise gc3libs.exceptions.ConfigurationError(
                "Unable to find GNU `time` installed on your system."
                " Please, install GNU time and set the `time_cmd`"
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
                "Mismatch of value `max_cores` on resource '%s':"
                " configuration file says `max_cores=%d` while it's actually `%d`."
                " Updating current value.",
                self.name, self.max_cores, max_cores)
            self.max_cores = max_cores

        if self.total_memory != self.max_memory_per_core:
            log.info(
                "Mismatch of value `max_memory_per_core` on resource %s:"
                " configuration file says `max_memory_per_core=%s` while it's"
                " actually `%s`. Updating current value.",
                self.name,
                self.max_memory_per_core,
                self.total_memory.to_str('%g%s', unit=Memory.MB))
            self.max_memory_per_core = self.total_memory

        self.available_memory = self.total_memory

    def _locate_gnu_time(self):
        """
        Return the command name to run the GNU `time` binary,
        or ``None`` if it cannot be found.
        """
        candidates = [
            'time',  # default on Linux systems
            'gtime', # MacOSX with Homebrew or MacPorts
        ]
        if self.time_cmd:
            # try this first
            candidates.insert(0, self.time_cmd)
        for time_cmd in candidates:
            gc3libs.log.debug(
                "Checking if GNU time is available as command `%s`", time_cmd)
            # We use `command` in order to force the shell to execute
            # the binary and not the shell builtin (cf. the POSIX
            # standard).  However note that the wrapper script will
            # execute `exec time_cmd` in order to replace the current
            # shell, but `exec` will never run the builtin.
            exit_code, stdout, stderr = self.transport.execute_command(
                'command %s --version 2>&1 | grep GNU' % time_cmd)
            if exit_code == 0:
                # command is GNU! Good!
                return time_cmd
        return None

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
        with self.transport.open(fname, 'rb') as fp:
            try:
                jobinfo = pickle.load(fp)
            except Exception as ex:
                log.error("Unable to read remote resource file %s: %s",
                          fname, ex)
                raise
        return jobinfo

    def _update_job_resource_file(self, pid, resources):
        """
        Update file in `self.resource_dir/PID` with `resources`.
        """
        self.transport.connect()
        # XXX: We should check for exceptions!
        log.debug("Updating resource file for pid %s", pid)
        with self.transport.open(
                posixpath.join(self.resource_dir, str(pid)), 'wb') as fp:
            pickle.dump(resources, fp, -1)

    def _delete_job_resource_file(self, pid):
        """
        Delete `self.resource_dir/PID` file
        """
        self.transport.connect()
        log.debug("Deleting resource file for pid %s ...", pid)
        pidfile = posixpath.join(self.resource_dir, str(pid))
        try:
            self.transport.remove(pidfile)
        except Exception as err:
            log.debug(
                "Ignored error deleting file `%s`: %s: %s",
                pidfile, err.__class__.__name__, err)

    @staticmethod
    def _filter_memory(job):
        if job['requested_memory'] is None or job['terminated']:
            return 0 * MB
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
            return 0 * MB
        else:
            return used_memory

    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        self.updated = False
        try:
            self.running_kernel
        except AttributeError:
            self._gather_machine_specs()

        self.job_infos = self._get_persisted_resource_state()
        used_memory = self._compute_used_memory(self.job_infos)
        self.available_memory = self.total_memory - used_memory
        self.updated = True
        log.debug("Recovered resource information from files in %s:"
                  " available memory: %s, memory used by jobs: %s",
                  self.resource_dir,
                  self.available_memory.to_str('%g%s',
                                               unit=Memory.MB,
                                               conv=float),
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
            if local_url.scheme in ['swift', 'swt', 'swifts', 'swts']:
                continue
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
            log.debug("Process with PID %s found."
                      " Checking its running status ...", pid)
            # Process exists. Check the status
            status = stdout.split()[2]
            if status[0] == 'T':
                # Job stopped
                app.execution.state = Run.State.STOPPED
            elif status[0] in ['R', 'I', 'U', 'S', 'D', 'W']:
                # Job is running. Check manpage of ps both on linux
                # and BSD to know the meaning of these statuses.
                app.execution.state = Run.State.RUNNING
                # if `requested_walltime` is set, enforce it as a
                # running time limit
                if app.requested_walltime is not None:
                    exit_code2, stdout2, stderr2 = self.transport.execute_command(
                        "ps -p %d -o etime=" % pid)
                    if exit_code2 != 0:
                        # job terminated already, do cleanup and return
                        self._cleanup_terminating_task(app, pid)
                        return app.execution.state
                    cancel = False
                    elapsed = _parse_time_duration(stdout2.strip())
                    if elapsed > self.max_walltime:
                        log.warning("Task %s ran for %s, exceeding max_walltime %s of resource %s: cancelling it.",
                                    app, elapsed.to_timedelta(), self.max_walltime, self.name)
                        cancel = True
                    if elapsed > app.requested_walltime:
                        log.warning("Task %s ran for %s, exceeding own `requested_walltime` %s: cancelling it.",
                                    app, elapsed.to_timedelta(), app.requested_walltime)
                        cancel = True
                    if cancel:
                        self.cancel_job(app)
                        # set signal to SIGTERM in termination status
                        self._cleanup_terminating_task(app, pid, termstatus=(15, -1))
                        return app.execution.state
        else:
            log.debug(
                "Process with PID %d not found,"
                " assuming task %s has finished running.",
                pid, app)
            self._cleanup_terminating_task(app, pid)

        self._get_persisted_resource_state()
        return app.execution.state

    def _cleanup_terminating_task(self, app, pid, termstatus=None):
        app.execution.state = Run.State.TERMINATING
        if termstatus is not None:
            app.execution.returncode = termstatus
        if pid in self.job_infos:
            self.job_infos[pid]['terminated'] = True
            if app.requested_memory is not None:
                assert (app.requested_memory
                        == self.job_infos[pid]['requested_memory'])
                self.available_memory += app.requested_memory
        wrapper_filename = posixpath.join(
            app.execution.lrms_execdir,
            ShellcmdLrms.WRAPPER_DIR,
            ShellcmdLrms.WRAPPER_OUTPUT_FILENAME)
        try:
            log.debug(
                "Reading resource utilization from wrapper file `%s` for task %s ...",
                wrapper_filename, app)
            with self.transport.open(wrapper_filename, 'r') as wrapper_file:
                outcome = self._parse_wrapper_output(wrapper_file)
                app.execution.update(outcome)
                if termstatus is None:
                    app.execution.returncode = outcome.returncode
        except Exception as err:
            msg = ("Could not open wrapper file `{0}` for task `{1}`: {2}"
                   .format(wrapper_filename, app, err))
            log.warning("%s -- Termination status and resource utilization fields will not be set.", msg)
            raise gc3libs.exceptions.InvalidValue(msg)
        finally:
            self._delete_job_resource_file(pid)

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
        except gc3libs.exceptions.TransportError as ex:
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
                   available_memory.to_str('%g%s', unit=Memory.MB),)
            )

        log.debug("Executing local command '%s' ...",
                  str.join(" ", app.arguments))

        # Check if spooldir is a valid directory
        if not self.spooldir:
            ex, stdout, stderr = self.transport.execute_command(
                'cd "$TMPDIR" && pwd')
            if ex != 0 or stdout.strip() == '' or not stdout[0] == '/':
                log.debug(
                    "Unable to recover a valid absolute path for spooldir."
                    " Using `/var/tmp`.")
                self.spooldir = '/var/tmp'
            else:
                self.spooldir = stdout.strip()

        # determine execution directory
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
            if local_path.scheme != 'file':
                continue
            remote_path = posixpath.join(execdir, remote_path)
            remote_parent = os.path.dirname(remote_path)
            try:
                if (remote_parent not in ['', '.']
                        and not self.transport.exists(remote_parent)):
                    log.debug("Making remote directory '%s'", remote_parent)
                    self.transport.makedirs(remote_parent)
                log.debug("Transferring file '%s' to '%s'",
                          local_path.path, remote_path)
                self.transport.put(local_path.path, remote_path)
                # preserve execute permission on input files
                if os.access(local_path.path, os.X_OK):
                    self.transport.chmod(remote_path, 0o755)
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
                self.transport.chmod(
                    posixpath.join(execdir, app.arguments[0][2:]),
                    0o755)
                # os.chmod(app.arguments[0], 0755)
            except:
                log.error(
                    "Failed setting execution flag on remote file '%s'",
                    posixpath.join(execdir, app.arguments[0]))

        # set up redirection
        redirection_arguments = ''
        if app.stdin is not None:
            # stdin = open(app.stdin, 'r')
            redirection_arguments += " <%s" % app.stdin

        if app.stdout is not None:
            redirection_arguments += " >%s" % app.stdout
            stdout_dir = os.path.dirname(app.stdout)
            if stdout_dir:
                self.transport.makedirs(posixpath.join(execdir, stdout_dir))

        if app.join:
            redirection_arguments += " 2>&1"
        else:
            if app.stderr is not None:
                redirection_arguments += " 2>%s" % app.stderr
                stderr_dir = os.path.dirname(app.stderr)
                if stderr_dir:
                    self.transport.makedirs(posixpath.join(execdir, stderr_dir))

        # set up environment
        env_commands = []
        for k, v in app.environment.iteritems():
            env_commands.append(
                "export {k}={v};"
                .format(k=sh_quote_safe(k), v=sh_quote_unsafe(v)))

        # Create the directory in which pid, output and wrapper script
        # files will be stored
        wrapper_dir = posixpath.join(
            execdir,
            ShellcmdLrms.WRAPPER_DIR)

        if not self.transport.isdir(wrapper_dir):
            try:
                self.transport.makedirs(wrapper_dir)
            except:
                log.error("Failed creating remote folder '%s'"
                          % wrapper_dir)
                self.free(app)
                raise

        # Set up scripts to download/upload the swift/http files
        downloadfiles = []
        uploadfiles = []
        wrapper_downloader_filename = posixpath.join(
            wrapper_dir,
            ShellcmdLrms.WRAPPER_DOWNLOADER)

        for url, outfile in app.inputs.items():
            if url.scheme in ['swift', 'swifts', 'swt', 'swts', 'http', 'https']:
                downloadfiles.append("python '%s' download '%s' '%s'" % (wrapper_downloader_filename, str(url), outfile))

        for infile, url in app.outputs.items():
            if url.scheme in ['swift', 'swt', 'swifts', 'swts']:
                uploadfiles.append("python '%s' upload '%s' '%s'" % (wrapper_downloader_filename, str(url), infile))
        if downloadfiles or uploadfiles:
            # Also copy the downloader.
            with open(resource_filename(Requirement.parse("gc3pie"),
                                        "gc3libs/etc/downloader.py")) as fd:
                wrapper_downloader = self.transport.open(
                    wrapper_downloader_filename, 'w')
                wrapper_downloader.write(fd.read())
                wrapper_downloader.close()

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
            commands = (
                r"""#!/bin/sh
                echo $$ >{pidfilename}
                cd {execdir}
                exec {redirections}
                {environment}
                {downloadfiles}
                '{time_cmd}' -o '{wrapper_out}' -f '{fmt}' {command}
                rc=$?
                {uploadfiles}
                rc2=$?
                if [ $rc -ne 0 ]; then exit $rc; else exit $rc2; fi
                """.format(
                    pidfilename=pidfilename,
                    execdir=execdir,
                    time_cmd=self.time_cmd,
                    wrapper_out=wrapper_output_filename,
                    fmt=ShellcmdLrms.TIMEFMT,
                    redirections=redirection_arguments,
                    environment=str.join('\n', env_commands),
                    downloadfiles=str.join('\n', downloadfiles),
                    uploadfiles=str.join('\n', uploadfiles),
                    command=(str.join(' ',
                                      (sh_quote_unsafe(arg)
                                      for arg in app.arguments))),
            ))
            wrapper_script.write(commands)
            wrapper_script.close()
            #log.info("Wrapper script: <<<%s>>>", commands)
        except gc3libs.exceptions.TransportError:
            log.error("Freeing resources used by failed application")
            self.free(app)
            raise

        try:
            self.transport.chmod(wrapper_script_fname, 0o755)

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
            except gc3libs.exceptions.TransportError as ex:
                if '[Errno 2]' in str(ex):  # no such file or directory
                    time.sleep(retry)
                    continue
                else:
                    raise
        if pidfile is None:
            # XXX: probably self.free(app) should go here as well
            raise gc3libs.exceptions.LRMSSubmitError(
                "Unable to get PID file of submitted process from"
                " execution directory `%s`: %s"
                % (execdir, pidfilename))
        pid = pidfile.read().strip()
        try:
            pid = int(pid)
        except ValueError:
            # XXX: probably self.free(app) should go here as well
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
            if url.scheme not in ['file', 'swift', 'swifts', 'swt', 'swts', 'http', 'https']:
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
        acctinfo = Struct()
        for line in wrapper_file:
            if '=' not in line:
                continue
            k, v = line.strip().split('=', 1)
            if k not in self.TIMEFMT_CONV:
                gc3libs.log.warning(
                    "Unknown key '%s' in wrapper output file - ignoring!", k)
                continue
            name, conv = self.TIMEFMT_CONV[k]
            # the `time` man page states that: "Any character
            # following a percent sign that is not listed in the table
            # below causes a question mark (`?') to be output [...] to
            # indicate that an invalid resource specifier was given."
            # Actually, `time` seems to print a question mark also
            # when a value is not available (e.g., corresponding data
            # not available/collected by the kernel) so we just set a
            # field to ``None`` if there is a question mark in it.
            if v.startswith('?'):
                acctinfo[name] = None
            else:
                acctinfo[name] = conv(v)

        # apprently GNU time does not report the total CPU time, used
        # so compute it here
        acctinfo['used_cpu_time'] = (
            acctinfo['shellcmd_user_time'] + acctinfo['shellcmd_kernel_time'])

        return acctinfo

# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
