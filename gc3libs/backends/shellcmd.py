#! /usr/bin/env python

"""
Run applications as processes starting them from the shell.
"""

# Copyright (C) 2009-2019  University of Zurich. All rights reserved.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


# make coding more python3-ish, must be the first statement
from __future__ import (absolute_import, division, print_function)

from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import range
from builtins import object
from future.utils import with_metaclass

# module doc and other metadata
__docformat__ = 'reStructuredText'


# imports and other dependencies

# stdlib imports
from abc import ABCMeta, abstractmethod
from collections import defaultdict
import pickle as pickle
from getpass import getuser
import os
import os.path
import posixpath
import time

from pkg_resources import Requirement

# GC3Pie imports
import gc3libs
import gc3libs.exceptions
import gc3libs.backends.transport
from gc3libs import log, Run
import gc3libs.defaults
from gc3libs.utils import same_docstring_as, Struct, sh_quote_safe, sh_quote_unsafe
from gc3libs.backends import LRMS
from gc3libs.quantity import Duration, Memory, MB


## helper functions
#
# Mainly for parsing output of shell programs.
#

def _parse_process_status(pstat):
    """
    Map `ps` process status letter to a `Run.State` label.

    Running:

        R: in run queue
        S: interruptible sleep
        D: uninterruptible sleep (Linux)
        U: uninterruptible sleep (MacOSX)
        I: idle (= sleeping > 20s, MacOSX)
        W: paging (Linux, no longer valid since the 2.6.xx kernel)
        Z: "zombie" process

    Stopped:

        T: stopped by job control signal
        t: stopped by debugger during the tracing
        X: dead (should never be seen)

    """
    # Check manpage of ``ps`` both on linux and MacOSX/BSD to know the meaning
    # of these statuses
    if pstat.startswith(('R', 'S', 'D', 'Y', 'I', 'W', 'Z')):
        return Run.State.RUNNING
    elif pstat.startswith(('T', 't', 'X')):
        return Run.State.STOPPED
    else:
        raise KeyError("Unknown process status code `{0}`".format(pstat[0]))


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


## interface to different OS
#
#

class _Machine(with_metaclass(ABCMeta, object)):
    """
    Base class for OS-specific shell services.

    Linux and MacOSX require slightly different command incantations
    to achieve the same task. The `Machine` class abstract these into
    a uniform interface.
    """

    def __init__(self, transport):
        self.transport = transport

    @staticmethod
    def detect(transport):
        """Factory method to create a `_Machine` instance based on the running kernel."""
        exit_code, stdout, stderr = transport.execute_command('uname -s')
        running_kernel = stdout.strip()
        if running_kernel == 'Linux':
            return _LinuxMachine(transport)
        elif running_kernel == 'Darwin':
            return _MacOSXMachine(transport)
        else:
            raise RuntimeError(
                "Unexpected kernel name: got {0},"
                " expecting one of 'Linux', 'Darwin'"
                .format(running_kernel))

    def _run_command(self, cmd):
        """
        Run a command and return its STDOUT, or raise exception if it errors out.

        This is like `subprocess.check_call`, with the following differences:

        * the slave command is executed through `Transport.execute_command`
          so it need not be locally executed.
        * a `RuntimeError` exception is raised if either the exit code from the
          process is non-zero or STDERR contains any text.
        """
        exit_code, stdout, stderr = self.transport.execute_command(cmd)
        if exit_code != 0 or stderr:
            raise RuntimeError("Got error running command `{0}` (exit code {1}): {2}"
                               .format(cmd, exit_code, stderr.strip()))
        return stdout

    def get_architecture(self):
        cmd = 'uname -m'
        stdout = self._run_command(cmd)
        return gc3libs.config._parse_architecture(stdout)

    def get_process_state(self, pid):
        """
        Return the 1-letter state of process PID.
        (See the ``ps`` man page for a list of possible codes and explanations.)

        Raise ``LookupError`` if no process is identified by the given PID.
        """
        cmd = 'ps -p {0} -o state='.format(pid)
        rc, stdout, stderr = self.transport.execute_command(cmd)
        if rc == 1:
            raise LookupError('No process with PID {0}'.format(pid))
        elif rc == 0:
            return stdout.strip()
        else:
            raise RuntimeError("Got error running command `{0}` (exit code {1}): {2}"
                               .format(cmd, exit_code, stderr.strip()))

    def get_process_running_time(self, pid):
        """
        Return elapsed time since start of process identified by PID.

        Raise ``LookupError`` if no process is identified by the given PID.
        """
        cmd = 'ps -p {0} -o etime='.format(pid)
        rc, stdout, stderr = self.transport.execute_command(cmd)
        if rc == 1:
            raise LookupError('No process with PID {0}'.format(pid))
        elif rc == 0:
            etime = stdout.strip()
            return _parse_time_duration(etime)
        else:
            raise RuntimeError("Got error running command `{0}` (exit code {1}): {2}"
                               .format(cmd, exit_code, stderr.strip()))

    def get_total_cores(self):
        """Return total nr. of CPU cores."""
        cmd = self._get_total_cores_command()
        stdout = self._run_command(cmd)
        try:
            return int(stdout)
        except (ValueError, TypeError) as err:
            raise RuntimeError("Cannot parse output `{0}` of command `{1}`"
                               " as total number of CPU cores: {2}"
                               .format(stdout.strip(), cmd, err))

    # This could be a property of the class, but then we won't be able to
    # use the `@abstractmethod` constructor to enforce that derived classes
    # provide an implementation
    @abstractmethod
    def _get_total_cores_command(self):
        """Command to run to print the nr. of CPU cores to STDOUT."""
        pass

    def get_total_memory(self):
        """
        Return amount of total amount of RAM as a `gc3libs.quantity.Memory` object.
        """
        parts, index, unit = self._get_total_memory_impl()
        try:
            qty = parts[index]
            amount = int(qty)
            return amount * unit
        except KeyError:  # index out of bounds
            raise AssertionError(
                "Call to {0} returned out-of-bounds index {1} into sequence {2}"
                .format(self._get_total_memory_impl, index, parts))
        except (ValueError, TypeError) as err:
            raise RuntimeError("Cannot `{0}` as a memory amount: {1}"
                               .format(qty, err))

    @abstractmethod
    def _get_total_memory_impl(self):
        """Machine-specific part of `get_total_memory`."""
        pass

    def list_process_tree(self, root_pid="1"):
        """
        Return list of PIDs of children of the given process.

        The returned list is empty if no process whose PID is
        `root_pid` can be found.

        Otherwise, the list is composed by walking the tree
        breadth-first, so it always starts with `root_pid` and ends
        with leaf processes (i.e., those which have no children).
        """
        ps_output = self._run_command(self._list_pids_and_ppids_command())

        children = defaultdict(list)
        for line in ps_output.split('\n'):
            line = line.strip()
            if not line:
                continue
            pid, ppid = line.split()
            children[str(ppid)].append(str(pid))
        if root_pid not in children:
            return []

        result = []
        queue = [root_pid]
        while queue:
            node = queue.pop()  # dequeue
            result.append(node)
            for child in children[node]:
                queue.insert(0, child)  # enqueue

        return result


class _LinuxMachine(_Machine):
    """Linux-specific shell tools."""

    def _get_total_cores_command(self):
        """Return nr. of CPU cores from ``nproc``"""
        return 'nproc'

    def _get_total_memory_impl(self):
        with self.transport.open('/proc/meminfo', 'r') as fd:
            for line in fd:
                if line.startswith('MemTotal'):
                    return (line.split(), 1,  Memory.KiB)

    def _list_pids_and_ppids_command(self):
        return 'ps --no-header -o pid,ppid'


class _MacOSXMachine(_Machine):
    """MacOSX-specific shell tools."""

    def _get_total_cores_command(self):
        """Return nr. of CPU cores from ``sysctl hw.ncpu``"""
        return 'sysctl -n hw.ncpu'

    def _get_total_memory_impl(self):
        """Return amount of total memory from ``sysctl hw.memsize``"""
        cmd = 'sysctl hw.memsize'
        stdout = self._run_command(cmd)
        return (stdout.split(':'), 1, Memory.B)

    def _list_pids_and_ppids_command(self):
        return 'ps -o pid=,ppid='


## the main LRMS class
#
#
#

class ShellcmdLrms(LRMS):
    """
    Execute an `Application`:class: instance through the shell.

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
      Transport to use to connect to the resource. Valid values are
      ``'ssh'`` or ``'local'``.

    :param str frontend:
      If `transport` is ``'ssh'``, then `frontend` is the hostname of the
      remote machine where the jobs will be executed.

    :param bool ignore_ssh_host_key:
      When connecting to a remote resource using the ``'ssh'`` transport the
      server's SSH public key is usually checked against a database of known
      hosts, and if the key is found but it does not match with the one saved
      in the database, the connection will fail. Setting `ignore_ssh_host_key`
      to `True` will disable this check, thus introducing a potential security
      issue but allowing connection even though the database contains
      old/invalid keys. (The main use case is when connecting to VMs on a IaaS
      cloud, since the IP is usually reused and therefore the ssh key is
      recreated.)

    :param bool override:
      `ShellcmdLrms` by default will try to gather information on the
      machine the resource is running on, including the number of
      cores and the available memory. These values may be different
      from the values stored in the configuration file. If `override`
      is ``True``, then the values automatically discovered will be used
      instead of the ones in the configuration file. If `override` is
      False, instead, the values in the configuration file will be
      used.

    :param int ssh_timeout:
      If `transport` is ``'ssh'``, this value will be used as timeout (in
      seconds) for connecting to the SSH TCP socket.

    :param gc3libs.quantity.Memory large_file_threshold:
      Copy files below this size in one single SFTP GET operation;
      see `SshTransport.get`:meth: for more information.
      Only used if `transport` is ``'ssh'``.

    :param gc3libs.quantity.Memory large_file_chunk_size:
      Copy files that are over the above-mentioned threshold by
      sequentially transferring chunks of this size.
      see `SshTransport.get`:meth: for more information.
      Only used if `transport` is ``'ssh'``.
    """

    TIMEFMT = '\n'.join([
        'WallTime=%es',
        'KernelTime=%Ss',
        'UserTime=%Us',
        'CPUUsage=%P',
        'MaxResidentMemory=%MkB',
        'AverageResidentMemory=%tkB',
        'AverageTotalMemory=%KkB',
        'AverageUnsharedMemory=%DkB',
        'AverageUnsharedStack=%pkB',
        'AverageSharedMemory=%XkB',
        'PageSize=%ZB',
        'MajorPageFaults=%F',
        'MinorPageFaults=%R',
        'Swaps=%W',
        'ForcedSwitches=%c',
        'WaitSwitches=%w',
        'Inputs=%I',
        'Outputs=%O',
        'SocketReceived=%r',
        'SocketSent=%s',
        'Signals=%k',
        'ReturnCode=%x',
    ])
    """
    Format string for running commands with ``/usr/bin/time``.
    It is used by GC3Pie to capture resource usage data for commands
    executed through the shell.

    The value used here lists all the resource usage values that *GNU
    time* can capture, with the same names used by the ARC Resource
    Manager (for historical reasons).
    """

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
    """
    How to translate *GNU time* output into values stored in the ``.execution`` attribute.

    The dictionary maps key names (as used in the `TIMEFMT` string) to
    a pair *(attribute name, converter function)* consisting of the
    name of an attribute that will be set on a task's ``.execution``
    object, and a function to convert the (string) value gotten from
    *GNU time* output into the actual Python value written.
    """

    PRIVATE_DIR = '.gc3pie_shellcmd'
    """
    Subdirectory of a tasks's execution directory reserved for storing
    `ShellcmdLrms`:class: files.
    """

    WRAPPER_SCRIPT = 'wrapper_script.sh'
    """
    Name of the task launcher script (within `PRIVATE_DIR`).

    The `ShellcmdLrms`:class: writes here that wrap an application's
    payload script, to collect resource usage or download/upload
    result files, etc.
    """

    WRAPPER_OUTPUT_FILENAME = 'resource_usage.txt'
    """
    Name of the file where resource usage is written to.

    (Relative to `PRIVATE_DIR`.)
    """

    WRAPPER_PID = 'wrapper.pid'
    """
    Name of the file where the wrapper script's PID is stored.

    (Relative to `PRIVATE_DIR`).
    """

    MOVER_SCRIPT = 'mover.py'
    """
    Name of the data uploader/downloader script (within `PRIVATE_DIR`).
    """

    RESOURCE_DIR = '$HOME/.gc3/shellcmd.d'
    """
    Path to the directory where bookkeeping files are stored.
    (This is on the target machine where `ShellcmdLrms`:class:
    executes commands.)

    It may contain environmental variable references, which are
    expanded through the (remote) shell.
    """

    def __init__(self, name,
                 # these parameters are inherited from the `LRMS` class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime,
                 auth=None,
                 # these are specific to `ShellcmdLrms`
                 frontend='localhost', transport='local',
                 time_cmd=None,
                 override='False',
                 spooldir=gc3libs.defaults.SPOOLDIR,
                 resourcedir=None,
                 # SSH-related options; ignored if `transport` is 'local'
                 ssh_config=None,
                 keyfile=None,
                 ignore_ssh_host_keys=False,
                 ssh_timeout=None,
                 large_file_threshold=None,
                 large_file_chunk_size=None,
                 **extra_args):

        # init base class
        LRMS.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth, **extra_args)

        # whether actual machine params (cores, memory) should be
        # auto-detected on first use
        self.override = override

        self.spooldir = spooldir

        # Configure transport
        if transport == 'local':
            self.transport = gc3libs.backends.transport.LocalTransport()
            self._username = getuser()
            self.frontend = 'localhost'
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
                large_file_threshold=large_file_threshold,
                large_file_chunk_size=large_file_chunk_size,
            )
            self.frontend = frontend
        else:
            raise AssertionError("Unknown transport '{0}'" .format(transport))

        # Init bookkeeping
        self.updated = False  # data may not reflect actual state
        self.free_slots = self.max_cores
        self.user_run = 0
        self.user_queued = 0
        self.queued = 0
        self.total_memory = max_memory_per_core
        self.available_memory = self.total_memory
        self._job_infos = {}

        # Some init parameters can only be discovered / checked when a
        # connection to the target resource is up.  We want to delay
        # this until the time they are actually used, to avoid opening
        # up a network connection when the backend is initialized
        # (could be e.g. a `ginfo -n` call that has no need to operate
        # on remote objects).
        self._resourcedir_raw = resourcedir or ShellcmdLrms.RESOURCE_DIR
        self._time_cmd = time_cmd
        self._time_cmd_ok = False  # check on first use

    @property
    def frontend(self):
        return self._frontend

    @frontend.setter
    def frontend(self, value):
        self._frontend = value
        self.transport.set_connection_params(value)

    @property
    def resource_dir(self):
        try:
            return self._resource_dir
        except AttributeError:
            # Since RESOURCE_DIR contains the `$HOME` variable, we
            # have to expand it by connecting to the remote
            # host. However, we don't want to do that during
            # initialization, so we do it the first time this is
            # actually needed.
            self._init_resource_dir()
            return self._resource_dir

    @resource_dir.setter
    def resource_dir(self, value):
        self._resource_dir = value

    def _init_resource_dir(self):
        self.transport.connect()
        # expand env variables in the `resource_dir` setting
        exit_code, stdout, stderr = self.transport.execute_command(
            'echo %s' % sh_quote_unsafe(self._resourcedir_raw))
        self.resource_dir = stdout.strip()

        if not self.transport.exists(self.resource_dir):
            try:
                log.info("Creating resource directory: '%s' ...", self.resource_dir)
                self.transport.makedirs(self.resource_dir)
            except Exception as ex:
                log.error("Failed creating resource directory '%s': %s: %s",
                          self.resource_dir, type(ex), ex)
                # cannot continue
                raise

    @property
    def time_cmd(self):
        if not self._time_cmd_ok:
            self._time_cmd = self._locate_gnu_time()
            self._time_cmd_ok = True
        return self._time_cmd

    def _gather_machine_specs(self):
        """
        Gather information about target machine and update config.
        The following attributes are set (or reset) as an effect
        of calling this method:

        - ``_machine``: Set to the an appropriate instance of
          `_Machine`:class:, detected through connection via
          `self.transport`.
        - ``_resource_dir``: Set to the expansion of whatever was
          passed as ``resource`` construction parameter.
        - ``max_cores``: If ``self.override`` is true, set to the
          number of processors on the target.
        - ``total_memory``: If ``self.override`` is true, set to total
          amount of memory on the target.
        """
        self.transport.connect()
        self._machine = _Machine.detect(self.transport)
        self._init_arch()
        if self.override:
            self._init_max_cores()
            self._init_total_memory()
            self._update_resource_usage_info()

    def _init_arch(self):
        arch = self._machine.get_architecture()
        if not (arch <= self.architecture):
            if self.override:
                log.info(
                    "Mismatch of value `architecture` on resource %s:"
                    " configuration file says `architecture=%s`"
                    " but GC3Pie detected `%s`. Updating current value.",
                    self.name,
                    ','.join(self.architecture),
                    ','.join(arch))
                self.architecture = arch
            else:
                raise gc3libs.exceptions.ConfigurationError(
                    "Invalid architecture: configuration file says `%s` but "
                    "it actually is `%s`" % (', '.join(self.architecture),
                                             ', '.join(arch)))

    def _init_max_cores(self):
        max_cores = self._machine.get_total_cores()
        if max_cores != self.max_cores:
            log.info(
                "Mismatch of value `max_cores` on resource '%s':"
                " configuration file says `max_cores=%d` while it's actually `%d`."
                " Updating current value.",
                self.name, self.max_cores, max_cores)
            self.max_cores = max_cores

    def _init_total_memory(self):
        self.total_memory = self._machine.get_total_memory()
        if self.total_memory != self.max_memory_per_core:
            log.info(
                "Mismatch of value `max_memory_per_core` on resource %s:"
                " configuration file says `max_memory_per_core=%s` while it's"
                " actually `%s`. Updating current value.",
                self.name,
                self.max_memory_per_core,
                self.total_memory.to_str('%g%s', unit=Memory.MB))
            self.max_memory_per_core = self.total_memory

    def _locate_gnu_time(self):
        """
        Return the command path to run the GNU `time` binary.

        :raise ConfigurationError:
          if no GNU ``time`` executable can be located.
        """
        candidates = [
            'time',  # default on Linux systems
            'gtime', # MacOSX with Homebrew or MacPorts
        ]
        if self._time_cmd:
            # try this first
            candidates.insert(0, self._time_cmd)
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
        raise gc3libs.exceptions.ConfigurationError(
            "Unable to find GNU `time` on resource `{name}`."
            " Please, install GNU time and set the `time_cmd`"
            " configuration option in gc3pie.conf."
            .format(name=self.name))

    ## Bookkeeping
    #
    # The following methods deal with internal book-keeping: how much
    # of the target's configured resources have been used by GC3Pie.
    # Presently, book-keeping is so complicated (and requires
    # recomputing at each invocation) because GC3Pie makes the
    # assumption that the target resource is *shared*, i.e., other
    # GC3Pie processes run concurrently by the user may compete for
    # the same resources.
    #

    def count_running_tasks(self):
        """
        Returns number of currently running tasks.

        .. note::

          1. The count of running tasks includes also tasks that may
             have been started by another GC3Pie process so this count
             can be positive when the resource has just been opened.

          2. The count is updated every time the resource is updated,
             so the returned number can be stale if the
             `ShellcmdLrms.get_resource_status()` has not been called
             for a while.
        """
        return sum(1 for info in list(self._job_infos.values())
                   if not info['terminated'])

    def count_used_cores(self):
        """
        Return total nr. of cores used by running tasks.

        Similar caveats as in `ShellcmdLrms.count_running_tasks`:meth:
        apply here.
        """
        return sum(info['requested_cores']
                   for info in list(self._job_infos.values())
                   if not info['terminated'])

    def count_used_memory(self):
        """
        Return total amount of memory used by running tasks.

        Similar caveats as in `ShellcmdLrms.count_running_tasks`:meth:
        apply here.
        """
        return sum(
            # FIXME: if `requested_memory==None` then just do not
            # account a task's memory usage.  This is of course
            # incorrect and leads to situations where a single task
            # can wreak havoc on an entire compute node, but is
            # consistent with what we do during scheduling /
            # requirements check.  (OTOH, it's *not* consistent with
            # what other backends do: SLURM, for example, takes the
            # pessimistic stance that a job with no memory
            # requirements is using (DefMemPerCPU * NumCPUs)
            ((info['requested_memory'] or 0*MB)
             for info in list(self._job_infos.values())
             if not info['terminated']), 0*MB)


    def _get_persisted_job_info(self):
        """
        Get information on total resources from the files stored in
        `self.resource_dir`. It then returns a dictionary {PID: {key:
        values}} with informations for each job which is associated to
        a running process.
        """
        self.transport.connect()
        job_infos = {}
        pidfiles = self.transport.listdir(self.resource_dir)
        if pidfiles:
            log.debug("Checking status of the following PIDs: %s",
                      ", ".join(pidfiles))
            for pid in pidfiles:
                job = self._read_job_info_file(pid)
                if job:
                    job_infos[str(pid)] = job
                else:
                    # Process not found, ignore it
                    continue
        return job_infos

    def _read_job_info_file(self, pid):
        """
        Get resource information on job with pid `pid`, if it
        exists. Returns None if it does not exist.
        """
        self.transport.connect()
        log.debug("Reading job info file for pid %r", pid)
        jobinfo = None
        path = posixpath.join(self.resource_dir, str(pid))
        with self.transport.open(path, 'rb') as fp:
            try:
                jobinfo = pickle.load(fp)
            except Exception as ex:
                log.error("Unable to read remote job info file %s: %s",
                          path, ex)
                raise
        return jobinfo

    def _write_job_info_file(self, pid, resources):
        """
        Update file in `self.resource_dir/PID` with `resources`.
        """
        self.transport.connect()
        # XXX: We should check for exceptions!
        log.debug("Updating job info file for pid %s", pid)
        with self.transport.open(
                posixpath.join(self.resource_dir, str(pid)), 'wb') as fp:
            pickle.dump(resources, fp, -1)

    def _delete_job_info_file(self, pid):
        """
        Delete `self.resource_dir/PID` file
        """
        self.transport.connect()
        log.debug("Deleting job info file for pid %s ...", pid)
        pidfile = posixpath.join(self.resource_dir, pid)
        try:
            self.transport.remove(pidfile)
        except Exception as err:
            msg = str(err)
            if 'OSError: [Errno 2]' not in msg:
                log.debug(
                    "Ignored error deleting file `%s`: %s: %s",
                    pidfile, err.__class__.__name__, err)


    ## Backend interface implementation
    #
    # These methods provide what is expected of any LRMS class.
    #

    def _connect(self):
        """Ensure transport to remote resource works."""
        try:
            self.transport.connect()
        except gc3libs.exceptions.TransportError as err:
            log.error("Unable to connect to host `%s`: %s", self.frontend, err)
            raise
        try:
            self._machine
        except AttributeError:
            self._gather_machine_specs()

    def cancel_job(self, app):
        """
        Kill all children processes of the given task `app`.

        The PID of the wrapper script (which is the root of the PID
        tree we are going to send a "TERM" signal) must have been
        stored (by `submit_job`:meth:) as `app.execution.lrms_jobid`.
        """
        try:
            root_pid = app.execution.lrms_jobid
        except ValueError:
            raise gc3libs.exceptions.InvalidArgument(
                "Invalid field `lrms_jobid` in Task '{0}':"
                " expected a number, got '{{2}) instead"
                .format(app, app.execution.lrms_jobid,
                        type(app.execution.lrms_jobid)))

        self._connect()

        pids_to_kill = self._machine.list_process_tree(root_pid)
        if not pids_to_kill:
            log.debug(
                "No process identified by PID %s in `ps` output,"
                " assuming task %s is already terminated.", root_pid, app)
        else:
            assert (root_pid == pids_to_kill[0])

            # list `pids_to_kill` starts with the root process and ends
            # with leaf ones; we want to kill them in reverse order (leaves first)
            kill_args = ' '.join(str(procid) for procid in reversed(pids_to_kill))
            log.debug(
                "Cancelling task %s on resource `%s`:"
                " sending SIGTERM to processes with PIDs %s",
                app, self.name, kill_args)
            # ignore exit code and STDERR from `kill`: if any process
            # exists while we're killing them, `kill` will error out
            # but that error should be ignored...
            self.transport.execute_command("kill {0}".format(kill_args))

            # now double-check and send SIGKILLs
            attempt = 1
            waited_on_first = False
            # XXX: should these be configurable?
            wait = 60
            max_attempts = 5
            while pids_to_kill and attempt < max_attempts:
                for target in list(reversed(pids_to_kill)):
                    log.debug("Checking if PID %s is still running ...", target)
                    try:
                        pstat = self._machine.get_process_state(target)
                    except LookupError:
                        log.debug("Process %s can no longer be found in process table.", target)
                        pids_to_kill.remove(target)
                        continue
                    # see comments in `_parse_process_status` for the
                    # meaning of the letters
                    if pstat in ['R', 'S', 'I', 'W', 'T', 't']:
                        if not waited_on_first:
                            # wait some time to allow disk I/O before termination
                            self._grace_time(wait)
                            waited_on_first = True
                        exit_code, stdout, stderr = self.transport.execute_command(
                            'kill -9 {0}'.format(target))
                        if exit_code == 0:
                            pids_to_kill.remove(target)
                        else:
                            log.debug(
                                "Could not send SIGKILL"
                                " to process %s on resource '%s':"
                                " %s (exit code: %d)",
                                target, self.name, stderr.strip(), exit_code)
                    elif pstat in ['D', 'U']:
                        log.error(
                            "Process %s on resource %s is"
                            " in uninterruptible sleep and cannot be killed.",
                            target, self.name)
                    elif pstat in ['X', 'Z']:
                        log.warning(
                            "Process %s on resource %s is already dead"
                            " but process entry has not been cleared."
                            " This might be a bug in GC3Pie or in `%s`.",
                                 target, self.name, self.time_cmd)
                        pids_to_kill.remove(target)
                if not pids_to_kill:
                    break
                self._grace_time(wait)
                attempt += 1

            if pids_to_kill:
                log.error(
                    "Not all processes belonging to task %s could be killed:"
                    " processes %s still alive on resource '%s'.",
                    app, (' '.join(str(p) for p in pids_to_kill)), self.name)

        try:
            self._job_infos[root_pid]['terminated'] = True
        except KeyError:
            # It may happen than `cancel_job()` is called without the
            # resource state having been updated (hence
            # `self._job_infos` is empty); this happens e.g. with the
            # `gkill` command.  If that happens, just ignore the
            # error.  (XXX: There might be a better way to handle this...)
            if self.updated:
                raise
            else:
                # ignore
                pass

    def _grace_time(self, wait):
        if wait:
            log.info("Waiting %s seconds to allow processes to terminate ...", wait)
            for _ in range(wait):  # Python ignores SIGINT while in `time.sleep()`
                time.sleep(1)


    @same_docstring_as(LRMS.close)
    def close(self):
        # XXX: free any resources in use?
        pass


    def free(self, app):
        """
        Delete the temporary directory where a child process has run.
        The temporary directory is removed with all its content,
        recursively.

        If deletion is successful, the `lrms_execdir` attribute in
        `app.execution` is reset to `None`; subsequent invocations of
        this method on the same applications do nothing.
        """
        try:
            if (hasattr(app.execution, 'lrms_execdir') and app.execution.lrms_execdir is not None):
                log.debug('Deleting working directory of task `%s` ...', app)
                self.transport.connect()
                if self.transport.isdir(app.execution.lrms_execdir):
                    self.transport.remove_tree(app.execution.lrms_execdir)
                app.execution.lrms_execdir = None
        except Exception as ex:
            log.warning("Could not remove directory '%s': %s: %s",
                        app.execution.lrms_execdir, ex.__class__.__name__, ex)

        try:
            pid = app.execution.lrms_jobid
            self._delete_job_info_file(pid)
        except AttributeError:
            # lrms_jobid not yet assigned; probably submit
            # failed -- ignore and continue
            pass


    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        self.updated = False
        self._connect()
        self._update_resource_usage_info()
        self.updated = True
        return self

    def _update_resource_usage_info(self):
        """
        Helper method for (re)reading resource usage from disk.
        """
        self._job_infos = self._get_persisted_job_info()
        used_memory = self.count_used_memory()
        self.available_memory = self.total_memory - used_memory
        self.free_slots = self.max_cores - self.count_used_cores()
        self.user_run = self.count_running_tasks()
        log.debug("Recovered resource information from files in %s:"
                  " total nr. of cores: %s, requested by jobs: %s;"
                  " available memory: %s, requested by jobs: %s.",
                  self.resource_dir,
                  self.max_cores, (self.max_cores - self.free_slots),
                  self.available_memory.to_str('%g%s', unit=Memory.MB, conv=float),
                  used_memory.to_str('%g%s', unit=Memory.MB, conv=float))


    @same_docstring_as(LRMS.get_results)
    def get_results(self, app, download_dir,
                    overwrite=False, changed_only=True):
        if app.output_base_url is not None:
            raise gc3libs.exceptions.DataStagingError(
                "Retrieval of output files to non-local destinations"
                " is not supported in the ShellCmd backend.")

        self._connect()
        # Make list of files to copy, in the form of (remote_path,
        # local_path) pairs.  This entails walking the
        # `Application.outputs` list to expand wildcards and
        # directory references.
        stageout = list()
        for remote_relpath, local_url in app.outputs.items():
            if local_url.scheme in ['swift', 'swt', 'swifts', 'swts']:
                continue
            local_relpath = local_url.path
            if remote_relpath == gc3libs.ANY_OUTPUT:
                remote_relpath = ''
                local_relpath = ''
            stageout += self._get_remote_and_local_path_pair(
                app, remote_relpath, download_dir, local_relpath)

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

    def _get_remote_and_local_path_pair(self, app, remote_relpath,
                                         local_root_dir, local_relpath):
        """
        Scan remote directory and return list of corresponding remote and local paths.

        The return value is a list of *(remote_path, local_path)* pairs: each
        `remote_path` is an existing file on the remote end of the transport,
        and *local_path* is the corresponding local path, constructed by
        prepending `local_root_dir` to the relative remote path.
        """
        # see https://github.com/fabric/fabric/issues/306 about why it is
        # correct to use `posixpath.join` for remote paths (instead of
        # `os.path.join`)
        remote_path = posixpath.join(app.execution.lrms_execdir, remote_relpath)
        local_path = os.path.join(local_root_dir, local_relpath)
        if self.transport.isdir(remote_path):
            # recurse, accumulating results
            result = list()
            for entry in self.transport.listdir(remote_path):
                result += self._get_remote_and_local_path_pair(
                    app, posixpath.join(remote_relpath, entry), local_path, entry)
            return result
        else:
            return [(remote_path, local_path)]


    def has_running_tasks(self):
        """
        Return ``True`` if tasks are running on the resource.

        See `ShellcmdLrms.count_running_tasks`:meth: for caveats
        about the count of "running jobs" upon which this boolean
        check is based.
        """
        return self.user_run > 0


    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        # `remote_filename` must be relative to the execution directory
        assert not os.path.isabs(remote_filename)
        self._connect()
        # see https://github.com/fabric/fabric/issues/306 about why it
        # is correct to use `posixpath.join` for remote paths (instead
        # of `os.path.join`)
        remote_filename = posixpath.join(app.execution.lrms_execdir, remote_filename)
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
                # ... no, then treat it as a file name
                with open(local_file, 'w+b') as locally:
                    locally.write(data)


    def submit_job(self, app):
        """
        Run an `Application` instance as a shell process.

        :see: `LRMS.submit_job`
        """
        #
        # This is a rather lengthy (if straightforward) function, whose actual
        # implementation is brokwn down into several smaller pieces, listed
        # hereafter. The body of `submit_jobs()` should give a high-level
        # overview, and the helper functions carry the implementation details.
        #

        # if any failure happens here, we just raise an `LRMSSubmitError` and
        # be done with it: there is nothing to clean up
        try:
            self._connect()
            self._check_app_requirements(app)
        except gc3libs.exceptions.LRMSSubmitError:
            raise  # no need to convert
        except Exception as err:
            raise gc3libs.exceptions.LRMSSubmitError(
                "Failed submitting task {0} to resource `{1}`: {2}"
                .format(app, self.name, err))

        log.debug("Executing command '%s' through the shell on `%s` ...",
                  " ".join(app.arguments), self.frontend)

        # from this point on, any failure needs clean-up by calling `self.free(app)`
        try:
            app.execution.lrms_execdir = self._setup_app_execution_directory(app)
            self._stage_app_input_files(app)
            self._ensure_app_command_is_executable(app)

            redirection_command = self._setup_redirection(app)
            env_commands = self._setup_environment(app)

            wrapper_dir = self._setup_wrapper_dir(app)
            download_cmds, upload_cmds = self._setup_data_movers(app, wrapper_dir)
            pidfilename = posixpath.join(wrapper_dir, self.WRAPPER_PID)
            wrapper_output_path = posixpath.join(wrapper_dir, self.WRAPPER_OUTPUT_FILENAME)
            wrapper_script_path = posixpath.join(wrapper_dir, self.WRAPPER_SCRIPT)

            # create the wrapper script
            with self.transport.open(wrapper_script_path, 'wt') as wrapper:
                wrapper.write(
                    r"""#!/bin/sh
                    echo $$ >'{pidfilename}'
                    cd {execdir}
                    {redirections}
                    {environment}
                    {download_cmds}
                    '{time_cmd}' -o '{wrapper_out}' -f '{fmt}' {command}
                    rc=$?
                    {upload_cmds}
                    rc2=$?
                    if [ $rc -ne 0 ]; then exit $rc; else exit $rc2; fi
                    """.format(
                        pidfilename=pidfilename,
                        execdir=app.execution.lrms_execdir,
                        time_cmd=self.time_cmd,
                        wrapper_out=wrapper_output_path,
                        fmt=ShellcmdLrms.TIMEFMT,
                        redirections=redirection_command,
                        environment=('\n'.join(env_commands)),
                        download_cmds=('\n'.join(download_cmds)),
                        upload_cmds=('\n'.join(upload_cmds)),
                       command=(' '.join(sh_quote_unsafe(arg) for arg in app.arguments)),
                ))
            self.transport.chmod(wrapper_script_path, 0o755)

            # execute the script in background
            self.transport.execute_command(wrapper_script_path, detach=True)
            pid = self._read_app_process_id(pidfilename)
            app.execution.lrms_jobid = pid

        except gc3libs.exceptions.LRMSSubmitError:
            self.free(app)
            raise  # no need to convert

        except Exception as err:
            self.free(app)
            raise gc3libs.exceptions.LRMSSubmitError(
                "Failed submitting task {0} to resource `{1}`: {2}"
                .format(app, self.name, err))

        # Update application and current resources
        #
        self.free_slots -= app.requested_cores
        self.user_run += 1
        if app.requested_memory:
            self.available_memory -= app.requested_memory
        self._job_infos[pid] = {
            'requested_cores': app.requested_cores,
            'requested_memory': app.requested_memory,
            'execution_dir': app.execution.lrms_execdir,
            'terminated': False,
        }
        self._write_job_info_file(pid, self._job_infos[pid])

        return app

    ## implementation of the `submit_job()` method

    def _check_app_requirements(self, app, update=True):
        """Raise exception if application requirements cannot be satisfied."""
        # update resource status to get latest data on free cores, memory, etc
        if update:
            self.get_resource_status()

        if self.free_slots == 0:  # or free_slots == 0:
            if self.override:
                errmsg = (
                    "Resource {0} already running maximum allowed number of jobs"
                    .format(self.name))
            else:
                errmsg = (
                    "Resource {0} already running maximum allowed number of jobs"
                    " ({1}). Increase 'max_cores' to raise."
                    .format(self.name, self.max_cores))
            raise gc3libs.exceptions.MaximumCapacityReached(errmsg)

        if self.free_slots < app.requested_cores:
            raise gc3libs.exceptions.MaximumCapacityReached(
                "Resource {0} does not have enough available execution slots:"
                " {1} requested total, but only {2} available."
                .format(self.name, app.requested_cores, self.free_slots))

        if (app.requested_memory and self.available_memory < app.requested_memory):
            raise gc3libs.exceptions.MaximumCapacityReached(
                "Resource {0} does not have enough available memory:"
                " {1} requested total, but only {2} available."
                .format(
                    self.name,
                    app.requested_memory.to_str('%g%s', unit=Memory.MB),
                    self.available_memory.to_str('%g%s', unit=Memory.MB))
            )

    def _setup_app_execution_directory(self, app):
        """
        Create a temporary subdirectory and return its (remote) path.

        This is intended to be used as part of the "submit" process;
        in case of any failure, raises a `SpoolDirError`.
        """
        target = posixpath.join(self.spooldir, 'shellcmd_job.XXXXXX')
        cmd =  ("mkdir -p {0} && mktemp -d {1}" .format(self.spooldir, target))
        exit_code, stdout, stderr = self.transport.execute_command(cmd)
        if exit_code != 0 or stderr:
            raise gc3libs.exceptions.SpoolDirError(
                "Cannot create temporary job working directory"
                " `{0}` on host `{1}`; command `{2}` exited"
                " with code {3} and error output: `{4}`."
                .format(target, self.frontend, cmd, exit_code, stderr))
        return stdout.strip()

    def _stage_app_input_files(self, app):
        destdir = app.execution.lrms_execdir
        for local_path, remote_path in list(app.inputs.items()):
            if local_path.scheme != 'file':
                log.debug(
                    "Ignoring input URL `%s` for task %s:"
                    " only 'file:' schema supported by the ShellcmdLrms backend.",
                    local_path, app)
                continue
            remote_path = posixpath.join(destdir, remote_path)
            remote_parent = os.path.dirname(remote_path)
            try:
                if (remote_parent not in ['', '.']
                        and not self.transport.exists(remote_parent)):
                    self.transport.makedirs(remote_parent)
                self.transport.put(local_path.path, remote_path)
                # preserve execute permission on input files
                if os.access(local_path.path, os.X_OK):
                    self.transport.chmod(remote_path, 0o755)
            except Exception as err:
                log.error(
                    "Staging input file `%s` to host `%s` failed: %s",
                    local_path.path, self.frontend, err)
                raise

    def _ensure_app_command_is_executable(self, app):
        """
        Give an `Application`'s command file ``a+x`` permissions.

        Ignore all failures: might be a link to a file we do not own.

        In addition, only do this for "local" executable files: by documented
        convention, any command name *not* starting with `./` will be searched
        in `$PATH` and not in the current working directory (i.e., is not
        something staged by GC3Pie).
        """
        execdir = app.execution.lrms_execdir
        cmd = app.arguments[0]
        if cmd.startswith('./'):
            try:
                self.transport.chmod(posixpath.join(execdir, cmd[2:]), 0o755)
            except:
                log.warning(
                    "Failed setting execution flag on remote file '%s'",
                    posixpath.join(execdir, cmd))

    def _setup_redirection(self, app):
        """
        Return shell redirection operators to be applied when executing `app`.

        Also ensure that directories where STDOUT and STDERR files should be
        captured exist (on the remote side).
        """
        execdir = app.execution.lrms_execdir
        redirections = ['exec']
        if app.stdin is not None:
            redirections.append("<'%s'" % app.stdin)
        if app.stdout is not None:
            redirections.append(">'%s'" % app.stdout)
            stdout_dir = os.path.dirname(app.stdout)
            if stdout_dir:
                self.transport.makedirs(posixpath.join(execdir, stdout_dir))
        if app.join or (app.stderr and app.stderr == app.stdout):
            redirections.append("2>&1")
        else:
            if app.stderr is not None:
                redirections.append("2>'%s'" % app.stderr)
                stderr_dir = os.path.dirname(app.stderr)
                if stderr_dir:
                    self.transport.makedirs(posixpath.join(execdir, stderr_dir))
        if len(redirections) > 1:
            return ' '.join(redirections)
        else:
            return ''  # nothing to do

    def _setup_environment(self, app):
        """Return commands to set up the environment for `app`."""
        env_commands = []
        for k, v in app.environment.items():
            env_commands.append(
                "export {k}={v};"
                .format(k=sh_quote_safe(k), v=sh_quote_unsafe(v)))
        return env_commands

    def _setup_wrapper_dir(self, app):
        """
        Create and return the directory in which GC3Pie aux files for `app` will be stored.
        """
        wrapper_dir = posixpath.join(app.execution.lrms_execdir, self.PRIVATE_DIR)
        if not self.transport.isdir(wrapper_dir):
            try:
                self.transport.makedirs(wrapper_dir)
            except Exception as err:
                log.error("Failed creating remote folder '%s': %s", wrapper_dir, err)
                raise
        return wrapper_dir

    # FIXME: data mover functionality should be available in all backends!
    def _setup_data_movers(self, app, destdir):
        """
        Set up scripts to download/upload the swift/http files.
        """
        download_cmds = []
        upload_cmds = []
        mover_path = posixpath.join(destdir, ShellcmdLrms.MOVER_SCRIPT)
        for url, outfile in list(app.inputs.items()):
            if url.scheme in ['swift', 'swifts', 'swt', 'swts', 'http', 'https']:
                download_cmds.append(
                    "python '{mover}' download '{url}' '{outfile}'"
                    .format(mover=mover_path, url=str(url), outfile=outfile))
        for infile, url in list(app.outputs.items()):
            if url.scheme in ['swift', 'swt', 'swifts', 'swts']:
                upload_cmds.append(
                    "python '{mover}' upload '{url}' '{infile}'"
                    .format(mover=mover_path, url=str(url), infile=infile))
        if download_cmds or upload_cmds:
            # copy the downloader script
            mover_src = resource_path(Requirement.parse("gc3pie"), "gc3libs/etc/mover.py")
            with open(mover_src, 'r') as src:
                with self.transport.open(mover_path, 'w') as dst:
                    dst.write(src.read())
        return download_cmds, upload_cmds

    def _read_app_process_id(self, pidfile_path):
        """
        Read and return the PID stored in `pidfile_path`.
        """
        # Just after the script has been started the pidfile should be
        # filled in with the correct pid.
        #
        # However, the script can have not been able to write the
        # pidfile yet, so we have to wait a little bit for it...
        for retry in gc3libs.utils.ExponentialBackoff():
            try:
                with self.transport.open(pidfile_path, 'r') as pidfile:
                    pid = pidfile.read().strip()
                    # it happens that the PID file exists, but `.read()`
                    # returns the empty string... just wait and retry.
                    if pid == '':
                        continue
                    return pid
            except gc3libs.exceptions.TransportError as ex:
                if '[Errno 2]' in str(ex):  # no such file or directory
                    time.sleep(retry)
                    continue
                else:
                    raise
        else:  # I wouldn't have imagined I'd ever use `for: .. else:`!
            raise gc3libs.exceptions.LRMSSubmitError(
                "Unable to read PID file of submitted process from"
                " execution directory `%s`: %s"
                % (execdir, pidfile_path))


    def update_job_state(self, app):
        """
        Query the running status of the local process whose PID is
        stored into `app.execution.lrms_jobid`, and map the POSIX
        process status to GC3Libs `Run.State`.
        """
        self._connect()
        pid = app.execution.lrms_jobid
        try:
            pstat = self._machine.get_process_state(pid)
            app.execution.state = _parse_process_status(pstat)
            if app.execution.state == Run.State.TERMINATING:
                self._cleanup_terminating_task(app, pid)
            else:
                self._kill_if_over_time_limits(app)
        except LookupError:
            log.debug(
                "Process with PID %s not found,"
                " assuming task %s has finished running.",
                pid, app)
            self._cleanup_terminating_task(app, pid)
        return app.execution.state

    def _kill_if_over_time_limits(self, app):
        pid = app.execution.lrms_jobid
        elapsed = self._machine.get_process_running_time(pid)
        # determine whether to kill, depending on wall-clock time
        cancel = False
        if elapsed > self.max_walltime:
            log.warning("Task %s has run for %s, exceeding max_walltime %s of resource %s: cancelling it.",
                        app, elapsed.to_timedelta(), self.max_walltime, self.name)
            cancel = True
        if (app.requested_walltime and elapsed > app.requested_walltime):
            log.warning("Task %s has run for %s, exceeding own `requested_walltime` %s: cancelling it.",
                        app, elapsed.to_timedelta(), app.requested_walltime)
            cancel = True
        if cancel:
            self.cancel_job(app)
            # set signal to SIGTERM in termination status
            self._cleanup_terminating_task(app, pid, termstatus=(15, -1))
            return

    def _cleanup_terminating_task(self, app, pid, termstatus=None):
        app.execution.state = Run.State.TERMINATING
        # if `self._job_infos` records this task as terminated, then
        # updates to resource utilization records has already been
        # done by `get_resource_status()`
        if (pid in self._job_infos and not self._job_infos[pid]['terminated']):
            self._job_infos[pid]['terminated'] = True
            self._write_job_info_file(pid, self._job_infos[pid])
            # do in-memory bookkeeping
            assert (self._job_infos[pid]['requested_memory'] == app.requested_memory)
            assert (self._job_infos[pid]['requested_cores'] == app.requested_cores)
            self.free_slots += app.requested_cores
            self.user_run -= 1
            if app.requested_memory is not None:
                self.available_memory += app.requested_memory
        wrapper_filename = posixpath.join(
            app.execution.lrms_execdir,
            ShellcmdLrms.PRIVATE_DIR,
            ShellcmdLrms.WRAPPER_OUTPUT_FILENAME)
        try:
            log.debug(
                "Reading resource utilization from wrapper file `%s` for task %s ...",
                wrapper_filename, app)
            with self.transport.open(wrapper_filename, 'r') as wrapper_file:
                termstatus, outcome, valid = \
                    self._parse_wrapper_output(wrapper_file, termstatus)
                if valid:
                    app.execution.update(outcome)
                if termstatus is not None:
                    app.execution.returncode = termstatus
                else:
                    app.execution.returncode = (Run.Signals.RemoteError, -1)
        except EnvironmentError as err:
            msg = ("Could not read wrapper file `{0}` for task `{1}`: {2}"
                   .format(wrapper_filename, app, err))
            log.warning("%s -- Termination status and resource utilization fields will not be set.", msg)
            raise gc3libs.exceptions.InvalidValue(msg)

    def _parse_wrapper_output(self, wrapper_file, termstatus=None):
        """
        Parse the file saved by the wrapper in
        `ShellcmdLrms.WRAPPER_OUTPUT_FILENAME` inside the PRIVATE_DIR
        in the job's execution directory and return a `Struct`:class:
        containing the values found on the file.

        `wrapper_file` is an opened file. This method will rewind the
        file before reading.
        """
        valid = True  # optimistic default
        wrapper_file.seek(0)
        acctinfo = Struct()
        for line in wrapper_file:
            line = line.strip()
            # if the executed command was killed by a signal, then GNU
            # `time` will output `0` for the `%x` format specifier but
            # still mark the exit signal in the output.
            if line.startswith('Command terminated by signal'):
                # since `Run.shellexit_to_returncode()` returns a
                # pair, we need to use a pair as default for
                # `returncode` here as well, to avoid comparing an
                # integer with a tuple in the `max()` invocation
                # at line 1670 below.
                termstatus = (int(line.split()[4]), -1)
                continue
            if '=' not in line:
                continue
            k, v = line.split('=', 1)
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
            elif name == 'returncode':
                # be sure not to overwrite here the error exit set in
                # lines 1643--1645 above with the successful exit
                # reported by GNU time's `%x` specifier
                if termstatus is not None:
                    termstatus = max(termstatus, conv(v))
                else:
                    termstatus = conv(v)
            else:
                acctinfo[name] = conv(v)

        # apprently GNU time does not report the total CPU time, used
        # so compute it here
        try:
            acctinfo['used_cpu_time'] = (
                acctinfo['shellcmd_user_time'] + acctinfo['shellcmd_kernel_time'])
        except KeyError:
            valid = False

        return termstatus, acctinfo, valid


    def validate_data(self, data_file_list=[]):
        """
        Return `False` if any of the URLs in `data_file_list` cannot
        be handled by this backend.

        The `shellcmd`:mod: backend can handle the following URL schemas:

        - ``file`` (natively, read/write);
        - ``swift``/``swifts``/``swt``/``swts`` (with Python-based remote helper, read/write);
        - ``http``/``https`` (with Python-based remote helper, read-only).
        """
        for url in data_file_list:
            if url.scheme not in ['file', 'swift', 'swifts', 'swt', 'swts', 'http', 'https']:
                return False
        return True


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
