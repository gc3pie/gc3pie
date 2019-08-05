#! /usr/bin/env python

"""
Job control on PBS/Torque clusters (possibly connecting to the
front-end via SSH).
"""

# Copyright (C) 2009-2014, 2016, 2019  University of Zurich. All rights reserved.
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

from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
__docformat__ = 'reStructuredText'


import datetime
import re
import time

from gc3libs import log, Run
from gc3libs.backends import LRMS
import gc3libs.exceptions
from gc3libs.quantity import Memory
from gc3libs.quantity import Duration
from gc3libs.utils import (same_docstring_as, sh_quote_safe_cmdline,
                           sh_quote_unsafe_cmdline)

from . import batch


# data for parsing PBS commands output

# regexps for extracting relevant strings

_qsub_jobid_re = re.compile(r'(?P<jobid>\d+).*', re.I)

_qstat_line_re = re.compile(
    r'^(?P<jobid>\d+)[^\d]+\s+'
    r'(?P<jobname>[^\s]+)\s+'
    r'(?P<username>[^\s]+)\s+'
    r'(?P<time_used>[^\s]+)\s+'
    r'(?P<state>[^\s]+)\s+'
    r'(?P<queue>[^\s]+)')

# convert data to GC3Pie internal format


def _to_memory(val):
    """
    Convert a memory quantity as it appears in the PBS logs to GC3Pie
    `Memory` value.

    Examples::

      >>> from gc3libs.quantity import kB, MB, GB
      >>> _to_memory('44kb') == 44*kB
      True
      >>> _to_memory('12mb') == 12*MB
      True
      >>> _to_memory('2gb') == 2*GB
      True
      >>> _to_memory('1024') == Memory(1024, Memory.B)
      True

    """
    # extract the `kb`, `mb`, etc. suffix, if there is one
    unit = val[-2:]
    # XXX: check that PBS uses base-2 units
    if unit == 'kb':
        return int(val[:-2]) * Memory.kB
    elif unit == 'mb':
        return int(val[:-2]) * Memory.MB
    elif unit == 'gb':
        return int(val[:-2]) * Memory.GB
    elif unit == 'tb':
        return int(val[:-2]) * Memory.TB
    else:
        if val[-1] == 'b':
            # XXX bytes
            val = int(val[:-1])
        else:
            # a pure number
            val = int(val)
        return int(val) * Memory.B


def _parse_asctime(val):
    try:
        # XXX: replace with datetime.strptime(...) in Python 2.5+
        return datetime.datetime(
            *(time.strptime(val, '%m/%d/%Y %H:%M:%S')[0:6]))
    except Exception as err:
        gc3libs.log.error(
            "Cannot parse '%s' as a PBS-format time stamp: %s: %s",
            val, err.__class__.__name__, str(err))
        return None


# code

def count_jobs(qstat_output, whoami):
    """
    Parse PBS/Torque's ``qstat`` output (as contained in string `qstat_output`)
    and return a quadruple `(R, Q, r, q)` where:

      * `R` is the total number of running jobs in the PBS/Torque cell
        (from any user);

      * `Q` is the total number of queued jobs in the PBS/Torque cell
        (from any user);

      * `r` is the number of running jobs submitted by user `whoami`;

      * `q` is the number of queued jobs submitted by user `whoami`
    """
    total_running = 0
    total_queued = 0
    own_running = 0
    own_queued = 0
    for line in qstat_output.split('\n'):
        log.info("Output line: %s" % line)
        m = _qstat_line_re.match(line)
        if not m:
            continue
        if m.group('state') in ['R']:
            total_running += 1
            if m.group('username') == whoami:
                own_running += 1
        elif m.group('state') in ['Q']:
            total_queued += 1
            if m.group('username') == whoami:
                own_queued += 1
        log.info("running: %d, queued: %d" % (total_running, total_queued))

    return (total_running, total_queued, own_running, own_queued)


class PbsLrms(batch.BatchSystem):

    """
    Job control on PBS/Torque clusters (possibly by connecting via SSH
    to a submit node).
    """

    _batchsys_name = 'PBS/TORQUE'

    def __init__(self, name,
                 # this are inherited from the base LRMS class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime,
                 auth,  # ignored if `transport` is 'local'
                 # these are inherited from `BatchSystem`
                 frontend, transport,
                 # these are specific to this backend
                 queue=None,
                 # (Note that optional arguments to the `BatchSystem` class,
                 # e.g.:
                 #     keyfile=None, accounting_delay=15,
                 # are collected into `extra_args` and should not be explicitly
                 # spelled out in this signature.)
                 **extra_args):

        # init base class
        batch.BatchSystem.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth,
            frontend, transport,
            **extra_args)

        # backend-specific setup
        self.queue = queue
        self.qsub = self._get_command_argv('qsub')

        # PBS/TORQUE commands
        self._qdel = self._get_command('qdel')
        self._qstat = self._get_command('qstat')
        self._tracejob = self._get_command('tracejob')

    def _parse_submit_output(self, output):
        return self.get_jobid_from_submit_output(output, _qsub_jobid_re)

    def _submit_command(self, app):
        qsub_argv, app_argv = app.qsub_pbs(self)
        if self.queue is not None:
            qsub_argv += ['-q', ('%s' % self.queue)]
        return (sh_quote_safe_cmdline(qsub_argv),
                'cd "$PBS_O_WORKDIR"; ' + sh_quote_unsafe_cmdline(app_argv))

    def _stat_command(self, job):
        return "%s %s | grep ^%s" % (
            self._qstat, job.lrms_jobid, job.lrms_jobid)

    def _acct_command(self, job):
        return "%s %s" % (self._tracejob, job.lrms_jobid)

    def _secondary_acct_command(self, job):
        return "%s -x -f %s" % (self._qstat, job.lrms_jobid)

    def _parse_stat_output(self, stdout, stderr):
        # parse `qstat` output
        pbs_status = stdout.split()[4]
        log.debug("translating PBS/Torque's `qstat` code"
                  " '%s' to gc3libs.Run.State", pbs_status)
        if pbs_status in ['Q', 'W']:
            state = Run.State.SUBMITTED
        elif pbs_status in ['R']:
            state = Run.State.RUNNING
        elif pbs_status in ['S', 'H', 'T'] or 'qh' in pbs_status:
            state = Run.State.STOPPED
        elif pbs_status in ['C', 'E', 'F']:
            state = Run.State.TERMINATING
        else:
            state = Run.State.UNKNOWN
        return self._stat_result(state, None)  # no term status info

    _tracejob_queued_re = re.compile(
        r'(?P<submission_time>\d+/\d+/\d+\s+\d+:\d+:\d+)\s+.\s+'
        r'Job Queued at request of .*job name =\s*(?P<job_name>[^,]+),'
        r'\s+queue =\s*(?P<queue>[^,]+)')

    _tracejob_run_re = re.compile(
        r'(?P<running_time>\d+/\d+/\d+\s+\d+:\d+:\d+)\s+.\s+'
        r'Job Run at request of .*')

    _tracejob_last_re = re.compile(
        r'(?P<end_time>\d+/\d+/\d+\s+\d+:\d+:\d+)\s+.'
        r'\s+Exit_status=(?P<exit_status>\d+)\s+'
        r'resources_used.cput=(?P<used_cpu_time>[^ ]+)\s+'
        r'resources_used.mem=(?P<mem>[^ ]+)\s+'
        r'resources_used.vmem=(?P<used_memory>[^ ]+)\s+'
        r'resources_used.walltime=(?P<used_walltime>[^ ]+)')

    _tracejob_keyval_mapping = {
        # regexp group name
        # |               `Task.execution` attribute
        # |               |
        # |               |                        converter function
        # |               |                        |
        # |               |                        |
        #   ... common backend attrs (see Issue 78) ...
        'exit_status':   ('exitcode',              int),
        'used_cpu_time': ('used_cpu_time',         Duration),
        'used_walltime': ('duration',              Duration),
        'used_memory':   ('max_used_memory',       _to_memory),
        #   ... PBS-only attrs ...
        'mem':           ('pbs_max_used_ram',      _to_memory),
        'submission_time': ('pbs_submission_time', _parse_asctime),
        'running_time':  ('pbs_running_time',      _parse_asctime),
        'end_time':      ('pbs_end_time',          _parse_asctime),
        'queue':         ('pbs_queue',             str),
        'job_name':      ('pbs_jobname',           str),
    }

    def _parse_acct_output(self, stdout, stderr):
        """Parse `tracejob` output."""
        acctinfo = {}
        for line in stdout.split('\n'):
            for pattern, carry_on in [
                    # regexp                   exit loop?
                    # =====================    ==========
                    (self._tracejob_queued_re, True),
                    (self._tracejob_run_re,    True),
                    (self._tracejob_last_re,   False),
            ]:
                match = pattern.match(line)
                if match:
                    for key, value in match.groupdict().items():
                        attr, conv = self._tracejob_keyval_mapping[key]
                        acctinfo[attr] = conv(value)
                    if carry_on:
                        continue
                    else:
                        break
        assert 'exitcode' in acctinfo, (
            "Could not extract exit code from `tracejob` output")
        acctinfo['termstatus'] = Run.shellexit_to_returncode(
            acctinfo.pop('exitcode'))
        return acctinfo

    _pbspro_keyval_mapping = {
        # PBS output key
        # |               `Task.execution` attribute
        # |                          |                  converter function
        # |                          |                  |
        # |                          |                  |
        #   ... common backend attrs (see Issue 78) ...
        'Exit_status':             ('exitcode',         int),
        'resources_used.cpupt':    ('used_cpu_time',    Duration),
        'resources_used.cput':     ('used_cpu_time',    Duration),
        'resources_used.vmem':     ('used_memory',      _to_memory),
        'resources_used.walltime': ('used_walltime',    Duration),
        #   ... PBS-only attrs ...
        'etime':                   ('pbs_queued_at',    _parse_asctime),
        'queue':                   ('pbs_queue',        str),
        'resources_used.mem':      ('pbs_max_used_ram', _to_memory),
        'stime':                   ('pbs_started_at',   _parse_asctime),
    }

    def _parse_secondary_acct_output(self, stdout, stderr):
        """Parse `qstat -x -f` output (PBSPro only)."""
        acctinfo = {}
        # FIXME: could be a bit smarter and not use a dumb quadratic
        # complexity algo...
        for line in stdout.split('\n'):
            for key, (attr, conv) in self._pbspro_keyval_mapping:
                if (key + ' = ') in line:
                    value = line.split('=')[1].strip()
                    acctinfo[attr] = conv(value)
        assert 'exitcode' in acctinfo, (
            "Could not extract exit code from `qstat -x -f` output")
        acctinfo['termstatus'] = Run.shellexit_to_returncode(
            acctinfo.pop('exitcode'))
        return acctinfo

    def _cancel_command(self, jobid):
        return ("%s %s" % (self._qdel, jobid))

    @same_docstring_as(LRMS.get_resource_status)
    @LRMS.authenticated
    def get_resource_status(self):
        self.updated = False
        try:
            self.transport.connect()

            _command = ('%s -a' % self._qstat)
            log.debug("Running `%s`...", _command)
            exit_code, qstat_stdout, stderr \
                = self.transport.execute_command(_command)
            if exit_code != 0:
                # cannot continue
                raise gc3libs.exceptions.LRMSError(
                    "PBS backend failed executing '%s':"
                    " exit code: %d; stdout: '%s', stderr: '%s'"
                    % (_command, exit_code, qstat_stdout, stderr))

            log.debug("Computing updated values for total/available slots ...")
            (total_running, self.queued, self.user_run, self.user_queued) \
                = count_jobs(qstat_stdout, self._username)
            self.total_run = total_running
            self.free_slots = -1
            self.used_quota = -1

            log.info("Updated resource '%s' status:"
                     " free slots: %d,"
                     " total running: %d,"
                     " own running jobs: %d,"
                     " own queued jobs: %d,"
                     " total queued jobs: %d",
                     self.name,
                     self.free_slots,
                     self.total_run,
                     self.user_run,
                     self.user_queued,
                     self.queued,
                     )
            return self

        except Exception as ex:
            # self.transport.close()
            log.error("Error querying remote LRMS, see debug log for details.")
            log.debug("Error querying LRMS: %s: %s",
                      ex.__class__.__name__, str(ex), exc_info=True)
            raise


# main: run tests
if "__main__" == __name__:
    import doctest
    doctest.testmod(name="pbs",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
