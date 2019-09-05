#! /usr/bin/env python
#
"""
Job control on SLURM clusters (possibly connecting to the front-end via SSH).
"""
# Copyright (C) 2012-2016, 2018, 2019  University of Zurich. All rights reserved.
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
#
from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
__docformat__ = 'reStructuredText'


import datetime
import os
import re
import time

from gc3libs import log, Run
from gc3libs.backends import LRMS
import gc3libs.backends.batch as batch
import gc3libs.exceptions
from gc3libs.quantity import Memory, bytes, kB, MB, GB
from gc3libs.quantity import Duration, seconds, minutes, hours, days
from gc3libs.utils import (same_docstring_as, sh_quote_safe_cmdline,
                           sh_quote_unsafe_cmdline)


## data for parsing SLURM commands output

# `sbatch` examples:
#
# $ sbatch -N4 myscript
# salloc: Granted job allocation 65537
#
# $ sbatch -N4 <<EOF
# > #!/bin/sh
# > srun hostname |sort
# > EOF
# sbatch: Submitted batch job 65541
#
_sbatch_jobid_re = re.compile(
    r'(sbatch:\s*)?(Granted job allocation|Submitted batch job)'
    r' (?P<jobid>\d+)')


## code

def count_jobs(squeue_output, whoami):
    """
    Parse SLURM's ``squeue`` output and return a quadruple `(R, Q, r,
    q)` where:

      * `R` is the total number of running jobs (from any user);
      * `Q` is the total number of queued jobs (from any user);
      * `r` is the number of running jobs submitted by user `whoami`;
      * `q` is the number of queued jobs submitted by user `whoami`

    The `squeue_output` must contain the results of an invocation of
    ``squeue --noheader --format='%i^%T^%u^%U^%r^%R'``.
    """
    total_running = 0
    total_queued = 0
    own_running = 0
    own_queued = 0
    for line in squeue_output.split('\n'):
        if line == '':
            continue
        # the choice of format string makes it easy to parse squeue output
        jobid, state, username, uid, reason, nodelist = line.split('^')
        if state in ['RUNNING', 'COMPLETING']:
            total_running += 1
            if username == whoami:
                own_running += 1
        # XXX: State CONFIGURING is described in the squeue(1) man
        # page as "Job has been allocated resources, but are waiting
        # for them to become ready for use (e.g. booting).".  Should
        # it be classified as "running" instead?
        elif state in ['PENDING', 'CONFIGURING']:
            total_queued += 1
            if username == whoami:
                own_queued += 1
    return (total_running, total_queued, own_running, own_queued)


class SlurmLrms(batch.BatchSystem):

    """
    Job control on SLURM clusters (possibly by connecting via SSH to a
    submit node).
    """

    _batchsys_name = 'SLURM'

    def __init__(self, name,
                 # this are inherited from the base LRMS class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime,
                 auth,  # ignored if `transport` is 'local'
                 # these are inherited from `BatchSystem`
                 frontend, transport,
                 # these are specific to this backend (Note that
                 # optional arguments to the `BatchSystem` class,
                 # e.g.: keyfile=None, accounting_delay=15, are
                 # collected into `extra_args` and should not be
                 # explicitly spelled out in this signature.)
                 **extra_args):

        # init base class
        batch.BatchSystem.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth,
            frontend, transport,
            **extra_args)

        # backend-specific setup
        self.sbatch = self._get_command_argv('sbatch')
        self.srun = self._get_command_argv('srun')

        # SLURM commands
        self._scancel = self._get_command('scancel')
        self._squeue = self._get_command('squeue')
        self._sacct = self._get_command('sacct')

    def _parse_submit_output(self, output):
        return self.get_jobid_from_submit_output(output, _sbatch_jobid_re)

    # submit cmd: sbatch --job-name="jobname" --mem-per-cpu="MBs" --input="filename" --output="filename" --no-requeue -n "number of slots" --cpus-per-task=1 --time="minutes" script.sh  # noqa
    #
    # You can only submit scripts with `sbatch`, attempts to directly
    # run a command fail with this error message:
    #
    #    $ sbatch sleep 180
    #    sbatch: error: This does not look like a batch script.  The first
    # sbatch: error: line must start with #! followed by the path to an interpreter.  # noqa
    # sbatch: error: For instance: #!/bin/sh
    #
    # If the server is not running or unreachable, the following error
    # is displayed:
    #
    #    $ sbatch -n 1 --no-requeue /tmp/sleep.sh
    # sbatch: error: Batch job submission failed: Unable to contact slurm controller (connect failure)  # noqa
    #
    # Note: SLURM has a flexible way of assigning CPUs/cores/etc. to a
    # job; here we treat all execution slots as equal and map 1 task
    # to 1 core/thread.
    #
    # Acceptable time formats include "minutes", "minutes:seconds",
    # "hours:minutes:seconds", "days-hours", "days-hours:minutes" and
    # "days-hours:minutes:seconds".
    #
    def _submit_command(self, app):
        sbatch_argv, app_argv = app.sbatch(self)
        return (sh_quote_safe_cmdline(sbatch_argv),
                sh_quote_unsafe_cmdline(app_argv))

    # stat cmd: squeue --noheader --format='%i^%T^%u^%U^%r^%R'  -j jobid1,jobid2,...  # noqa
    #   %i: job id
    #   %T: Job state, extended form: PENDING, RUNNING, SUSPENDED,
    #       CANCELLED, COMPLETING, COMPLETED, CONFIGURING, FAILED,
    #       TIMEOUT, PREEMPTED, and NODE_FAIL.
    #   %R: For pending jobs: the reason a job is waiting for
    #       execution is printed within parenthesis. For terminated
    #       jobs with failure: an explanation as to why the job failed
    #       is printed within parenthesis.  For all other job states:
    #       the list of allocate nodes.
    #   %r: reason a job is in its current state
    #   %u: username of the submitting user
    #   %U: numeric UID of the submitting user
    #
    def _stat_command(self, job):
        return ("{squeue} --noheader -o GC3Pie^%%i^%%T^%%r -j {jobid}"
                .format(squeue=self._squeue, jobid=job.lrms_jobid))

    def _parse_stat_output(self, stdout, stderr):
        """
        Parse output of ``squeue --noheader -o %i:%T:%r``.
        """
        state = Run.State.UNKNOWN
        for line in stdout.split('\n'):
            line = line.strip()
            # sites might wrap basic SLURM commands like `squeue` or
            # `sacct` to provide additional information to users; we
            # need to tell the actual SLURM output from the sites' own
            # info; fortunately, SLURM's `--format` option allows
            # arbitrary string prefixes which we can leverage to tag
            # the interesting output lines.
            if line.startswith('GC3Pie^'):
                # parse stdout
                _, job_id, job_state_code, reason = stdout.split('^')
                log.debug(
                    "translating SLURM state `%s` to gc3libs.Run.State",
                    job_state_code)
                if job_state_code in ['PENDING', 'CONFIGURING']:
                    # XXX: see comments in `count_jobs` for a discussion
                    # of whether 'CONFIGURING' should be grouped with
                    # 'RUNNING' or not; here it's likely the correct
                    # choice to group it with 'PENDING' as the
                    # "configuring" phase may last a few minutes during
                    # which the job is not yet really running.
                    state = Run.State.SUBMITTED
                elif job_state_code in ['RUNNING', 'COMPLETING']:
                    state = Run.State.RUNNING
                elif job_state_code in ['SUSPENDED']:
                    state = Run.State.STOPPED
                elif job_state_code in ['COMPLETED', 'CANCELLED', 'FAILED',
                                        'NODE_FAIL', 'PREEMPTED', 'TIMEOUT']:
                    state = Run.State.TERMINATING
                else:
                    state = Run.State.UNKNOWN
                break
        else:
            # No `GC3pie:` line found in output:
            #
            # * If stdout is empty and `squeue -j` exitcode is 0, then
            # the job has recently completed (but we still need to
            # call `sacct` to reap the termination status).
            #
            # * If the job has been removed from the controllers'
            # memory, then `squeue -j` exits with code 1.
            state = Run.State.TERMINATING
        return self._stat_result(state, None)  # no term status info

    # acct cmd: sacct --noheader --parsable --format jobid,ncpus,cputimeraw,elapsed,submit,eligible,reserved,start,end,exitcode,maxrss,maxvmsize,totalcpu -j JOBID  # noqa
    #
    # where:
    #   * jobid:      Job ID
    #   * alloccpus:  Count of allocated processors
    #   * cputimeraw: CPU time in seconds
    #   * elapsed:    Job duration, as [DD-][hh:]mm:ss
    #   * submit:     Time the job was submitted, as MM/DD-hh:mm:ss
    #                 (timestamp in UTC) or ISO8601 format depending on
    #                 compilation option
    #   * eligible:   When the job became eligible to run
    #   * reserved:   Difference between `start` and `eligible`
    #   * start:      Job start time, as MM/DD-hh:mm:ss (timestamp in UTC) or
    #                 ISO8601 format depending on compilation option
    #   * end:        Termination time of the job, as MM/DD-hh:mm:ss
    #                 (timestamp in UTC) or ISO8601 format depending on
    #                 compilation option
    #   * exitcode:   exit code, ':', killing signal number
    #   * maxrss:     The maximum RSS across all tasks
    #   * maxvmsize:  The maximum virtual memory used across all tasks
    #   * totalcpu: Total CPU time used by the job (does not include
    #               child processes, if any)
    #
    # Examples:
    #
    # $ sudo sacct --noheader --parsable --format jobid,ncpus,cputimeraw,elapsed,submit,eligible,reserved,start,end,exitcode,maxrss,maxvmsize,totalcpu -j 14  # noqa
    # 14|1|10|00:00:10|2012-09-23T23:38:41|2012-09-23T23:38:41|49710-06:28:06|2012-09-23T23:38:31|2012-09-23T23:38:41|0:0|||00:00:00|  # noqa
    #
    # $ sudo sacct --noheader --parsable --format jobid,ncpus,cputimeraw,elapsed,submit,start,end,exitcode,maxrss,maxvmsize,totalcpu -j 19  # noqa
    # 19|1|66|00:01:06|2012-09-24T10:48:34|2012-09-24T10:47:28|2012-09-24T10:48:34|0:0|||00:00:00|      # noqa
    # 19.0|1|65|00:01:05|2012-09-24T10:47:29|2012-09-24T10:47:29|2012-09-24T10:48:34|0:0|0|0|00:00:00|  # noqa
    #
    # $ env SLURM_TIME_FORMAT=standard sacct -S 0901 --noheader --parsable --format jobid,ncpus,cputimeraw,elapsed,submit,eligible,reserved,start,end,exitcode,maxrss,maxvmsize,totalcpu  # noqa
    # 449002|32|128|00:00:04|2012-09-04T11:09:26|2012-09-04T11:09:38|2012-09-04T11:09:42|0:0|||00:00:00|                  # noqa
    # 449018|64|1472|00:00:23|2012-09-04T11:18:06|2012-09-04T11:18:24|2012-09-04T11:18:47|0:0|||00:01.452|                # noqa
    # 449018.batch|1|23|00:00:23|2012-09-04T11:18:24|2012-09-04T11:18:24|2012-09-04T11:18:47|0:0|7884K|49184K|00:01.452|  # noqa
    # 449057|3200|659200|00:03:26|2012-09-04T11:19:45|2012-09-04T11:19:57|2012-09-04T11:23:23|0:0|||00:01.620|            # noqa
    # 449057.batch|1|206|00:03:26|2012-09-04T11:19:57|2012-09-04T11:19:57|2012-09-04T11:23:23|0:0|7896K|49184K|00:01.620| # noqa
    #
    # Warning: the `SLURM_TIME_FORMAT` environment variable influences
    # how the times are reported, so it should always be set to
    # `standard` to get ISO8601 reporting.  See below for an example
    # of non-standard (SLURM_TIME_FORMAT=relative) report:
    #
    # $ sacct --noheader --parsable --format jobid,ncpus,cputimeraw,elapsed,submit,eligible,reserved,start,end,exitcode,maxrss,maxvmsize,totalcpu -S 0901  # noqa
    # 449002|32|128|00:00:04|4 Sep 11:09|4 Sep 11:09|00:00:12|4 Sep 11:09|4 Sep 11:09|0:0|||00:00:00|                    # noqa
    # 449018|64|1472|00:00:23|4 Sep 11:18|4 Sep 11:18|00:00:18|4 Sep 11:18|4 Sep 11:18|0:0|||00:01.452|                  # noqa
    # 449018.batch|1|23|00:00:23|4 Sep 11:18|4 Sep 11:18|00:00:-02|4 Sep 11:18|4 Sep 11:18|0:0|7884K|49184K|00:01.452|   # noqa
    # 449057|3200|659200|00:03:26|4 Sep 11:19|4 Sep 11:19|00:00:12|4 Sep 11:19|4 Sep 11:23|0:0|||00:01.620|              # noqa
    # 449057.batch|1|206|00:03:26|4 Sep 11:19|4 Sep 11:19|00:00:-02|4 Sep 11:19|4 Sep 11:23|0:0|7896K|49184K|00:01.620|  # noqa
    # 449217|3200|0|00:00:00|4 Sep 11:42|4 Sep 11:42|00:00:05|4 Sep 11:43|4 Sep 11:43|0:0|||00:00.720|                   # noqa
    #
    # If SLURM accounting is disabled (default), then `sacct` outputs
    # an error message:
    #
    #    $ sacct 2
    #    SLURM accounting storage is disabled
    #
    def _acct_command(self, job):
        return ('env SLURM_TIME_FORMAT=standard %s --noheader --parsable'
                ' --format jobid,exitcode,state,ncpus,elapsed,totalcpu,'
                'submit,start,end,maxrss,maxvmsize -j %s' %
                (self._sacct, job.lrms_jobid))

    def _parse_acct_output(self, stdout, stderr):
        acct = {
            'cores':            0,
            'duration':         Duration(0, unit=seconds),
            'used_cpu_time':    Duration(0, unit=seconds),
            'max_used_memory':  Memory(0, unit=bytes)
        }
        exitcode = None
        signal = None
        for line in stdout.split('\n'):
            line = line.strip()
            if line == '':
                continue
            # because of the trailing `|` we have an extra empty field
            jobid, exit, state, ncpus, elapsed, totalcpu, submit,\
                start, end, maxrss, maxvmsize, _ = line.split('|')

            # In some case the state can contain a specification,
            # e.g. "CANCELLED by 1000"
            state = state.split()[0]

            # SLURM job IDs have the form `jobID[.step]`: only the
            # lines with the `step` part carry resource usage records,
            # whereas the total `jobID` line carries the exit codes
            # and overall duration/timing information.
            if '.' not in jobid:
                if state not in [
                        'BOOT_FAIL',
                        'CANCELLED',
                        'COMPLETED',
                        'FAILED',
                        'NODE_FAIL',
                        'PREEMPTED',
                        'TIMEOUT',
                ]:
                    raise gc3libs.exceptions.UnexpectedJobState(
                        "Unexpected SLURM job state '{state}'"
                        " encountered in parsing `sacct` output"
                        .format(state=state)
                    )
                # master job record
                acct['duration'] = SlurmLrms._parse_duration(elapsed)
                acct['used_cpu_time'] = SlurmLrms._parse_duration(totalcpu)
                if state in ['CANCELLED', 'TIMEOUT']:
                    # In this case, the exit code of the master job is
                    # `0:0` or `0:1`, but we want to keep track of the
                    # fact that the job was killed by the system (or
                    # the user).
                    exitcode = os.EX_TEMPFAIL
                    signal = int(Run.Signals.RemoteKill)
                elif state == 'NODE_FAIL':
                    exitcode = os.EX_TEMPFAIL
                    signal = int(Run.Signals.RemoteError)
                else:
                    # compute POSIX exit status
                    exitcode_, signal_ = exit.split(':')
                    exitcode = int(exitcode_)
                    signal = int(signal_)
                # XXX: the master job record seems to report the
                # *requested* slots, whereas the step records report
                # the actual usage.  In our case these should be the
                # same, as the job script only runs one single step.
                # However, in the general case computing the *actual*
                # CPU usage is a mess, as we would have to check which
                # steps were executed simultaneously and which ones
                # were executed one after the other...
                acct['cores'] = int(ncpus)
                # provide starting point for resource usage records
                acct['max_used_memory'] = Memory(0, unit=MB)
                acct['slurm_max_used_ram'] = Memory(0, unit=MB)
                # XXX: apparently, Ubuntu's SLURM 2.3 has a bug
                # wherein `submit` == `end` in the master job record,
                # and the actual start time must be gathered from the
                # step records... try to work around
                submit = SlurmLrms._parse_timestamp(submit)
                start = SlurmLrms._parse_timestamp(start)
                end = SlurmLrms._parse_timestamp(end)
                acct['slurm_submission_time'] = min(submit, start)
                acct['slurm_start_time'] = end  # actually computed below
                acct['slurm_completion_time'] = max(submit, start, end)
            else:
                # common resource usage records (see Issue 78)
                vmem = SlurmLrms._parse_memspec(maxvmsize)
                if vmem is not None:
                    acct['max_used_memory'] = max(vmem, acct['max_used_memory'])
                # SLURM-specific resource usage records
                mem = SlurmLrms._parse_memspec(maxrss)
                if mem is not None:
                    acct['slurm_max_used_ram'] = max(mem, acct['slurm_max_used_ram'])
                # XXX: see above for timestamps
                submit = SlurmLrms._parse_timestamp(submit)
                start = SlurmLrms._parse_timestamp(start)
                acct['slurm_submission_time'] = min(
                    submit, acct['slurm_submission_time'])
                acct['slurm_start_time'] = min(start, acct['slurm_start_time'])
        # must compute termination status since it's not provided by `squeue`
        if signal is not None and exitcode is not None:
            acct['termstatus'] = (signal & 0x7f) + ((exitcode & 0xff) << 8)
        return acct

    @staticmethod
    def _parse_duration(d):
        """
        Parse a SLURM duration expression, in the form ``DD-HH:MM:SS.UUU``.

        The ``DD``, ``HH`` and ``.UUU`` parts are optional.
        """
        total = Duration(0, unit=seconds)
        if '-' in d:
            # DD-HH:MM:SS
            ndays, d = d.split('-')
            total = Duration(int(ndays), unit=days)
        parts = list(reversed(d.split(':')))
        assert len(parts) > 0
        secs = parts[0]
        if '.' in secs:
            # SS.UUU
            total += Duration(float(secs), unit=seconds)
        else:
            total += Duration(int(secs), unit=seconds)
        if len(parts) > 1:
            total += Duration(int(parts[1]), unit=minutes)
        if len(parts) > 2:
            total += Duration(int(parts[2]), unit=hours)
        return total

    @staticmethod
    def _parse_memspec(m):
        # upon NODE_FAIL (and possibly some other occasions), SLURM does not
        # report usage information -- return ``None`` in this case
        if not m:
            return None
        unit = m[-1]
        if unit == 'G':
            return Memory(int(round(float(m[:-1]))), unit=GB)
        elif unit == 'M':
            return Memory(int(round(float(m[:-1]))), unit=MB)
        elif unit == 'K':  # XXX: not sure which one is used
            return Memory(int(round(float(m[:-1]))), unit=kB)
        else:
            # XXX: what does SLURM use as a default?
            return Memory(int(round(float(m))), unit=bytes)

    @staticmethod
    def _parse_timestamp(ts):
        """
        Parse a SLURM timestamp.

        The 'standard' format for SLURM timestamps is ISO8601;
        raise an error if any other format is detected.
        """
        try:
            return datetime.datetime.strptime(ts, SlurmLrms._TIMEFMT_ISO8601)
        except ValueError as err:
            gc3libs.log.error(
                "Could not parse '%s' as an SLURM 'standard' (ISO8601)"
                " timestamp: %s: %s Please set the environment variable"
                " 'SLURM_TIME_FORMAT' to 'standard' on the SLURM frontend"
                " computer.", ts, err.__class__.__name__, err)
            # XXX: this results in an invalid timestamp...
            return None

    _TIMEFMT_ISO8601 = '%Y-%m-%dT%H:%M:%S'
    """
    A strptime() format string for parsing ISO8601 timestamps.
    """

    # kill cmd: scancel
    #
    # Just call it with the space-separated list of job IDs.
    #
    # - on successful cancellation, `scancel` emits no output::
    #
    #    $ scancel 2
    #
    # - for non-existing job IDs, `scancel` outputs an error message:
    #
    #    $ scancel 15
    #    scancel: error: Kill job error on job id 15: Invalid job id specified
    #
    def _cancel_command(self, jobid):
        return ("%s %s" % (self._scancel, jobid))

    @same_docstring_as(LRMS.get_resource_status)
    @LRMS.authenticated
    def get_resource_status(self):
        self.updated = False
        try:
            self.transport.connect()

            _command = ("%s --noheader -o '%%i^%%T^%%u^%%U^%%r^%%R'" %
                        self._squeue)
            log.debug("Running `%s`...", _command)
            exitcode, stdout, stderr = self.transport.execute_command(_command)
            if exitcode != 0:
                # cannot continue
                raise gc3libs.exceptions.LRMSError(
                    "SLURM backend failed executing '%s':"
                    " exit code: %d; stdout: '%s', stderr: '%s'"
                    % (_command, exitcode, stdout, stderr))

            log.debug("Computing updated values for total/available slots ...")
            (total_running, self.queued, self.user_run, self.user_queued) \
                = count_jobs(stdout, self._username)
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
    doctest.testmod(name="slurm",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
