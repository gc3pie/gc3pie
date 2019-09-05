#! /usr/bin/env python
#
"""
Test interaction with the SLURM batch-queueing system.
"""
# Copyright (C) 2011-2013, 2015, 2016, 2019  University of Zurich. All rights reserved.
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
from builtins import object
__docformat__ = 'reStructuredText'

import datetime
import os
import tempfile

import mock
import pytest

import gc3libs
import gc3libs.core
import gc3libs.config
from gc3libs.exceptions import LRMSError
from gc3libs.quantity import kB, seconds, minutes

from faketransport import FakeTransport


def sbatch_submit_ok(jobid=123):
    return (
        # command exit code
        0,
        # stdout
        ("Submitted batch job %d\n" % jobid),
        # stderr
        '')

def sbatch_submit_failed(jobid=123):
    return (
        # command exit code
        1,
        # stdout
        '',
        # stderr
        'sbatch: error: Batch job submission failed:'
        ' Invalid account or account/partition combination specified')

def sacct_no_accounting(jobid=123):
    return (
        # command exitcode
        1,
        # stdout
        '',
        # stderr
        'SLURM accounting storage is disabled\n')


def squeue_pending(jobid=123):
    # squeue --noheader --format='GC3Pie^%i^%T^%r' -j $jobid
    return (
        # command exitcode
        0,
        # stdout
        ('GC3Pie^%d^PENDING^Resources' % jobid),
        # stderr
        '')


def squeue_running(jobid=123):
    # squeue --noheader --format='GC3Pie^%i^%T^%r' -j $jobid
    return (
        # command exitcode
        0,
        # stdout
        ('GC3Pie^%d^RUNNING^None' % jobid),
        # stderr
        '')


def squeue_recently_completed(jobid=123):
    # squeue --noheader --format='GC3Pie^%i^%T^%r' -j $jobid
    return (
        # command exitcode
        0,
        # stdout
        '',
        # stderr
        '')


def squeue_pending_with_additions(jobid=123):
    # squeue --noheader --format='GC3Pie^%i^%T^%r' -j $jobid
    return (
        # command exitcode
        0,
        # stdout
        ('Site-specific preamble here.\nGC3Pie^%d^PENDING^Resources\n\nAdditional info.' % jobid),
        # stderr
        '')


def squeue_running_with_additions(jobid=123):
    # squeue --noheader --format='GC3Pie^%i^%T^%r' -j $jobid
    return (
        # command exitcode
        0,
        # stdout
        ('Site-specific preamble here.\nGC3Pie^%d^RUNNING^None\n\nAdditional info.' % jobid),
        # stderr
        '')


def squeue_recently_completed_with_additions(jobid=123):
    # squeue --noheader --format='GC3Pie^%i^%T^%r' -j $jobid
    return (
        # command exitcode
        0,
        # stdout
        'Site-specific output here.',
        # stderr
        '')


def squeue_notfound(jobid=123):
    # squeue --noheader --format='GC3Pie^%i^%T^%r' -j $jobid
    return (
        # command exitcode
        1,
        # stdout
        '',
        # stderr
        'slurm_load_jobs error: Invalid job id specified')


def scancel_success(jobid=123):
    return (0, "", "")


def scancel_notfound(jobid=123):
    return (
        # command exitcode
        1,
        # stdout
        '',
        # stderr
        ('scancel: error: Kill job error on job id %d: '
         'Invalid job id specified' % jobid))


def scancel_permission_denied(jobid=123):
    return (
        # command exitcode (yes, it's really 0!)
        0,
        # stdout
        "",
        # stderr
        "scancel: error: Kill job error on job id %d: "
        "Access/permission denied\n" % jobid)


def sacct_notfound(jobid=123):
    # SLURM 2.6.5 and 14.11.8 on Ubuntu 14.04.2
    return (
        # command exitcode (yes, it's really 0!)
        0,
        # stdout
        "",
        # stderr
        "")


def sacct_done_ok(jobid=123):
    # SLURM 2.6.5 on Ubuntu 14.04.2
    return (
        # command exitcode (yes, it's really 0!)
        0,
        # stdout
        """
{jobid}|0:0|COMPLETED|1|00:08:07|05:05.002|2016-02-16T12:16:33|2016-02-16T14:24:46|2016-02-16T14:32:53|||
{jobid}.batch|0:0|COMPLETED|1|00:08:07|05:05.002|2016-02-16T14:24:46|2016-02-16T14:24:46|2016-02-16T14:32:53|1612088K|7889776K|
        """.strip().format(jobid=jobid),
        # stderr
        "")


def sacct_done_bad_timestamps(jobid=123):
    #    $ sudo sacct --noheader --parsable --format jobid,ncpus,cputimeraw,\
    #            elapsed,submit,start,end,exitcode,maxrss,maxvmsize,totalcpu \
    #            -j 19
    return (
        # command exitcode (yes, it's really 0!)
        0,
        # stdout
        """
{jobid}|0:0|COMPLETED|1|00:01:06|00:00:00|2012-09-24T10:48:34|2012-09-24T10:47:28|2012-09-24T10:48:34|||
{jobid}.0|0:0|COMPLETED|1|00:01:05|00:00:00|2012-09-24T10:47:29|2012-09-24T10:47:29|2012-09-24T10:48:34|0|0|
        """.strip().format(jobid=jobid),
        # stderr
        "")


def sacct_done_parallel(jobid=123):
    # sample gotten from J.-G. Piccinali, CSCS
    return (
        # command exitcode
        0,
        # stdout
        """
{jobid}|0:0|COMPLETED|64|00:00:23|00:01.452|2012-09-04T11:18:06|2012-09-04T11:18:24|2012-09-04T11:18:47|||
{jobid}.batch|0:0|COMPLETED|1|00:00:23|00:01.452|2012-09-04T11:18:24|2012-09-04T11:18:24|2012-09-04T11:18:47|7884K|49184K|
        """.strip().format(jobid=jobid),
        # stderr
        "")

def sacct_done_cancelled(jobid=123):
    return (
        # command exitcode
        0,
        # stdout
        """
{jobid}|0:0|CANCELLED by 1000|4|00:00:05|00:00:00|2014-12-11T17:13:39|2014-12-11T17:13:39|2014-12-11T17:13:44|||
{jobid}.batch|0:15|CANCELLED|1|00:00:05|00:00:00|2014-12-11T17:13:39|2014-12-11T17:13:39|2014-12-11T17:13:44|0|0|
        """.strip().format(jobid=jobid),
        # stderr
        "")

def sacct_done_node_fail(jobid=123):
    return (
        # command exitcode
        0,
        # stdout
        '''
{jobid}|1:0|NODE_FAIL|8|01:21:47|00:00:00|2016-09-23T18:44:37|2016-09-23T18:44:37|2016-09-23T20:06:24|||
{jobid}.0|-2:0|CANCELLED by 1000|8|01:21:47|00:00:00|2016-09-23T18:44:37|2016-09-23T18:44:37|2016-09-23T20:06:24|||
        '''.strip().format(jobid=jobid),
        # stderr
        '')

def sacct_done_fail_early(jobid=123):
    return (
        # command exitcode
        0,
        # stdout
        '''
{jobid}|1:0|FAILED|14|00:00:01|00:00:00|2016-10-14T16:14:49|2016-10-14T16:14:49|2016-10-14T16:14:50|||
{jobid}.batch|1:0|FAILED|14|00:00:01|00:00:00|2016-10-14T16:14:49|2016-10-14T16:14:49|2016-10-14T16:14:50|||
        '''.strip().format(jobid=jobid),
        # stderr
        '')

def sacct_done_timeout(jobid=123):
    return (
        # command exitcode
        0,
        # stdout
        """
{jobid}|0:1|TIMEOUT|4|00:01:11|00:00:00|2014-12-11T17:10:23|2014-12-11T17:10:23|2014-12-11T17:11:34|||
{jobid}.batch|0:15|CANCELLED|1|00:01:11|00:00:00|2014-12-11T17:10:23|2014-12-11T17:10:23|2014-12-11T17:11:34|0|0|
        """.strip().format(jobid=jobid),
        # stderr
        "")

def sacct_done_relative_timestamps(jobid=123):
    # sample gotten from J.-G. Piccinali, CSCS using SLURM_TIME_FORMAT=relative
    return (
        # command exitcode
        0,
        # stdout
        """
{jobid}|0:0|COMPLETED|64|00:00:23|00:01.452|4 Sep 11:18|4 Sep 11:18|4 Sep 11:18|||
{jobid}.batch|0:0|COMPLETED|1|00:00:23|00:01.452|4 Sep 11:18|4 Sep 11:18|\
4 Sep 11:18|7884K|49184K|
        """.strip().format(jobid=jobid),
        # stderr
        "")


def sacct_done_fractional_rusage(jobid=123):
    # sample gotten from Denisa Rodila, University of Geneva
    return (
        # command exitcode
        0,
        # stdout
        """
{jobid}|0:0|COMPLETED|16|00:07:29|58:10.420|2013-08-30T23:16:22|2013-08-30T23:16:22|2013-08-30T23:23:51|||
{jobid}.batch|0:0|COMPLETED|1|00:07:29|00:02.713|2013-08-30T23:16:22|2013-08-30T23:16:22|2013-08-30T23:23:51|62088K|4115516K|
{jobid}.0|0:0|COMPLETED|1|00:06:56|05:44.992|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:26|73784K|401040K|
{jobid}.1|0:0|COMPLETED|1|00:07:01|05:44.968|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:31|74360K|401656K|
{jobid}.2|0:0|COMPLETED|1|00:07:13|05:51.685|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:43|74360K|401720K|
{jobid}.3|0:0|COMPLETED|1|00:07:21|06:01.088|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:51|73644K|401656K|
{jobid}.4|0:0|COMPLETED|1|00:07:16|05:52.315|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:46|69092K|401096K|
{jobid}.5|0:0|COMPLETED|1|00:07:01|05:46.964|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:31|74364K|401104K|
{jobid}.6|0:0|COMPLETED|1|00:07:10|05:46.222|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:40|69148K|401108K|
{jobid}.7|0:0|COMPLETED|1|00:07:10|05:49.074|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:40|74364K|401592K|
{jobid}.8|0:0|COMPLETED|1|00:07:06|05:44.432|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:36|74404K|401688K|
{jobid}.9|0:0|COMPLETED|1|00:07:04|05:45.962|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:34|72.50M|401652K|
        """.strip().format(jobid=jobid),
        # stderr
        "")


def sacct_almost_done(jobid=123):
    """All steps completed, but main job still reported as RUNNING."""
    # SLURM 2.6.5 running on Ubuntu 14.04
    return (
        # command exitcode
        0,
        # stdout
        """
{jobid}|0:0|RUNNING|16|00:00:08|00:00.168|2016-03-31T09:33:25|2016-03-31T09:33:25|Unknown|||
{jobid}.batch|0:0|COMPLETED|1|00:00:08|00:00.168|2016-03-31T09:33:25|2016-03-31T09:33:25|2016-03-31T09:33:33|22212K|67032K|
        """.strip().format(jobid=jobid),
        # stderr
        "")


def sacct_done_ok2(jobid=123):
    """All steps and job allocation in state COMPLETED."""
    # SLURM 2.6.5 running on Ubuntu 14.04
    return (
        # command exitcode
        0,
        # stdout
        """
{jobid}|0:0|COMPLETED|16|00:00:08|00:00.168|2016-03-31T09:33:25|2016-03-31T09:33:25|2016-03-31T09:33:33|||
{jobid}.batch|0:0|COMPLETED|1|00:00:08|00:00.168|2016-03-31T09:33:25|2016-03-31T09:33:25|2016-03-31T09:33:33|22212K|67032K|
        """.strip().format(jobid=jobid),
        # stderr
        "")


State = gc3libs.Run.State


class FakeApp(gc3libs.Application):

    def __init__(self):
        gc3libs.Application.__init__(
            self,
            arguments=['/bin/hostname'],               # mandatory
            inputs=[],                  # mandatory
            outputs=[],                 # mandatory
            output_dir="./fakedir",    # mandatory
            stdout="stdout.txt",
            stderr="stderr.txt",
            requested_cores=1,)


class TestBackendSlurm(object):

    CONF = """
[resource/example]
type=slurm
auth=ssh
transport=ssh
frontend=example.org
max_cores_per_job=128
max_memory_per_core=2
max_walltime=2
max_cores=80
architecture=x86_64
enabled=True
accounting_delay = 5

[auth/ssh]
type=ssh
username=NONEXISTENT
"""

    @pytest.fixture(autouse=True)
    def setUp(self):
        (fd, self.tmpfile) = tempfile.mkstemp()
        f = os.fdopen(fd, 'w+')
        f.write(TestBackendSlurm.CONF)
        f.close()

        self.cfg = gc3libs.config.Configuration()
        self.cfg.merge_file(self.tmpfile)

        self.core = gc3libs.core.Core(self.cfg)

        self.backend = self.core.get_backend('example')
        self.backend.transport = FakeTransport()
        self.transport = self.backend.transport
        # basic responses
        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['scancel'] = scancel_notfound()

        yield

        os.remove(self.tmpfile)

    def test_sbatch_submit_ok(self):
        """Test `squeue` output parsing with a job in PENDING state."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)
        assert app.execution.state == State.SUBMITTED

    def test_sbatch_submit_failed(self):
        """Test `squeue` output parsing with a job in PENDING state."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_failed()
        with pytest.raises(LRMSError):
            self.core.submit(app)
        #assert_equal(app.execution.state, State.NEW)

    def test_parse_squeue_output_pending(self):
        """Test `squeue` output parsing with a job in PENDING state."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)

        self.transport.expected_answer['squeue'] = squeue_pending()
        self.core.update_job_state(app)
        assert app.execution.state == State.SUBMITTED

    def test_parse_squeue_output_with_additions_pending(self):
        """Test `squeue` output parsing with a job in PENDING state."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)

        self.transport.expected_answer['squeue'] = squeue_pending_with_additions()
        self.core.update_job_state(app)
        assert app.execution.state == State.SUBMITTED

    def test_parse_squeue_output_pending_then_running(self):
        """Test `squeue` output parsing with a job in PENDING and then RUNNING state."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)

        self.transport.expected_answer['squeue'] = squeue_pending()
        self.core.update_job_state(app)
        assert app.execution.state == State.SUBMITTED

        self.transport.expected_answer['squeue'] = squeue_running()
        self.core.update_job_state(app)
        assert app.execution.state == State.RUNNING

    def test_parse_squeue_output_with_additions_pending_then_running(self):
        """Test `squeue` output parsing with a job in PENDING and then RUNNING state."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)

        self.transport.expected_answer['squeue'] = squeue_pending_with_additions()
        self.core.update_job_state(app)
        assert app.execution.state == State.SUBMITTED

        self.transport.expected_answer['squeue'] = squeue_running_with_additions()
        self.core.update_job_state(app)
        assert app.execution.state == State.RUNNING

    def test_parse_squeue_output_immediately_running(self):
        """Test `squeue` output parsing with a job that turns immediately to RUNNING state."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)

        self.transport.expected_answer['squeue'] = squeue_running()
        self.core.update_job_state(app)
        assert app.execution.state == State.RUNNING

    def test_parse_squeue_output_with_additions_immediately_running(self):
        """Test `squeue` output parsing with a job that turns immediately to RUNNING state."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)

        self.transport.expected_answer['squeue'] = squeue_running_with_additions()
        self.core.update_job_state(app)
        assert app.execution.state == State.RUNNING

    def test_job_termination1(self):
        """Test that job termination status is correctly reaped if `squeue` fails but `sacct` does not."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)

        self.transport.expected_answer['squeue'] = squeue_running()
        self.core.update_job_state(app)

        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_done_ok()
        self.core.update_job_state(app)
        assert app.execution.state == State.TERMINATING

        self._check_parse_sacct_done_ok(app.execution)

    def test_job_termination2(self):
        """Test that job termination status is correctly reaped if neither `squeue` nor `sacct` fails."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)

        self.transport.expected_answer['squeue'] = squeue_running()
        self.core.update_job_state(app)

        self.transport.expected_answer['squeue'] = squeue_recently_completed()
        self.transport.expected_answer['env'] = sacct_done_ok()
        self.core.update_job_state(app)
        assert app.execution.state == State.TERMINATING

        self._check_parse_sacct_done_ok(app.execution)

    def test_job_termination3(self):
        """Test that no state update is performed if both `squeue` and `sacct` fail."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)

        self.transport.expected_answer['squeue'] = squeue_running()
        self.core.update_job_state(app)

        # if the second `sacct` fails, we should not do any update
        self.transport.expected_answer['squeue'] = squeue_recently_completed()
        self.transport.expected_answer['env'] = sacct_notfound()
        self.core.update_job_state(app)
        assert app.execution.state == State.RUNNING

        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_done_ok()
        self.core.update_job_state(app)
        assert app.execution.state == State.TERMINATING

        self._check_parse_sacct_done_ok(app.execution)

    def _check_parse_sacct_done_ok(self, job):
        # common job reporting values (see Issue 78)
        assert job.cores == 1
        assert job.exitcode == 0
        assert job.signal == 0
        assert job.duration == 8*minutes + 7*seconds
        assert job.used_cpu_time == 5*minutes + 5.002*seconds
        assert job.max_used_memory == 7889776 * kB
        # SLURM-specific values
        assert job.slurm_max_used_ram == 1612088 * kB
        assert (job.slurm_submission_time ==
                     datetime.datetime(year=2016,
                                       month=2,
                                       day=16,
                                       hour=12,
                                       minute=16,
                                       second=33))
        assert (job.slurm_start_time ==
                     datetime.datetime(year=2016,
                                       month=2,
                                       day=16,
                                       hour=14,
                                       minute=24,
                                       second=46))
        assert (job.slurm_completion_time ==
                     datetime.datetime(year=2016,
                                       month=2,
                                       day=16,
                                       hour=14,
                                       minute=32,
                                       second=53))

    @mock.patch('gc3libs.backends.batch.time')
    def test_accounting_delay1(self, mock_time):
        """
        Test that no state update is performed if both `squeue` and `sacct` fail repeatedly within the accounting delay.
        """
        mock_time.time.return_value = 0

        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)

        self.transport.expected_answer['squeue'] = squeue_running()
        self.core.update_job_state(app)
        assert app.execution.state == State.RUNNING

        # fail repeatedly within the acct delay, no changes to `app.execution`
        for t in 0, 1, 2:
            mock_time.time.return_value = t
            self.transport.expected_answer['squeue'] = squeue_notfound()
            self.transport.expected_answer['env'] = sacct_notfound()
            self.core.update_job_state(app)
            assert app.execution.state == State.RUNNING
            assert hasattr(app.execution, 'stat_failed_at')
            assert app.execution.stat_failed_at == 0

        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_done_ok()
        self.core.update_job_state(app)
        assert app.execution.state == State.TERMINATING

    @mock.patch('gc3libs.backends.batch.time')
    def test_accounting_delay2(self, mock_time):
        """
        Test that an error is raised if both `squeue` and `sacct` fail repeatedly, exceeding the accounting delay.
        """
        mock_time.time.return_value = 0

        gc3libs.log.setLevel(10)

        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)

        self.transport.expected_answer['squeue'] = squeue_running()
        self.core.update_job_state(app)
        assert app.execution.state == State.RUNNING

        # fail first within the acct delay, no changes to `app.execution`
        mock_time.time.return_value = 0
        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_notfound()
        self.core.update_job_state(app)
        assert app.execution.state == State.RUNNING
        assert hasattr(app.execution, 'stat_failed_at')
        assert app.execution.stat_failed_at == 0

        # fail again, outside the accounting delay
        mock_time.time.return_value = 2000
        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_notfound()
        self.core.update_job_state(app)
        assert app.execution.state == State.UNKNOWN

    @mock.patch('gc3libs.backends.batch.time')
    def test_accounting_delay3(self, mock_time):
        """
        Test that no state update is performed if `squeue` and `sacct` disagree on the job status, within the "accounting delay" limit.
        """
        mock_time.time.return_value = 0

        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)

        self.transport.expected_answer['squeue'] = squeue_running()
        self.core.update_job_state(app)
        assert app.execution.state == State.RUNNING

        # fail repeatedly within the acct delay, no changes to `app.execution`
        for t in 1, 2, 3:
            mock_time.time.return_value = t
            self.transport.expected_answer['squeue'] = squeue_notfound()
            self.transport.expected_answer['env'] = sacct_almost_done()
            self.core.update_job_state(app)
            assert app.execution.state == State.RUNNING
            assert hasattr(app.execution, 'stat_failed_at')
            assert app.execution.stat_failed_at == 1

        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_done_ok2()
        self.core.update_job_state(app)
        assert app.execution.state == State.TERMINATING

    def test_parse_sacct_output_parallel(self):
        """Test `sacct` output with a successful parallel job."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)
        self.transport.expected_answer['squeue'] = squeue_notfound()
        # self.transport.expected_answer['sacct'] = sacct_done_parallel()
        self.transport.expected_answer['env'] = sacct_done_parallel()
        self.core.update_job_state(app)
        assert app.execution.state == State.TERMINATING

        job = app.execution
        # common job reporting values (see Issue 78)
        assert job.cores == 64
        assert job.exitcode == 0
        assert job.signal == 0
        assert job.duration == 23 * seconds
        assert job.max_used_memory == 49184 * kB
        assert job.used_cpu_time == 1.452 * seconds
        # SLURM-specific values
        assert job.slurm_max_used_ram == 7884 * kB
        assert (job.slurm_submission_time ==
                     datetime.datetime(year=2012,
                                       month=9,
                                       day=4,
                                       hour=11,
                                       minute=18,
                                       second=6))
        assert (job.slurm_start_time ==
                     datetime.datetime(year=2012,
                                       month=9,
                                       day=4,
                                       hour=11,
                                       minute=18,
                                       second=24))
        assert (job.slurm_completion_time ==
                     datetime.datetime(year=2012,
                                       month=9,
                                       day=4,
                                       hour=11,
                                       minute=18,
                                       second=47))

    def test_parse_sacct_output_bad_timestamps(self):
        """Test `sacct` output with out-of-order timestamps."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)
        self.transport.expected_answer['squeue'] = squeue_notfound()
        # self.transport.expected_answer['sacct'] = sacct_done_bad_timestamps()
        self.transport.expected_answer['env'] = sacct_done_bad_timestamps()
        self.core.update_job_state(app)
        assert app.execution.state == State.TERMINATING

        job = app.execution
        # common job reporting values (see Issue 78)
        assert job.exitcode == 0
        assert job.signal == 0
        assert job.duration == 66 * seconds
        assert job.max_used_memory == 0 * kB
        assert job.used_cpu_time == 0 * seconds
        # SLURM-specific values
        assert job.slurm_max_used_ram == 0 * kB
        assert (job.slurm_submission_time ==
                     datetime.datetime(year=2012,
                                       month=9,
                                       day=24,
                                       hour=10,
                                       minute=47,
                                       second=28))
        assert (job.slurm_start_time ==
                     datetime.datetime(year=2012,
                                       month=9,
                                       day=24,
                                       hour=10,
                                       minute=47,
                                       second=29))
        assert (job.slurm_completion_time ==
                     datetime.datetime(year=2012,
                                       month=9,
                                       day=24,
                                       hour=10,
                                       minute=48,
                                       second=34))

    def test_parse_sacct_output_fractional_rusage(self):
        """Test `sacct` output with fractional resource usage."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)
        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_done_fractional_rusage()
        self.core.update_job_state(app)
        assert app.execution.state == State.TERMINATING

        job = app.execution
        # common job reporting values (see Issue 78)
        assert job.exitcode == 0
        assert job.signal == 0
        assert job.duration == 7 * minutes + 29 * seconds
        assert job.max_used_memory == 4115516 * kB
        assert job.used_cpu_time == 58 * minutes + 10.420 * seconds
        # SLURM-specific values
        assert job.slurm_max_used_ram == 74404 * kB
        assert (job.slurm_submission_time ==
                     datetime.datetime(year=2013,
                                       month=8,
                                       day=30,
                                       hour=23,
                                       minute=16,
                                       second=22))
        assert (job.slurm_start_time ==
                     datetime.datetime(year=2013,
                                       month=8,
                                       day=30,
                                       hour=23,
                                       minute=16,
                                       second=22))
        assert (job.slurm_completion_time ==
                     datetime.datetime(year=2013,
                                       month=8,
                                       day=30,
                                       hour=23,
                                       minute=23,
                                       second=51))

    def test_parse_sacct_output_timeout(self):
        """Test `sacct` when job reaches its time limit"""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)
        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_done_timeout()
        self.core.update_job_state(app)
        job = app.execution

        assert job.state ==    State.TERMINATING
        assert job.exitcode == os.EX_TEMPFAIL
        assert job.signal ==   int(gc3libs.Run.Signals.RemoteKill)

    def test_parse_sacct_output_node_fail(self):
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)
        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_done_node_fail()
        self.core.update_job_state(app)
        job = app.execution

        assert job.state ==    State.TERMINATING
        assert job.exitcode == os.EX_TEMPFAIL
        assert job.signal ==   int(gc3libs.Run.Signals.RemoteError)

    def test_parse_sacct_output_fail_early(self):
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)
        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_done_fail_early()
        self.core.update_job_state(app)
        job = app.execution

        assert job.state ==    State.TERMINATING
        assert job.exitcode == 1
        assert job.signal ==   0

    def test_parse_sacct_output_job_cancelled(self):
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)
        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_done_cancelled()
        self.core.update_job_state(app)
        job = app.execution

        assert job.state ==    State.TERMINATING
        assert job.exitcode == os.EX_TEMPFAIL
        assert job.signal ==   int(gc3libs.Run.Signals.RemoteKill)

    def test_cancel_job1(self):
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)

        self.transport.expected_answer['scancel'] = scancel_success()
        self.transport.expected_answer['env'] = sacct_done_cancelled()
        self.core.kill(app)
        assert app.execution.state == State.TERMINATED

    def test_cancel_job2(self):
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = sbatch_submit_ok()
        self.core.submit(app)
        assert app.execution.state == State.SUBMITTED

        self.transport.expected_answer['scancel'] = scancel_permission_denied()
        self.core.kill(app)
        assert app.execution.state == State.TERMINATED

    def test_get_command(self):
        assert self.backend.sbatch == ['sbatch']
        assert self.backend._sacct == 'sacct'
        assert self.backend._scancel == 'scancel'
        assert self.backend._squeue == 'squeue'


def test_get_command_alternate():
    (fd, tmpfile) = tempfile.mkstemp()
    f = os.fdopen(fd, 'w+')
    f.write("""
[auth/ssh]
type=ssh
username=NONEXISTENT

[resource/example]
# mandatory stuff
type=slurm
auth=ssh
transport=ssh
frontend=example.org
max_cores_per_job=128
max_memory_per_core=2
max_walltime=2
max_cores=80
architecture=x86_64

# alternate command paths
sbatch  = /usr/local/bin/sbatch --constraint=gpu
sacct   = /usr/local/sbin/sacct
scancel = /usr/local/bin/scancel # comments are ignored!
squeue  = /usr/local/bin/squeue
""")
    f.close()

    cfg = gc3libs.config.Configuration()
    cfg.merge_file(tmpfile)
    os.remove(tmpfile)
    b = cfg.make_resources()['example']

    assert b.sbatch == ['/usr/local/bin/sbatch', '--constraint=gpu']
    assert b._sacct == '/usr/local/sbin/sacct'
    assert b._scancel == '/usr/local/bin/scancel'
    assert b._squeue == '/usr/local/bin/squeue'


def test_count_jobs():
    squeue_stdout = '''474867^PENDING^first_user^21913^Priority^(Priority)
474870^PENDING^first_user^21913^Priority^(Priority)
475753^PENDING^first_user^21913^Resources^(Resources)
475747^PENDING^first_user^21913^Resources^(Resources)
475751^PENDING^first_user^21913^Priority^(Priority)
474723^RUNNING^third_user^20345^None^nid0[0002,0005,0013,0017,0022-0023,\
0028-0029,0040,0046,0050,0058-0059,0061,0066,0068-0069,0081,0091,0100,0110,\
0122-0123,0132-0133,0186-0187,0193,0212-0213,0230-0231,0234-0235,0240,\
0276-0277,0280-0281,0294-0295,0298-0299,0302,0314,0325,0332-0333,0335,0337,\
0341,0344-0345,0352-0353,0358-0359,0370-0371,0386-0387,0429,0444-0445,\
0450-0451,0466-0467,0492,0508,0528-0529,0544-0545,0551,0562-0563,0578-0579,\
0608,0628,0636-0637,0642-0644,0651,0654-0655,0658,0670-0673,0684-0685,\
0698-0699,0709,0735,0744-0745,0771,0776-0777,0780-0781,0802-0803,0818,\
0822-0823,0828,0832-0833,0840,0885,0894,0906-0907,0909,0946,0948,0972-0973,\
1011,1022,1024-1025,1052,1059,1074,1098,1100-1103,1106,1122,1132-1133,1137,\
1139,1141-1143,1164-1165,1169,1198-1199,1202,1232-1233,1254-1255,1262,\
1274-1275,1304,1315,1324-1325,1334-1335,1356-1359,1394-1395,1420-1421,1424,\
1455,1459,1488,1492-1493,1519]
475738^RUNNING^first_user^21913^None^nid00[136-137]
475744^RUNNING^first_user^21913^None^nid00[182-183]
475438^RUNNING^second_user^21239^None^nid00[686,720-721,751]
475440^RUNNING^second_user^21239^None^nid00[363,875,916-917]
475448^RUNNING^second_user^21239^None^nid0[0306-0307,1206-1207]
475450^RUNNING^second_user^21239^None^nid0[1263-1265,1296]
475452^RUNNING^second_user^21239^None^nid0[0656,0687,1070,1105]
475736^RUNNING^first_user^21913^None^nid0[0041,1512]
475651^RUNNING^second_user^21239^None^nid00[026,217,867,924]
475726^RUNNING^first_user^21913^None^nid00[742-743]
'''
    # first user has 4 running jobs and 5 queued ones
    R, Q, r, q = gc3libs.backends.slurm.count_jobs(squeue_stdout, 'first_user')
    assert R == 11
    assert Q == 5
    assert r == 4
    assert q == 5
    # second user has 6 running jobs and no queued ones
    R, Q, r, q = gc3libs.backends.slurm.count_jobs(squeue_stdout,
                                                   'second_user')
    assert R == 11
    assert Q == 5
    assert r == 6
    assert q == 0
    # third user has only 1 running job
    R, Q, r, q = gc3libs.backends.slurm.count_jobs(squeue_stdout, 'third_user')
    assert R == 11
    assert Q == 5
    assert r == 1
    assert q == 0


if __name__ == "__main__":
    pytest.main(["-v", __file__])
