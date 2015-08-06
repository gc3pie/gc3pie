#! /usr/bin/env python
#
"""
Test interaction with the SLURM batch-queueing system.
"""
# Copyright (C) 2011-2013, 2015 S3IT, Zentrale Informatik, University of Zurich. All rights reserved.
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
__version__ = '$Revision$'

import datetime
import os
import tempfile

from nose.tools import assert_equal

import gc3libs
import gc3libs.core
import gc3libs.config
from gc3libs.quantity import kB, seconds, minutes

from faketransport import FakeTransport


def correct_submit(jobid=123):
    return (
        # command exit code
        0,
        # stdout
        ("Submitted batch job %d\n" % jobid),
        # stderr
        '')


def sacct_no_accounting(jobid=123):
    return (
        # command exitcode
        1,
        # stdout
        '',
        # stderr
        'SLURM accounting storage is disabled\n')


def squeue_pending(jobid=123):
    # squeue --noheader --format='%i^%T^%r' -j $jobid
    return (
        # command exitcode
        0,
        # stdout
        ('%d^PENDING^Resources' % jobid),
        # stderr
        '')


def squeue_running(jobid=123):
    # squeue --noheader --format='%i^%T^%u^%U^%r^%R' -j $jobid
    return (
        # command exitcode
        0,
        # stdout
        ('%d^RUNNING^None' % jobid),
        # stderr
        '')


def squeue_recently_completed(jobid=123):
    # squeue --noheader --format='%i^%T^%u^%U^%r^%R' -j $jobid
    return (
        # command exitcode
        0,
        # stdout
        '',
        # stderr
        '')


def squeue_notfound(jobid=123):
    # squeue --noheader --format='%i^%T^%u^%U^%r^%R' -j $jobid
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


def sacct_done_bad_timestamps(jobid=123):
    #    $ sudo sacct --noheader --parsable --format jobid,ncpus,cputimeraw,\
    #            elapsed,submit,start,end,exitcode,maxrss,maxvmsize,totalcpu \
    #            -j 19
    return (
        # command exitcode (yes, it's really 0!)
        0,
        # stdout
        """
123|0:0|COMPLETED|1|00:01:06|00:00:00|2012-09-24T10:48:34|2012-09-24T10:47:28|2012-09-24T10:48:34|||
123.0|0:0|COMPLETED|1|00:01:05|00:00:00|2012-09-24T10:47:29|2012-09-24T10:47:29|2012-09-24T10:48:34|0|0|
        """.strip(),
        # stderr
        "")


def sacct_done_parallel(jobid=123):
    # sample gotten from J.-G. Piccinali, CSCS
    return (
        # command exitcode
        0,
        # stdout
        """
123|0:0|COMPLETED|64|00:00:23|00:01.452|2012-09-04T11:18:06|2012-09-04T11:18:24|2012-09-04T11:18:47|||
123.batch|0:0|COMPLETED|1|00:00:23|00:01.452|2012-09-04T11:18:24|2012-09-04T11:18:24|2012-09-04T11:18:47|7884K|49184K|
        """.strip(),
        # stderr
        "")

def sacct_done_cancelled(jobid=123):
    return (
        # command exitcode
        0,
        # stdout
        """
123|0:0|CANCELLED by 1000|4|00:00:05|00:00:00|2014-12-11T17:13:39|2014-12-11T17:13:39|2014-12-11T17:13:44|||
123.batch|0:15|CANCELLED|1|00:00:05|00:00:00|2014-12-11T17:13:39|2014-12-11T17:13:39|2014-12-11T17:13:44|0|0|
        """.strip(),
        # stderr
        "")

def sacct_done_timeout(jobid=123):
    return (
        # command exitcode
        0,
        # stdout
        """
123|0:1|TIMEOUT|4|00:01:11|00:00:00|2014-12-11T17:10:23|2014-12-11T17:10:23|2014-12-11T17:11:34|||
123.batch|0:15|CANCELLED|1|00:01:11|00:00:00|2014-12-11T17:10:23|2014-12-11T17:10:23|2014-12-11T17:11:34|0|0|
        """.strip(),
        # stderr
        "")

def sacct_done_relative_timestamps(jobid=123):
    # sample gotten from J.-G. Piccinali, CSCS using SLURM_TIME_FORMAT=relative
    return (
        # command exitcode
        0,
        # stdout
        """
123|0:0|COMPLETED|64|00:00:23|00:01.452|4 Sep 11:18|4 Sep 11:18|4 Sep 11:18|||
123.batch|0:0|COMPLETED|1|00:00:23|00:01.452|4 Sep 11:18|4 Sep 11:18|\
4 Sep 11:18|7884K|49184K|
        """.strip(),
        # stderr
        "")


def sacct_done_fractional_rusage(jobid=123):
    # sample gotten from Denisa Rodila, University of Geneva
    return (
        # command exitcode
        0,
        # stdout
        """
123|0:0|COMPLETED|16|00:07:29|58:10.420|2013-08-30T23:16:22|2013-08-30T23:16:22|2013-08-30T23:23:51|||
123.batch|0:0|COMPLETED|1|00:07:29|00:02.713|2013-08-30T23:16:22|2013-08-30T23:16:22|2013-08-30T23:23:51|62088K|4115516K|
123.0|0:0|COMPLETED|1|00:06:56|05:44.992|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:26|73784K|401040K|
123.1|0:0|COMPLETED|1|00:07:01|05:44.968|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:31|74360K|401656K|
123.2|0:0|COMPLETED|1|00:07:13|05:51.685|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:43|74360K|401720K|
123.3|0:0|COMPLETED|1|00:07:21|06:01.088|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:51|73644K|401656K|
123.4|0:0|COMPLETED|1|00:07:16|05:52.315|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:46|69092K|401096K|
123.5|0:0|COMPLETED|1|00:07:01|05:46.964|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:31|74364K|401104K|
123.6|0:0|COMPLETED|1|00:07:10|05:46.222|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:40|69148K|401108K|
123.7|0:0|COMPLETED|1|00:07:10|05:49.074|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:40|74364K|401592K|
123.8|0:0|COMPLETED|1|00:07:06|05:44.432|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:36|74404K|401688K|
123.9|0:0|COMPLETED|1|00:07:04|05:45.962|2013-08-30T23:16:30|2013-08-30T23:16:30|2013-08-30T23:23:34|72.50M|401652K|
        """.strip(),
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

[auth/ssh]
type=ssh
username=NONEXISTENT
"""

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

    def tearDown(self):
        os.remove(self.tmpfile)

    def test_slurm_basic_workflow(self):
        app = FakeApp()

        # Succesful submission:
        self.transport.expected_answer['sbatch'] = correct_submit()
        self.core.submit(app)
        assert_equal(app.execution.state, State.SUBMITTED)

        # Update state. We would expect the job to be SUBMITTED
        self.transport.expected_answer['squeue'] = squeue_pending()
        self.core.update_job_state(app)
        assert_equal(app.execution.state, State.SUBMITTED)

        # Update state. We would expect the job to be RUNNING
        self.transport.expected_answer['squeue'] = squeue_running()
        self.core.update_job_state(app)
        assert_equal(app.execution.state, State.RUNNING)

        # Job done. qstat doesn't find it, tracejob should.
        self.transport.expected_answer['squeue'] = squeue_recently_completed()
        # XXX: alternatively:
        # self.transport.expected_answer['squeue'] = squeue_notfound()
        # self.transport.expected_answer['sacct'] = sacct_done()
        self.core.update_job_state(app)
        assert_equal(app.execution.state, State.TERMINATING)

    def test_parse_sacct_output_parallel(self):
        """Test `sacct` output with a successful parallel job."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = correct_submit()
        self.core.submit(app)
        self.transport.expected_answer['squeue'] = squeue_notfound()
        # self.transport.expected_answer['sacct'] = sacct_done_parallel()
        self.transport.expected_answer['env'] = sacct_done_parallel()
        self.core.update_job_state(app)
        assert_equal(app.execution.state, State.TERMINATING)

        job = app.execution
        # common job reporting values (see Issue 78)
        assert_equal(job.cores, 64)
        assert_equal(job.exitcode, 0)
        assert_equal(job.signal, 0)
        assert_equal(job.duration, 23 * seconds)
        assert_equal(job.max_used_memory, 49184 * kB)
        assert_equal(job.used_cpu_time, 1.452 * seconds)
        # SLURM-specific values
        assert_equal(job.slurm_max_used_ram, 7884 * kB)
        assert_equal(job.slurm_submission_time,
                     datetime.datetime(year=2012,
                                       month=9,
                                       day=4,
                                       hour=11,
                                       minute=18,
                                       second=6))
        assert_equal(job.slurm_start_time,
                     datetime.datetime(year=2012,
                                       month=9,
                                       day=4,
                                       hour=11,
                                       minute=18,
                                       second=24))
        assert_equal(job.slurm_completion_time,
                     datetime.datetime(year=2012,
                                       month=9,
                                       day=4,
                                       hour=11,
                                       minute=18,
                                       second=47))

    def test_parse_sacct_output_bad_timestamps(self):
        """Test `sacct` output with out-of-order timestamps."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = correct_submit()
        self.core.submit(app)
        self.transport.expected_answer['squeue'] = squeue_notfound()
        # self.transport.expected_answer['sacct'] = sacct_done_bad_timestamps()
        self.transport.expected_answer['env'] = sacct_done_bad_timestamps()
        self.core.update_job_state(app)
        assert_equal(app.execution.state, State.TERMINATING)

        job = app.execution
        # common job reporting values (see Issue 78)
        assert_equal(job.exitcode, 0)
        assert_equal(job.signal, 0)
        assert_equal(job.duration, 66 * seconds)
        assert_equal(job.max_used_memory, 0 * kB)
        assert_equal(job.used_cpu_time, 0 * seconds)
        # SLURM-specific values
        assert_equal(job.slurm_max_used_ram, 0 * kB)
        assert_equal(job.slurm_submission_time,
                     datetime.datetime(year=2012,
                                       month=9,
                                       day=24,
                                       hour=10,
                                       minute=47,
                                       second=28))
        assert_equal(job.slurm_start_time,
                     datetime.datetime(year=2012,
                                       month=9,
                                       day=24,
                                       hour=10,
                                       minute=47,
                                       second=29))
        assert_equal(job.slurm_completion_time,
                     datetime.datetime(year=2012,
                                       month=9,
                                       day=24,
                                       hour=10,
                                       minute=48,
                                       second=34))

    def test_parse_sacct_output_fractional_rusage(self):
        """Test `sacct` output with fractional resource usage."""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = correct_submit()
        self.core.submit(app)
        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_done_fractional_rusage()
        self.core.update_job_state(app)
        assert_equal(app.execution.state, State.TERMINATING)

        job = app.execution
        # common job reporting values (see Issue 78)
        assert_equal(job.exitcode, 0)
        assert_equal(job.signal, 0)
        assert_equal(job.duration, 7 * minutes + 29 * seconds)
        assert_equal(job.max_used_memory, 4115516 * kB)
        assert_equal(job.used_cpu_time, 58 * minutes + 10.420 * seconds)
        # SLURM-specific values
        assert_equal(job.slurm_max_used_ram, 74404 * kB)
        assert_equal(job.slurm_submission_time,
                     datetime.datetime(year=2013,
                                       month=8,
                                       day=30,
                                       hour=23,
                                       minute=16,
                                       second=22))
        assert_equal(job.slurm_start_time,
                     datetime.datetime(year=2013,
                                       month=8,
                                       day=30,
                                       hour=23,
                                       minute=16,
                                       second=22))
        assert_equal(job.slurm_completion_time,
                     datetime.datetime(year=2013,
                                       month=8,
                                       day=30,
                                       hour=23,
                                       minute=23,
                                       second=51))

    def test_parse_sacct_output_timeout(self):
        """Test `sacct` when job reaches its time limit"""
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = correct_submit()
        self.core.submit(app)
        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_done_timeout()
        self.core.update_job_state(app)
        job = app.execution

        assert_equal(job.state,    State.TERMINATING)
        assert_equal(job.exitcode, os.EX_TEMPFAIL)
        assert_equal(job.signal,   int(gc3libs.Run.Signals.RemoteKill))

    def test_parse_sacct_output_job_cancelled(self):
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = correct_submit()
        self.core.submit(app)
        self.transport.expected_answer['squeue'] = squeue_notfound()
        self.transport.expected_answer['env'] = sacct_done_cancelled()
        self.core.update_job_state(app)
        job = app.execution

        assert_equal(job.state,    State.TERMINATING)
        assert_equal(job.exitcode, os.EX_TEMPFAIL)
        assert_equal(job.signal,   int(gc3libs.Run.Signals.RemoteKill))

    def test_cancel_job1(self):
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = correct_submit()
        self.core.submit(app)

        self.transport.expected_answer['scancel'] = scancel_success()
        self.transport.expected_answer['env'] = sacct_done_cancelled()
        self.core.kill(app)
        assert_equal(app.execution.state, State.TERMINATED)

    def test_cancel_job2(self):
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = correct_submit()
        self.core.submit(app)
        assert_equal(app.execution.state, State.SUBMITTED)

        self.transport.expected_answer['scancel'] = scancel_permission_denied()
        self.core.kill(app)
        assert_equal(app.execution.state, State.TERMINATED)

    def test_get_command(self):
        assert_equal(self.backend.sbatch, ['sbatch'])
        assert_equal(self.backend._sacct, 'sacct')
        assert_equal(self.backend._scancel, 'scancel')
        assert_equal(self.backend._squeue, 'squeue')


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

    assert_equal(b.sbatch, ['/usr/local/bin/sbatch', '--constraint=gpu'])
    assert_equal(b._sacct, '/usr/local/sbin/sacct')
    assert_equal(b._scancel, '/usr/local/bin/scancel')
    assert_equal(b._squeue, '/usr/local/bin/squeue')


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
    assert_equal(R, 11)
    assert_equal(Q, 5)
    assert_equal(r, 4)
    assert_equal(q, 5)
    # second user has 6 running jobs and no queued ones
    R, Q, r, q = gc3libs.backends.slurm.count_jobs(squeue_stdout,
                                                   'second_user')
    assert_equal(R, 11)
    assert_equal(Q, 5)
    assert_equal(r, 6)
    assert_equal(q, 0)
    # third user has only 1 running job
    R, Q, r, q = gc3libs.backends.slurm.count_jobs(squeue_stdout, 'third_user')
    assert_equal(R, 11)
    assert_equal(Q, 5)
    assert_equal(r, 1)
    assert_equal(q, 0)


if __name__ == "__main__":
    import nose
    nose.runmodule()
