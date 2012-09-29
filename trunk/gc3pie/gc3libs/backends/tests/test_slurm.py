#! /usr/bin/env python
#
"""
Test interaction with the SLURM batch-queueing system.
"""
# Copyright (C) 2011, 2012, GC3, University of Zurich. All rights reserved.
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


from nose.plugins.skip import SkipTest
from nose.tools import assert_equal

import gc3libs
from gc3libs.backends.slurm import count_jobs
import gc3libs.core
import gc3libs.config
from gc3libs.quantity import Memory, KiB, MiB, GiB, Duration, seconds, minutes, hours

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
    # squeue --noheader --format='%i|%T|%r' -j $jobid
    return (
        # command exitcode
        0,
        # stdout
        ('%d:PENDING:Resources' % jobid),
        # stderr
        '')

def squeue_running(jobid=123):
    # squeue --noheader --format='%i|%T|%u|%U|%r|%R' -j $jobid
    return (
        # command exitcode
        0,
        # stdout
        ('%d:RUNNING:None' % jobid),
        # stderr
        '')

def squeue_recently_completed(jobid=123):
    # squeue --noheader --format='%i|%T|%u|%U|%r|%R' -j $jobid
    return (
        # command exitcode
        0,
        # stdout
        '',
        # stderr
        '')

def squeue_notfound(jobid=123):
    # squeue --noheader --format='%i|%T|%u|%U|%r|%R' -j $jobid
    return (
        # command exitcode
        0,
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
        ('scancel: error: Kill job error on job id %d: Invalid job id specified' % jobid))

def scancel_permission_denied(jobid=123):
    return (
        # command exitcode (yes, it's really 0!)
        0,
        # stdout
        "",
        # stderr
        "scancel: error: Kill job error on job id %d: Access/permission denied\n" % jobid)


def sacct_done():
    # FIXME: missing sacct output!
    return (0, '', '')


#import gc3libs.Run.State as State
State = gc3libs.Run.State


class FakeApp(gc3libs.Application):
    def __init__(self):
        gc3libs.Application.__init__(
            self,
            arguments = ['/bin/hostname'],               # mandatory
            inputs = [],                  # mandatory
            outputs = [],                 # mandatory
            output_dir = "./fakedir",    # mandatory
            stdout = "stdout.txt",
            stderr = "stderr.txt",
            requested_cores = 1,)


class TestBackendSlurm(object):

    CONF="""
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
        #self.transport.expected_answer['squeue'] = squeue_notfound()
        #self.transport.expected_answer['sacct'] = sacct_done()
        self.core.update_job_state(app)
        assert_equal(app.execution.state, State.TERMINATING)


    def test_parse_sacct_output(self):
        raise SkipTest("FIXME: sacct output parsing not yet implemented!")
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = correct_submit()
        self.core.submit(app)
        self.transport.expected_answer['sacct'] = sacct_done()
        self.core.update_job_state(app)
        assert_equal(app.execution.state, State.TERMINATING)

        job = app.execution
        # common job reporting values (see Issue 78)
        assert_equal(job.exitcode,        0)
        assert_equal(job.duration,        2*minutes + 5*seconds)
        assert_equal(job.max_used_memory, 190944*KiB)
        assert_equal(job.used_cpu_time,   0*seconds)
        # SLURM-specific values
        assert_equal(job.slurm_queue,       'short')
        assert_equal(job.slurm_jobname,     'DemoSLURMApp')
        assert_equal(job.slurm_max_used_ram, 2364*KiB)
        assert_equal(job.slurm_submission_time,
                     datetime.datetime(year=2012, month=3, day=9, hour=9, minute=31, second=53))
        assert_equal(job.slurm_running_time,
                     datetime.datetime(year=2012, month=3, day=9, hour=9, minute=32, second=3))
        assert_equal(job.slurm_end_time,
                     datetime.datetime(year=2012, month=3, day=9, hour=9, minute=34, second=8))

    def test_cancel_job1(self):
        app = FakeApp()
        self.transport.expected_answer['sbatch'] = correct_submit()
        self.core.submit(app)

        self.transport.expected_answer['scancel'] = scancel_success()
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
        assert_equal(self.backend.sbatch,   ['sbatch'])
        assert_equal(self.backend._sacct,   'sacct')
        assert_equal(self.backend._scancel, 'scancel')
        assert_equal(self.backend._squeue,  'squeue')


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
    b = cfg.make_resources()['example']

    assert_equal(b.sbatch,   ['/usr/local/bin/sbatch', '--constraint=gpu'])
    assert_equal(b._sacct,    '/usr/local/sbin/sacct')
    assert_equal(b._scancel,  '/usr/local/bin/scancel')
    assert_equal(b._squeue,   '/usr/local/bin/squeue')

def test_count_jobs():
    squeue_stdout = '''474867:PENDING:first_user:21913:Priority:(Priority)
474870:PENDING:first_user:21913:Priority:(Priority)
475753:PENDING:first_user:21913:Resources:(Resources)
475747:PENDING:first_user:21913:Resources:(Resources)
475751:PENDING:first_user:21913:Priority:(Priority)
474723:RUNNING:third_user:20345:None:nid0[0002,0005,0013,0017,0022-0023,0028-0029,0040,0046,0050,0058-0059,0061,0066,0068-0069,0081,0091,0100,0110,0122-0123,0132-0133,0186-0187,0193,0212-0213,0230-0231,0234-0235,0240,0276-0277,0280-0281,0294-0295,0298-0299,0302,0314,0325,0332-0333,0335,0337,0341,0344-0345,0352-0353,0358-0359,0370-0371,0386-0387,0429,0444-0445,0450-0451,0466-0467,0492,0508,0528-0529,0544-0545,0551,0562-0563,0578-0579,0608,0628,0636-0637,0642-0644,0651,0654-0655,0658,0670-0673,0684-0685,0698-0699,0709,0735,0744-0745,0771,0776-0777,0780-0781,0802-0803,0818,0822-0823,0828,0832-0833,0840,0885,0894,0906-0907,0909,0946,0948,0972-0973,1011,1022,1024-1025,1052,1059,1074,1098,1100-1103,1106,1122,1132-1133,1137,1139,1141-1143,1164-1165,1169,1198-1199,1202,1232-1233,1254-1255,1262,1274-1275,1304,1315,1324-1325,1334-1335,1356-1359,1394-1395,1420-1421,1424,1455,1459,1488,1492-1493,1519]
475738:RUNNING:first_user:21913:None:nid00[136-137]
475744:RUNNING:first_user:21913:None:nid00[182-183]
475438:RUNNING:second_user:21239:None:nid00[686,720-721,751]
475440:RUNNING:second_user:21239:None:nid00[363,875,916-917]
475448:RUNNING:second_user:21239:None:nid0[0306-0307,1206-1207]
475450:RUNNING:second_user:21239:None:nid0[1263-1265,1296]
475452:RUNNING:second_user:21239:None:nid0[0656,0687,1070,1105]
475736:RUNNING:first_user:21913:None:nid0[0041,1512]
475651:RUNNING:second_user:21239:None:nid00[026,217,867,924]
475726:RUNNING:first_user:21913:None:nid00[742-743]
'''
    # first user has 4 running jobs and 5 queued ones
    R, Q, r, q = gc3libs.backends.slurm.count_jobs(squeue_stdout, 'first_user')
    assert_equal(R, 11)
    assert_equal(Q, 5)
    assert_equal(r, 4)
    assert_equal(q, 5)
    # second user has 6 running jobs and no queued ones
    R, Q, r, q = gc3libs.backends.slurm.count_jobs(squeue_stdout, 'second_user')
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
