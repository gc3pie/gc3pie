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
    # squeue --noheader --format='%i|%T|%u|%U|%r|%R' -j $jobid
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
            executable = '/bin/hostname', # mandatory
            arguments = [],               # mandatory
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


if __name__ == "__main__":
    import nose
    nose.runmodule()
