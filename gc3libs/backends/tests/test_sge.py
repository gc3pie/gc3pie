#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, 2012,  University of Zurich. All rights reserved.
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
import shutil
import tempfile

import pytest

import gc3libs
import gc3libs.core
import gc3libs.config
from gc3libs.quantity import MB, seconds

State = gc3libs.Run.State

from faketransport import FakeTransport

files_to_remove = []


def correct_submit(jobid=123):
    out = """Your job %s ("DemoSGEApp") has been submitted
""" % jobid
    return (0, out, "")


def correct_qstat_queued(jobid=123):
    out = """  %s 0.00000 DemoSGEApp antonio      qw    03/15/2012 08:53:34 \
                                   1
""" % jobid
    return (0, out, "")


def correct_qstat_running(jobid=123):
    out = """  %s 0.55500 DemoSGEApp antonio      r     03/15/2012 08:53:42 \
all.q@compute-0-2.local            1
""" % jobid
    return (0, out, "")


def qacct_notfound(jobid=123):
    err = """error: job id %s not found
""" % jobid
    return (1, "", err)


def qdel_notfound(jobid=123):
    return (1, "",  """denied: job "%s" does not exist""" % jobid)


def qstat_notfound(jobid=123):
    return (1, "", "")


def correct_qacct_done(jobid=123):
    out = """
==============================================================
qname        all.q
hostname     compute-0-2.local
group        antonio
owner        antonio
project      NONE
department   defaultdepartment
jobname      DemoSGEApp
jobnumber    %s
taskid       undefined
account      sge
priority     0
qsub_time    Thu Mar 15 08:42:46 2012
start_time   Thu Mar 15 08:43:00 2012
end_time     Thu Mar 15 08:43:10 2012
granted_pe   NONE
slots        1
failed       0
exit_status  0
ru_wallclock 10
ru_utime     0.154
ru_stime     0.094
ru_maxrss    0
ru_ixrss     0
ru_ismrss    0
ru_idrss     0
ru_isrss     0
ru_minflt    22296
ru_majflt    0
ru_nswap     0
ru_inblock   0
ru_oublock   0
ru_msgsnd    0
ru_msgrcv    0
ru_nsignals  0
ru_nvcsw     306
ru_nivcsw    157
cpu          0.248
mem          0.000
io           0.006
iow          0.000
maxvmem      13.152M
arid         undefined
""" % jobid

    return (0, out, "")


def qsub_failed_jobnamestartswithdigit(jobname='123DemoSGEApp'):
    out = ""
    err = """Unable to run job: denied: "%s" is not a valid object name \
(cannot start with a digit).
Exiting.
""" % jobname
    return (1, out, err)

# def qsub_failed_acl():
#     out = ""
#     err = """qsub: Unauthorized Request  MSG=user ACL rejected the \
# submitting user: user amessina@argo.ictp.it, queue cm1
# """
#     return (159, out, err)


def qdel_success(jobid=123):
    return (0, "antonio has registered the job %s for deletion" % jobid, "")


def qdel_failed_acl(jobid=123):
    err = """antonio - you do not have the necessary privileges to delete \
the job "%s"
""" % jobid
    out = ""
    return (1, out, err)


class FakeApp(gc3libs.Application):

    def __init__(self, **extra_args):
        gc3libs.Application.__init__(
            self,
            arguments=['/bin/hostname'],  # mandatory
            inputs=[],                    # mandatory
            outputs=[],                   # mandatory
            output_dir="./fakedir",       # mandatory
            stdout="stdout.txt",
            stderr="stderr.txt",
            requested_cores=1, **extra_args)


class TestBackendSge(object):

    CONF = """
[resource/example]
type=sge
auth=ssh
transport=ssh
frontend=example.org
max_cores_per_job=128
max_memory_per_core=2
max_walltime=2
max_cores=80
architecture=x86_64
queue=testing
enabled=True

[auth/ssh]
type=ssh
username=NONEXISTENT
"""

    @pytest.fixture(autouse=True)
    def setUp(self):
        (fd, self.tmpfile) = tempfile.mkstemp()
        f = os.fdopen(fd, 'w+')
        f.write(TestBackendSge.CONF)
        f.close()

        self.cfg = gc3libs.config.Configuration()
        self.cfg.merge_file(self.tmpfile)

        self.core = gc3libs.core.Core(self.cfg)

        self.backend = self.core.get_backend('example')
        self.backend.transport = FakeTransport()
        self.transport = self.backend.transport
        # Basic responses
        self.transport.expected_answer['qstat'] = qstat_notfound()
        self.transport.expected_answer['tracejob'] = qacct_notfound()
        self.transport.expected_answer['qdel'] = qdel_notfound()

        yield
        os.remove(self.tmpfile)

    def test_submission_failed(self):
        app = FakeApp()
        # Submission failed (jobname starting with a digit, cfr issue #250
        # at https://github.com/uzh/gc3pie/issues/250) This
        # first test will show the answer you would get if the job name
        # starts with a digit.
        self.transport.expected_answer['qsub'] = \
            qsub_failed_jobnamestartswithdigit()
        try:
            self.core.submit(app)
            assert False
        except Exception as e:
            assert isinstance(e, gc3libs.exceptions.LRMSError)
        assert app.execution.state == State.NEW

        # This second example will show how the Application.__init__()
        # method changes the jobname in order not to have a digit at the
        # beginning of it.
        app = FakeApp(jobname='123Demo')
        assert app.jobname == 'GC3Pie.123Demo'
        self.transport.expected_answer['qsub'] = correct_submit()
        self.core.submit(app)
        assert app.execution.state == State.SUBMITTED

    # Submission failed (unauthrozed user):
    #     t.expected_answer['qsub'] = qsub_failed_acl()
    #     try:
    #         self.core.submit(app)
    #     except Exception, e:
    #         assert isinstance(e, gc3libs.exceptions.LRMSError)
    #     assert_equal(app.execution.state, State.NEW)

    def test_sge_basic_workflow(self):
        app = FakeApp()
        # Succesful submission:
        self.transport.expected_answer['qsub'] = correct_submit()
        self.core.submit(app)
        assert app.execution.state == State.SUBMITTED

        # Update state. We would expect the job to be SUBMITTED
        self.transport.expected_answer['qstat'] = correct_qstat_queued()
        self.transport.expected_answer['qacct'] = qacct_notfound()
        self.core.update_job_state(app)
        assert app.execution.state == State.SUBMITTED

        # Update state. We would expect the job to be RUNNING
        self.transport.expected_answer['qstat'] = correct_qstat_running()
        self.transport.expected_answer['qacct'] = qacct_notfound()
        self.core.update_job_state(app)
        assert app.execution.state == State.RUNNING

        # Job done. qstat doesn't find it, qacct should.
        self.transport.expected_answer['qstat'] = qstat_notfound()
        self.transport.expected_answer['qacct'] = correct_qacct_done()
        self.core.update_job_state(app)
        assert app.execution.state == State.TERMINATING

    def test_qacct_parsing(self):
        app = FakeApp()
        self.transport.expected_answer['qsub'] = correct_submit()
        self.core.submit(app)
        self.transport.expected_answer['qacct'] = correct_qacct_done()
        self.core.update_job_state(app)
        assert app.execution.state == State.TERMINATING

        job = app.execution
        # common job reporting values (see Issue 78)
        assert job.exitcode == 0
        assert job.returncode == 0
        assert job.duration == 10 * seconds
        assert (job.max_used_memory - 13.152 * MB) < 1 * MB  # floating approx
        assert job.used_cpu_time == 0.248 * seconds
        # SGE-specific values
        assert job.sge_queue == 'all.q'
        assert job.sge_jobname == 'DemoSGEApp'
        assert (job.sge_submission_time ==
                     datetime.datetime(year=2012,
                                       month=3,
                                       day=15,
                                       hour=8,
                                       minute=42,
                                       second=46))
        assert (job.sge_start_time ==
                     datetime.datetime(year=2012,
                                       month=3,
                                       day=15,
                                       hour=8,
                                       minute=43,
                                       second=0))
        assert (job.sge_completion_time ==
                     datetime.datetime(year=2012,
                                       month=3,
                                       day=15,
                                       hour=8,
                                       minute=43,
                                       second=10))
        assert job.sge_failed == 0

    def test_delete_job(self):
        app = FakeApp()
        self.transport.expected_answer['qsub'] = correct_submit()
        self.core.submit(app)

        self.transport.expected_answer['qdel'] = qdel_success()
        self.core.kill(app)
        assert app.execution.state == State.TERMINATED

    def test_delete_job_twice(self):
        app = FakeApp()
        self.transport.expected_answer['qsub'] = correct_submit()
        self.core.submit(app)
        assert app.execution.state == State.SUBMITTED

        self.transport.expected_answer['qdel'] = qdel_failed_acl()
        self.core.kill(app)
        assert app.execution.state == State.TERMINATED


def tearDownModule():
    for fname in files_to_remove:
        if os.path.isdir(fname):
            shutil.rmtree(fname)
        else:
            os.remove(fname)


def test_get_command():
    (fd, tmpfile) = tempfile.mkstemp()
    files_to_remove.append(tmpfile)
    f = os.fdopen(fd, 'w+')
    f.write("""
[auth/ssh]
type=ssh
username=NONEXISTENT

[resource/example]
# mandatory stuff
type=sge
auth=ssh
transport=ssh
frontend=example.org
max_cores_per_job=128
max_memory_per_core=2
max_walltime=2
max_cores=80
architecture=x86_64

# alternate command paths
qsub = /usr/local/bin/qsub -q testing
qacct = /usr/local/sbin/qacct
qstat = /usr/local/bin/qstat
qdel = /usr/local/bin/qdel # comments are ignored!
""")
    f.close()

    cfg = gc3libs.config.Configuration()
    cfg.merge_file(tmpfile)
    b = cfg.make_resources()['example']

    assert b.qsub == ['/usr/local/bin/qsub', '-q', 'testing']

    assert b._qacct == '/usr/local/sbin/qacct'
    assert b._qdel == '/usr/local/bin/qdel'
    assert b._qstat == '/usr/local/bin/qstat'


if __name__ == "__main__":
    import pytest
    pytest.main(["-v", __file__])
