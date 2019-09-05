#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, 2012, 2016,  University of Zurich. All rights reserved.
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
from gc3libs.quantity import kB, seconds, minutes

from faketransport import FakeTransport

files_to_remove = []


def correct_submit(jobid=123):
    out = """%s
    """ % jobid
    return (0, out, "")


def tracejob_notfound(jobid=123):
    err = """/var/spool/torque/server_priv/accounting/20120309: \
Permission denied
/var/spool/torque/mom_logs/20120309: No such file or directory
/var/spool/torque/sched_logs/20120309: No such file or directory
"""

    return (0, "", err)


def correct_qstat_queued(jobid=123):
    out = """%s                 antani     amessina                0 Q short
""" % jobid
    return (0, out, "")


def correct_tracejob_queued(jobid=123):
    out = """Job: %s

03/14/2012 18:42:04  S    enqueuing into short, state 1 hop 1
03/14/2012 18:42:04  S    Job Queued at request of amessina@argo.ictp.it, \
owner = amessina@argo.ictp.it, job name = STDIN, queue = short
""" % jobid
    err = """/var/spool/torque/server_priv/accounting/20120314: \
Permission denied
/var/spool/torque/mom_logs/20120314: No such file or directory
/var/spool/torque/sched_logs/20120314: No such file or directory
"""
    return (0, out, err)


def correct_qstat_running(jobid=123):
    out = """%s                 cam_icbc         idiallo         \
01:01:18 R short
""" % jobid
    return (0, out, "")


def correct_tracejob_running(jobid=123):
    out = """Job: %s

03/14/2012 18:42:04  S    enqueuing into short, state 1 hop 1
03/14/2012 18:42:04  S    Job Queued at request of amessina@argo.ictp.it, \
owner = amessina@argo.ictp.it, job name = STDIN, queue = short
03/14/2012 18:45:25  S    Job Run at request of root@argo.ictp.it
03/14/2012 18:45:25  S    Not sending email: User does not want mail \
of this type.
""" % jobid
    err = """/var/spool/torque/server_priv/accounting/20120314: \
Permission denied
/var/spool/torque/mom_logs/20120314: No such file or directory
/var/spool/torque/sched_logs/20120314: No such file or directory
"""
    return (0, out, err)


def qdel_notfound(jobid=123):
    return (153, "", """qdel: Unknown Job Id %s""" % jobid)


def qstat_notfound(jobid=123):
    err = """qstat: Unknown Job Id %s
""" % jobid
    return (153, "", err)


def correct_tracejob_done(jobid=123):
    out = """
Job: %s

03/09/2012 09:31:53  S    enqueuing into short, state 1 hop 1
03/09/2012 09:31:53  S    Job Queued at request of amessina@argo.ictp.it, \
owner = amessina@argo.ictp.it, job name = DemoPBSApp, queue = short
03/09/2012 09:32:03  S    Job Run at request of root@argo.ictp.it
03/09/2012 09:32:03  S    Not sending email: User does not want mail \
of this type.
03/09/2012 09:34:08  S    Not sending email: User does not want mail \
of this type.
03/09/2012 09:34:08  S    Exit_status=0 resources_used.cput=00:00:00 \
resources_used.mem=2364kb resources_used.vmem=190944kb \
resources_used.walltime=00:02:05
03/09/2012 09:34:08  S    dequeuing from short, state COMPLETE
""" % jobid

    err = """/var/spool/torque/server_priv/accounting/20120309: \
Permission denied
/var/spool/torque/mom_logs/20120309: No such file or directory
/var/spool/torque/sched_logs/20120309: No such file or directory
"""

    return (0, out, err)


def qsub_failed_resources():
    out = ""
    err = """qsub: Job exceeds queue resource limits MSG=cannot locate \
feasible nodes
"""
    return (190, out, err)


def qsub_failed_acl():
    out = ""
    err = """qsub: Unauthorized Request  MSG=user ACL rejected the \
submitting user: user amessina@argo.ictp.it, queue cm1
"""
    return (159, out, err)


def qdel_success():
    return (0, "", "")


def qdel_failed_acl(jobid=123):
    err = """qdel: Unauthorized Request  MSG=operation not permitted %s
""" % jobid
    out = ""
    return (159, out, err)

# import gc3libs.Run.State as State
State = gc3libs.Run.State


class FakeApp(gc3libs.Application):

    def __init__(self):
        gc3libs.Application.__init__(
            self,
            arguments=['/bin/hostname'],  # mandatory
            inputs=[],                    # mandatory
            outputs=[],                   # mandatory
            output_dir="./fakedir",       # mandatory
            stdout="stdout.txt",
            stderr="stderr.txt",
            requested_cores=1,)


class TestBackendPbs(object):

    CONF = """
[resource/example]
type=pbs
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
        f.write(TestBackendPbs.CONF)
        f.close()

        self.cfg = gc3libs.config.Configuration()
        self.cfg.merge_file(self.tmpfile)

        self.core = gc3libs.core.Core(self.cfg)

        self.backend = self.core.get_backend('example')
        self.backend.transport = FakeTransport()
        self.transport = self.backend.transport
        # Basic responses
        self.transport.expected_answer['qstat'] = qstat_notfound()
        self.transport.expected_answer['tracejob'] = tracejob_notfound()
        self.transport.expected_answer['qdel'] = qdel_notfound()

        yield

        os.remove(self.tmpfile)

    def test_submission_failed(self):
        app = FakeApp()

        # Submission failed (unable to find resources):
        self.transport.expected_answer['qsub'] = qsub_failed_resources()
        try:
            self.core.submit(app)
        except Exception as e:
            assert isinstance(e, gc3libs.exceptions.LRMSError)
        assert app.execution.state == State.NEW

        # Submission failed (unauthrozed user):
        self.transport.expected_answer['qsub'] = qsub_failed_acl()
        try:
            self.core.submit(app)
        except Exception as e:
            assert isinstance(e, gc3libs.exceptions.LRMSError)
        assert app.execution.state == State.NEW

    def test_pbs_basic_workflow(self):
        app = FakeApp()

        # Succesful submission:
        self.transport.expected_answer['qsub'] = correct_submit()
        self.core.submit(app)
        assert app.execution.state == State.SUBMITTED

        # Update state. We would expect the job to be SUBMITTED
        self.transport.expected_answer['qstat'] = correct_qstat_queued()
        self.transport.expected_answer['tracejob'] = correct_tracejob_queued()
        self.core.update_job_state(app)
        assert app.execution.state == State.SUBMITTED

        # Update state. We would expect the job to be RUNNING
        self.transport.expected_answer['qstat'] = correct_qstat_running()
        self.transport.expected_answer['tracejob'] = correct_tracejob_running()
        self.core.update_job_state(app)
        assert app.execution.state == State.RUNNING

        # Job done. qstat doesn't find it, tracejob should.
        self.transport.expected_answer['qstat'] = qstat_notfound()
        self.transport.expected_answer['tracejob'] = correct_tracejob_done()
        self.core.update_job_state(app)
        assert app.execution.state == State.TERMINATING

    def test_tracejob_parsing(self):
        app = FakeApp()
        self.transport.expected_answer['qsub'] = correct_submit()
        self.core.submit(app)
        self.transport.expected_answer['tracejob'] = correct_tracejob_done()
        self.core.update_job_state(app)
        assert app.execution.state == State.TERMINATING

        job = app.execution
        # common job reporting values (see Issue 78)
        assert job.exitcode == 0
        assert job.duration == 2 * minutes + 5 * seconds
        assert job.max_used_memory == 190944 * kB
        assert job.used_cpu_time == 0 * seconds
        # PBS-specific values
        assert job.pbs_queue == 'short'
        assert job.pbs_jobname == 'DemoPBSApp'
        assert job.pbs_max_used_ram == 2364 * kB
        assert (job.pbs_submission_time ==
                     datetime.datetime(year=2012,
                                       month=3,
                                       day=9,
                                       hour=9,
                                       minute=31,
                                       second=53))
        assert (job.pbs_running_time ==
                     datetime.datetime(year=2012,
                                       month=3,
                                       day=9,
                                       hour=9,
                                       minute=32,
                                       second=3))
        assert (job.pbs_end_time ==
                     datetime.datetime(year=2012,
                                       month=3,
                                       day=9,
                                       hour=9,
                                       minute=34,
                                       second=8))

    def test_delete_job(self):
        app = FakeApp()
        self.transport.expected_answer['qsub'] = correct_submit()
        self.core.submit(app)

        self.transport.expected_answer['qdel'] = qdel_success()
        self.core.kill(app)
        assert app.execution.state == State.TERMINATED

    def test_delete_already_deleted_job(self):
        app = FakeApp()
        self.transport.expected_answer['qsub'] = correct_submit()
        self.core.submit(app)
        assert app.execution.state == State.SUBMITTED

        self.transport.expected_answer['qdel'] = qdel_failed_acl()
        self.core.kill(app)
        assert app.execution.state == State.TERMINATED

    def test_parse_acct_output(self):
        rc, stdout, stderr = correct_tracejob_done()
        status = self.backend._parse_acct_output(stdout, stderr)
        # common job reporting values (see Issue 78)
        assert status['termstatus'] == (0, 0)
        assert status['duration'] == 2 * minutes + 5 * seconds
        assert status['max_used_memory'] == 190944 * kB
        assert status['used_cpu_time'] == 0 * seconds
        # PBS-specific values
        assert status['pbs_queue'] == 'short'
        assert status['pbs_jobname'] == 'DemoPBSApp'
        assert status['pbs_max_used_ram'] == 2364 * kB
        assert (status['pbs_submission_time'] ==
                     datetime.datetime(year=2012,
                                       month=3,
                                       day=9,
                                       hour=9,
                                       minute=31,
                                       second=53))
        assert (status['pbs_running_time'] ==
                     datetime.datetime(year=2012,
                                       month=3,
                                       day=9,
                                       hour=9,
                                       minute=32,
                                       second=3))
        assert (status['pbs_end_time'] ==
                     datetime.datetime(year=2012,
                                       month=3,
                                       day=9,
                                       hour=9,
                                       minute=34,
                                       second=8))


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
type=pbs
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
qstat = /usr/local/bin/qstat
qdel = /usr/local/bin/qdel # comments are ignored!
tracejob = /usr/local/sbin/tracejob
""")
    f.close()

    cfg = gc3libs.config.Configuration()
    cfg.merge_file(tmpfile)
    b = cfg.make_resources()['example']

    assert b.qsub == ['/usr/local/bin/qsub', '-q', 'testing']

    assert b._qstat == '/usr/local/bin/qstat'
    assert b._qdel == '/usr/local/bin/qdel'
    assert b._tracejob == '/usr/local/sbin/tracejob'


if __name__ == "__main__":
    import pytest
    pytest.main(["-v", __file__])
