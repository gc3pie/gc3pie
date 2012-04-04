#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, GC3, University of Zurich. All rights reserved.
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


def correct_submit(jobid=123):
    out = """%s
    """ % jobid
    return (0, out, "")

def tracejob_notfound(jobid=123):
    err = """/var/spool/torque/server_priv/accounting/20120309: Permission denied
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
03/14/2012 18:42:04  S    Job Queued at request of amessina@argo.ictp.it, owner = amessina@argo.ictp.it, job name = STDIN, queue = short
""" % jobid
    err = """/var/spool/torque/server_priv/accounting/20120314: Permission denied
/var/spool/torque/mom_logs/20120314: No such file or directory
/var/spool/torque/sched_logs/20120314: No such file or directory
"""
    return (0, out, err)


def correct_qstat_running(jobid=123):
    out = """%s                 cam_icbc         idiallo         01:01:18 R short
""" % jobid
    return (0, out, "")

def correct_tracejob_running(jobid=123):
    out = """Job: %s

03/14/2012 18:42:04  S    enqueuing into short, state 1 hop 1
03/14/2012 18:42:04  S    Job Queued at request of amessina@argo.ictp.it, owner = amessina@argo.ictp.it, job name = STDIN, queue = short
03/14/2012 18:45:25  S    Job Run at request of root@argo.ictp.it
03/14/2012 18:45:25  S    Not sending email: User does not want mail of this type.
""" % jobid
    err = """/var/spool/torque/server_priv/accounting/20120314: Permission denied
/var/spool/torque/mom_logs/20120314: No such file or directory
/var/spool/torque/sched_logs/20120314: No such file or directory
"""
    return (0, out, err)


def qdel_notfound(jobid=123):
    return (153,"",  """qdel: Unknown Job Id %s""" % jobid)

def qstat_notfound(jobid=123):
    err = """qstat: Unknown Job Id %s
""" % jobid
    return (153, "", err)

def correct_tracejob_done(jobid=123):
    out = """
Job: %s

03/09/2012 09:31:53  S    enqueuing into short, state 1 hop 1
03/09/2012 09:31:53  S    Job Queued at request of amessina@argo.ictp.it, owner = amessina@argo.ictp.it, job name = DemoPBSApp, queue = short
03/09/2012 09:32:03  S    Job Run at request of root@argo.ictp.it
03/09/2012 09:32:03  S    Not sending email: User does not want mail of this type.
03/09/2012 09:34:08  S    Not sending email: User does not want mail of this type.
03/09/2012 09:34:08  S    Exit_status=0 resources_used.cput=00:00:00 resources_used.mem=2364kb resources_used.vmem=190944kb resources_used.walltime=00:02:05
03/09/2012 09:34:08  S    dequeuing from short, state COMPLETE
""" % jobid

    err = """/var/spool/torque/server_priv/accounting/20120309: Permission denied
/var/spool/torque/mom_logs/20120309: No such file or directory
/var/spool/torque/sched_logs/20120309: No such file or directory
"""
    
    return (0, out, err)


def qsub_failed_resources():
    out = ""
    err = """qsub: Job exceeds queue resource limits MSG=cannot locate feasible nodes
"""
    return (190, out, err)

def qsub_failed_acl():
    out = ""
    err = """qsub: Unauthorized Request  MSG=user ACL rejected the submitting user: user amessina@argo.ictp.it, queue cm1
"""
    return (159, out, err)

def qdel_success():
    return (0, "", "")

def qdel_failed_acl(jobid=123):
    err = """qdel: Unauthorized Request  MSG=operation not permitted %s
""" % jobid
    out = ""
    return (159, out, err)
    
from gc3libs.Resource import Resource
from gc3libs.authentication import Auth
import gc3libs, gc3libs.core
#import gc3libs.Run.State as State
State = gc3libs.Run.State

from faketransport import FakeTransport

def _setup_conf():
    resource = Resource(**{
    'name' : 'argo',
    'type' : 'pbs',
     'auth' : 'ssh',
     'transport' : 'ssh',
     'frontend' : 'argo.ictp.it',
     'max_cores_per_job' : 128,
     'max_memory_per_core' : 2,
     'max_walltime' : 2,
     'ncores' : 80,
     'architecture' : 'x86_64',
     'queue' : 'testing',
    'enabled' : True,})

    auth = Auth({'ssh':{'name': 'ssh','type' : 'ssh','username' : 'amessina',}}, True)
    
    return ([resource], auth)

def _setup_core():
    (res, auth) = _setup_conf()
    return gc3libs.core.Core(res, auth, True)


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

def _common_setup():
    g = _setup_core()
    b = g.get_backend('argo')
    b.transport = FakeTransport()
    t = b.transport
    # Basic responses
    t.expected_answer['qstat'] = qstat_notfound()
    t.expected_answer['tracejob'] = tracejob_notfound()
    t.expected_answer['qdel'] = qdel_notfound()
    
    app = FakeApp()
    return (g,t, app)

def test_submission_failed():
    (g, t, app)  = _common_setup()

    # Submission failed (unable to find resources):
    t.expected_answer['qsub'] = qsub_failed_resources()
    try:
        g.submit(app)
    except Exception, e:
        assert isinstance(e, gc3libs.exceptions.LRMSError)
    assert app.execution.state == State.NEW

    # Submission failed (unauthrozed user):
    t.expected_answer['qsub'] = qsub_failed_acl()
    try:
        g.submit(app)
    except Exception, e:
        assert isinstance(e, gc3libs.exceptions.LRMSError)
    assert app.execution.state == State.NEW

    
def test_pbs_basic_workflow():
    (g, t, app)  = _common_setup()

    # Succesful submission:
    t.expected_answer['qsub'] = correct_submit()
    g.submit(app)
    assert app.execution.state == State.SUBMITTED


    # Update state. We would expect the job to be SUBMITTED
    t.expected_answer['qstat'] = correct_qstat_queued()
    t.expected_answer['tracejob'] = correct_tracejob_queued()
    g.update_job_state(app)
    assert app.execution.state == State.SUBMITTED

    # Update state. We would expect the job to be RUNNING
    t.expected_answer['qstat'] = correct_qstat_running()
    t.expected_answer['tracejob'] = correct_tracejob_running()    
    g.update_job_state(app)
    assert app.execution.state == State.RUNNING

    # Job done. qstat doesn't find it, tracejob should.
    t.expected_answer['qstat'] = qstat_notfound()
    t.expected_answer['tracejob'] = correct_tracejob_done()    
    g.update_job_state(app)
    assert app.execution.state == State.TERMINATING
    

def test_tracejob_parsing():
    (g, t, app)  = _common_setup()
    t.expected_answer['qsub'] = correct_submit()
    g.submit(app)
    t.expected_answer['tracejob'] = correct_tracejob_done()    
    g.update_job_state(app)
    assert app.execution.state == State.TERMINATING

    job = app.execution
    assert job.exitcode == 0
    assert job.returncode == 0
    assert job['queue'] == 'short'
    assert job['used_walltime'] == '00:02:05'
    assert job['used_memory'] == '190944kb'
    assert job['mem'] == '2364kb'
    assert job['used_cpu_time'] == '00:00:00'

def test_delete_job():
    (g, t, app)  = _common_setup()
    t.expected_answer['qsub'] = correct_submit()
    g.submit(app)

    t.expected_answer['qdel'] = qdel_success()
    g.kill(app)
    assert app.execution.state == State.TERMINATED

def test_delete_job2():
    (g, t, app)  = _common_setup()
    t.expected_answer['qsub'] = correct_submit()
    g.submit(app)
    assert app.execution.state == State.SUBMITTED
    
    t.expected_answer['qdel'] = qdel_failed_acl()
    g.kill(app)
    assert app.execution.state == State.TERMINATED
    
if __name__ == "__main__":
    test_submission_failed()
    test_pbs_basic_workflow()
    test_tracejob_parsing()
    test_delete_job()
    test_delete_job2()
