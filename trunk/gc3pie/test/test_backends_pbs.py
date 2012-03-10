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

qsub_out = """35488.argo.ictp.it
"""

qstat_notfound_err = """qstat: Unknown Job Id %s
"""

qstat_queued_out = """36236.argo                 antani     amessina                0 Q short
"""
qstat_running_out = """36248.argo                 cam_icbc         idiallo         01:01:18 R short
"""

tracejob_out = """
Job: 36174.argo.ictp.it

03/09/2012 09:31:53  S    enqueuing into short, state 1 hop 1
03/09/2012 09:31:53  S    Job Queued at request of amessina@argo.ictp.it, owner = amessina@argo.ictp.it, job name = DemoPBSApp, queue = short
03/09/2012 09:32:03  S    Job Run at request of root@argo.ictp.it
03/09/2012 09:32:03  S    Not sending email: User does not want mail of this type.
03/09/2012 09:34:08  S    Not sending email: User does not want mail of this type.
03/09/2012 09:34:08  S    Exit_status=0 resources_used.cput=00:00:00 resources_used.mem=2364kb resources_used.vmem=190944kb resources_used.walltime=00:02:05
03/09/2012 09:34:08  S    dequeuing from short, state COMPLETE
"""

tracejob_err = """/var/spool/torque/server_priv/accounting/20120309: Permission denied
/var/spool/torque/mom_logs/20120309: No such file or directory
/var/spool/torque/sched_logs/20120309: No such file or directory
"""

from gc3libs.Resource import Resource
from gc3libs.authentication import Auth
import gc3libs, gc3libs.core
#import gc3libs.Run.State as State
State = gc3libs.Run.State

from gc3libs.backends.transport import LocalTransport

class FakePBSTransport(LocalTransport):
    def __init__(self):
        LocalTransport.__init__(self)        
        self.status = State.NEW
        
    def set_job_status(self, status):
        self.status = status
    
    def execute_command(self, command):
        """parse the command and return fake output and error codes
        depending on the current suppose status of the job"""
        commands = []
        # this is a excessively complex routine to split a command like "cmd1 ; cmd2 && cmd3 || cmd4"
        
        _ = command.split(';')
        for i in _:
            commands.extend(i.strip().split('&&'))
        
        for cmd in commands:
            args = cmd.strip().split()
            if args[0] == 'qsub':
                return (0, qsub_out, "")
            elif args[0] == 'tracejob':
                # Job is done, returning output
                return (0, tracejob_out, tracejob_err)
            elif args[0] == 'qstat':
                if len(args)==5 and args[2] == '|' and args[3] == 'grep':
                    # qstat jobid | grep ^jobid
                    if self.status == State.RUNNING:
                        return (0, qstat_running_out, "")
                    elif self.status == State.SUBMITTED:
                        return (0, qstat_queued_out, "")
                    elif self.status == State.TERMINATING:
                        return (153, "", qstat_notfound_err % args[1])
            elif args[0] == 'tracejob':
                    return (0, tracejob_out, tracejob_err)
            else:
                continue
        return LocalTransport.execute_command(self, command)
    
def _setup_conf():
    resource = Resource({
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



def test_pbs_basic_workflow():
    g = _setup_core()
    b = g.get_backend('argo')
    b.transport = FakePBSTransport()

    app = FakeApp()
    g.submit(app)
    b.transport.set_job_status(app.execution.state)
    assert app.execution.state == State.SUBMITTED

    g.update_job_state(app)
    b.transport.set_job_status(app.execution.state)
    
    b.transport.set_job_status(State.RUNNING)
    g.update_job_state(app)
    b.transport.set_job_status(app.execution.state)
    assert app.execution.state == State.RUNNING
    
    b.transport.set_job_status(State.TERMINATING)
    g.update_job_state(app)
    b.transport.set_job_status(app.execution.state)
    assert app.execution.state == State.TERMINATING

    

if __name__ == "__main__":
    test_pbs_basic_workflow()
