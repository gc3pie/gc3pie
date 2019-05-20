#! /usr/bin/env python
#
"""
Unit tests for the `gc3libs.backends.shellcmd` module.
"""
# Copyright (C) 2011-2015, 2018  University of Zurich. All rights reserved.
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
from future import standard_library
standard_library.install_aliases()
from builtins import object
__docformat__ = 'reStructuredText'


from io import StringIO
import os
import shutil
import tempfile
import time

import pytest

import gc3libs
from gc3libs.backends.shellcmd import ShellcmdLrms
import gc3libs.config
import gc3libs.core
from gc3libs.quantity import Duration, Memory, s, kB
from gc3libs.testing.helpers import (
    SuccessfulApp,
    temporary_core,
    temporary_directory,
)


class TestBackendShellcmd(object):
    CONF = """
[resource/localhost_test]
type=shellcmd
transport=local
# time_cmd=/usr/bin/time
# Use unusual values so that we can easily spot if the `override` option works
max_cores=123
max_cores_per_job=123
max_memory_per_core=999
max_walltime=2
architecture=x64_64
auth=noauth
enabled=True
override=False
resourcedir=%s

[auth/noauth]
type=none
"""

    @pytest.fixture(autouse=True)
    def setUp(self):
        (fd, cfgfile) = tempfile.mkstemp()
        f = os.fdopen(fd, 'w+')
        CONFIG = TestBackendShellcmd.CONF % (cfgfile + '.d')
        f.write(CONFIG)
        f.close()
        self.files_to_remove = [cfgfile, cfgfile + '.d']
        self.apps_to_kill = []

        self.cfg = gc3libs.config.Configuration()
        self.cfg.merge_file(cfgfile)

        self.core = gc3libs.core.Core(self.cfg)
        self.backend = self.core.get_backend('localhost_test')
        # Update resource status
        self.backend.get_resource_status()

        yield

        for fname in self.files_to_remove:
            if os.path.isdir(fname):
                shutil.rmtree(fname)
            elif os.path.exists(fname):
                os.remove(fname)

        for app in self.apps_to_kill:
            try:
                self.core.kill(app)
            except:
                pass
            try:
                self.core.free(app)
            except:
                pass

    def cleanup_file(self, fname):
        self.files_to_remove.append(fname)

    def run_until_terminating(self, app, max_wait=10, polling_interval=0.1):
        """
        Wait until the given `app` task is done,
        but timeout and raise an error if it takes too much time.
        """
        waited = 0
        while (app.execution.state != gc3libs.Run.State.TERMINATING
                and waited < max_wait):
            time.sleep(polling_interval)
            waited += polling_interval
            self.core.update_job_state(app)


    def test_submission_ok(self):
        """Test a successful submission cycle and the backends' resource book-keeping"""
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')

        app = gc3libs.Application(
            arguments=['/usr/bin/env'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            stdout="stdout.txt",
            stderr="stderr.txt",
            requested_cores=1, )
        self.core.submit(app)
        self.apps_to_kill.append(app)

        self.cleanup_file(tmpdir)
        self.cleanup_file(app.execution.lrms_execdir)

        # there's no SUBMITTED state here: jobs go immediately into
        # RUNNING state
        assert app.execution.state == gc3libs.Run.State.SUBMITTED
        assert self.backend.free_slots == 122
        assert self.backend.user_queued == 0
        assert self.backend.user_run == 1

        self.run_until_terminating(app)
        try:
            assert app.execution.state == gc3libs.Run.State.TERMINATING
            assert self.backend.free_slots == 123
            assert self.backend.user_queued == 0
            assert self.backend.user_run == 0
        except:
            self.core.fetch_output(app)
            self.core.free(app)
            raise

        self.core.fetch_output(app)
        try:
            assert app.execution.state == gc3libs.Run.State.TERMINATED
            assert self.backend.free_slots == 123
            assert self.backend.user_queued == 0
            assert self.backend.user_run == 0
        except:
            self.core.free(app)
            raise

    def test_check_app_after_reloading_session(self):
        """Check that the job status is still available the end of the starter script"""

        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        self.cleanup_file(tmpdir)

        app = gc3libs.Application(
            arguments=['/usr/bin/env'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            stdout="stdout.txt",
            stderr="stderr.txt",
            requested_cores=1, )
        self.core.submit(app)
        self.apps_to_kill.append(app)

        self.cleanup_file(app.execution.lrms_execdir)

        # The wrapper process should die and write the final status
        # and the output to a file, so that `Core` will be able to
        # retrieve it.
        self.run_until_terminating(app)
        assert app.execution.state == gc3libs.Run.State.TERMINATING
        assert app.execution.returncode == 0

    def test_app_argument_with_spaces(self):
        """Check that arguments with spaces are not split"""
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        self.cleanup_file(tmpdir)

        app = gc3libs.Application(
            arguments=['/bin/ls', '-d', '/ /'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            stdout="stdout.txt",
            stderr="stderr.txt",
            requested_cores=1, )
        self.core.submit(app)
        self.apps_to_kill.append(app)
        self.cleanup_file(app.execution.lrms_execdir)

        self.run_until_terminating(app)
        assert app.execution.state == gc3libs.Run.State.TERMINATING
        assert app.execution.returncode != 0

    def test_resource_usage(self):
        """Check book-keeping of core and memory resources"""
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        self.cleanup_file(tmpdir)

        app = gc3libs.Application(
            arguments=['/bin/echo', 'Hello', 'World'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            requested_cores=2,
            requested_memory=10 * Memory.MB, )
        cores_before = self.backend.free_slots
        mem_before = self.backend.available_memory
        self.core.submit(app)
        self.apps_to_kill.append(app)

        cores_after = self.backend.free_slots
        mem_after = self.backend.available_memory
        assert cores_before == cores_after + 2
        assert mem_before == mem_after + app.requested_memory
        self.run_until_terminating(app)
        assert self.backend.free_slots == cores_before
        assert self.backend.available_memory == mem_before

    def test_env_vars_definition(self):
        """Check that `Application.environment` settings are correctly propagated"""
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        self.cleanup_file(tmpdir)

        app = gc3libs.Application(
            arguments=['/bin/echo', '$MSG'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            stdout='stdout.txt',
            environment={'MSG':'OK'})
        self.core.submit(app)
        self.apps_to_kill.append(app)

        self.run_until_terminating(app)

        self.core.fetch_output(app)
        stdout_file = os.path.join(app.output_dir, app.stdout)
        assert os.path.exists(stdout_file)
        assert os.path.isfile(stdout_file)
        stdout_contents = open(stdout_file, 'r').read()
        assert stdout_contents == 'OK\n'

    def test_stdout_in_directory(self):
        """Check that `Application.stdout` can include a full path"""
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        self.cleanup_file(tmpdir)

        app = gc3libs.Application(
            arguments=['/bin/echo', 'OK'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            stdout='logs/stdout.txt')
        self.core.submit(app)
        self.apps_to_kill.append(app)

        self.run_until_terminating(app)

        self.core.fetch_output(app)
        stdout_dir = os.path.join(app.output_dir, 'logs')
        stdout_file = os.path.join(stdout_dir, 'stdout.txt')
        assert os.path.exists(stdout_dir)
        assert os.path.isdir(stdout_dir)
        assert os.path.exists(stdout_file)
        assert os.path.isfile(stdout_file)
        stdout_contents = open(stdout_file, 'r').read()
        assert stdout_contents == 'OK\n'

    def test_not_enough_cores_usage(self):
        """Check that a `NoResources` exception is raised if more cores are requested than available"""
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        self.cleanup_file(tmpdir)
        bigapp = gc3libs.Application(
            arguments=['/bin/echo', 'Hello', 'World'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            requested_cores=self.backend.free_slots + 1,
            requested_memory=10 * Memory.MiB, )
        with pytest.raises(gc3libs.exceptions.NoResources):
            self.core.submit(bigapp)

    def test_not_enough_memory_usage(self):
        """Check that a `NoResources` exception is raised if more memory is requested than available"""
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        self.cleanup_file(tmpdir)
        bigapp = gc3libs.Application(
            arguments=['/bin/echo', 'Hello', 'World'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            requested_cores=1,
            requested_memory=self.backend.total_memory + Memory.B, )
        with pytest.raises(gc3libs.exceptions.NoResources):
            self.core.submit(bigapp)


class TestBackendShellcmdCFG(object):
    CONF = """
[resource/localhost_test]
type=shellcmd
transport=local
time_cmd=/usr/bin/time
max_cores=1000
max_cores_per_job=4
max_memory_per_core=2
max_walltime=2
architecture=x64_64
auth=noauth
enabled=True
override=%s
resourcedir=%s

[auth/noauth]
type=none
"""

    @pytest.fixture(autouse=True)
    def setUp(self):
        self.files_to_remove = []

        yield
        for fname in self.files_to_remove:
            if os.path.isdir(fname):
                shutil.rmtree(fname)
            elif os.path.exists(fname):
                os.remove(fname)

    def cleanup_file(self, fname):
        self.files_to_remove.append(fname)


    def test_override_cfg_flag(self):
        (fd, cfgfile) = tempfile.mkstemp()
        f = os.fdopen(fd, 'w+')
        f.write(TestBackendShellcmdCFG.CONF % ("True", cfgfile + '.d'))
        f.close()
        self.files_to_remove = [cfgfile, cfgfile + '.d']

        self.cfg = gc3libs.config.Configuration()
        self.cfg.merge_file(cfgfile)

        self.core = gc3libs.core.Core(self.cfg)
        self.backend = self.core.get_backend('localhost_test')
        # Update resource status
        self.backend.get_resource_status()

        assert self.backend.max_cores < 1000

    def test_do_not_override_cfg_flag(self):
        (fd, cfgfile) = tempfile.mkstemp()
        f = os.fdopen(fd, 'w+')
        f.write(TestBackendShellcmdCFG.CONF % ("False", cfgfile + '.d'))
        f.close()
        self.files_to_remove = [cfgfile, cfgfile + '.d']

        self.cfg = gc3libs.config.Configuration()
        self.cfg.merge_file(cfgfile)

        self.core = gc3libs.core.Core(self.cfg)
        self.backend = self.core.get_backend('localhost_test')
        # Update resource status

        assert self.backend.max_cores == 1000

    def test_resource_sharing_w_multiple_backends(self):
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        (fd, cfgfile) = tempfile.mkstemp()
        f = os.fdopen(fd, 'w+')
        f.write(TestBackendShellcmdCFG.CONF % ("False", cfgfile + '.d'))
        f.close()
        self.files_to_remove = [cfgfile, cfgfile + '.d', tmpdir]

        cfg1 = gc3libs.config.Configuration()
        cfg1.merge_file(cfgfile)

        cfg2 = gc3libs.config.Configuration()
        cfg2.merge_file(cfgfile)

        core1 = gc3libs.core.Core(cfg1)
        core2 = gc3libs.core.Core(cfg2)

        backend1 = core1.get_backend('localhost_test')
        backend2 = core2.get_backend('localhost_test')

        app = gc3libs.Application(
            arguments=['/bin/echo', 'Hello', 'World'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            requested_cores=1,
            requested_memory=10 * Memory.MiB, )

        try:
            core1.submit(app)
            assert (backend1.free_slots ==
                         backend1.max_cores - app.requested_cores)

            assert backend2.free_slots == backend2.max_cores
            backend2.get_resource_status()
            assert (backend2.free_slots ==
                         backend2.max_cores - app.requested_cores)
        finally:
            core1.kill(app)
            core1.free(app)


    def test_pid_list_is_string(self):
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        (fd, cfgfile) = tempfile.mkstemp()
        f = os.fdopen(fd, 'w+')
        f.write(TestBackendShellcmdCFG.CONF % ("False", cfgfile + '.d'))
        f.close()
        self.files_to_remove = [cfgfile, cfgfile + '.d']

        self.cfg = gc3libs.config.Configuration()
        self.cfg.merge_file(cfgfile)

        self.core = gc3libs.core.Core(self.cfg)
        self.backend = self.core.get_backend('localhost_test')
        # Update resource status

        app = gc3libs.Application(
        arguments=['/bin/echo', 'Hello', 'World'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            requested_cores=1,
            requested_memory=10 * Memory.MiB, )

        try:
            self.core.submit(app)
            for pid in list(self.backend._job_infos.keys()):
                assert isinstance(pid,str)
        finally:
            self.core.kill(app)
            self.core.free(app)

def test_shellcmd_backend_create_spooldir():
    with temporary_directory() as tmpdir:
        with temporary_core(spooldir=tmpdir) as core:
            assert 'test' in core.resources
            backend = core.get_backend('test')
            app = SuccessfulApp()
            core.submit(app)
            assert os.path.isdir(backend.spooldir)


def test_shellcmd_backend_parse_wrapper_output_successful():
    resource_usage = StringIO("""
WallTime=2.10s
KernelTime=0.61s
UserTime=0.67s
CPUUsage=61%
MaxResidentMemory=61604kB
AverageResidentMemory=0kB
AverageTotalMemory=0kB
AverageUnsharedMemory=0kB
AverageUnsharedStack=0kB
AverageSharedMemory=0kB
PageSize=4096B
MajorPageFaults=0
MinorPageFaults=101799
Swaps=0
ForcedSwitches=82
WaitSwitches=487
Inputs=0
Outputs=400
SocketReceived=0
SocketSent=0
Signals=0
ReturnCode=0
    """)
    with temporary_core(type='shellcmd') as core:
        assert 'test' in core.resources
        backend = core.get_backend('test')
        termstatus, acctinfo, valid = backend._parse_wrapper_output(resource_usage)
        assert valid
        assert acctinfo.duration == 2.10*s
        assert acctinfo.shellcmd_kernel_time == 0.61*s
        assert acctinfo.shellcmd_user_time == 0.67*s
        assert acctinfo.max_used_memory == 61604*kB
        assert acctinfo.shellcmd_average_total_memory == 0*kB
        assert acctinfo.shellcmd_filesystem_inputs == 0
        assert acctinfo.shellcmd_filesystem_outputs == 400
        assert acctinfo.shellcmd_swapped == 0
        assert termstatus == (0, 0)


def test_shellcmd_backend_parse_wrapper_output_signalled():
    resource_usage = StringIO("""Command terminated by signal 15
WallTime=6.44s
KernelTime=0.00s
UserTime=0.00s
CPUUsage=0%
MaxResidentMemory=2004kB
AverageResidentMemory=0kB
AverageTotalMemory=0kB
AverageUnsharedMemory=0kB
AverageUnsharedStack=0kB
AverageSharedMemory=0kB
PageSize=4096B
MajorPageFaults=0
MinorPageFaults=73
Swaps=0
ForcedSwitches=0
WaitSwitches=2
Inputs=0
Outputs=0
SocketReceived=0
SocketSent=0
Signals=0
ReturnCode=0
    """)
    with temporary_core(type='shellcmd') as core:
        assert 'test' in core.resources
        backend = core.get_backend('test')
        termstatus, acctinfo, valid = backend._parse_wrapper_output(resource_usage)
        assert valid
        assert acctinfo.duration == 6.44*s
        assert acctinfo.shellcmd_kernel_time == 0.*s
        assert acctinfo.shellcmd_user_time == 0.*s
        assert acctinfo.max_used_memory == 2004*kB
        assert acctinfo.shellcmd_average_total_memory == 0*kB
        assert acctinfo.shellcmd_filesystem_inputs == 0
        assert acctinfo.shellcmd_filesystem_outputs == 0
        assert acctinfo.shellcmd_swapped == 0
        assert termstatus == (15, -1)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
