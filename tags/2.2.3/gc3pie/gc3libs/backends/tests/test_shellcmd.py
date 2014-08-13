#! /usr/bin/env python
#
"""
Unit tests for the `gc3libs.backends.shellcmd` module.
"""
# Copyright (C) 2011-2013 GC3, University of Zurich. All rights reserved.
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


import errno
import os
import shutil
import sys
import tempfile
import time

from nose.tools import raises, assert_equal, assert_not_equal

import gc3libs
from gc3libs.authentication import Auth
import gc3libs.config
import gc3libs.core
from gc3libs.quantity import Memory


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

    def cleanup_file(self, fname):
        self.files_to_remove.append(fname)

    def tearDown(self):
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

    def test_submission_ok(self):
        """
        Test a successful submission cycle and the backends' resource
        book-keeping.
        """
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
        assert_equal(app.execution.state, gc3libs.Run.State.SUBMITTED)
        assert_equal(self.backend.free_slots,  122)
        assert_equal(self.backend.user_queued, 0)
        assert_equal(self.backend.user_run,    1)

        # wait until the test job is done, but timeout and raise an error
        # if it takes too much time...
        MAX_WAIT = 10  # seconds
        WAIT = 0.1  # seconds
        waited = 0
        while app.execution.state != gc3libs.Run.State.TERMINATING \
                and waited < MAX_WAIT:
            time.sleep(WAIT)
            waited += WAIT
            self.core.update_job_state(app)
        try:
            assert_equal(app.execution.state, gc3libs.Run.State.TERMINATING)
            assert_equal(self.backend.free_slots,  123)
            assert_equal(self.backend.user_queued, 0)
            assert_equal(self.backend.user_run,    0)
        except:
            self.core.fetch_output(app)
            self.core.free(app)
            raise

        self.core.fetch_output(app)
        try:
            assert_equal(app.execution.state, gc3libs.Run.State.TERMINATED)
            assert_equal(self.backend.free_slots,  123)
            assert_equal(self.backend.user_queued, 0)
            assert_equal(self.backend.user_run,    0)
        except:
            self.core.free(app)
            raise

    def test_check_app_after_reloading_session(self):
        """Check if we are able to check the status of a job after the
        script which started the job has died.
        """
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
        pid = app.execution.lrms_jobid

        # The wrapper process should die and write the final status
        # and the output to a file, so that `Core` will be able to
        # retrieve it.

        # wait until the test job is done, but timeout and raise an error
        # if it takes too much time...
        MAX_WAIT = 10  # seconds
        WAIT = 0.1  # seconds
        waited = 0
        while app.execution.state != gc3libs.Run.State.TERMINATING \
                and waited < MAX_WAIT:
            time.sleep(WAIT)
            waited += WAIT
            self.core.update_job_state(app)

        assert_equal(app.execution.state, gc3libs.Run.State.TERMINATING)
        assert_equal(app.execution.returncode, 0)

    def test_app_argument_with_spaces(self):
        """Check that arguments with spaces are not splitted
        """
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
        MAX_WAIT = 10  # seconds
        WAIT = 0.1  # seconds
        waited = 0
        while app.execution.state != gc3libs.Run.State.TERMINATING \
                and waited < MAX_WAIT:
            time.sleep(WAIT)
            waited += WAIT
            self.core.update_job_state(app)
        assert_equal(app.execution.state, gc3libs.Run.State.TERMINATING)
        assert_not_equal(app.execution.returncode, 0)

    # def test_time_cmd_args(self):
    #     assert_equal(self.backend.time_cmd, '/usr/bin/time')

    def test_resource_usage(self):
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
        assert_equal(cores_before, cores_after + 2)
        assert_equal(mem_before, mem_after + app.requested_memory)
        MAX_WAIT = 10  # seconds
        WAIT = 0.1  # seconds
        waited = 0
        while app.execution.state != gc3libs.Run.State.TERMINATING \
                and waited < MAX_WAIT:
            time.sleep(WAIT)
            waited += WAIT
            self.core.update_job_state(app)
        assert_equal(self.backend.free_slots, cores_before)
        avail = self.backend.available_memory
        assert_equal(self.backend.available_memory, mem_before)

    @raises(gc3libs.exceptions.NoResources)
    def test_not_enough_cores_usage(self):
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        self.cleanup_file(tmpdir)
        bigapp = gc3libs.Application(
            arguments=['/bin/echo', 'Hello', 'World'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            requested_cores=self.backend.free_slots + 1,
            requested_memory=10 * Memory.MiB, )
        self.core.submit(bigapp)

    @raises(gc3libs.exceptions.NoResources)
    def test_not_enough_memory_usage(self):
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        self.cleanup_file(tmpdir)
        bigapp = gc3libs.Application(
            arguments=['/bin/echo', 'Hello', 'World'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            requested_cores=1,
            requested_memory=self.backend.total_memory + Memory.B, )
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

    def setUp(self):
        self.files_to_remove = []

    def cleanup_file(self, fname):
        self.files_to_remove.append(fname)

    def tearDown(self):
        for fname in self.files_to_remove:
            if os.path.isdir(fname):
                shutil.rmtree(fname)
            elif os.path.exists(fname):
                os.remove(fname)

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

        assert_equal(self.backend.max_cores, 1000)

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
            assert_equal(backend1.free_slots, backend1.max_cores - app.requested_cores)

            assert_equal(backend2.free_slots, backend2.max_cores)
            backend2.get_resource_status()
            assert_equal(backend2.free_slots, backend2.max_cores - app.requested_cores)
        finally:
            core1.kill(app)
            core1.free(app)


if __name__ == "__main__":
    import nose
    nose.runmodule()
