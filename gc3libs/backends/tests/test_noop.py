#! /usr/bin/env python
#
"""
Unit tests for the `gc3libs.backends.noop` module.
"""
# Copyright (C) 2011-2013, 2015  University of Zurich. All rights reserved.
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


import os
import shutil
import tempfile
import time

import pytest

import gc3libs
import gc3libs.config
import gc3libs.core
from gc3libs.quantity import Memory


class TestBackendNoop(object):
    CONF = """
[resource/noop_test]
type=noop
transport=local
max_cores=123
max_cores_per_job=123
max_memory_per_core=999
max_walltime=2
architecture=x64_64
auth=none
enabled=True
"""

    @pytest.fixture(autouse=True)
    def setUp(self):
        (fd, cfgfile) = tempfile.mkstemp()
        f = os.fdopen(fd, 'w+')
        f.write(self.CONF)
        f.close()
        self.files_to_remove = [cfgfile, cfgfile + '.d']
        self.apps_to_kill = []

        self.cfg = gc3libs.config.Configuration()
        # enabled no-op backend
        self.cfg.TYPE_CONSTRUCTOR_MAP['noop'] = ('gc3libs.backends.noop', 'NoOpLrms')
        self.cfg.merge_file(cfgfile)

        self.core = gc3libs.core.Core(self.cfg)
        self.backend = self.core.get_backend('noop_test')

        yield

        # since TYPE_CONSTRUCTOR_MAP is a class-level variable, we
        # need to clean up otherwise other tests will see the No-Op
        # backend
        del self.cfg.TYPE_CONSTRUCTOR_MAP['noop']
        for fname in self.files_to_remove:
            if os.path.isdir(fname):
                shutil.rmtree(fname)
            elif os.path.exists(fname):
                os.remove(fname)

    def cleanup_file(self, fname):
        self.files_to_remove.append(fname)


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
            requested_cores=1, )
        self.core.submit(app)

        self.cleanup_file(tmpdir)

        # app must be in SUBMITTED state here
        assert app.execution.state == gc3libs.Run.State.SUBMITTED
        assert self.backend.free_slots == 122
        assert self.backend.queued ==       1
        assert self.backend.user_queued ==  1
        assert self.backend.user_run ==     0

        # transition to RUNNING
        self.core.update_job_state(app)
        assert app.execution.state == gc3libs.Run.State.RUNNING
        assert self.backend.free_slots == 122
        assert self.backend.queued ==       0
        assert self.backend.user_queued ==  0
        assert self.backend.user_run ==     1

        # transition to TERMINATING
        self.core.update_job_state(app)
        assert app.execution.state == gc3libs.Run.State.TERMINATING
        assert self.backend.free_slots == 123
        assert self.backend.queued ==       0
        assert self.backend.user_queued ==  0
        assert self.backend.user_run ==     0

        # transition to TERMINATED
        self.core.fetch_output(app)
        assert app.execution.state == gc3libs.Run.State.TERMINATED
        assert self.backend.free_slots == 123
        assert self.backend.queued ==       0
        assert self.backend.user_queued ==  0
        assert self.backend.user_run ==     0

    def test_resource_usage(self):
        """Test slots/memory book-keeping on the backend."""
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

        # app in state SUBMITTED, resources are allocated
        self.core.submit(app)
        cores_after = self.backend.free_slots
        mem_after = self.backend.available_memory
        assert cores_before == cores_after + 2
        assert mem_before == mem_after + app.requested_memory

        # app in state RUNNING, no change
        self.core.update_job_state(app)
        assert app.execution.state == gc3libs.Run.State.RUNNING
        assert cores_before == cores_after + 2
        assert mem_before == mem_after + app.requested_memory

        # app in state TERMINATED, resources are released
        self.core.update_job_state(app)
        assert app.execution.state == gc3libs.Run.State.TERMINATING
        assert self.backend.free_slots ==       cores_before
        assert self.backend.available_memory == mem_before

    def test_slots_usage(self):
        """Test slots (but no memory) book-keeping on the backend."""
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        self.cleanup_file(tmpdir)

        app = gc3libs.Application(
            arguments=['/bin/echo', 'Hello', 'World'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            requested_cores=2)
        cores_before = self.backend.free_slots
        mem_before = self.backend.available_memory

        # app in state SUBMITTED, resources are allocated
        self.core.submit(app)
        cores_after = self.backend.free_slots
        mem_after = self.backend.available_memory
        assert cores_before == cores_after + 2
        assert mem_before == mem_after

        # app in state RUNNING, no change
        self.core.update_job_state(app)
        assert app.execution.state == gc3libs.Run.State.RUNNING
        assert cores_before == cores_after + 2
        assert mem_before == mem_after

        # app in state TERMINATED, resources are released
        self.core.update_job_state(app)
        assert app.execution.state == gc3libs.Run.State.TERMINATING
        assert self.backend.free_slots ==       cores_before
        assert self.backend.available_memory == mem_before

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
        with pytest.raises(gc3libs.exceptions.NoResources):
            self.core.submit(bigapp)

    def test_not_enough_memory_usage(self):
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        self.cleanup_file(tmpdir)
        bigapp = gc3libs.Application(
            arguments=['/bin/echo', 'Hello', 'World'],
            inputs=[],
            outputs=[],
            output_dir=tmpdir,
            requested_cores=1,
            requested_memory=self.backend.available_memory + Memory.B, )
        with pytest.raises(gc3libs.exceptions.NoResources):
            self.core.submit(bigapp)


if __name__ == "__main__":
    import pytest
    pytest.main(["-v", __file__])
