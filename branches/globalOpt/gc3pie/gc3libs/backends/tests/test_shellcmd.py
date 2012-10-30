#! /usr/bin/env python
#
"""
Unit tests for the `gc3libs.backends.shellcmd` module.
"""
# Copyright (C) 2011-2012 GC3, University of Zurich. All rights reserved.
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


import os
import shutil
import sys
import tempfile
import time

from nose.tools import raises, assert_equal

import gc3libs
from gc3libs.authentication import Auth
import gc3libs.core, gc3libs.config


class TestBackendShellcmd(object):
    CONF="""
[resource/localhost_test]
type=shellcmd
transport=local
time_cmd=/usr/bin/time
max_cores=2
max_cores_per_job=2
max_memory_per_core=2
max_walltime=2
architecture=x64_64
auth=noauth
enabled=True

[auth/noauth]
type=none
"""

    def setUp(self):
        (fd, cfgfile) = tempfile.mkstemp()
        f = os.fdopen(fd, 'w+')
        f.write(TestBackendShellcmd.CONF)
        f.close()
        self.files_to_remove = [cfgfile]

        self.cfg = gc3libs.config.Configuration()
        self.cfg.merge_file(cfgfile)

        self.core = gc3libs.core.Core(self.cfg)
        self.backend = self.core.get_backend('localhost_test')

    def cleanup_file(self, fname):
        self.files_to_remove.append(fname)

    def tearDown(self):
        for fname in self.files_to_remove:
            if os.path.isdir(fname):
                shutil.rmtree(fname)
            elif os.path.exists(fname):
                os.remove(fname)

    def test_backend_creation(self):
        """
        Test that the initial resource parameters match those specified in the test config.
        """
        assert_equal(self.backend.free_slots, 2)
        assert_equal(self.backend.user_run, 0)
        assert_equal(self.backend.user_queued, 0)


    def test_submission_ok(self):
        """
        Test a successful submission cycle and the backends' resource book-keeping.
        """
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')

        app = gc3libs.Application(
            arguments = ['/usr/bin/env'],
            inputs = [],
            outputs = [],
            output_dir = tmpdir,
            stdout = "stdout.txt",
            stderr = "stderr.txt",
            requested_cores = 1,
            )
        self.core.submit(app)
        self.cleanup_file(tmpdir)
        self.cleanup_file(app.execution.lrms_execdir)

        # there's no SUBMITTED state here: jobs go immediately into RUNNING state
        assert_equal(app.execution.state, gc3libs.Run.State.SUBMITTED)
        assert_equal(self.backend.free_slots,  1)
        assert_equal(self.backend.user_queued, 0)
        assert_equal(self.backend.user_run,    1)

        # wait until the test job is done, but timeout and raise an error
        # if it takes too much time...
        MAX_WAIT = 10 # seconds
        WAIT = 0.1 # seconds
        waited = 0
        while app.execution.state != gc3libs.Run.State.TERMINATING and waited < MAX_WAIT:
            time.sleep(WAIT)
            waited += WAIT
            self.core.update_job_state(app)
        assert_equal(app.execution.state, gc3libs.Run.State.TERMINATING)
        assert_equal(self.backend.free_slots,  2)
        assert_equal(self.backend.user_queued, 0)
        assert_equal(self.backend.user_run,    0)

        self.core.fetch_output(app)
        assert_equal(app.execution.state, gc3libs.Run.State.TERMINATED)
        assert_equal(self.backend.free_slots,  2)
        assert_equal(self.backend.user_queued, 0)
        assert_equal(self.backend.user_run,    0)



    @raises(gc3libs.exceptions.LRMSSubmitError)
    def test_submission_too_many_jobs(self):

        app1 = gc3libs.Application(
            arguments = ['/usr/bin/env'],
            inputs = [],
            outputs = [],
            output_dir = ".",
            stdout = "stdout.txt",
            stderr = "stderr.txt",
            requested_cores = self.backend.free_slots,
            )
        self.core.submit(app1)
        self.cleanup_file(app1.execution.lrms_execdir)
        assert_equal(app1.execution.state, gc3libs.Run.State.SUBMITTED)

        # this fails, as the number of cores exceeds the resource total
        app2 = gc3libs.Application(
            arguments = ['/usr/bin/env'],
            inputs = [],
            outputs = [],
            output_dir = ".",
            stdout = "stdout.txt",
            stderr = "stderr.txt",
            requested_cores = 1,
            )
        self.core.submit(app2)
        self.cleanup_file(app2.execution.lrms_execdir)
        assert False # should not happen


    def test_check_app_after_reloading_session(self):
        """Check if we are able to check the status of a job after the script which started the job has died.
        """
        tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')
        self.cleanup_file(tmpdir)

        app = gc3libs.Application(
            arguments = ['/usr/bin/env'],
            inputs = [],
            outputs = [],
            output_dir = tmpdir,
            stdout = "stdout.txt",
            stderr = "stderr.txt",
            requested_cores = 1,
            )
        self.core.submit(app)
        self.cleanup_file(app.execution.lrms_execdir)
        # import nose.tools; nose.tools.set_trace()
        pid = app.execution.lrms_jobid

        # Forget about the child process.
        os.waitpid(pid, 0)

        # The wrapper process should die and write the final status
        # and the output to a file, so that `Core` will be able to
        # retrieve it.

        # wait until the test job is done, but timeout and raise an error
        # if it takes too much time...
        MAX_WAIT = 10 # seconds
        WAIT = 0.1 # seconds
        waited = 0
        while app.execution.state != gc3libs.Run.State.TERMINATING and waited < MAX_WAIT:
            time.sleep(WAIT)
            waited += WAIT
            self.core.update_job_state(app)

        assert_equal(app.execution.state, gc3libs.Run.State.TERMINATING)
        assert_equal(app.execution.returncode, 0)

    def test_time_cmd_args(self):
        assert_equal( self.backend.time_cmd , '/usr/bin/time')

if __name__ =="__main__":
    import nose
    nose.runmodule()
