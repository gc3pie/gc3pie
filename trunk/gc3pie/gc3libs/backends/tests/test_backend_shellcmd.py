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
from gc3libs.Resource import Resource
from gc3libs.authentication import Auth
import gc3libs.core


def _setup_core():
    resource = Resource(
        name='localhost_test',
        type='shellcmd',
        transport='local',
        ncores=2,
        max_cores_per_job=2,
        max_memory_per_core=2,
        max_walltime=2,
        architecture=gc3libs.Run.Arch.X86_64,
        auth='none',
        enabled=True,
        )

    auth = Auth({'none':dict(
        name='noauth',
        type='none',
        )}, True)

    g = gc3libs.core.Core([resource], auth, True)
    b = g.get_backend('localhost_test')
    return (g, b)


def test_backend_creation():
    """
    Test that the initial resource parameters match those specified in the test config.
    """
    g, b = _setup_core()
    assert_equal(b._resource.free_slots, 2)
    assert_equal(b._resource.user_run, 0)
    assert_equal(b._resource.user_queued, 0)


def test_submission_ok():
    """
    Test a successful submission cycle and the backends' resource book-keeping.
    """
    g, b = _setup_core()
    tmpdir = tempfile.mkdtemp(prefix=__name__, suffix='.d')

    app = gc3libs.Application(
        executable = '/usr/bin/env',
        arguments = [],
        inputs = [],
        outputs = [],
        output_dir = tmpdir,
        stdout = "stdout.txt",
        stderr = "stderr.txt",
        requested_cores = 1,
        )
    g.submit(app)
    # there's no SUBMITTED state here: jobs go immediately into RUNNING state
    assert_equal(app.execution.state, gc3libs.Run.State.SUBMITTED)
    assert_equal(b._resource.free_slots,  1)
    assert_equal(b._resource.user_queued, 0)
    assert_equal(b._resource.user_run,    1)

    # wait until the test job is done, but timeout and raise an error
    # if it takes too much time...
    MAX_WAIT = 10 # seconds
    WAIT = 0.1 # seconds
    waited = 0
    while app.execution.state != gc3libs.Run.State.TERMINATING and waited < MAX_WAIT:
        time.sleep(WAIT)
        waited += WAIT
        g.update_job_state(app)
    assert_equal(app.execution.state, gc3libs.Run.State.TERMINATING)
    assert_equal(b._resource.free_slots,  2)
    assert_equal(b._resource.user_queued, 0)
    assert_equal(b._resource.user_run,    0)

    g.fetch_output(app)
    assert_equal(app.execution.state, gc3libs.Run.State.TERMINATED)
    assert_equal(b._resource.free_slots,  2)
    assert_equal(b._resource.user_queued, 0)
    assert_equal(b._resource.user_run,    0)

    # cleanup
    shutil.rmtree(tmpdir, ignore_errors=True)


@raises(gc3libs.exceptions.LRMSSubmitError)
def test_submission_too_many_jobs():
    g, b = _setup_core()

    app1 = gc3libs.Application(
        executable = '/usr/bin/env',
        arguments = [],
        inputs = [],
        outputs = [],
        output_dir = ".",
        stdout = "stdout.txt",
        stderr = "stderr.txt",
        requested_cores = b._resource.free_slots,
        )
    g.submit(app1)
    assert_equal(app1.execution.state, gc3libs.Run.State.SUBMITTED)

    # this fails, as the number of cores exceeds the resource total
    app2 = gc3libs.Application(
        executable = '/usr/bin/env',
        arguments = [],
        inputs = [],
        outputs = [],
        output_dir = ".",
        stdout = "stdout.txt",
        stderr = "stderr.txt",
        requested_cores = 1,
        )
    g.submit(app2)
    assert False # should not happen


if __name__ == "__main__":
    import nose
    nose.runmodule()
