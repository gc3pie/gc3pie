#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012  University of Zurich. All rights reserved.
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
from builtins import range
from builtins import object
__docformat__ = 'reStructuredText'

import logging
import os
import shutil
import tempfile

from gc3libs import Application, Run, configure_logger, create_engine
from gc3libs.workflow import SequentialTaskCollection

loglevel = logging.ERROR
configure_logger(loglevel, "test_issue_335")


class MySequentialCollection(SequentialTaskCollection):

    def __init__(self, *args, **kwargs):
        SequentialTaskCollection.__init__(self, *args, **kwargs)
        self.next_called_n_times = 0

    def next(self, x):
        """count times next() is called"""
        self.next_called_n_times += 1
        return SequentialTaskCollection.next(self, x)


class test_issue_335(object):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        CONF_FILE = """
[auth/dummy]
type = ssh
username = dummy

[resource/localhost]
enabled = true
auth = dummy
type = shellcmd
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2
max_walltime = 8
max_cores = 2
architecture = x86_64
override = False
resourcedir = %s
"""
        self.cfgfile = os.path.join(self.tmpdir, 'gc3pie.conf')
        self.resourcedir = os.path.join(self.tmpdir, 'shellcmd.d')
        fp = open(self.cfgfile, 'w')
        fp.write(CONF_FILE % self.resourcedir)
        fp.close()

    def test_issue_335(self):
        """Test that SequentialTasksCollection goes in TERMINATED state when
        all of its tasks are in TERMINATED state."""
        num_tasks_in_seq = 5
        seq = MySequentialCollection([
            Application(
                ['echo', 'test1'],
                [], [],
                os.path.join(self.tmpdir, 'test.%d.d' % i))
            for i in range(num_tasks_in_seq)
        ])
        engine = create_engine(self.cfgfile, auto_enable_auth=True)
        engine.add(seq)
        while True:
            engine.progress()
            if (len([task for task in seq.tasks
                     if task.execution.state == Run.State.TERMINATED])
                    == num_tasks_in_seq):
                engine.progress()
                # check that final SequentialCollection state is TERMINATED
                assert seq.execution.state == Run.State.TERMINATED
                break
        # check that next() has been called once per each task
        assert seq.next_called_n_times == num_tasks_in_seq

    def tearDown(self):
        shutil.rmtree(self.tmpdir)


if "__main__" == __name__:
    import pytest
    pytest.main(["-v", __file__])
