#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012, GC3, University of Zurich. All rights reserved.
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

import sys
import logging
import os
import shutil
import tempfile

from nose.tools import assert_equal

from gc3libs import Application, Run, configure_logger
from gc3libs.workflow import SequentialTaskCollection
import gc3libs.config
import gc3libs.core

loglevel = logging.ERROR
configure_logger(loglevel, "test_isse_335")

class MySequentialCollection(SequentialTaskCollection):
    def next(self, x):
        """ensure that the next() is called only once per task."""
        if not hasattr(self, 'next_called_n_times'):
            self.next_called_n_times = 1
        else:
            self.next_called_n_times += 1
        return SequentialTaskCollection.next(self, x)

class test_issue_335(object):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_issue(self):
        """Test that SequentialTasksCollection goes in terminated state when all of its tasks are in TERMINATED state."""
        self.ptasks = 5
        task = MySequentialCollection(
            [
                Application(
                    ['echo','test1'],
                    [],[],
                    os.path.join(self.tmpdir, 'test.%d.d' % i)) for i in range(self.ptasks)
                ]
            )
        cfg = gc3libs.config.Configuration(
            *gc3libs.Default.CONFIG_FILE_LOCATIONS,
            **{'auto_enable_auth': True})
        core = gc3libs.core.Core(cfg)
        engine = gc3libs.core.Engine(core)
        engine.add(task)
        while True:
            engine.progress()

            if len([t for t in task.tasks if t.execution.state == Run.State.TERMINATED]) == self.ptasks:
                engine.progress()
                assert_equal(task.execution.state, Run.State.TERMINATED)
                break
        assert_equal(task.next_called_n_times, self.ptasks)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)


    
if "__main__" == __name__:
    import nose
    nose.runmodule()
