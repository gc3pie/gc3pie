#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, 2012, GC3, University of Zurich. All rights reserved.
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


from nose.tools import raises
from nose.plugins.skip import SkipTest

from gc3libs import Application, Run, Task
from gc3libs.workflow import RetryableTask
import gc3libs.exceptions

class TestApplication(Application):

    def __init__(self):
        Application.__init__(self,
                             arguments = [''],
                             inputs = [],
                             outputs = [],
                             output_dir = None)
        self.execution.state = 'TERMINATED'
        self.execution.returncode = 0
        self.changed = False

    def update_state(self):
        # self.changed = False
        pass

@raises(AssertionError)
def test_persisted_change():
    app = TestApplication()
    task = RetryableTask(app)
    # task.execution.state = 'RUNNING'

    # This is supposed to alter the state of the task
    # thus mark it as 'changed'
    task.update_state()

    # We expect task.changed to be true
    assert(task.changed == False)

if "__main__" == __name__:
    import nose
    nose.runmodule()
