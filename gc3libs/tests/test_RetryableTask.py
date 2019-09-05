#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, 2012,  University of Zurich. All rights reserved.
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
__docformat__ = 'reStructuredText'


import pytest

from gc3libs import Application
from gc3libs.workflow import RetryableTask


class MyApplication(Application):

    def __init__(self):
        Application.__init__(self,
                             arguments=[''],
                             inputs=[],
                             outputs=[],
                             output_dir=None)
        self.execution.state = 'TERMINATED'
        self.execution.returncode = 0
        self.changed = False

    def update_state(self):
        # self.changed = False
        pass


def test_persisted_change():
    app = MyApplication()
    task = RetryableTask(app)
    # task.execution.state = 'RUNNING'

    # This is supposed to alter the state of the task
    # thus mark it as 'changed'
    task.update_state()

    # We expect task.changed to be true
    assert task.changed


if "__main__" == __name__:
    # pylint: disable=ungrouped-imports
    import pytest
    pytest.main(["-v", __file__])
