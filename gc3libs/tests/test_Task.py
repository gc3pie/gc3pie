#! /usr/bin/env python
#
"""
Test class `Task`:class:.
"""
# Copyright (C) 2011, 2012, 2018, 2019,  University of Zurich. All rights reserved.
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


## imports
import pytest

from gc3libs import Run, Task


from gc3libs.testing.helpers import SuccessfulApp, UnsuccessfulApp, temporary_core


## tests

def test_task_progress():
    with temporary_core() as core:
        task = SuccessfulApp()
        task.attach(core)
        task.submit()
        assert task.execution.state == Run.State.SUBMITTED

        # run until terminated
        while task.execution.state != Run.State.TERMINATED:
            task.progress()
        assert task.execution.state == Run.State.TERMINATED


def test_task_state_handlers():
    report = {
        'submitted': False,
        'terminating': False,
        'terminated': False,
    }

    class HandlerTestApp(SuccessfulApp):
        def __init__(self, report):
            super(HandlerTestApp, self).__init__()
            self.report = report
        def submitted(self):
            self.report['submitted'] = True
            print("Submitted!")
        def terminating(self):
            self.report['terminating'] = True
            print("Terminating!")
        def terminated(self):
            print("Terminated!")
            self.report['terminated'] = True

    with temporary_core() as core:
        task = HandlerTestApp(report)
        task.attach(core)
        task.submit()
        assert report['submitted']

        # run until terminated
        while task.execution.state != Run.State.TERMINATED:
            task.progress()
        assert report['terminating']
        assert report['terminated']


def test_task_redo1():
    """Test correct use of `Task.redo()`"""
    with temporary_core() as core:
        task = SuccessfulApp()
        task.attach(core)
        task.submit()
        assert task.execution.state == Run.State.SUBMITTED

        # run until terminated
        while task.execution.state != Run.State.TERMINATED:
            task.progress()
        assert task.execution.state == Run.State.TERMINATED

        # no do it all over again
        task.redo()
        assert task.execution.state == Run.State.NEW

        task.progress()
        assert task.execution.state != Run.State.NEW
        assert task.execution.state in [Run.State.SUBMITTED, Run.State.RUNNING]


def test_task_redo2():
    """Test that `.redo()` raises if called on a Task that is not TERMINATED."""
    with temporary_core() as core:
        task = SuccessfulApp()
        task.attach(core)
        task.submit()
        assert task.execution.state == Run.State.SUBMITTED

        # cannot redo a task that is not yet terminated
        with pytest.raises(AssertionError):
            task.redo()
            pytest.fail("`Task.redo()` succeeded on task not yet finished")


def test_task_redo3():
    """Test that `.redo()` is a no-op if the Task is still NEW."""
    with temporary_core() as core:
        task = SuccessfulApp()
        task.attach(core)
        task.redo()


# main: run tests

if "__main__" == __name__:
    import pytest
    pytest.main(["-v", __file__])
