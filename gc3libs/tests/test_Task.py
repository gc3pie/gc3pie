#! /usr/bin/env python
#
"""
Test class `Task`:class:.
"""
# Copyright (C) 2011, 2012, University of Zurich. All rights reserved.
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


## imports

from gc3libs import Run, Task

from nose.tools import raises, assert_equal

from helpers import SuccessfulApp, UnsuccessfulApp, temporary_core


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


@raises(AssertionError)
def test_task_redo2():
    """Test that `.redo()` raises if called on a Task that is not TERMINATED."""
    with temporary_core() as core:
        task = SuccessfulApp()
        task.attach(core)
        task.submit()
        assert task.execution.state == Run.State.SUBMITTED

        # cannot redo a task that is not yet terminated
        task.redo()


def test_task_redo3():
    """Test that `.redo()` is a no-op if the Task is still NEW."""
    with temporary_core() as core:
        task = SuccessfulApp()
        task.attach(core)
        task.redo()


# main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()
