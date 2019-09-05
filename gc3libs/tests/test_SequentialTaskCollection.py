#! /usr/bin/env python
#
"""
Test class `SequentialTaskCollection`:class:.
"""
# Copyright (C) 2011, 2012, 2018,  University of Zurich. All rights reserved.
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

from gc3libs import Run, Task
from gc3libs.workflow import SequentialTaskCollection


from gc3libs.testing.helpers import SimpleSequentialTaskCollection, SuccessfulApp, UnsuccessfulApp, temporary_core


## tests

def test_SequentialTaskCollection_progress():
    with temporary_core() as core:
        seq = SimpleSequentialTaskCollection(3)
        seq.attach(core)

        # run until terminated
        while seq.execution.state != Run.State.TERMINATED:
            seq.progress()
        assert seq.stage().jobname == 'stage2'
        assert seq.stage().execution.state == Run.State.TERMINATED


def test_SequentialTaskCollection_redo1():
    with temporary_core() as core:
        seq = SimpleSequentialTaskCollection(3)
        seq.attach(core)

        # run until terminated
        while seq.execution.state != Run.State.TERMINATED:
            seq.progress()
        assert seq.stage().jobname == 'stage2'
        assert seq.stage().execution.state == Run.State.TERMINATED

        seq.redo()
        assert seq.stage().jobname == 'stage0'
        assert seq.stage().execution.state == Run.State.NEW
        assert seq.execution.state == Run.State.NEW

        # run until terminated, again
        while seq.execution.state != Run.State.TERMINATED:
            seq.progress()
        assert seq.stage().jobname == 'stage2'
        assert seq.stage().execution.state == Run.State.TERMINATED


def test_SequentialTaskCollection_redo2():
    with temporary_core() as core:
        seq = SimpleSequentialTaskCollection(3)
        seq.attach(core)

        # run until terminated
        while seq.execution.state != Run.State.TERMINATED:
            seq.progress()
        assert seq.stage().jobname == 'stage2'
        assert seq.stage().execution.state == Run.State.TERMINATED

        seq.redo(1)
        assert seq.stage().jobname == 'stage1'
        assert seq.stage().execution.state == Run.State.NEW
        assert seq.execution.state == Run.State.NEW

        # run until terminated, again
        while seq.execution.state != Run.State.TERMINATED:
            seq.progress()
        assert seq.stage().jobname == 'stage2'
        assert seq.stage().execution.state == Run.State.TERMINATED


def test_SequentialTaskCollection_redo3():
    """Test that we can re-do a partially terminated sequence."""
    with temporary_core() as core:
        seq = SimpleSequentialTaskCollection(3)
        seq.attach(core)

        # run until stage1 is terminated
        while seq.tasks[1].execution.state != Run.State.TERMINATED:
            seq.progress()
        assert seq.stage().jobname == 'stage2'

        core.kill(seq)

        seq.redo(0)
        assert seq.stage().jobname == 'stage0'
        assert seq.stage().execution.state == Run.State.NEW
        assert seq.execution.state == Run.State.NEW

        # run until terminated
        while seq.execution.state != Run.State.TERMINATED:
            seq.progress()
        assert seq.stage().jobname == 'stage2'
        assert seq.stage().execution.state == Run.State.TERMINATED


def test_empty_SequentialTaskCollection_progress():
    with temporary_core() as core:
        seq = SimpleSequentialTaskCollection(0)
        seq.attach(core)

        # run until terminated
        while seq.execution.state != Run.State.TERMINATED:
            seq.progress()
        assert seq.execution.state == Run.State.TERMINATED
        assert seq.execution.returncode == 0


# main: run tests

if "__main__" == __name__:
    import pytest
    pytest.main(["-v", __file__])
