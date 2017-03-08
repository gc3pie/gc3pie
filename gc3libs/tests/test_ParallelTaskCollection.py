#! /usr/bin/env python
#
"""
Test class `ParallelTaskCollection`:class:.
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


## imports

from gc3libs import Run, Task
from gc3libs.workflow import ParallelTaskCollection


from gc3libs.testing.helpers import SimpleParallelTaskCollection, SuccessfulApp, UnsuccessfulApp, temporary_core


## tests

def test_ParallelTaskCollection_progress():
    with temporary_core(max_cores=10) as core:
        par = SimpleParallelTaskCollection(5)
        par.attach(core)

        # run until terminated
        while par.execution.state != Run.State.TERMINATED:
            par.progress()
        for task in par.tasks:
            assert task.execution.state == Run.State.TERMINATED


def test_ParallelTaskCollection_redo():
    with temporary_core(max_cores=10) as core:
        par = SimpleParallelTaskCollection(5)
        par.attach(core)

        # run until terminated
        while par.execution.state != Run.State.TERMINATED:
            par.progress()

        par.redo()
        assert par.execution.state == Run.State.NEW
        for task in par.tasks:
            assert task.execution.state == Run.State.NEW


# main: run tests

if "__main__" == __name__:
    import pytest
    pytest.main(["-v", __file__])
