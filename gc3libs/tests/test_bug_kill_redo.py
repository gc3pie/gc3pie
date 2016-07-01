#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012-2013, GC3, University of Zurich. All rights reserved.
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

from helpers import SimpleSequentialTaskCollection, temporary_engine
from nose.tools import assert_equal

def test_kill_redo():
    """Emulate a sequential which is killed and then restarted"""

    # This fixes a bug in GC3Pie when you try to kill and then
    # resubmit a job.
    with temporary_engine() as engine:
        seq = SimpleSequentialTaskCollection(3)
        engine.add(seq)

        while seq.execution.state != 'RUNNING':
            engine.progress()
        assert_equal(
            set([i.execution.state for i in seq.tasks]),
            set(('NEW', 'TERMINATED'))
        )
        engine.kill(seq)

        assert_equal(
            set([i.execution.state for i in seq.tasks]),
            set(('TERMINATED', 'NEW'))
        )
        assert_equal(seq.execution.state, 'RUNNING')
        engine.redo(seq, from_stage=0)
        assert_equal(
            set([i.execution.state for i in seq.tasks]),
            set(('NEW',))
        )
        assert_equal(seq.execution.state, 'NEW')

        while seq.execution.state != 'TERMINATED':
            engine.progress()

        

if __name__ == "__main__":
    import nose
    nose.runmodule()
