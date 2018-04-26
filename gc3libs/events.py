#! /usr/bin/env python
#
"""
Support for communication between parts of code through "events".

This file collects definitions of event classes used across the
library code. The actual subscription and notification mechanisms
come from the implementation of the `Observable/Observer pattern`__
provided by Python's library `generic`__.

.. __: https://en.wikipedia.org/wiki/Observer_pattern
.. __: http://generic.readthedocs.io/en/latest/event_system.html#event-system
"""
# Copyright (C) 2018 University of Zurich
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
__docformat__ = 'reStructuredText'


# import these names here, so we can use `from gc3libs.events import
# subscribe` elsewhere in the code, and leave the dependency on
# `generic.events` as an implementation detail
from generic.event import fire as emit, subscribe, unsubscribe


# FIXME: rewrite with `attrs` when we drop support for Py2.6!
class TaskStateChange(object):
    """
    Fired when a `Task`:class: execution state changes.

    No guarantee is given as to whether *task* is still in the old
    *from_state* or has already transitioned to the new *to_state*.
    """

    __slots__ = ('task', 'from_state', 'to_state')

    def __init__(self, task, from_state, to_state):
        self.task = task
        self.from_state = from_state
        self.to_state = to_state
