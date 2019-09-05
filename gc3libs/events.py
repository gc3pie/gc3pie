#! /usr/bin/env python

"""
Support for communication between parts of code through "events".

This file collects definitions of event classes used across the
library code. The actual subscription and notification mechanisms
come from the implementation of the `Observable/Observer pattern`__
provided by Python's library `generic`__.

.. __: https://en.wikipedia.org/wiki/Observer_pattern
.. __: http://generic.readthedocs.io/en/latest/event_system.html#event-system
"""

# Copyright (C) 2018  University of Zurich. All rights reserved.
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

from __future__ import absolute_import, print_function, unicode_literals
__docformat__ = 'reStructuredText'


# do not make symbols imported from `blinker` public: use of `blinker`
# here is an implementation detail
from blinker import signal as _signal


TaskStateChange = _signal('task_state_change')

TermStatusChange = _signal('task_termstatus_change')
