#! /usr/bin/env python
#
"""
Session manager daemon: manage and progress tasks in an existing session.

This code basically only instanciates the stock ``SessionBasedDaemon``
class: no new tasks are ever created (as the ``new_tasks`` hook, and the
``created``/``modified``/``deleted`` handlers are not overridden), but one
can still connect to the XML-RPC interface and manage existing tasks.
"""
# Copyright (C) 2018, University of Zurich. All rights reserved.
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

from __future__ import (absolute_import, division, print_function)

from gc3libs.cmdline import SessionBasedDaemon


class SessionManagerDaemon(SessionBasedDaemon):
    """
    Session manager daemon.

    Load and progress all tasks from an existing session.
    Allow managing said tasks via the server's XML-RPC interface.
    """
    version = '1.0'


## main: run server

if "__main__" == __name__:
    from sessionmgr import SessionManagerDaemon
    SessionManagerDaemon().run()
