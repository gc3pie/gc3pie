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

from fnmatch import fnmatch
import os
from os.path import basename
import sys

import gc3libs
from gc3libs.cmdline import SessionBasedDaemon, executable_file


class InboxProcessingDaemon(SessionBasedDaemon):
    """
    Run a given command on all files created within the given inboxj
    directories.  Each command runs as a separate GC3Pie task.
    Task and session management is possible using the usual server
    XML-RPC interface.
    """

    # setting `version` is required to instanciate the
    # `SessionBasedDaemon` class
    version = '1.0'

    # add command-line options specific to this script
    def setup_options(self):
        self.add_param('-p', '--pattern', metavar='PATTERN',
                       action='store', default=None,
                       help=("Only process files that match this pattern."))


    # set up processing of positional arguments on the command line
    def setup_args(self):
        self.add_param('program', metavar='PROGRAM',
                       action='store', type=executable_file,
                       help=("Run this program on each file"
                             " that is created in the inbox(es)."))

        # last arg is always INBOX [INBOX ...] -- changing it would
        # require overriding `_start_inboxes()` as well
        super(InboxProcessingDaemon, self).setup_args()


    def created(self, inbox, subject):
        """
        Add a new task for each file that is created in the inbox.
        """
        input_name = basename(subject.path)
        if pattern and not fnmatch(input_name, pattern):
            # do nothing if file does not patch the given pattern
            return

        # `self.extra` contains task parameters that may be set from
        # the command-line, like base output directory, or
        # wall-time/RAM limits, etc.
        extra = self.extra.copy()
        extra.setdefault('output_dir', input_name + '.out')

        self.add(
            Application(
                [self.params.program, input_name],
                inputs=[subject.path],
                outputs=gc3libs.ANY_OUTPUT,
                stdout='execution_log.txt',
                stderr='execution_log.txt',
                **extra))


## main: run server

if "__main__" == __name__:
    from inbox_server import InboxProcessingDaemon
    InboxProcessingDaemon().run()
