#! /usr/bin/env python
#
"""
Interactive client shell for the XML-RPC server.

This code demoes how the stock `DaemonClient` class can be adapted to
provide an interactive "shell" for running commands through the
XML-RPC interface.

To keep the code simple, interactivity features are kept to an
absolute minimum; in particular, there is no line editing, nor history
recording, nor scripting support.
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

import os
import sys

from gc3libs.cmdline import DaemonClient


class InteractiveClient(DaemonClient):
    """
    Interactive client shell for the XML-RPC server.

    Prompt user for a command line, split it at whitespace, and run
    the resulting command (with optional arguments) on the server.
    Any result returned by the server is immediately displayed on the
    console.

    Interactivity features are very limited though: no history or
    editing is possible.
    """

    # the COMMAND and ARGS positional arguments are not needed in this
    # interactive shell, so override `DaemonClient.setup_args()` to
    # only require the SERVER connect string.
    def setup_args(self):
        """
        Define positional command-line arguments.
        """
        self.add_param('server', metavar='SERVER',
                       help=("Path to the file containing hostname and port of the"
                             " XML-RPC enpdoint of the daemon"))

    def main(self):
        server = self._connect_to_server(self.params.server)
        if server is None:
            return os.EX_NOHOST
        # print banner
        print("""
Connected: {0}

Type `help` followed by a newline to get a list of commands available
on the server.

Press Ctrl+C at the prompt to exit.

        """.format(str(server)))
        # `str(server)` looks like: <ServerProxy for 127.0.0.1:34847/>
        prompt = '{0}>'.format(str(server)[17:-2])
        while True:
            print(prompt, end=' ')
            try:
                argv = sys.stdin.readline().strip().split()
            except EOFError:
                break
            # ignore empty lines
            if not argv:
                continue
            cmd = argv.pop(0)
            self._run_command(server, cmd, *argv)
            print()


if "__main__" == __name__:
    InteractiveClient().run()
