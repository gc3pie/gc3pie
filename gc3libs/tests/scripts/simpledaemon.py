#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2017-2018, University of Zurich. All rights reserved.
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

import os
import gc3libs
from gc3libs.daemon import SessionBasedDaemon
import inotifyx

class SimpleDaemon(SessionBasedDaemon):
    """Simple daemon"""
    version = '1.0'

    def new_tasks(self, extra, epath=None, emask=0):
        app = None
        if not epath:
            app = gc3libs.Application(
                ['/bin/echo', 'first run'],
                [],
                gc3libs.ANY_OUTPUT,
                stdout='stdout.txt',
                stderr='stderr.txt',
                jobname='EchoApp',
                **extra)
            return [app]
        else:
            # A new file has been created. Process it.
            if emask & inotifyx.IN_CLOSE_WRITE:
                app = gc3libs.Application(
                    ['/bin/echo', epath],
                    inputs={epath:'foo'},
                    outputs=gc3libs.ANY_OUTPUT,
                    stdout='stdout.txt',
                    stderr='stderr.txt',
                    jobname=('LSApp.' + os.path.basename(epath.path)),
                    **extra)
                return [app]

        # No app created, return empty list
        return []

## main: run tests

if "__main__" == __name__:
    from simpledaemon import SimpleDaemon
    SimpleDaemon().run()
