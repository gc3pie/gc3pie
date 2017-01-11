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

import os
import gc3libs
from gc3libs.cmdline import SessionBasedDaemon
import inotifyx

class SimpleDaemon(SessionBasedDaemon):
    """Simple daemon"""
    version = '1.0'

    def new_tasks(self, extra, epath=None, emask=0):
        app = None
        if not epath:
            # Startup: just submit an Hello World application
            extra['jobname'] = 'EchoApp'
            app = gc3libs.Application(
                ['/bin/echo', 'first run'],
                [],
                gc3libs.ANY_OUTPUT,
                stdout='stdout.txt',
                stderr='stderr.txt',
                **extra)
            return [app]
        else:
            # A new file has been created. Process it.
            extra['jobname'] = 'LSApp.%s' % os.path.basename(epath.path)
            # inputs = [epath] if epath.scheme == 'file' else []
            inputs = {epath:'foo'}
            if emask & inotifyx.IN_CLOSE_WRITE:
                app = gc3libs.Application(
                    ['/bin/echo', epath],
                    inputs,
                    gc3libs.ANY_OUTPUT,
                    stdout='stdout.txt',
                    stderr='stderr.txt',
                    **extra)
                return [app]

        # No app created, return empty list
        return []

## main: run tests

if "__main__" == __name__:
    from simpledaemon import SimpleDaemon
    SimpleDaemon().run()
