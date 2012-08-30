#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012, GC3, University of Zurich. All rights reserved.
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

from gc3libs.cmdline import SessionBasedScript
from gc3libs import Application


class SimpleScript(SessionBasedScript):
    """stupid class"""
    version = '1'

    def new_tasks(self, extra):
        def myfunc(*args, **kw):
            return Application(args[0],
                               args[1:],
                               [],
                               ['SimpleScript.stdout'],
                               'SimpleScript.out.d')
        yield ('MyJob',
               myfunc,
               ('/bin/bash', '-c', '/bin/echo ciao > SimpleScript.stdout'),
               {})

## main: run tests

if "__main__" == __name__:
    app = SimpleScript()
    app.run()
