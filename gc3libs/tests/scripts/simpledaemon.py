#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2017-2019,  University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
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
from __future__ import absolute_import, print_function, unicode_literals
__docformat__ = 'reStructuredText'

import os
import gc3libs
from gc3libs.cmdline import SessionBasedDaemon


class SimpleDaemon(SessionBasedDaemon):
    """Simple daemon"""
    version = '1.0'

    def new_tasks(self, extra):
        """
        Populate session with initial tasks.
        """
        jobname = 'EchoApp'
        extra['output_dir'] = os.path.join(self.params.working_dir, jobname)
        return [
            gc3libs.Application(
                ['/bin/echo', 'first run'],
                inputs=[],
                outputs=gc3libs.ANY_OUTPUT,
                stdout='stdout.txt',
                stderr='stderr.txt',
                jobname=jobname,
                **extra)
            ]

    def created(self, inbox, subject):
        """
        A new file has been created. Process it.
        """
        path = subject.path
        jobname = ('LSApp.' + os.path.basename(path))
        extra = self.extra.copy()
        extra['output_dir'] = os.path.join(self.params.working_dir, jobname)
        self.add(
            gc3libs.Application(
                ['/bin/echo', path],
                inputs={path:'foo'},
                outputs=gc3libs.ANY_OUTPUT,
                stdout='stdout.txt',
                stderr='stderr.txt',
                jobname=jobname,
                **extra)
        )


## main: run tests

if "__main__" == __name__:
    from simpledaemon import SimpleDaemon
    SimpleDaemon().run()
