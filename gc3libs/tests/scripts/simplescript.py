#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012  University of Zurich. All rights reserved.
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
from __future__ import absolute_import, print_function, unicode_literals
__docformat__ = 'reStructuredText'

from gc3libs.cmdline import SessionBasedScript
from gc3libs import Application


class SimpleScript(SessionBasedScript):

    """stupid class"""
    version = '1'

    def new_tasks(self, extra):

        default_output_dir = extra.pop('output_dir')
        return [
            # old-style
            ('MyJob',
             Application,
               [
                   # arguments
                   ('/bin/bash', '-c', 'echo ciao > SimpleScript.stdout'),
                   # inputs
                   [],
                   # outputs
                   ['SimpleScript.stdout'],
               ],
             dict(output_dir='SimpleScript.out.d', join=True)),
            # new style, with explicit output dir
            Application(
                # arguments
                ('/bin/bash', '-c', 'echo ciao > SimpleScript.stdout'),
                # inputs
                [],
                # outputs
                ['SimpleScript.stdout'],
                # output_dir
                'SimpleScript.out2.d',
                # extra args
                join=True,
                **extra
            ),
            # new style, with output dir gotten from `self.extra`
            Application(
                # arguments
                ('/bin/bash', '-c', 'echo ciao > SimpleScript.stdout'),
                # inputs
                [],
                # outputs
                ['SimpleScript.stdout'],
                join=True,
                output_dir=default_output_dir,
                **extra
            ),
        ]

# main: run tests

if "__main__" == __name__:
    app = SimpleScript()
    app.run()
