#! /usr/bin/env python
#
"""
Tests for the cmdline module
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

import os
import tempfile
import re

import gc3libs.cmdline

import cli.test
from nose.tools import assert_true


class TestScript(cli.test.FunctionalTest):
    def __init__(self, *args, **kw):
        cli.test.FunctionalTest.__init__(self, *args, **kw)
        self.scriptdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')

    def test_simplescript(self):
        """Test a very simple script based on `SessionBasedScript`:class:

        The script is found in ``gc3libs/tests/scripts/simplescript.py``
        """
        # XXX: WARNING: This will only work if you have a "localhost"
        # section in gc3pie.conf!!!
        result = self.run_script('python',
                                 os.path.join(self.scriptdir, 'simplescript.py'),
                                 '-C', '1',
                                 '-s', 'TestOne',
                                 '-r', 'localhost')

        assert_true(re.match('.*TERMINATED\s+1/1\s+\(100.0%\).*', result.stdout, re.S))

        assert_true(
            os.path.isdir(
                os.path.join(self.env.base_path, 'SimpleScript.out.d')
                )
            )

        assert_true(
            os.path.isdir(
                os.path.join(self.env.base_path, 'TestOne.jobs')
                )
            )

## main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()
