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

import cli.test
from nose.tools import assert_true

import gc3libs.cmdline
import gc3libs.session

class TestScript(cli.test.FunctionalTest):
    def __init__(self, *args, **extra_args):
        cli.test.FunctionalTest.__init__(self, *args, **extra_args)
        self.scriptdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')


    def setUp(self):
        cli.test.FunctionalTest.setUp(self)
        CONF_FILE="""
[auth/dummy]
type = ssh
username = dummy

[resource/localhost]
enabled = true
auth = dummy
type = subprocess
frontend = localhost
transport = local
max_cores_per_job = 2
max_memory_per_core = 2
max_walltime = 8
max_cores = 2
architecture = x86_64
"""
        (fd, self.cfgfile) = tempfile.mkstemp()
        f = os.fdopen(fd, 'w')
        f.write(CONF_FILE)
        f.close()

    def tearDown(self):
        os.remove(self.cfgfile)
        cli.test.FunctionalTest.tearDown(self)

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
                                 '--config-files', self.cfgfile,
                                 '-r', 'localhost')

        assert_true(re.match('.*TERMINATED\s+3/3\s+\(100.0+%\).*', result.stdout, re.S))

        # FIXME: output dir should be inside session dir
        session_dir = os.path.join(self.env.base_path, 'TestOne')
        assert_true(
            os.path.isdir(
                os.path.join(self.env.base_path, 'SimpleScript.out.d')
                )
            )
        assert_true(
            os.path.isfile(
                os.path.join(self.env.base_path, 'SimpleScript.out.d', 'SimpleScript.stdout')
                )
            )

        assert_true(
            os.path.isdir(
                os.path.join(self.env.base_path, 'SimpleScript.out2.d')
                )
            )
        assert_true(
            os.path.isfile(
                os.path.join(self.env.base_path, 'SimpleScript.out2.d', 'SimpleScript.stdout')
                )
            )

        assert_true(
            os.path.isdir(session_dir)
            )

        assert_true(
            os.path.isfile(
                os.path.join(session_dir, gc3libs.session.Session.INDEX_FILENAME, )
                )
            )

        assert_true(
            os.path.isfile(
                os.path.join(session_dir, gc3libs.session.Session.STORE_URL_FILENAME, )
                )
            )


## main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()
