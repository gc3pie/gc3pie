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

# System imports
import getpass

# Nose imports
from nose.tools import assert_true, assert_false, assert_equal, raises, set_trace
from nose.plugins.skip import SkipTest

# GC3 imports
from gc3libs.backends import transport
from gc3libs.exceptions import TransportError

class StubForTestTransport:

    def setUp(self):
        self.transport = None

    def tearDown(self):
        self.transport.close()

    def test_get_remote_username(self):
        user = getpass.getuser()
        assert_equal(user, self.transport.get_remote_username())

class TestLocalTransport(StubForTestTransport):
    def setUp(self):
        self.transport = transport.LocalTransport()
        self.transport.connect()

class TestSshTransport(StubForTestTransport):
    def setUp(self):
        self.transport = transport.SshTransport('localhost')
        try:
            self.transport.connect()
        except TransportError:
            raise SkipTest("Unable to connect to localhost via ssh. Please enable passwordless authentication to localhost in order to pass this test.")

## main: run tests

if __name__ == "__main__":
    import nose
    nose.runmodule()
