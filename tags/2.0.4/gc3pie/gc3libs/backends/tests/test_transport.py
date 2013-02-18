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
import os
import getpass
import tempfile

# Nose imports
from nose.tools import assert_true, assert_false, assert_equal, raises, set_trace
from nose.plugins.skip import SkipTest

# GC3 imports
from gc3libs.backends import transport
from gc3libs.exceptions import TransportError

class StubForTestTransport:

    def extraSetup(self):
        (exitcode, tmpdir, stderror) = self.transport.execute_command('mktemp -d')
        self.tmpdir = tmpdir.strip()

    def tearDown(self):
        self.transport.remove_tree(self.tmpdir)
        self.transport.close()

    def test_get_remote_username(self):
        user = getpass.getuser()
        assert_equal(user, self.transport.get_remote_username())

    def test_isdir(self):
        assert_true(self.transport.isdir(self.tmpdir))

    def test_listdir(self):
        assert_equal(len(self.transport.listdir(self.tmpdir)), 0)

    def test_makedirs(self):
        self.transport.makedirs(os.path.join(self.tmpdir, 'testdir'))
        children = self.transport.listdir(self.tmpdir)
        assert_equal(children, ['testdir'])

    def test_recursive_makedirs(self):
        self.transport.makedirs(os.path.join(self.tmpdir, 'testdir/testdir2'))
        children = self.transport.listdir(self.tmpdir)
        assert_equal(children, ['testdir'])

        nephews = self.transport.listdir(
            os.path.join(self.tmpdir, 'testdir'))
        assert_equal(nephews, ['testdir2'])

    def test_open(self):
        fd = self.transport.open(os.path.join(self.tmpdir, 'testfile'), 'w+')
        fd.write("GC3")
        fd.close()

        fd = self.transport.open(os.path.join(self.tmpdir, 'testfile'), 'r')
        assert_equal(fd.read(), "GC3")

    def test_remove(self):
        fd = self.transport.open(os.path.join(self.tmpdir, 'testfile'), 'w+')
        fd.close()
        self.transport.remove(os.path.join(self.tmpdir, 'testfile'))
        assert_equal(self.transport.listdir(self.tmpdir), [])

    def test_chmod(self):
        remotefile = os.path.join(self.tmpdir, 'unauth')
        fd = self.transport.open(remotefile, 'w+')
        fd.close()
        
        self.transport.chmod(remotefile, 0000)

        try:
            fd = self.transport.open(remotefile, 'r')
        except TransportError:
            pass

    def test_get_and_put(self):
        # create a local temporary file
        (fd, tmpfile) = tempfile.mkstemp()
        try:
            os.write(fd, "Test file")
            os.close(fd)

            # copy it to the remote end using Transport.put()
            destfile = os.path.join(self.tmpdir, os.path.basename(tmpfile))
            self.transport.put(tmpfile, destfile)
            # remove the local file
            os.remove(tmpfile)

            # get the file using Transport.get()
            self.transport.get(destfile, tmpfile)

            # check the content
            fd = open(tmpfile)
            assert_equal(fd.read(), "Test file")
        finally:
            os.remove(tmpfile)

    @raises(TransportError)
    def test_open_failure_nonexistent_file(self):
        fd = self.transport.open(
            os.path.join(self.tmpdir, 'nonexistent'), 'r')

    @raises(TransportError)
    def test_open_failure_unauthorized(self):
        fd = self.transport.open('/proc/kcore', 'r')

    @raises(TransportError)
    def test_remove_failure(self):
        self.transport.remove(
            os.path.join(self.tmpdir, 'nonexistent'))

class TestLocalTransport(StubForTestTransport):
    def setUp(self):
        self.transport = transport.LocalTransport()
        self.transport.connect()
        StubForTestTransport.extraSetup(self)



class TestSshTransport(StubForTestTransport):
    def setUp(self):
        self.transport = transport.SshTransport('localhost')
        try:
            self.transport.connect()
        except TransportError:
            raise SkipTest("Unable to connect to localhost via ssh. Please enable passwordless authentication to localhost in order to pass this test.")
        StubForTestTransport.extraSetup(self)

## main: run tests

if __name__ == "__main__":
    import nose
    nose.runmodule()
