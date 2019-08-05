#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012, 2019  University of Zurich. All rights reserved.
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
from builtins import object
__docformat__ = 'reStructuredText'

# System imports
import os
import getpass
from tempfile import NamedTemporaryFile

# Nose imports
import pytest

# GC3 imports
from gc3libs.backends import transport
from gc3libs.exceptions import TransportError


class StubForTestTransport(object):

    def extra_setup(self):
        (exitcode, tmpdir, stderror) = self.transport.execute_command(
            'mktemp -d /tmp/test_transport.XXXXXXXXX')
        self.tmpdir = tmpdir.strip()

    def tearDown(self):
        self.transport.remove_tree(self.tmpdir)
        self.transport.close()

    def test_get_remote_username(self):
        user = getpass.getuser()
        assert user == self.transport.get_remote_username()

    def test_isdir(self):
        assert self.transport.isdir(self.tmpdir)

    def test_listdir(self):
        assert len(self.transport.listdir(self.tmpdir)) == 0

    def test_makedirs(self):
        self.transport.makedirs(os.path.join(self.tmpdir, 'testdir'))
        children = self.transport.listdir(self.tmpdir)
        assert children == ['testdir']

    def test_recursive_makedirs(self):
        self.transport.makedirs(os.path.join(self.tmpdir, 'testdir/testdir2'))
        children = self.transport.listdir(self.tmpdir)
        assert children == ['testdir']

        nephews = self.transport.listdir(
            os.path.join(self.tmpdir, 'testdir'))
        assert nephews == ['testdir2']

    def test_open(self):
        fd = self.transport.open(os.path.join(self.tmpdir, 'testfile'), 'w+')
        fd.write("GC3")
        fd.close()

        fd = self.transport.open(os.path.join(self.tmpdir, 'testfile'), 'r')
        assert fd.read() == "GC3"

    def test_remove(self):
        fd = self.transport.open(os.path.join(self.tmpdir, 'testfile'), 'w+')
        fd.close()
        self.transport.remove(os.path.join(self.tmpdir, 'testfile'))
        assert self.transport.listdir(self.tmpdir) == []

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
        with NamedTemporaryFile(prefix='gc3libs.test.',
                                suffix='.tmp',
                                mode='wt',
                                delete=False) as tmpfile:
            try:
                path = tmpfile.name  # save for later

                # create file with some content
                tmpfile.write("Test file")
                tmpfile.close()

                # copy it to the remote end using Transport.put()
                destfile = os.path.join(self.tmpdir, os.path.basename(path))
                self.transport.put(path, destfile)
                # remove the local file
                os.remove(path)

                # get the file using Transport.get()
                self.transport.get(destfile, path)

                # check the content
                assert open(path).read() == "Test file"

            finally:
                try:
                    os.unlink(path)
                except:
                    pass

    def test_open_failure_nonexistent_file(self):
        with pytest.raises(TransportError):
            # pylint: disable=invalid-name,unused-variable
            fd = self.transport.open(
                os.path.join(self.tmpdir, 'nonexistent'), 'r')

    def test_open_failure_unauthorized(self):
        # we cannot rely on *any* file being unreadable, as tests may
        # be running as `root` (e.g., in a Docker container), so we
        # need to explicitly create one
        with NamedTemporaryFile(prefix='gc3libs.test.',
                                suffix='.tmp',
                                mode='wt',
                                delete=False) as tmpfile:
            try:
                path = tmpfile.name  # save for later

                fd = tmpfile.fileno()
                os.fchmod(fd, 0o000)
                tmpfile.close()

                # now re-open with normal Python functions
                with pytest.raises(TransportError):
                    # pylint: disable=no-member
                    with self.transport.open(path, 'r') as stream:
                        assert stream is False
            finally:
                try:
                    os.unlink(path)
                except:
                    pass

    def test_remove_failure(self):
        with pytest.raises(TransportError):
            # pylint: disable=no-member
            self.transport.remove(
                os.path.join(self.tmpdir, 'nonexistent'))


class TestLocalTransport(StubForTestTransport):

    @pytest.fixture(autouse=True)
    def setUp(self):
        self.transport = transport.LocalTransport()
        self.transport.connect()
        self.extra_setup()

@pytest.mark.skipif(
    'SshTransport' not in os.environ.get('GC3PIE_TESTS_ALLOW', ''),
    reason=("Skipping SSH test: SSH to localhost not allowed"
            " (set env variable `GC3PIE_TESTS_ALLOW` to `SshTransport` to run)"))
class TestSshTransport(StubForTestTransport):

    @pytest.fixture(autouse=True)
    def setUp(self):
        self.transport = transport.SshTransport('localhost',
                                                ignore_ssh_host_keys=True)
        self.transport.connect()
        self.extra_setup()

# main: run tests

if __name__ == "__main__":
    pytest.main(["-v", __file__])
