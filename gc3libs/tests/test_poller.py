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
import shutil
import tempfile

import pytest

import gc3libs.poller as plr

class TestPollers(object):
    @pytest.fixture(autouse=True)
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        yield
        shutil.rmtree(self.tmpdir)

    def test_filepoller(self):
        poller = plr.FilePoller(self.tmpdir, 0)
        fpath = os.path.join(self.tmpdir, 'foo')
        with open(fpath, 'w'):
            events = poller.get_events()
            assert len(events) == 1
            url, mask = events[0]
            assert url.path == fpath
            assert mask == plr.events['IN_CLOSE_WRITE']|plr.events['IN_CREATE']
        os.remove(fpath)
        events = poller.get_events()
        assert len(events) == 1
        url, mask = events[0]
        assert url.path == fpath
        assert mask == plr.events['IN_DELETE']

    def test_inotifypoller(self):
        poller = plr.INotifyPoller(self.tmpdir, plr.events['IN_ALL_EVENTS'])
        fpath = os.path.join(self.tmpdir, 'foo')
        fd = open(fpath, 'w')
        fd.close()
        events = poller.get_events()
        # In this case, we will receive 3 events:
        # * create
        # * open
        # * close write
        assert len(events) == 3

        url, mask = events[0]
        assert url.path == fpath
        assert mask == plr.events['IN_CREATE']

        url, mask = events[1]
        assert url.path == fpath
        assert mask == plr.events['IN_OPEN']

        url, mask = events[2]
        assert url.path == fpath
        assert mask == plr.events['IN_CLOSE_WRITE']

        os.remove(fpath)
        events = poller.get_events()
        assert len(events) == 1
        url, mask = events[0]
        assert url.path == fpath
        assert mask == plr.events['IN_DELETE']


## main: run tests

if "__main__" == __name__:
    pytest.main(["-v", __file__])
