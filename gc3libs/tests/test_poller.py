#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2017-2018, University of Zurich. All rights reserved.
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
__docformat__ = 'reStructuredText'

import os
import shutil
import tempfile
from time import sleep

import pytest

import gc3libs.poller as plr
from gc3libs.utils import write_contents


# for readability
def _check_events(poller, path, expected):
    events = [event for event in poller.get_events()
              if event[0].path == path]
    assert len(events) == len(expected)
    for n, tags in enumerate(expected):
        url, flags = events[n]
        assert url.path == path
        for tag in tags:
            assert (flags & plr.events[tag]) != 0


class TestPollers(object):

    @pytest.fixture(autouse=True)
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        yield
        shutil.rmtree(self.tmpdir)


    def test_filepoller(self):
        poller = plr.FilePoller(self.tmpdir)
        assert self.tmpdir in poller._watched

        # test create file
        fpath = os.path.join(self.tmpdir, 'foo')
        write_contents(fpath, 'test')
        _check_events(poller, fpath, [['IN_CLOSE_WRITE', 'IN_CREATE']])
        assert fpath in poller._watched

        # test remove file
        os.remove(fpath)
        _check_events(poller, fpath, [['IN_DELETE']])
        assert fpath not in poller._watched


    def test_inotifypoller(self):
        poller = plr.INotifyPoller(self.tmpdir)

        # test create file
        fpath = os.path.join(self.tmpdir, 'foo')
        write_contents(fpath, 'test')
        _check_events(poller, fpath, [
            # inotify sends 4 distinct events
            ['IN_CREATE'],
            ['IN_OPEN'],
            ['IN_MODIFY'],
            ['IN_CLOSE_WRITE']
        ])

        # test remove file
        os.remove(fpath)
        _check_events(poller, fpath, [['IN_DELETE']])

## main: run tests

if "__main__" == __name__:
    pytest.main(["-v", __file__])
