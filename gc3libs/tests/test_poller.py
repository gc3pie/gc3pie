#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2017-2018,  University of Zurich. All rights reserved.
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
from builtins import object
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
    events = [event for event in poller.get_new_events()
              if event[0].path == path]
    assert len(events) == len(expected)
    for n, expected_event in enumerate(expected):
        url, actual_event = events[n]
        assert url.path == path
        assert actual_event == expected_event


class TestPollers(object):

    @pytest.fixture(autouse=True)
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        yield
        shutil.rmtree(self.tmpdir)


    def check_file_accesspoller(self):
        poller = plr.FilePoller(self.tmpdir)
        assert self.tmpdir in poller._watched

        # test create file
        fpath = os.path.join(self.tmpdir, 'foo')
        write_contents(fpath, 'test')
        _check_events(poller, fpath, ['created'])
        assert fpath in poller._watched

        # since the `mtime` field only has 1-second resolution, we
        # need to pause a bit to ensure the following
        # `.get_new_events()` are able to see any modification
        sleep(2)

        # no new events here
        _check_events(poller, fpath, [])

        # test modify file
        write_contents(fpath, 'test2')
        _check_events(poller, fpath, ['modified'])

        # test remove file
        os.remove(fpath)
        _check_events(poller, fpath, ['deleted'])
        assert fpath not in poller._watched


    def test_inotifypoller(self):
        poller = plr.INotifyPoller(self.tmpdir)

        # test create file
        fpath = os.path.join(self.tmpdir, 'foo')
        write_contents(fpath, 'test')
        _check_events(poller, fpath, ['created'])

        # no new events here
        _check_events(poller, fpath, [])

        # test modify file
        write_contents(fpath, 'test2')
        _check_events(poller, fpath, ['modified'])

        # test remove file
        os.remove(fpath)
        _check_events(poller, fpath, ['deleted'])


## main: run tests

if "__main__" == __name__:
    pytest.main(["-v", __file__])
