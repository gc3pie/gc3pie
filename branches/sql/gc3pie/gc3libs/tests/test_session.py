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

import os
import tempfile
import cPickle as Pickle

from gc3libs.session import Session
from gc3libs.persistence import Persistable
import gc3libs.persistence.sql
import sqlalchemy.sql as sql

from nose.tools import assert_true, assert_equal


class TestSession(object):
    def setUp(self):
        tmpfname = tempfile.mktemp(dir='.')
        self.tmpfname = tmpfname
        self.s = Session(tmpfname)

    def tearDown(self):
        self.s.remove_session()

    def test_directory_creation(self):
        self.s.save_session()
        assert_true(os.path.isdir(self.s.path))
        assert_true(os.path.samefile(
            os.path.join(
            os.path.abspath('.'),
            self.tmpfname), self.s.path))

    def test_store_url(self):
        self.s.save_session()
        storefile = os.path.join(self.s.path, Session.STORE_URL_FILENAME)
        assert_true(os.path.isfile(storefile))

    def test_load_and_save(self):
        assert_equal(self.s.store.list(), [])

        self.s.save(Persistable())
        self.s.save_session()

        fd_job_ids = open(os.path.join(self.s.path, 'job_ids.db'), 'r')
        ids = Pickle.load(fd_job_ids)
        assert_equal(ids, self.s.job_ids)
        assert_equal(len(ids),  1)

    def test_reload_session(self):
        self.s.save(Persistable())
        self.s.save_session()
        s2 = Session(self.s.path)
        s2.job_ids == self.s.job_ids


class TestSqlSession(TestSession):
    def setUp(self):
        tmpfname = tempfile.mktemp(dir='.')
        self.tmpfname = os.path.relpath(tmpfname)
        self.s = Session(
            tmpfname,
            store_url="sqlite:////%s/store.db" % os.path.abspath(self.tmpfname))

    def test_sqlite_store(self):
        jobid = self.s.save(Persistable())
        self.s.save_session()

        q = sql.select(
            [self.s.store.t_store.c.id]
            ).where(
            self.s.store.t_store.c.id == jobid
            )
        conn = self.s.store._SqlStore__engine.connect()
        results = conn.execute(q)
        rows = results.fetchall()
        assert_equal(len(rows), 1)
        assert_equal(rows[0][0], jobid)

## main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()
