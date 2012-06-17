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
import shutil
import tempfile
import cPickle as Pickle

import gc3libs.exceptions
from gc3libs.session import Session
from gc3libs.persistence import Persistable
import gc3libs.persistence.sql
import sqlalchemy
import sqlalchemy.sql as sql

from nose.tools import assert_true, assert_equal, raises
from nose.plugins.skip import SkipTest



class TestSession(object):
    def setUp(self):
        tmpfname = tempfile.mktemp(dir='.')
        self.tmpfname = tmpfname
        self.s = Session(tmpfname)

    def tearDown(self):
        try:
            self.s.remove_session()
        except:
            # after test_remove_session() we will get an error
            pass

    def test_directory_creation(self):
        self.s.flush()
        assert_true(os.path.isdir(self.s.path))
        assert_true(os.path.samefile(
            os.path.join(
            os.path.abspath('.'),
            self.tmpfname), self.s.path))

    def test_store_url(self):
        self.s.flush()
        storefile = os.path.join(self.s.path, Session.STORE_URL_FILENAME)
        assert_true(os.path.isfile(storefile))

    def test_load_and_save(self):
        self.s.save(Persistable())
        self.s.flush()

        fd_job_ids = open(os.path.join(self.s.path, self.s.JOBIDS_DB), 'r')
        ids = Pickle.load(fd_job_ids)
        assert_equal(ids, self.s.job_ids)
        assert_equal(len(ids),  1)

    def test_reload_session(self):
        self.s.save(Persistable())
        self.s.flush()
        s2 = Session(self.s.path)
        s2.job_ids == self.s.job_ids

    @raises(gc3libs.exceptions.LoadError,sqlalchemy.exc.OperationalError)
    def test_remove_session(self):
        jobid = self.s.save(Persistable())
        self.s.flush()
        self.s.remove_session()
        self.s.load(jobid)

    def test_incomplete_session_dir(self):
        tmpfname = tempfile.mktemp(dir='.')
        os.mkdir(tmpfname)
        incomplete_s = Session(tmpfname)
        assert os.path.exists(os.path.join(tmpfname, Session.JOBIDS_DB))
        assert os.path.exists(os.path.join(tmpfname, Session.STORE_URL_FILENAME))
        shutil.rmtree(tmpfname)

    @raises(gc3libs.exceptions.LoadError)
    def test_load_external_jobid(self):
        """Check if we are able to load jobid which does not belong to the session"""
        extraid = self.s.store.save(Persistable())
        self.s.load(extraid)


class StubForSqlSession(TestSession):

    def test_sqlite_store(self):
        jobid = self.s.save(Persistable())
        self.s.flush()

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


class TestSqliteSession(StubForSqlSession):

    def setUp(self):
        tmpfname = tempfile.mktemp(dir='.')
        self.tmpfname = os.path.basename(tmpfname)
        self.s = Session(
                tmpfname,
                store_url="sqlite:////%s/store.db" % os.path.abspath(self.tmpfname))

    def tearDown(self):
        if os.path.exists(self.tmpfname):
            shutil.rmtree(self.tmpfname)


class TestMysqlSession(StubForSqlSession):

    @classmethod
    def setup_class(cls):
        # we skip MySQL tests if no MySQLdb module is present
        try:
            import MySQLdb
        except:
            raise SkipTest("MySQLdb module not installed.")

    def setUp(self):
        tmpfname = tempfile.mktemp(dir='.')
        self.tmpfname = os.path.basename(tmpfname)
        try:
            self.s = Session(
                tmpfname,
                store_url="mysql://gc3user:gc3pwd@localhost/gc3")
        except sqlalchemy.exc.OperationalError:
            raise SkipTest("Cannot connect to MySQL database.")

    def tearDown(self):
        self.s.remove_session()


## main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()
