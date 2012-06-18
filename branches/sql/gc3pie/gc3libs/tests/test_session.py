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
import csv

import gc3libs.exceptions
from gc3libs.session import Session
from gc3libs.persistence import Persistable, make_store
import gc3libs.persistence.sql
import sqlalchemy
import sqlalchemy.sql as sql

from nose.tools import assert_true, assert_equal, raises, set_trace
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

        fd_job_ids = open(os.path.join(self.s.path, self.s.JOBIDS_DB_FILENAME), 'r')
        ids = [row[0] for row in csv.reader(fd_job_ids)]
        assert_equal(ids, [str(i) for i in self.s.job_ids])
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
        assert os.path.exists(os.path.join(tmpfname, Session.JOBIDS_DB_FILENAME))
        assert os.path.exists(os.path.join(tmpfname, Session.STORE_URL_FILENAME))
        shutil.rmtree(tmpfname)

    @raises(gc3libs.exceptions.LoadError)
    def test_load_external_jobid(self):
        """Check if we are able to load jobid which does not belong to the session"""
        extraid = self.s.store.save(Persistable())
        self.s.load(extraid)

    def test_load_oldstyle_session(self):
        """Check if Session is able to load an old-style session"""
        jobid_filename = self.s.path + '.csv'
        store_directory = os.path.abspath(self.s.path+'.jobs')
        store_url = "file://%s" % store_directory

        # Load the old store
        oldstore = make_store(store_url)
        # save something in it
        jobid = oldstore.save(Persistable())

        try:
            # Create the old-style csv file containing the jobid of
            # the saved object
            jobidfile = open(jobid_filename, 'w')
            jobline = {'jobname': 'test job',
                   'persistent_id' : jobid,
                   'state': 'UNKNWON',
                   'info': ''}
            csv.DictWriter(jobidfile,
                       ['jobname', 'persistent_id', 'state', 'info'],
                      extrasaction='ignore').writerow(jobline)
            jobidfile.close()

            # Create a new session. It should load the old one.
            session = Session(self.s.path)
            session._load_oldstyle_session()

            # Check if the job list is correct
            assert_true(jobid in session.job_ids)
            session.load(jobid)

            # This should create a new-style session, but using the
            # old store
            session.flush()

            assert_true(os.path.isdir(session.path))
            assert_true('job_ids.csv' in os.listdir(session.path))
            assert_true('store.url' in os.listdir(session.path))
            assert_equal(
                gc3libs.utils.read_contents(
                    os.path.join(session.path, 'store.url')),
                store_url)

        finally:
            jobidfile.close()
            os.remove(jobid_filename)
            shutil.rmtree(store_directory)

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
