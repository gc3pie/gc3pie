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

# stdlib imports
import csv
import os
import shutil
import tempfile

from nose.tools import assert_true, assert_equal, raises, set_trace
from nose.plugins.skip import SkipTest

import sqlalchemy
import sqlalchemy.sql as sql

# GC3Pie imports
import gc3libs.exceptions
from gc3libs.persistence import Persistable, make_store
import gc3libs.persistence.sql
from gc3libs.session import Session
from gc3libs.utils import Struct


class _PStruct(Struct, Persistable):
    """
    A Persistable+Struct mix-in.

    This class just exists so that we are able to persist something in
    the tests and make a non-trivial equality check afterwards.
    """
    pass


def test_create():
    tmpdir = tempfile.mktemp(dir='.')
    sess = Session(tmpdir)
    assert os.path.isdir(sess.path)

@raises(gc3libs.exceptions.LoadError,sqlalchemy.exc.OperationalError)
def test_destroy():
    tmpdir = tempfile.mktemp(dir='.')
    sess = Session(tmpdir)
    tid = sess.add(_PStruct(a=1, b='foo'))
    sess.destroy()
    # destroy should kill all traces of the sessiondir
    assert not os.path.exists(sess.path)
    # in particular, no task can be loaded
    sess.load(tid)


class TestOldstyleConversion:

    def setUp(self):
        self.path = tempfile.mktemp(dir='.')
        self.jobs_dir = os.path.abspath(self.path+'.jobs')
        # create old-style session
        self.index_csv = self.path + '.csv'
        # Load the old store
        store_url = "file://%s" % self.jobs_dir
        oldstore = make_store(store_url)
        # save something in it
        self.test_task_id = oldstore.save(_PStruct(a=1, b='foo'))
        jobidfile = open(self.index_csv, 'w')
        jobline = {
            'jobname':       'test',
            'persistent_id': self.test_task_id,
            'state':         'UNKNOWN',
            'info':          '',
            }
        csv.DictWriter(
            jobidfile,
            ['jobname', 'persistent_id', 'state', 'info'],
            extrasaction='ignore').writerow(jobline)
        jobidfile.close()
        # create new-style session
        self.sess = Session(self.path)

    def tearDown(self):
        if os.path.exists(self.index_csv):
            os.remove(self.index_csv)
        if os.path.exists(self.jobs_dir):
            shutil.rmtree(self.jobs_dir)
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def test_load_oldstyle_session(self):
        """Check that Session is able to load an old-style session"""
        # Check if the job list is correct
        assert_true(self.test_task_id in self.sess.tasks)
        assert_equal(self.sess.load(self.test_task_id), self.sess.tasks[self.test_task_id])

    def test_convert_oldstyle_session(self):
        assert os.path.isdir(self.sess.path)
        assert not os.path.exists(self.index_csv)
        assert not os.path.exists(self.jobs_dir)


class TestSession(object):
    def setUp(self):
        tmpdir = tempfile.mktemp(dir='.')
        self.tmpdir = tmpdir
        self.sess = Session(tmpdir)

    def tearDown(self):
        self.sess.destroy()

    def test_session_directory_created(self):
        assert_true(os.path.isdir(self.sess.path))
        assert_true(os.path.samefile(
            os.path.join(os.path.abspath('.'), self.tmpdir),
            self.sess.path))

    def test_store_url_file_exists(self):
        self.sess.flush()
        storefile = os.path.join(self.sess.path, Session.STORE_URL_FILENAME)
        assert_true(os.path.isfile(storefile))

    def test_add(self):
        tid = self.sess.add(_PStruct(a=1, b='foo'))
        assert_equal(len(self.sess), 1)
        assert_equal([tid], self.sess.list())

    def test_add_updates_metadata(self):
        """Check that on-disk metadata is changed on add(..., flush=True)."""
        self.sess.add(_PStruct(a=1, b='foo'), flush=True)
        fd_job_ids = open(os.path.join(self.sess.path, self.sess.INDEX_FILENAME), 'r')
        ids = fd_job_ids.read().split('\n')
        assert_equal(len(ids),  1)
        assert_equal(ids, [str(i) for i in self.sess.tasks])

    def test_add_no_flush(self):
        """Check that on-disk metadata is not changed on add(..., flush=False)."""
        tid = self.sess.add(_PStruct(a=1, b='foo'), flush=False)
        # in-memory metadata is updated
        assert_equal(len(self.sess), 1)
        assert_equal([tid], self.sess.list())
        # on-disk metadata is not
        fd_job_ids = open(os.path.join(self.sess.path, self.sess.INDEX_FILENAME), 'r')
        assert_equal('', fd_job_ids.read())

    def test_remove(self):
        # add tasks
        tid1 = self.sess.add(_PStruct(a=1, b='foo'))
        tid2 = self.sess.add(_PStruct(a=1, b='foo'))
        assert_equal(len(self.sess), 2)
        self.sess.remove(tid1)
        assert_equal(len(self.sess), 1)
        self.sess.remove(tid2)
        assert_equal(len(self.sess), 0)

    def test_reload_session(self):
        self.sess.add(_PStruct(a=1, b='foo'))
        sess2 = Session(self.sess.path)
        assert_equal(len(sess2), 1)
        for task2, task1 in zip(sess2.tasks.values(), self.sess.tasks.values()):
            assert_equal(task1, task2)

    def test_incomplete_session_dir(self):
        tmpdir = tempfile.mktemp(dir='.')
        os.mkdir(tmpdir)
        incomplete_sess = Session(tmpdir)
        assert os.path.exists(os.path.join(tmpdir, Session.INDEX_FILENAME))
        assert os.path.exists(os.path.join(tmpdir, Session.STORE_URL_FILENAME))
        incomplete_sess.destroy()

    def test_load_external_jobid(self):
        """Check that we are able to load an object which does not belong to the session"""
        obj1 = _PStruct(a=1, b='foo')
        extraid = self.sess.store.save(obj1)
        obj2 = self.sess.load(extraid)
        assert_equal(obj1, obj2)


class StubForSqlSession(TestSession):

    def test_sqlite_store(self):
        jobid = self.sess.save(_PStruct(a=1, b='foo'))
        self.sess.flush()

        q = sql.select(
            [self.sess.store.t_store.c.id]
            ).where(
            self.sess.store.t_store.c.id == jobid
            )
        conn = self.sess.store._SqlStore__engine.connect()
        results = conn.execute(q)
        rows = results.fetchall()
        assert_equal(len(rows), 1)
        assert_equal(rows[0][0], jobid)


class TestSqliteSession(StubForSqlSession):

    def setUp(self):
        tmpdir = tempfile.mktemp(dir='.')
        self.tmpdir = os.path.basename(tmpdir)
        self.sess = Session(
                tmpdir,
                store_url="sqlite:///%s/store.db" % os.path.abspath(self.tmpdir))

    def tearDown(self):
        if os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)


class TestMysqlSession(StubForSqlSession):

    @classmethod
    def setup_class(cls):
        # we skip MySQL tests if no MySQLdb module is present
        try:
            import MySQLdb
        except:
            raise SkipTest("MySQLdb module not installed.")

    def setUp(self):
        tmpdir = tempfile.mktemp(dir='.')
        self.tmpdir = os.path.basename(tmpdir)
        try:
            self.sess = Session(
                tmpdir,
                store_url="mysql://gc3user:gc3pwd@localhost/gc3")
        except sqlalchemy.exc.OperationalError:
            raise SkipTest("Cannot connect to MySQL database.")

    def tearDown(self):
        self.sess.remove_session()


## main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()
