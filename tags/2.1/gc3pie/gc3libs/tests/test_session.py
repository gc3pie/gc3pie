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

from nose.tools import assert_true, assert_false, assert_equal, raises
from nose.tools import set_trace
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
    try:
        sess = Session(tmpdir)
        assert_true(os.path.isdir(sess.path))
        sess.destroy()
    except:
        if os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)
        raise


@raises(gc3libs.exceptions.LoadError, sqlalchemy.exc.OperationalError)
def test_destroy():
    tmpdir = tempfile.mktemp(dir='.')
    try:
        sess = Session(tmpdir)
        tid = sess.add(_PStruct(a=1, b='foo'))
        sess.destroy()
        # destroy should kill all traces of the sessiondir
        assert_false(os.path.exists(sess.path))
        # in particular, no task can be loaded
        sess.load(tid)
    except:
        if os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)
        raise


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
            'info':          '', }
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
        assert_equal(self.sess.load(self.test_task_id),
                     self.sess.tasks[self.test_task_id])

    def test_convert_oldstyle_session(self):
        assert_true(os.path.isdir(self.sess.path))
        assert_false(os.path.exists(self.index_csv))
        assert_false(os.path.exists(self.jobs_dir))
        assert_true(self.sess.created > 0)


class TestSession(object):
    def setUp(self):
        tmpdir = tempfile.mktemp(dir='.')
        self.tmpdir = tmpdir
        self.sess = Session(tmpdir)
        self.extra_args = {}

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
        assert_equal([tid], self.sess.list_ids())

    def test_add_updates_metadata(self):
        """Check that on-disk metadata is changed on add(..., flush=True)."""
        self.sess.add(_PStruct(a=1, b='foo'), flush=True)
        fd_job_ids = open(os.path.join(self.sess.path,
                                       self.sess.INDEX_FILENAME), 'r')
        ids = fd_job_ids.read().split()
        assert_equal(len(ids),  1)
        assert_equal(ids, [str(i) for i in self.sess.tasks])

    def test_empty_lines_in_index_file(self):
        """Check that the index file is read correctly even when there
        are empty lines.

        Because of a bug, in some cases Session used to create invalid
        job ids equals to ''
        """
        self.sess.add(_PStruct(a=1, b='foo'), flush=True)
        fd_job_ids = open(os.path.join(self.sess.path,
                                       self.sess.INDEX_FILENAME), 'a')
        fd_job_ids.write('\n\n\n')
        if hasattr(self, 'extra_args'):
            self.sess = Session(self.sess.path, **self.extra_args)
        else:
            self.sess = Session(self.sess.path)
        ids = self.sess.list_ids()
        assert_equal(len(ids),  1)
        assert_equal(ids, [str(i) for i in self.sess.tasks])

    def test_add_no_flush(self):
        """Check that metadata is not changed on add(..., flush=False)."""
        tid = self.sess.add(_PStruct(a=1, b='foo'), flush=False)
        # in-memory metadata is updated
        assert_equal(len(self.sess), 1)
        assert_equal([tid], self.sess.list_ids())
        # on-disk metadata is not
        fd_job_ids = open(os.path.join(self.sess.path,
                                       self.sess.INDEX_FILENAME), 'r')
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

    def test_remove_children(self):
        """
        Test if the session is able to remove all children of a task
        """
        # Removing objects
        obj = _PStruct(name='GC3')
        obj.tasks = [_PStruct(name='GC3')]
        id = self.sess.add(obj)
        self.sess.remove(id)

        assert_equal(len(self.sess.store.list()), 0)

    def test_reload_session(self):
        self.sess.add(_PStruct(a=1, b='foo'))
        self.sess.add(_PStruct(a=2, b='bar'))
        self.sess.add(_PStruct(a=3, b='baz'))
        if hasattr(self, 'extra_args'):
            sess2 = Session(self.sess.path, **self.extra_args)
        else:
            sess2 = Session(self.sess.path)
        assert_equal(len(sess2), 3)
        for task_id in sess2.tasks.iterkeys():
            task = sess2.store.load(task_id)
            assert_equal(task, sess2.tasks[task_id])
        for task2_id, task1_id in zip(sorted(sess2.tasks.keys()),
                                      sorted(self.sess.tasks.keys())):
            assert_equal(self.sess.tasks[task1_id],
                         sess2.tasks[task2_id])

    def test_incomplete_session_dir(self):
        tmpdir = tempfile.mktemp(dir='.')
        os.mkdir(tmpdir)
        incomplete_sess = Session(tmpdir)
        assert_true(os.path.exists(os.path.join(tmpdir,
                                                Session.INDEX_FILENAME)))
        assert_true(os.path.exists(os.path.join(tmpdir,
                                                Session.STORE_URL_FILENAME)))
        incomplete_sess.destroy()

    def test_load_external_jobid(self):
        """Check if we are able to load an object not belonging to the session
        """
        obj1 = _PStruct(a=1, b='foo')
        extraid = self.sess.store.save(obj1)
        obj2 = self.sess.load(extraid)
        assert_equal(obj1, obj2)
        # remove object from the store, since self.sess.destroy() will
        # not remove it!
        self.sess.store.remove(extraid)

    def test_creation_of_timestamp_files(self):
        start_file = os.path.join(self.sess.path,
                                  self.sess.TIMESTAMP_FILES['start'])
        end_file = os.path.join(self.sess.path,
                                self.sess.TIMESTAMP_FILES['end'])

        assert_true(os.path.exists(start_file))
        assert_false(os.path.exists(end_file))

        assert_equal(os.stat(start_file).st_mtime, self.sess.created)

        self.sess.set_end_timestamp()
        assert_true(os.path.exists(end_file))
        assert_equal(os.stat(end_file).st_mtime, self.sess.finished)

    def test_load_session_reads_session_start_time(self):
        """Check if session reads the creation time from the `created` file"""
        session2 = Session(self.sess.path)
        start_file = os.path.join(self.sess.path,
                                  self.sess.TIMESTAMP_FILES['start'])
        assert_equal(os.stat(start_file).st_mtime, self.sess.created)
        assert_equal(os.stat(start_file).st_mtime, session2.created)


class StubForSqlSession(TestSession):

    def test_sqlite_store(self):
        jobid = self.sess.save(_PStruct(a=1, b='foo'))
        self.sess.flush()

        q = sql.select([self.sess.store.t_store.c.id]
                       ).where(self.sess.store.t_store.c.id == jobid
                               )
        conn = self.sess.store._engine.connect()
        results = conn.execute(q)
        rows = results.fetchall()
        assert_equal(len(rows), 1)
        assert_equal(rows[0][0], jobid)

        # remove object from the store, since self.sess.destroy() will
        # not remove it!
        self.sess.store.remove(jobid)


class TestSqliteSession(StubForSqlSession):

    @classmethod
    def setup_class(cls):
        # skip SQLite tests if no SQLite module present (Py 2.4)
        try:
            import sqlite3
        except ImportError:
            # SQLAlchemy uses `pysqlite2` on Py 2.4
            try:
                import pysqlite2
            except ImportError:
                raise SkipTest("No SQLite module installed.")

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
            self.extra_args = {'table_name': self.tmpdir}
            self.sess = Session(
                tmpdir,
                store_url="mysql://gc3user:gc3pwd@localhost/gc3",
                **self.extra_args)
        except sqlalchemy.exc.OperationalError:
            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir)
            raise SkipTest("Cannot connect to MySQL database.")

    def tearDown(self):
        self.sess.destroy()
        conn = self.sess.store._engine.connect()
        conn.execute("drop table `%s`" % self.tmpdir)

## main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()
