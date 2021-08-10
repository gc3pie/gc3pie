#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2021              Google LLC.
# Copyright (C) 2012, 2015, 2018  University of Zurich. All rights reserved.
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
from builtins import zip
from builtins import str
from builtins import range
from builtins import object
__docformat__ = 'reStructuredText'

# stdlib imports
import csv
import os
import shutil
import tempfile

import pytest

import sqlalchemy
import sqlalchemy.sql as sql

# GC3Pie imports
import gc3libs.exceptions
from gc3libs.persistence import Persistable, make_store
import gc3libs.persistence.sql
from gc3libs.session import Session
from gc3libs.utils import Struct
from gc3libs import Task
from gc3libs.workflow import TaskCollection


class _PStruct(Struct, Persistable):

    """
    A Persistable+Struct mix-in.

    This class just exists so that we are able to persist something in
    the tests and make a non-trivial equality check afterwards.
    """
    pass


def test_create():
    tmpdir = tempfile.mktemp(
        prefix=(os.path.basename(__file__) + '.'),
        suffix='.d')
    try:
        sess = Session(tmpdir)
        assert os.path.isdir(sess.path)
        sess.destroy()
    except:
        if os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)
        raise


def test_destroy():
    tmpdir = tempfile.mktemp(
        prefix=(os.path.basename(__file__) + '.'),
        suffix='.d')
    try:
        sess = Session(tmpdir)
        tid = sess.add(_PStruct(a=1, b='foo'))
        sess.destroy()
        # destroy should kill all traces of the sessiondir
        assert not os.path.exists(sess.path)
        # in particular, no task can be loaded
        with pytest.raises(gc3libs.exceptions.LoadError):
            sess.load(tid)
    except:
        if os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)
        raise


class TestSession(object):

    @pytest.fixture(autouse=True)
    def _with_tmp_session(self):
        tmpdir = tempfile.mktemp(
            prefix=(os.path.basename(__file__) + '.'),
            suffix='.d')
        self.tmpdir = tmpdir
        self.sess = Session(tmpdir)
        self.extra_args = {}

        yield

        self.sess.destroy()

    def test_session_directory_created(self):
        assert os.path.isdir(self.sess.path)
        assert os.path.samefile(
            os.path.join(os.path.abspath('.'), self.tmpdir),
            self.sess.path)

    def test_store_url_file_exists(self):
        self.sess.flush()
        storefile = os.path.join(self.sess.path, Session.STORE_URL_FILENAME)
        assert os.path.isfile(storefile)

    def test_add(self):
        tid = self.sess.add(_PStruct(a=1, b='foo'))
        assert len(self.sess) == 1
        assert [tid] == self.sess.list_ids()

    def test_add_updates_metadata(self):
        """Check that on-disk metadata is changed on add(..., flush=True)."""
        self.sess.add(_PStruct(a=1, b='foo'), flush=True)
        fd_job_ids = open(os.path.join(self.sess.path,
                                       self.sess.INDEX_FILENAME), 'r')
        ids = fd_job_ids.read().split()
        assert len(ids) == 1
        assert ids == [str(i) for i in self.sess.tasks]

    def test_empty_lines_in_index_file(self):
        """
        Check that the index file is read correctly even when there
        are empty lines.

        Because of a bug, in some cases the
        `gc3libs.session.Session`:class: used to create invalid job
        ids (equal to ``''``).
        """
        self.sess.add(_PStruct(a=1, b='foo'), flush=True)
        with open(os.path.join(self.sess.path,
                               self.sess.INDEX_FILENAME), 'a') as fp:
            fp.write('\n\n\n')
            fp.flush()
        self.sess = Session(self.sess.path, **self.extra_args)
        ids = self.sess.list_ids()
        assert len(ids) == 1
        assert ids == [str(task_id) for task_id in self.sess.tasks]

    def test_add_no_flush(self):
        """Check that metadata is not changed on add(..., flush=False)."""
        tid = self.sess.add(_PStruct(a=1, b='foo'), flush=False)
        # in-memory metadata is updated
        assert len(self.sess) == 1
        assert [tid] == self.sess.list_ids()
        # on-disk metadata is not
        fd_job_ids = open(os.path.join(self.sess.path,
                                       self.sess.INDEX_FILENAME), 'r')
        assert '' == fd_job_ids.read()

    def test_remove(self):
        # add tasks
        tid1 = self.sess.add(_PStruct(a=1, b='foo'))
        tid2 = self.sess.add(_PStruct(a=1, b='foo'))
        assert len(self.sess) == 2
        self.sess.remove(tid1)
        assert len(self.sess) == 1
        self.sess.remove(tid2)
        assert len(self.sess) == 0

    def test_remove_children(self):
        """
        Test if the session is able to remove all children of a task
        """
        # Removing objects
        obj = _PStruct(name='GC3')
        obj.tasks = [_PStruct(name='GC3')]
        id = self.sess.add(obj)
        self.sess.remove(id)

        assert len(self.sess.store.list()) == 0

    def test_reload_session(self):
        self.sess.add(_PStruct(a=1, b='foo'))
        self.sess.add(_PStruct(a=2, b='bar'))
        self.sess.add(_PStruct(a=3, b='baz'))
        sess2 = Session(self.sess.path, **self.extra_args)
        assert len(sess2) == 3
        for task_id in sess2.tasks.keys():
            task = sess2.store.load(task_id)
            assert task == sess2.tasks[task_id]
        for task2_id, task1_id in zip(sorted(sess2.tasks.keys()),
                                      sorted(self.sess.tasks.keys())):
            assert (self.sess.tasks[task1_id] ==
                         sess2.tasks[task2_id])

    def test_incomplete_session_dir(self):
        tmpdir = tempfile.mktemp(
            prefix=(os.path.basename(__file__) + '.'),
            suffix='.d')
        os.mkdir(tmpdir)
        incomplete_sess = Session(tmpdir)
        assert os.path.exists(os.path.join(tmpdir,
                                                Session.INDEX_FILENAME))
        assert os.path.exists(os.path.join(tmpdir,
                                                Session.STORE_URL_FILENAME))
        incomplete_sess.destroy()

    def test_load_external_jobid(self):
        """
        Check if we are able to load an object not belonging to the session.
        """
        obj1 = _PStruct(a=1, b='foo')
        extraid = self.sess.store.save(obj1)
        # note: `load(..., add=True) will also add `obj2` to the
        # session so it gets removed when the session is destroyed.
        obj2 = self.sess.load(extraid, add=True)
        assert obj1 == obj2

    def test_creation_of_timestamp_files(self):
        start_file = os.path.join(self.sess.path,
                                  self.sess.TIMESTAMP_FILES['start'])
        end_file = os.path.join(self.sess.path,
                                self.sess.TIMESTAMP_FILES['end'])

        assert os.path.exists(start_file)
        assert not os.path.exists(end_file)

        assert os.stat(start_file).st_mtime == self.sess.created

        self.sess.set_end_timestamp()
        assert os.path.exists(end_file)
        assert os.stat(end_file).st_mtime == self.sess.finished

    def test_load_session_reads_session_start_time(self):
        """Check if session reads the creation time from the `created` file"""
        session2 = Session(self.sess.path)
        start_file = os.path.join(self.sess.path,
                                  self.sess.TIMESTAMP_FILES['start'])
        assert os.stat(start_file).st_mtime == self.sess.created
        assert os.stat(start_file).st_mtime == session2.created

    def test_standard_session_iterator_for_tasks(self):
        self.sess.add(Task(jobname='task-1'))
        self.sess.add(Task(jobname='task-2'))
        self.sess.add(Task(jobname='task-3'))

        assert (set(('task-1', 'task-2', 'task-3')) ==
                     set(job.jobname for job in self.sess))

    def test_standard_session_iterator_for_tasks_and_task_collections(self):
        coll = TaskCollection(jobname='collection',
                              tasks=[Task() for i in range(3)])
        self.sess.add(coll)

        assert (['collection'] ==
                     [job.jobname for job in self.sess])

    def test_workflow_iterator_for_session(self):
        coll = TaskCollection(
            jobname='collection',
            tasks=[Task(jobname='task-%d' % i) for i in range(3)])
        coll2 = TaskCollection(
            jobname='collection-1',
            tasks=[Task(jobname='task-1-%d' % i) for i in range(3)])
        coll.tasks.append(coll2)

        self.sess.add(coll)

        assert (['collection', 'task-0', 'task-1', 'task-2',
                      'collection-1', 'task-1-0', 'task-1-1', 'task-1-2'] ==
                     [job.jobname for job in self.sess.iter_workflow()])


class StubForSqlSession(TestSession):

    def test_sql_store(self):
        jobid = self.sess.save(_PStruct(a=1, b='foo'))
        self.sess.flush()
        q = (sql.select([self.sess.store._tables.c.id])
             .where(self.sess.store._tables.c.id == jobid))
        conn = self.sess.store._engine.connect()
        results = conn.execute(q)
        rows = results.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == jobid

        # remove object from the store, since self.sess.destroy() will
        # not remove it!
        self.sess.store.remove(jobid)


class TestSqliteSession(StubForSqlSession):

    @classmethod
    def setup_class(cls):
        # skip SQLite tests if no SQLite module present (Py 2.4)
        sqlite3 = pytest.importorskip("sqlite3")

    @pytest.fixture(autouse=True)
    def _with_tmp_session(self):
        self.tmpdir = os.path.abspath(
            tempfile.mktemp(
                prefix=(os.path.basename(__file__) + '.'),
                suffix='.d'))
        self.sess = Session(
            self.tmpdir,
            create=True,
            store_or_url="sqlite:///{0}/store.db".format(self.tmpdir))
        self.extra_args = {}

        yield

        if os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)


class TestMysqlSession(StubForSqlSession):

    @classmethod
    def setup_class(cls):
        # we skip MySQL tests if no MySQLdb module is present
        MySQLdb = pytest.importorskip("MySQLdb")

    @pytest.fixture(autouse=True)
    def _with_tmp_session(self):
        self.tmpdir = tempfile.mktemp(
            prefix=(os.path.basename(__file__) + '.'),
            suffix='.d')
        self.tablename = os.path.basename(tmpdir)
        self.extra_args = {'table_name': self.tablename}
        try:
            self.sess = Session(
                tmpdir,
                store_url="mysql://gc3user:gc3pwd@localhost/gc3",
                **self.extra_args)
        except sqlalchemy.exc.OperationalError:
            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir)
            pytest.mark.skip("Cannot connect to MySQL database.")

        yield

        self.sess.destroy()
        conn = self.sess.store._engine.connect()
        conn.execute("drop table `%s`" % self.tablename)

# main: run tests

if "__main__" == __name__:
    pytest.main(["-v", __file__])
