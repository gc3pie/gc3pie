#! /usr/bin/env python
#
"""
Test persistence backends.
"""
# Copyright (C) 2011, 2012, 2019,  University of Zurich. All rights reserved.
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
from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
from builtins import range
from builtins import object
__docformat__ = 'reStructuredText'

# stdlib imports
import os
import shutil
from tempfile import NamedTemporaryFile, mkdtemp

# 3rd party imports
import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
sql = pytest.importorskip("sqlalchemy.sql")
sqlfunc = sqlalchemy.func

# GC3Pie imports
import gc3libs
from gc3libs import Run, Task
import gc3libs.workflow

import gc3libs.exceptions
from gc3libs.persistence import make_store, Persistable
from gc3libs.persistence.accessors import GET
from gc3libs.persistence.serialization import DEFAULT_PROTOCOL
from gc3libs.persistence.idfactory import IdFactory
from gc3libs.persistence.filesystem import FilesystemStore
from gc3libs.persistence.sql import SqlStore
from gc3libs.url import Url, UrlKeyDict


@pytest.mark.parametrize("cls", (SqlStore, FilesystemStore))
def test_store_ctor_with_extra_arguments(cls, tmpdir):
    """Test if `Store`:class: classess accept extra keyword arguments
    without complaining.

    This test will ensure that any `Store` class can be called with
    arguments which are valid only on other `Store` classes.
    """
    args = {
        'url': 'sqlite:///%s/test.sqlite' %
        os.path.abspath(str(tmpdir)),
        'table_name': 'store',
        'create': True,
        'directory': str(tmpdir),
        'idfactory': IdFactory(),
        'protocol': DEFAULT_PROTOCOL,
        'extra_fields': {
            sqlalchemy.Column(
                'extra',
                sqlalchemy.TEXT()): lambda x: "test"},
    }

    cls(**args)


def test_eq_persisted_objects():
    """Test that the comparison of two persisted objects are the same
    whenever `persistent_id` attribute is the same"""
    a = Persistable()
    b = Persistable()
    c = Persistable()
    a.persistent_id = '1'
    b.persistent_id = '1'
    c.persistnet_id = '2'
    assert a == b
    assert a is not b
    assert a != c
    L = [a, b, c]
    L.remove(a)
    L.remove(a)
    assert len(L) == 1

def test_eq_non_persisted_objects():
    """Test that the comparison of two not (yet) persisted objects falls
    back to default Python comparison"""
    a = Persistable()
    b = Persistable()
    assert a != b
    assert a is not b


# for testing basic functionality we do no need fully-fledged GC3Pie
# objects; let's define some simple make-do's.

class SimplePersistableObject(Persistable):

    def __init__(self, x):
        self.value = x

    def __eq__(self, other):
        return (self.value == other.value)


class SimplePersistableList(list, Persistable):

    """
    Add a `__dict__` to `list`, so that creating a `persistent_id`
    entry on an instance works.
    """
    pass


class SimpleTask(Task):
    # XXX: do we need this? why not instanciating `Task` directly?
    pass


class NonPersistableClassWithSlots(object):

    """
    A class to test that persistence of classes with `__slots__`.

    This and the `PersistableClassWithSlots`:class: are classes with
    `__slots__` attribute to check that persistence works also with
    this kind of classes. Usually you just need to use a binary
    protocol for pickle.

    This class will raise an error because `__slots__` does not
    contain `persistent_id`. The `PersistableClassWithSlots`:class:
    class instead, will work as expected.

    Check for instance:
    http://stackoverflow.com/questions/3522765/python-pickling-slots-error
    """

    __slots__ = ["attr", ]

    def __init__(self, attr):
        self.attr = attr


class PersistableClassWithSlots(NonPersistableClassWithSlots):
    __slots__ = ["attr", "persistent_id"]


class MyChunkedParameterSweep(gc3libs.workflow.ChunkedParameterSweep):

    def new_task(self, param, **extra_args):
        return Task(**extra_args)


class MyStagedTaskCollection(gc3libs.workflow.StagedTaskCollection):

    def stage0(self):
        return Task()


# def _run_generic_tests(store):
#     _generic_persistence_test(store)
#     _generic_nested_persistence_test(store)
#     _generic_persist_classes_with_slots(store)
#     _save_different_objects_separated(store)


class GenericStoreChecks(object):

    """
    A suite of tests for the generic `gc3libs.persistence.Store` interface.
    """

    def test_generic_persistence(self):
        """
        Test basic functionality of the persistence subsystem.
        """
        # check that basic save/load works
        obj = SimplePersistableObject('GC3')
        id = self.store.save(obj)
        del obj
        obj = self.store.load(id)
        assert obj.value == 'GC3'

    def test_duplicated_save(self):
        """
        Check that if an object is already on the db a call to
        `self.store.save` will not save a duplicate of the object, but
        it will override the old one.
        """
        obj = SimplePersistableObject('GC3')

        id1 = self.store.save(obj)
        id2 = self.store.save(obj)
        assert id1 == id2

    def test_ids_are_not_duplicated(self):
        """
        check that IDs are never duplicate
        """
        ids = []
        for i in range(10):
            ids.append(self.store.save(SimplePersistableObject(str(i))))
        assert len(ids) == len(set(ids))

    def test_list_method(self):
        """Test the `list` method of the `SqlStore` class"""
        num_objs = 10
        for i in range(num_objs):
            self.store.save(SimplePersistableObject('Object %d' % i))

        assert len(self.store.list()) == num_objs

    def test_remove_method(self):
        """
        Test remove method of a generic `Store` class
        """
        # Removing objects
        obj = SimplePersistableObject('GC3')
        id = self.store.save(obj)
        self.store.remove(id)

        with pytest.raises(gc3libs.exceptions.LoadError):
            obj = self.store.load(id)

    def test_replace_method(self):
        """Test the `replace` method of the `SqlStore` class"""
        # 1) save a new object
        obj = SimplePersistableObject("Original")
        id_ = self.store.save(obj)

        # 2) change it and replace it
        obj.x = "Updated"
        self.store.replace(obj.persistent_id, obj)
        assert id_ == obj.persistent_id

        # 3_ load it again and check if it has been updated
        obj2 = self.store.load(id_)
        assert obj2.x == "Updated"

    @pytest.mark.skip(reason="FIXME: Test code needs to be checked!")
    def test_persist_classes_with_slots(self):

        # FIXME: check this code!
        obj = PersistableClassWithSlots('GC3')
        assert obj.attr == 'GC3'
        id_ = self.store.save(obj)
        del obj
        obj2 = self.store.load(id_)
        assert obj2.attr == 'GC3'

        obj2 = NonPersistableClassWithSlots('GC3')
        try:
            self.store.save(obj2)
            raise AssertionError("We shouldn't reach this point")
        except AttributeError:
            pass

    def test_disaggregate_persistable_objects(self):
        """
        Check that `Persistable` instances are saved separately from
        their containers.

        Since we want to save `Task`s separately from the containing
        object (e.g. `SessionBasedScript`), this test will try to save a
        generic persisted container which contain another persisted and
        check that they are saved on different items.

        This is done by checking that all the objects have a unique
        `persistent_id` attribute.
        """
        container = SimplePersistableList()
        container.append(SimplePersistableObject('MyJob'))
        container_id = self.store.save(container)

        # check that IDs have been assigned
        assert hasattr(container, 'persistent_id')
        assert hasattr(container[0], 'persistent_id')

        # check that IDs are distinct
        assert container_id == container.persistent_id
        objid = container[0].persistent_id
        assert objid != container_id

        # check that loading the container re-creates the same contained object
        del container
        container = self.store.load(container_id)
        obj = self.store.load(objid)
        assert obj == container[0]

        # return objects for further testing
        return (container_id, objid)

    def test_persist_urlkeydict(self):
        """
        Test that we can persist GC3Pie's `UrlKeyDict` classes.
        """
        a = Task(attr=UrlKeyDict({'/tmp/1': 1, '/tmp/2': 2}))
        id_ = self.store.save(a)
        b = self.store.load(id_)
        assert b.attr == a.attr


    def test_persist_urlvaluedict(self):
        """
        Test that we can persist GC3Pie's `UrlValueDict` classes.
        """
        a = Task(attr=UrlKeyDict({'foo': '/tmp/1', 'bar': '/tmp/2'}))
        id_ = self.store.save(a)
        b = self.store.load(id_)
        assert b.attr == a.attr


@pytest.mark.parametrize("task", (Task(),
        gc3libs.workflow.TaskCollection(tasks=[Task(), Task()]),
        gc3libs.workflow.SequentialTaskCollection([Task(), Task()]),
        MyStagedTaskCollection(),
        gc3libs.workflow.ParallelTaskCollection(tasks=[Task(), Task()]),
        MyChunkedParameterSweep(1, 20, 1, 5),
        gc3libs.workflow.RetryableTask(Task())))
def test_task_objects_buggy(task):
    """
    Test that all `Task`-like objects are persistable
    """
    with NamedTemporaryFile(prefix='gc3libs.', suffix='.tmp') as tmp:
        store = make_store("sqlite://%s" % tmp.name)
        id = store.save(task)
        store.load(id)


class TestFilesystemStore(GenericStoreChecks):

    @pytest.fixture(autouse=True)
    def setUp(self):
        from gc3libs.persistence.filesystem import FilesystemStore
        self.tmpdir = mkdtemp(prefix='gc3libs.', suffix='.tmp.d')
        self.store = FilesystemStore(self.tmpdir)

        yield

        shutil.rmtree(self.tmpdir)

    # XXX: there's nothing which is `FilesystemStore`-specific here!
    def check_file_accesssystemstorage_pickler_class(self):
        """
        Check that `Persistable` instances are saved saparately.

        Namely, if you want to save two independent objects but one of
        them has a reference to the other, the standard behavior of
        Pickle is to save a copy of the contained object into the same
        file of the containing object.

        The FilesystemStorage.Pickler class is aimed to avoid this.
        """
        obj1 = SimplePersistableObject('parent')
        obj2 = SimplePersistableObject('children')
        obj1.children = obj2
        id2 = self.store.save(obj2)
        id1 = self.store.save(obj1)
        del obj1
        del obj2
        obj1 = self.store.load(id1)
        obj2 = self.store.load(id2)
        assert obj1.children.value == 'children'
        assert obj2.value == 'children'
        assert obj1.children == obj2

    def test_disaggregate_persistable_objects(self):
        """
        Check that `Persistable` instances are saved separately from
        their containers.

        In addition to the checks done in `GenericStoreChecks`, we
        also test that files have been created.
        """
        # std checks
        container_id, obj_id = super(
            TestFilesystemStore, self).test_disaggregate_persistable_objects()
        # check that files exist
        container_file = os.path.join(self.store._directory, str(container_id))
        assert os.path.exists(container_file)
        obj_file = os.path.join(self.store._directory, str(obj_id))
        assert os.path.exists(obj_file)


class SqlStoreChecks(GenericStoreChecks):

    """
    Extend `GenericStoreChecks` with additional SQL-related tests.

    In order for these tests to work, the class constructor must
    define these instance attributes:

    * `self.db_url`: SqlAlchemy URL to open the database
    * `self.store`: a `gc3libs.persistence.Store` instance to use for tests
    * `self.c`: a valid cursor for executing raw SQL queries.
    """

    def test_default_fields(self):
        app = gc3libs.Application(
            arguments=['/bin/true'],
            inputs=[],
            outputs=[],
            output_dir='/tmp',
            jobname='test_job_persistence')

        app.execution.state = Run.State.NEW
        app.execution.lrms_jobid = 1
        id_ = self.store.save(app)

        q = sql.select([
            self.store._tables.c.state
        ]).where(self.store._tables.c.id == id_)
        result = self.conn.execute(q)
        row = result.fetchone()

        assert row[0] == app.execution.state

    # the `jobname` attribute is optional in the `Application` ctor
    def test_persist_Application_with_no_job_name(self):
        app = gc3libs.Application(
            arguments=['/bin/true'],
            inputs=[],
            outputs=[],
            output_dir='/tmp')

        app.execution.state = Run.State.NEW
        app.execution.lrms_jobid = 1
        id_ = self.store.save(app)

        q = sql.select([
            self.store._tables.c.state
        ]).where(self.store._tables.c.id == id_)
        result = self.conn.execute(q)
        row = result.fetchone()
        assert row[0] == app.execution.state

    def test_sql_injection(self):
        """Test if the `SqlStore` class is vulnerable to SQL injection."""

        storetemp = make_store(
            self.db_url,
            table_name='sql_injection_test',
            extra_fields={
                sqlalchemy.Column(
                    'extra',
                    sqlalchemy.VARCHAR(
                        length=128)): GET.extra,
            })

        obj = SimpleTask()
        # obligatory XKCD citation ;-)
        # Ric, you can't just "cite" XKCD without inserting a
        # reference: http://xkcd.com/327/
        obj.extra = "Robert'); DROPT TABLE sql_injection_test; --"
        id_ = storetemp.save(obj)
        obj2 = storetemp.load(id_)
        self.conn.execute('drop table sql_injection_test')
        assert obj.execution.state == obj2.execution.state

    def test_sql_save_load_extra_fields(self):
        """
        Test if `SqlStore` reads and writes extra columns.
        """
        # extend the db
        self.conn.execute('alter table `%s` add column extra varchar(256)'
                          % self.store.table_name)

        # re-build store, as the table list is read upon `__init__`
        self.store = self._make_store(
            extra_fields={
                sqlalchemy.Column(
                    'extra',
                    sqlalchemy.VARCHAR(
                        length=128)): GET.foo.value,
            })

        # if this query does not error out, the column is defined
        q = sql.select([sqlfunc.count(self.store._tables.c.extra)]).distinct()
        results = self.conn.execute(q)
        rows = results.fetchall()
        assert len(rows) == 1

        # create and save an object
        obj = SimplePersistableObject('an object')
        obj.foo = SimplePersistableObject('an attribute')
        id_ = self.store.save(obj)

        # check that the value has been saved
        q = sql.select([self.store._tables.c.extra]).where(
            self.store._tables.c.id == id_)

        # Oops, apparently the store.save call will close our
        # connection too.
        self.conn = self.store._engine.connect()
        results = self.conn.execute(q)
        rows = results.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == obj.foo.value

    @pytest.mark.skip(reason="FIXME: Check if test is still valid")
    def test_sql_error_if_no_extra_fields(self):
        """
        Test if `SqlStore` reads and writes extra columns.
        """
        # re-build store with a non-existent extra column
        with pytest.raises(AssertionError):
            self._make_store(
                extra_fields={
                    sqlalchemy.Column(
                        'extra',
                        sqlalchemy.VARCHAR(
                            length=128)): (
                                lambda arg: arg.foo.value)})


class ExtraSqlChecks(object):

    """
    `SqlStore` that depend on the setup of a non-default table.
    """

    def _make_store(self, url, **kwargs):
        self.db_url = url
        self.store = make_store(url, **kwargs)
        self.c = self.store._engine.connect()

    def test_sql_create_extra_fields(self):
        """
        Test if `SqlStore` creates extra columns.
        """

        # extend the db
        self._make_store(
            self.db_url,
            extra_fields={
                sqlalchemy.Column(
                    'extra',
                    sqlalchemy.VARCHAR(
                        length=128)): (
                    lambda arg: arg.foo.value)})

        # if this query does not error out, the column is defined
        q = sql.select([sqlfunc.count(self.store._tables.c.extra)]).distinct()
        results = self.conn.execute(q)
        rows = results.fetchall()
        assert len(rows) == 1

        # create and save an object
        obj = SimplePersistableObject('an object')
        obj.foo = SimplePersistableObject('an attribute')
        id_ = self.store.save(obj)

        # check that the value has been saved
        q = sql.select([self.store._tables.c.extra]).where(
            self.store._tables.c.id == id_)
        # self.c.execute("select extra from %s where id=%d"
        #                % (self.store.table_name, id_))
        results = self.conn.execute(q)
        rows = results.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == obj.foo.value


class TestSqliteStore(SqlStoreChecks):

    """Test SQLite backend."""

    @classmethod
    def setup_class(cls):
        # skip SQLite tests if no SQLite module present (Py 2.4)
        sqlite3 = pytest.importorskip("sqlite3")

    @pytest.fixture(autouse=True)
    def setUp(self):
        with NamedTemporaryFile(prefix='gc3libs.', suffix='.tmp') as tmp:
            self.tmpfile = tmp.name
            self.db_url = Url('sqlite://%s' % self.tmpfile)
            self.store = self._make_store()

            # create a connection to the database
            self.conn = self.store._engine.connect()

            yield
            self.conn.close()

    def _make_store(self, **kwargs):
        return make_store(self.db_url, **kwargs)


class TestSqliteStoreWithAlternateTable(TestSqliteStore):

    """Test SQLite backend with a different table name."""

    @classmethod
    def setup_class(cls):
        # skip SQLite tests if no SQLite module present (Py 2.4)
        sqlite3 = pytest.importorskip("sqlite3")

    def _make_store(self, **kwargs):
        return make_store(self.db_url, table_name='another_store', **kwargs)

    def test_alternate_table_not_empty(self):
        obj = SimplePersistableObject('an object')
        self.store.save(obj)

        results = self.conn.execute(sql.select([self.store._tables.c.id]))
        rows = results.fetchall()
        assert len(rows) > 0


class TestMysqlStore(SqlStoreChecks):

    @classmethod
    def setup_class(cls):
        # we skip MySQL tests if no MySQLdb module is present
        MySQLdb = pytest.importorskip("MySQLdb")

    @classmethod
    def teardown_class(cls):
        pass

    @pytest.fixture(autouse=True)
    def setUp(self):
        # generate random table name
        from string import ascii_letters as letters
        import random
        self.table_name = 'test_' + (''.join(random.choice(letters) for _ in range(10)))

        try:
            self.db_url = Url('mysql://gc3user:gc3pwd@localhost/gc3')
            self.store = make_store(self.db_url, table_name=self.table_name)
        except sqlalchemy.exc.OperationalError:
            pytest.mark.skip("Cannot connect to MySQL database.")

        # create a connection to the database
        self.conn = self.store._engine.connect()

        yield

        self.conn.execute('drop table `%s`' % self.table_name)
        self.conn.close()
        # self.c.close()
        # os.remove(self.tmpfile)

    def _make_store(self, **kwargs):
        return make_store(self.db_url, table_name=self.table_name, **kwargs)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
