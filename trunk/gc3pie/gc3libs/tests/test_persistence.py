#! /usr/bin/env python
#
"""
Test persistence backends.
"""
# Copyright (C) 2011, 2012, GC3, University of Zurich. All rights reserved.
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
import os
import pickle
import shutil
import tempfile

# 3rd party imports
from nose.plugins.skip import SkipTest
from nose.tools import raises, assert_equal

# GC3Pie imports
import gc3libs; from gc3libs import Run, Task
import gc3libs.exceptions
from gc3libs.persistence import make_store, FilesystemStore, Persistable
import gc3libs.persistence.sql # needed to register the SQL stores
from gc3libs.url import Url

try:
    import sqlalchemy
    import sqlalchemy.sql as sql
    sqlfunc = sqlalchemy.func
except:
    pass

## for testing basic functionality we do no need fully-fledged GC3Pie
## objects; let's define some simple make-do's.

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

    


def _run_generic_tests(store):
    _generic_persistence_test(store)
    _generic_nested_persistence_test(store)
    _generic_persist_classes_with_slots(store)
    _save_different_objects_separated(store)


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

        # check that if an object is already on the db a call to
        # `self.store.save` will not save a duplicate of the object, but it
        # will override the old one.
        id1 = self.store.save(obj)
        id2 = self.store.save(obj)
        assert id1 == id2

        # Removing objects
        self.store.remove(id)
        try:
            obj = self.store.load(id)
            raise AssertionError("Object %s should not have been found" % id)
        except gc3libs.exceptions.LoadError:
            # OK, this is what we were expecting
            pass

        # check that IDs are never duplicate
        ids = []
        for i in range(10):
            ids.append(self.store.save(SimplePersistableObject(str(i))))
        assert len(ids) == len(set(ids))

        # cleanup
        for i in ids:
            self.store.remove(i)


    def test_persist_classes_with_slots(self):
        raise SkipTest("FIXME: Test code needs to be checked!")

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


    # XXX: what's the between difference this test and `test_disaggregate_persistable_obejcts`?
    def test_nested_persistence(self):
        """
        Check that nested objects are saved/loaded correctly.
        """
        obj = SimplePersistableList([
            SimplePersistableObject('j1'),
            SimplePersistableObject('j2'),
            SimplePersistableObject('j3'),
            ])

        # save
        id = self.store.save(obj)
        del obj

        # load
        obj = self.store.load(id)
        for i in range(3):
            assert obj[i].value == ('j%d' % (i+1))

        # remove
        self.store.remove(id)
        try:
            obj = self.store.load(id)
            raise AssertionError("Object %s should not have been found!" % id)
        except gc3libs.exceptions.LoadError:
            pass


    def test_disaggregate_persistable_objects(self):
        """
        Check that `Persistable` instances are saved separately from their containers.

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


class TestFilesystemStore(GenericStoreChecks):
    
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.store = FilesystemStore(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    # XXX: there's nothing which is `FilesystemStore`-specific here!
    def test_filesystemstorage_pickler_class(self):
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
        Check that `Persistable` instances are saved separately from their containers.

        In addition to the checks done in `GenericStoreChecks`, we
        also test that files have been created.
        """
        # std checks
        container_id, obj_id = super(TestFilesystemStore, self).test_disaggregate_persistable_objects()
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
            executable='/bin/true',
            arguments=[],
            inputs=[],
            outputs=[],
            output_dir='/tmp',
            jobname='test_job_persistence')

        app.execution.state = Run.State.NEW
        app.execution.lrms_jobid = 1

        id_ = self.store.save(app)
        q = sql.select([self.store.t_store.c.jobid,
                           self.store.t_store.c.jobname,
                           self.store.t_store.c.jobstatus]).where(self.store.t_store.c.id==id_)
        
        result = self.conn.execute(q)
        row = result.fetchone()
        # self.c.execute('select jobid,jobname,jobstatus from %s where id=%d'
        #           % (self.store.table_name, app.persistent_id))
        # row = self.c.fetchone()

        assert int(row[0]) == app.execution.lrms_jobid
        assert row[1] == app.jobname
        assert row[2] == app.execution.state


    def test_sql_injection(self):
        """Test if the `SqlStore` class is vulnerable to SQL injection."""
        obj = SimpleTask("Antonio's task")
        obj.execution.lrms_jobid = "my 'jobid' has a lot%! of strange SQL? characters; doesn't it?"
        id_ = self.store.save(obj)
        obj2 = self.store.load(id_)
        assert obj.jobname == obj2.jobname
        assert obj.execution.lrms_jobid == obj2.execution.lrms_jobid


    def test_sql_implicit_attribute_save(self):
        """
        Test if `SqlStore` saves extra attributes into columns of the same name.
        """
        # extend the db
        self.conn.execute('alter table %s add column value varchar(256)'
                       % self.store.table_name)

        # re-open the db. We need this because schema definition is
        # checked only in SqlStore.__init__
        self.store = self._make_store()

        obj = SimplePersistableObject('My App')
        id_ = self.store.save(obj)

        # check that the attribute `.value` has been saved in the dedicated col
        q = sql.select([self.store.t_store.c.value]).where(self.store.t_store.c.id==id_)
        results = self.conn.execute(q)
        rows = results.fetchall()
        assert_equal(len(rows), 1)
        assert_equal(rows[0][0], obj.value)


    def test_sql_save_load_extra_fields(self):
        """
        Test if `SqlStore` reads and writes extra columns.
        """
        # extend the db
        self.conn.execute('alter table %s add column extra varchar(256)'
                       % self.store.table_name)

        # re-build store, as the table list is read upon `__init__`
        self.store = self._make_store(extra_fields={ 'extra': (lambda arg: arg.foo.value) })

        # if this query does not error out, the column is defined
        q = sql.select([sqlfunc.count(self.store.t_store.c.extra)]).distinct()
        # self.c.execute("select distinct count(extra) from %s" % self.store.table_name)
        results = self.conn.execute(q)
        rows = results.fetchall()
        assert_equal(len(rows), 1)


        # create and save an object
        obj = SimplePersistableObject('an object')
        obj.foo = SimplePersistableObject('an attribute')
        id_ = self.store.save(obj)

        # check that the value has been saved
        q = sql.select([self.store.t_store.c.extra]).where(self.store.t_store.c.id==id_)
        # self.c.execute("select extra from %s where id=%d"
        #                % (self.store.table_name, id))
        results = self.conn.execute(q)
        rows = results.fetchall()
        assert_equal(len(rows), 1)
        assert_equal(rows[0][0], obj.foo.value)


    @raises(AssertionError)
    def test_sql_error_if_no_extra_fields(self):
        """
        Test if `SqlStore` reads and writes extra columns.
        """
        # re-build store with a non-existent extra column; should raise `AssertionError`
        self._make_store(extra_fields={ 'extra': (lambda arg: arg.foo.value) })




class ExtraSqlChecks(object):
    """
    `SqlStore` that depend on the setup of a non-default table.
    """

    def _make_store(self, url, **kwargs):
        self.db_url = url
        self.store = make_store(url, **kwargs)
        self.c = self.store.__engine.connect()


    def test_sql_create_extra_fields(self):
        """
        Test if `SqlStore` creates extra columns.
        """

        # extend the db
        self._make_store(self.db_url,
                         extra_fields={ 'extra': (lambda arg: arg.foo.value) })

        # if this query does not error out, the column is defined
        q = sql.select([sqlfunc.count(self.store.t_store.c.extra)]).distinct()
        results = self.conn.execute(q)
        # self.c.execute("select distinct count(extra) from %s" % self.store.table_name)
        rows = results.fetchall()
        assert_equal(len(rows), 1)

        # create and save an object
        obj = SimplePersistableObject('an object')
        obj.foo = SimplePersistableObject('an attribute')
        id_ = db.save(obj)

        # check that the value has been saved
        q = sql.select([self.store.t_store.c.extra]).where(self.store.t_store.c.id==id_)
        # self.c.execute("select extra from %s where id=%d"
        #                % (self.store.table_name, id_))
        results = self.conn.execute(q)
        rows = results.fetchall()
        assert_equal(len(rows), 1)
        assert_equal(rows[0][0], obj.foo.value)



class TestSqliteStore(SqlStoreChecks):
    """Test SQLite backend."""
    
    @classmethod
    def teardown_class(cls):
        pass

    def setUp(self):
        fd, self.tmpfile = tempfile.mkstemp()
        self.db_url = Url('sqlite://%s' % self.tmpfile)
        self.store = self._make_store()

        # create a connection to the database
        self.conn = self.store._SqlStore__engine.connect()

    def tearDown(self):
        self.conn.close()
        os.remove(self.tmpfile)

    def _make_store(self, **kwargs):
        return make_store(self.db_url, **kwargs)
    


class TestSqliteStoreWithAlternateTable(TestSqliteStore):
    """Test SQLite backend with a different table name."""

    def _make_store(self, **kwargs):
        return make_store(self.db_url, table_name='another_store', **kwargs)

    def test_alternate_table_not_empty(self):
        obj = SimplePersistableObject('an object')
        self.store.save(obj)

        results = self.conn.execute(sql.select([self.store.t_store.c.id]))
        rows = results.fetchall()
        assert len(rows) > 0


class TestMysqlStore(SqlStoreChecks):

    @classmethod
    def setup_class(cls):
        # we skip MySQL tests if no MySQLdb module is present
        try:
            import MySQLdb
        except:
            raise SkipTest("MySQLdb module not installed.")

    @classmethod
    def teardown_class(cls):
        pass

    def setUp(self):
        fd, tmpfile = tempfile.mkstemp()
        table_name = tmpfile.split('/')[-1] 
        self.table_created =set()
        self.table_created.add(table_name)
        try:
            self.db_url = Url('mysql://gc3user:gc3pwd@localhost/gc3')
            self.store = make_store(self.db_url, table_name=table_name)
        except sqlalchemy.exc.OperationalError:
            raise SkipTest("Cannot connect to MySQL database.")
        
        # create a connection to the database
        self.conn = self.store._SqlStore__engine.connect()

    def tearDown(self):
        self.table_created.add(self.store.table_name)
        for table in self.table_created:
            self.conn.execute('drop table %s' % table)
        self.conn.close()
        # self.c.close()
        # os.remove(self.tmpfile)

    def _make_store(self, **kwargs):
        self.table_created.add(self.store.table_name)
        return make_store(self.db_url, table_name='another_store', **kwargs)


if __name__ == "__main__":
    import nose
    nose.runmodule()
