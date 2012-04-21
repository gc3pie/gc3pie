#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, GC3, University of Zurich. All rights reserved.
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
"""Test for persistency backend(s)"""

__docformat__ = 'reStructuredText'
__version__ = '$Revision$'

from gc3libs.url import Url
from gc3libs.persistence import persistence_factory, FilesystemStore, Persistable
import gc3libs.exceptions as gc3ex
import tempfile, os, shutil
import pickle
from gc3libs import Task

try:
    import MySQLdb
except:
    MySQLdb = None

class MyObj(Persistable):
    def __init__(self, x):
        self.x = x

class MyList(list, Persistable):
    """
    Add a `__dict__` to `list`, so that creating a `persistent_id`
    entry on an instance works.
    """
    pass

class MyTask(Task):
    pass

class SlotClassWrong(object):
    """
    This and the SlotClass are class with __slots__ attribute to
    check that persistency works also with this kind of
    classes. Usually you just need to use a binary protocol for
    pickle.

    This class will raise an error because __slots__ does not contain "persistent_id". The SlotClass class instead, will work as expected.

    Check for instance:
    http://stackoverflow.com/questions/3522765/python-pickling-slots-error
    """
    
    __slots__ = ["attr", ]
    def __init__(self, attr):
        self.attr = attr

class SlotClass(SlotClassWrong):
    __slots__ = ["attr", "persistent_id"]
    
def _generic_persistency_test(driver):
    obj = MyObj('GC3')
    id = driver.save(obj)
    del obj
    obj = driver.load(id)
    assert obj.x == 'GC3'    

    # We assume tat if an object is already on the db a call to
    # `driver.save` will not save a duplicate of the object, but it
    # will override the old one.
    id1 = driver.save(obj)
    id2 = driver.save(obj)
    assert id1 == id2

    # Removing objects
    driver.remove(id)
    try:
        obj = driver.load(id)
        assert "Object %s should NOT be found" % id
    except gc3ex.LoadError:
        pass
    except Exception, e:
        raise e

    # test id consistency
    ids = []
    for i in range(10):
        ids.append(driver.save(MyObj(str(i))))
    assert len(ids) == len(set(ids))

    # cleanup
    for i in ids:
        driver.remove(i)
    
def _generic_nested_persistency_test(driver):
    obj = MyList([MyObj('j1'), MyObj('j2'), MyObj('j3'), ])

    id = driver.save(obj)
    del obj
    obj = driver.load(id)
    for i in range(3):
        assert obj[i].x == 'j%d' % (i+1)

    driver.remove(id)
    try:
        obj = driver.load(id)
        assert "Object %s should NOT be found" % id
    except gc3ex.LoadError:
        pass
    except Exception, e:
        raise e

def _save_different_objects_separated(driver):
    """Since we want to save Tasks separately from the containing
    object (e.g. SessionBasedScript) this test will try to save a
    generic persisted container which contain another persisted and
    check that they are saved on different items.

    This is done by checking that all the objects have a persistent_id
    attribute and """
    
    container = MyList()
    container.append(MyObj('MyJob'))
    id = driver.save(container)

    # Check that there are two files!
    assert id == container.persistent_id
    assert hasattr(container[0], 'persistent_id')
    
    objid = container[0].persistent_id
    if isinstance(driver, FilesystemStore):
        containerfile = os.path.join(driver._directory, "%s" % id)
        assert os.path.exists(containerfile)

        objfile = os.path.join(driver._directory, "%s" % objid)
        assert os.path.exists(objfile)

    del container
    container = driver.load(id)
    obj = driver.load(objid)

def _run_driver_tests(db):
    _generic_persistency_test(db)
    _generic_nested_persistency_test(db)
    _generic_newstile_slots_classes(db)
    _save_different_objects_separated(db)


def test_file_persistency():
    tmpdir = tempfile.mkdtemp()

    path = Url(tmpdir)
    fs = FilesystemStore(path.path)
    obj = MyObj('GC3')

    _run_driver_tests(fs)
    shutil.rmtree(tmpdir)

def test_filesystemstorage_pickler_class():
    """
    If you want to save two independent objects but one of them has a
    reference to the other, the standard behavior of Pickle is to save
    a copy of the contained object into the same file of the
    containing object.

    The FilesystemStorage.Pickler class is aimed to avoid this.
    """    
    tmpfname = tempfile.mkdtemp()
    try:
        fs = FilesystemStore(tmpfname)
        obj1 = MyObj('GC3_parent')
        obj2 = MyObj('GC3_children')
        id2 = fs.save(obj2)
        obj1.children = obj2
        assert obj1.children is obj2
        id1 = fs.save(obj1)
        del obj1
        del obj2
        obj1 = fs.load(id1)
        obj2 = fs.load(id2)
        assert obj1.children.x == 'GC3_children'
        # XXX: should this happen? I am not sure
        assert obj1.children is not obj2

        # cleanup
    finally:
        shutil.rmtree(tmpfname)

def _generic_newstile_slots_classes(db):
    return
    obj = SlotClass('GC3')
    assert obj.attr == 'GC3'
    id_ = db.save(obj)
    del obj
    obj2 = db.load(id_)
    assert obj2.attr == 'GC3'

    obj2 = SlotClassWrong('GC3')
    try:
        db.save(obj2)
        assert "We shouldn't reach this point" is False
    except AttributeError:
        pass
    
def test_sqlite_persistency():
    (fd, tmpfname) = tempfile.mkstemp()
    path = Url('sqlite://%s' % tmpfname)
    db = persistence_factory(path)
    obj = MyObj('GC3')
    
    try:
        _run_driver_tests(db)
    finally:
        os.remove(tmpfname)

def test_mysql_persistency():
    if not MySQLdb: return
    
    try:
        path = Url('mysql://gc3user:gc3pwd@localhost/gc3')    
        db = persistence_factory(path)
    except MySQLdb.OperationalError:
        # Ignore MySQL errors, since the mysql server may not be
        # running or not properly configured.
        return
    _run_driver_tests(db)


def test_sqlite_job_persistency():
    try:
        import sqlite3 as sqlite
    except ImportError:
        import pysqlite2.dbapi2 as sqlite
    import gc3libs
    from gc3libs.core import Run
    app = gc3libs.Application(executable='/bin/true', arguments=[], inputs=[], outputs=[], output_dir='/tmp')

    app.execution = MyObj('')
    app.execution.state = Run.State.NEW
    app.execution.lrms_jobid = 1
    app.jobname = 'GC3Test'

    (fd, tmpfname) = tempfile.mkstemp()
    path = Url('sqlite://%s' % tmpfname)
    db = persistence_factory(path)
    id_ = db.save(app)
    
    conn = sqlite.connect(tmpfname)
    c = conn.cursor()
    c.execute('select jobid,jobname, jobstatus from jobs where id=%d' % app.persistent_id)
    row = c.fetchone()
    try:
        assert int(row[0]) == app.execution.lrms_jobid
        assert row[1] == app.jobname
        assert row[2] == app.execution.state
    finally:
        c.close()
        os.remove(tmpfname)
    
def test_sql_injection():
    """Test if the `SQL` class is vulnerable to SQL injection"""
    (fd, tmpfname) = tempfile.mkstemp()
    path = Url('sqlite://%s' % tmpfname)
    db = persistence_factory(path)
    obj = MyTask("Antonio's task")
    obj.execution.lrms_jobid = "my'job'id'is'strange'"
    try:
        id_ = db.save(obj)
        obj2 = db.load(id_)
        assert obj.jobname == obj2.jobname
        assert obj.execution.lrms_jobid == obj2.execution.lrms_jobid
    finally:
        os.remove(path.path)

    
if __name__ == "__main__":
    # fix pickle error
    from test_persistence import MyObj, SlotClassWrong, SlotClass
    test_filesystemstorage_pickler_class()
    test_file_persistency()
    test_sqlite_persistency()
    test_mysql_persistency()
    test_sqlite_job_persistency()
    test_sql_injection()
