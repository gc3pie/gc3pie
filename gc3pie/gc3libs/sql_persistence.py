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
"""This file implements an SQL-based persistency driver to store
GC3pie objects in a SQL DB instead of using Pickle from
`persistent.FilesystemStore` class"""

__docformat__ = 'reStructuredText'
__version__ = '$Revision$'

from gc3libs.persistence import Store, IdFactory, Persistable, create_pickler, create_unpickler
from gc3libs.utils import same_docstring_as
import gc3libs.exceptions
from gc3libs import Task

import copy

import cPickle as pickle
import cStringIO as StringIO
import sqlalchemy as sqla
import sqlalchemy.sql as sql

class DummyObject:
    pass

def sql_next_id_factory(db):
    """
    This function will return a function which can be used as
    `next_id_fn` argument for the `IdFactory` class constructor.

    `db` is DB connection class conform to DB API2.0 specs (works also
    with SQLAlchemy engine types)

    The function returned has signature:

        sql_next_id(n=1)

    the id returned is the maximum `id` field in the `store` table plus
    1.
    """
    def sql_next_id(n=1):
        q = db.execute('select max(id) from store')
        nextid = q.fetchone()[0]
        if not nextid: nextid = 1
        else: nextid = int(nextid)+1
        return nextid
    
    return sql_next_id


class IntId(int):
    def __new__(cls, prefix, seqno):
        return int.__new__(cls, seqno)

    def __getnewargs__(self):
        return (None, int(self))

class SQL(Store):
    """
    Save and load objects in a SQL db, using python's `pickle` module
    to serialize objects into a specific field.

    Access to the DB is done via SQLAlchemy module, therefore any
    driver supported by SQLAlchemy will be supported by this class.

    The `url` argument is used to access the store. It is supposed to
    be a `gc3libs.url.Url`:class: class, and therefore may contain
    username, password, host and port if they are needed by the db
    used.

    The `table_name` argument is the name of the table to create. By
    default it's ``store``.

    The constructor will create the `table_name` table if it does not
    exist, but if there already is such a table it will assume the
    it's schema is compatible with our needs. A minimal table schema
    is as follow:

    The meaning of the fields is:

    `id`: this is the id returned by the `save()` method and
    univoquely identify a stored object.

    `data`: the serialization of the object.

        +-----------+--------------+------+-----+---------+
        | Field     | Type         | Null | Key | Default |
        +-----------+--------------+------+-----+---------+
        | id        | int(11)      | NO   | PRI | NULL    |
        | data      | blob         | YES  |     | NULL    |
        +-----------+--------------+------+-----+---------+

    If no optional `extra_fields` argument is passed, also the
    following fields will be created:

        +-----------+--------------+------+-----+---------+
        | Field     | Type         | Null | Key | Default |
        +-----------+--------------+------+-----+---------+
        | type      | varchar(128) | YES  |     | NULL    |
        | jobid     | varchar(128) | YES  |     | NULL    |
        | jobname   | varchar(255) | YES  |     | NULL    |
        | jobstatus | varchar(128) | YES  |     | NULL    |
        +-----------+--------------+------+-----+---------+

     The meaning of these optional fields is:

    `type`: equal to "job" if the object is an instance of the
    `Task`:class: class

    `jobstatus`: if the object is a `Task` istance this wil lbe the
    current execution state of the job

    `jobid`: if the object is a `Task` this will be the
    `obj.execution.lrms_jobid` attribute

    `jobname`: if the object is a `Task` this is its `jobname`
    attribute.    

    The `extra_fields` argument is used to extend the database. It
    must contain a mapping `<column>` : `<function>` where:

    `<column>` may be a `sqlalchemy.Column` object or string. If it is
    a string the corresponding column will be a `BLOB`.

    `<function>` is a function (or lambda) which accept the object as
    argument and will return the value to be stored into the
    database. Any exception raised by this function will be *ignored*.

    For each extra column the `save()` method will call the
    corresponding `<function>` in order to get the correct value to
    store into the db.


    >>> import tempfile, os
    >>> (fd, name) = tempfile.mkstemp()
    >>> from gc3libs.url import Url
    >>> url = Url('sqlite:///%s' % name)
    >>> db = SQL(url)
    >>> from sql_persistence import DummyObject
    >>> obj = DummyObject()
    >>> obj.x = 'test'
    >>> objid = db.save(obj)
    >>> isinstance(objid, int)
    True
    >>> db.list() == [objid]
    True
    >>> db.save(obj) == objid
    True
    >>> del obj
    >>> y = db.load(objid)
    >>> y.x
    'test'
    
    >>> import os
    >>> os.remove(name)
    """
    default_extra_fields = {
                sqla.Column('type', sqla.VARCHAR(length=128)) : lambda obj: isinstance(obj, Task) and 'job' or '',
                sqla.Column('jobid', sqla.VARCHAR(length=128)): lambda obj: obj.execution.lrms_jobid,
                sqla.Column('jobname', sqla.VARCHAR(length=255)): lambda obj: obj.jobname,
                sqla.Column('jobstatus', sqla.VARCHAR(length=128)) : lambda obj: obj.execution.state,
                }

    def __init__(self, url, table_name="store", idfactory=None, extra_fields=default_extra_fields):
        """
        Open a connection to the storage database identified by
        url. It will use the correct backend (MySQL, psql, sqlite3)
        based on the url.scheme value
        """

        # gc3libs.url.Url is not RFC compliant, check issue http://code.google.com/p/gc3pie/issues/detail?id=261
        if url.scheme in ('file', 'sqlite'):
            url = "%s://%s/%s" % (url.scheme, url.netloc, url.path)
        self.__engine = sqla.create_engine(str(url))
        self.tname = table_name

        self.__meta = sqla.MetaData(bind=self.__engine)
        self.__meta.reflect()
        self.extra_fields = {}

        # check if the db exists and already has a 'store' table
        if self.tname not in self.__meta.tables:
            # No table, let's create it
            
            table = sqla.Table(
                self.tname,
                self.__meta,
                sqla.Column('id', sqla.INTEGER(), primary_key=True, nullable=False),
                sqla.Column('data', sqla.BLOB()),
                )
            for col in extra_fields:
                if isinstance(col, sqla.Column):
                    table.append_column(col.copy())
                else:
                    table.append_column(sqla.Column(col, sqla.BLOB()))
            self.__meta.create_all()
        else:
            # A 'store' table exists. Check the column names and fill
            # self.extra_fields

            # Please note that the "i=i" in the lambda definition is
            # *needed*, since otherwise all the lambdas would share
            # the same outer 'i' object, which is the last used in the
            # loop.

            self.extra_fields = dict(((str(i), lambda x, i=i: getattr(x, i))  for i in self.__meta.tables[self.tname].columns.keys() if i not in ('id', 'data', 'type', 'jobid', 'jobname', 'jobstatus')))

        for (col, func) in extra_fields.iteritems():
            if hasattr(col, 'name'): col=col.name
            self.extra_fields[str(col)] = func

        self.t_store = self.__meta.tables[self.tname]
        
        self.idfactory = idfactory
        if not idfactory:
            self.idfactory = IdFactory(id_class=IntId)
            
    @same_docstring_as(Store.list)
    def list(self):
        q = sql.select([self.t_store.c.id])
        conn = self.__engine.connect()
        rows = conn.execute(q)

        ids = [i[0] for i in rows.fetchall()]
        conn.close()
        return ids

    @same_docstring_as(Store.replace)
    def replace(self, id_, obj):
        self._save_or_replace(id_, obj, 'replace')
                              
    # copied from FilesystemStore
    @same_docstring_as(Store.save)
    def save(self, obj):
        if not hasattr(obj, 'persistent_id'):
            obj.persistent_id = self.idfactory.new(obj)
        return self._save_or_replace(obj.persistent_id, obj, 'save')

    def _save_or_replace(self, id_, obj, action):

        fields={'id':id_}
        dstdata = StringIO.StringIO()
        pickler = create_pickler(self, dstdata, obj)
        pickler.dump(obj)
        pdata = dstdata.getvalue().encode('base64')
        fields['data'] = pdata
        # insert into db
        fields['type'] = ''
            
        for column in self.extra_fields:
            try:
                fields[column] = self.extra_fields[column](obj)
            except:
                pass

        q = sql.select([self.t_store.c.id]).where(self.t_store.c.id==id_)
        conn = self.__engine.connect()
        r = conn.execute(q)
        if not r.fetchone():
            # It's an insert
            q = self.t_store.insert().values(**fields)
            conn.execute(q)
        else:
            # it's an update
            q = self.t_store.update().where(self.t_store.c.id==id_).values(**fields)
            conn.execute(q)
        obj.persistent_id = id_
        conn.close()
        
        # return id
        return obj.persistent_id

    @same_docstring_as(Store.load)
    def load(self, id_):
        q = sql.select([self.t_store.c.data]).where(self.t_store.c.id == id_)
        conn = self.__engine.connect()
        r = conn.execute(q)
        rawdata = r.fetchone()
        
        if not rawdata:
            raise gc3libs.exceptions.LoadError("Unable to find object %d" % id_)
        srcdata = StringIO.StringIO(rawdata[0].decode('base64'))
        unpickler = create_unpickler(self, srcdata)
        obj = unpickler.load()
        conn.close()

        return obj

    @same_docstring_as(Store.remove)
    def remove(self, id_):
        conn = self.__engine.connect()
        conn.execute(self.t_store.delete().where(self.t_store.c.id==id_))
        conn.close()

    @staticmethod
    def escape(s):
        """escape string `s` so that it can be used in a sql query.

        Please note that for now we only escape "'" chars because of
        the queries we are doing, thus this function is not at all a
        fully-featured SQL escaping function!

        >>> SQL.escape("Antonio's boat")
        "Antonio''s boat"
        >>> SQL.escape(u"Antonio's unicode boat")
        u"Antonio''s unicode boat"
        >>> SQL.escape(9)
        9
        
        """
        if hasattr(s, 'replace'):
            return s.replace("'", "''")
        else:
            return s
        
## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="sql_persistence",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
