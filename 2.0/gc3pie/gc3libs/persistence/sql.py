#! /usr/bin/env python
#
"""
SQL-based storage of GC3pie objects.
"""
# Copyright (C) 2011-2012, GC3, University of Zurich. All rights reserved.
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
import copy
import cPickle as pickle
import cStringIO as StringIO
import os

import sqlalchemy as sqla
import sqlalchemy.sql as sql

# GC3Pie interface
from gc3libs import Task, Run
import gc3libs.exceptions
import gc3libs.utils
from gc3libs.utils import same_docstring_as, getattr_nested

from gc3libs.persistence.idfactory import IdFactory
from gc3libs.persistence.serialization import make_pickler, make_unpickler
from gc3libs.persistence.store import Store, Persistable


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
        if not nextid:
            nextid = 1
        else:
            nextid = int(nextid) + 1
        return nextid

    return sql_next_id


class IntId(int):
    def __new__(cls, prefix, seqno):
        return int.__new__(cls, seqno)

    def __getnewargs__(self):
        return (None, int(self))


class SqlStore(Store):
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

    `state`: if the object is a `Task` istance this wil lbe the
    current execution state of the job

        +-----------+--------------+------+-----+---------+
        | Field     | Type         | Null | Key | Default |
        +-----------+--------------+------+-----+---------+
        | id        | int(11)      | NO   | PRI | NULL    |
        | data      | blob         | YES  |     | NULL    |
        | state     | varchar(128) | YES  |     | NULL    |
        +-----------+--------------+------+-----+---------+


    The `extra_fields` argument is used to extend the database. It
    must contain a mapping `<column>` : `<function>` where:

    `<column>` is a `sqlalchemy.Column` object.

    `<function>` is a function which takes the object to be saved as
    argument and returns the value to be stored into the database. Any
    exception raised by this function will be *ignored*.  Classes
    `GetAttribute`:class: and `GetItem`:class: in module `get`:mod:
    provide convenient helpers to save object attributes into table
    columns.

    For each extra column the `save()` method will call the
    corresponding `<function>` in order to get the correct value to
    store into the db.

    Any extra keyword arguments are ignored for compatibility with
    `FilesystemStore`.

    """

    def __init__(self, url, table_name="store", idfactory=None,
                 extra_fields={}, create=True, **extra_args):
        """
        Open a connection to the storage database identified by
        url. It will use the correct backend (MySQL, psql, sqlite3)
        based on the url.scheme value
        """
        self.__engine = sqla.create_engine(str(url))
        self.table_name = table_name

        self.__meta = sqla.MetaData(bind=self.__engine)

        # create schema
        table = sqla.Table(
            self.table_name,
            self.__meta,
            sqla.Column('id',
                        sqla.INTEGER(),
                        primary_key=True, nullable=False),
            sqla.Column('data',
                        sqla.BLOB()),
            sqla.Column('state',
                        sqla.VARCHAR(length=128)))

        # create internal rep of table
        self.extra_fields = dict()
        for col, func in extra_fields.iteritems():
            assert isinstance(col, sqla.Column)
            table.append_column(col.copy())
            self.extra_fields[col.name] = func

        current_metadata = sqla.MetaData(bind=self.__engine)
        current_metadata.reflect()
        # check if the db exists and already has a 'store' table
        if create and self.table_name not in current_metadata.tables:
            self.__meta.create_all()

        self.t_store = self.__meta.tables[self.table_name]

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
        self._save_or_replace(id_, obj)

    # copied from FilesystemStore
    @same_docstring_as(Store.save)
    def save(self, obj):
        if not hasattr(obj, 'persistent_id'):
            obj.persistent_id = self.idfactory.new(obj)
        return self._save_or_replace(obj.persistent_id, obj)

    def _save_or_replace(self, id_, obj):
        fields = {'id': id_}

        dstdata = StringIO.StringIO()
        pickler = make_pickler(self, dstdata, obj)
        pickler.dump(obj)
        fields['data'] = dstdata.getvalue()

        try:
            fields['state'] = obj.execution.state
        except AttributeError:
            # If we cannot determine the state of a task, consider it UNKNOWN.
            fields['state'] = Run.State.UNKNOWN

        # insert into db
        for column in self.extra_fields:
            try:
                fields[column] = self.extra_fields[column](obj)
                gc3libs.log.debug("Writing value '%s' in column '%s' for object '%s'",
                                  fields[column], column, obj)
            except Exception, ex:
                gc3libs.log.warning("Error saving DB column '%s' of object '%s': %s: %s",
                                    column, obj, ex.__class__.__name__, str(ex))

        q = sql.select([self.t_store.c.id]).where(self.t_store.c.id == id_)
        conn = self.__engine.connect()
        r = conn.execute(q)
        if not r.fetchone():
            # It's an insert
            q = self.t_store.insert().values(**fields)
            conn.execute(q)
        else:
            # it's an update
            q = self.t_store.update().where(self.t_store.c.id == id_).values(**fields)
            conn.execute(q)
        obj.persistent_id = id_
        if hasattr(obj, 'changed'):
            obj.changed = False
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
            raise gc3libs.exceptions.LoadError(
                "Unable to find any object with ID '%s'" % id_)
        unpickler = make_unpickler(self, StringIO.StringIO(rawdata[0]))
        obj = unpickler.load()
        conn.close()

        return obj

    @same_docstring_as(Store.remove)
    def remove(self, id_):
        conn = self.__engine.connect()
        conn.execute(self.t_store.delete().where(self.t_store.c.id == id_))
        conn.close()


# register all URLs that SQLAlchemy can handle
def make_sqlstore(url, *args, **extra_args):
    """
    Return a `SqlStore`:class: instance, given a SQLAlchemy URL and
    optional initialization arguments.

    This function is a bridge between the generic factory functions
    provided by `gc3libs.persistence.make_store`:func: and
    `gc3libs.persistence.register`:func: and the class constructor
    `SqlStore`:class.

    Examples::

      | >>> ss1 = make_sqlstore(gc3libs.url.Url('sqlite:////tmp/foo.db'))
      | >>> ss1.__class__.__name__
      | 'SqlStore'

    """
    assert isinstance(url, gc3libs.url.Url)
    if url.scheme == 'sqlite':
        # create parent directories: avoid "OperationalError: unable to open database file None None"
        dir = gc3libs.utils.dirname(url.path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        # rewrite ``sqlite`` URLs to be RFC compliant, see:
        # http://code.google.com/p/gc3pie/issues/detail?id=261
        url = "%s://%s/%s" % (url.scheme, url.netloc, url.path)
    return SqlStore(str(url), *args, **extra_args)


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="sql",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
