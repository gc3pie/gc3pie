#! /usr/bin/env python
#
"""
SQL-based storage of GC3pie objects.
"""
# Copyright (C) 2011-2019  University of Zurich. All rights reserved.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

from __future__ import absolute_import, print_function, unicode_literals
from future import standard_library
standard_library.install_aliases()
from builtins import str
__docformat__ = 'reStructuredText'


# stdlib imports
from contextlib import closing
from io import BytesIO
import os
from urllib.parse import parse_qs
from warnings import warn
from weakref import WeakValueDictionary

import sqlalchemy as sqla
import sqlalchemy.sql as sql

# GC3Pie interface
from gc3libs import Run
import gc3libs.exceptions
from gc3libs.url import Url
import gc3libs.utils
from gc3libs.utils import same_docstring_as

from gc3libs.persistence.idfactory import IdFactory
from gc3libs.persistence.serialization import make_pickler, make_unpickler
from gc3libs.persistence.store import Store


# uncomment lines containing `_lvl` to show nested save/loads in logs
#_lvl = ''


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
    default it's ``store``.  Alternatively, the table name can be
    given in the "fragment" part of the database URL, as
    ``#table=...`` (replace ``...`` with the actual table name).  The
    constructor argument takes precedence over the table name
    specified in the DB URL.

    The constructor will create the `table_name` table if it does not
    exist, but if there already is such a table it will assume that
    its schema is compatible with our needs. A minimal table schema
    is as follows::

        +-----------+--------------+------+-----+---------+
        | Field     | Type         | Null | Key | Default |
        +-----------+--------------+------+-----+---------+
        | id        | int(11)      | NO   | PRI | NULL    |
        | data      | blob         | YES  |     | NULL    |
        | state     | varchar(128) | YES  |     | NULL    |
        +-----------+--------------+------+-----+---------+

    The meaning of the fields is:

    - `id`: this is the id returned by the `save()` method and
      uniquely identifies a stored object.

    - `data`: serialized Python object.

    - `state`: if the object is a `Task`:class: instance, this will be
      its current execution state.

    The `extra_fields` constructor argument is used to extend the
    database. It must contain a mapping `*column*: *function*`
    where:

    - *column* is a `sqlalchemy.Column` object.

    - *function* is a function which takes the object to be saved as
      argument and returns the value to be stored into the
      database. Any exception raised by this function will be
      *ignored*.  Classes `GetAttribute`:class: and `GetItem`:class:
      in module `get`:mod: provide convenient helpers to save object
      attributes into table columns.

    For each extra column the `save()` method will call the
    corresponding *function* in order to get the correct value to
    store into the DB.

    Any extra keyword arguments are ignored for compatibility with
    `FilesystemStore`:class:.
    """

    def __init__(self, url, table_name=None, idfactory=None,
                 extra_fields=None, create=True, **extra_args):
        """
        Open a connection to the storage database identified by `url`.

        DB backend (MySQL, psql, sqlite3) is chosen based on the
        `url.scheme` value.
        """
        super(SqlStore, self).__init__(url)
        if self.url.fragment:
            kv = parse_qs(self.url.fragment)
        else:
            kv = {}

        # init static public args
        self.idfactory = idfactory or IdFactory(id_class=IntId)

        url_table_names = kv.get('table')
        if url_table_names:
            url_table_name = url_table_names[-1]  # last wins
        else:
            url_table_name = ''
        if table_name is None:
            self.table_name = url_table_name or "store"
        else:
            if table_name != url_table_name:
                gc3libs.log.debug(
                    "DB table name given in store URL fragment,"
                    " but overriden by `table` argument to SqlStore()")
            self.table_name = table_name

        # save ctor args for lazy-initialization
        self._init_extra_fields = (extra_fields if extra_fields is not None else {})
        self._init_create = create

        # create slots for lazy-init'ed attrs
        self._real_engine = None
        self._real_extra_fields = None
        self._real_tables = None

        self._loaded = WeakValueDictionary()

    @staticmethod
    def _to_sqlalchemy_url(url):
        if url.scheme == 'sqlite':
            # rewrite ``sqlite`` URLs to be RFC compliant, see:
            # https://github.com/uzh/gc3pie/issues/261
            db_url = "%s://%s/%s" % (url.scheme, url.netloc, url.path)
        else:
            db_url = str(url)
        # remove fragment identifier, if any
        try:
            fragment_loc = db_url.index('#')
            db_url = db_url[:fragment_loc]
        except ValueError:
            pass
        return db_url

    def _delayed_init(self):
        """
        Perform initialization tasks that can interfere with
        forking/multiprocess setup.

        See `GC3Pie issue #550 <https://github.com/uzh/gc3pie/issues/550>`_
        for more details and motivation.
        """
        url = self._to_sqlalchemy_url(self.url)
        gc3libs.log.debug(
            "Initializing SQLAlchemy engine for `%s`...", url)
        self._real_engine = sqla.create_engine(url)

        # create schema
        meta = sqla.MetaData(bind=self._real_engine)
        table = sqla.Table(
            self.table_name,
            meta,
            sqla.Column('id',
                        sqla.Integer(),
                        primary_key=True, nullable=False),
            sqla.Column('data',
                        sqla.LargeBinary()),
            sqla.Column('state',
                        sqla.String(length=128)))

        # create internal rep of table
        self._real_extra_fields = {}
        for col, func in self._init_extra_fields.items():
            assert isinstance(col, sqla.Column)
            table.append_column(col.copy())
            self._real_extra_fields[col.name] = func

        # check if the db exists and already has a 'store' table
        current_meta = sqla.MetaData(bind=self._real_engine)
        current_meta.reflect()
        if self._init_create and self.table_name not in current_meta.tables:
            meta.create_all()

        self._real_tables = meta.tables[self.table_name]


    def pre_fork(self):
        """
        Dispose current SQLAlchemy engine (if any).
        A new SQLAlchemy engine will be initialized
        upon the next interaction with a DB.

        This method only exists to allow `SessionBasedDaemon`:class:
        and similar applications that can do DB operations after
        fork()ing to continue to operate, without incurring into a
        SQLAlchemy "OperationalError: (...) could not receive data
        from server: Transport endpoint is not connected"
        """
        if self._real_engine:
            self._real_engine.dispose()
        self._real_engine = None
        self._real_extra_fields = None
        self._real_tables = None


    @property
    def _engine(self):
        if self._real_engine is None:
            self._delayed_init()
        return self._real_engine

    @property
    def _tables(self):
        if self._real_tables is None:
            self._delayed_init()
        return self._real_tables

    # FIXME: Remove once the TissueMAPS code is updated not to use this any more!
    @property
    def t_store(self):
        """
        Deprecated compatibility alias for `SqlStore._tables`
        """
        warn("`SqlStore.t_store` has been renamed to `SqlStore._tables`;"
             " please update your code", DeprecationWarning, 2)
        return self._tables

    @property
    def extra_fields(self):
        if self._real_extra_fields is None:
            self._delayed_init()
        return self._real_extra_fields


    @same_docstring_as(Store.invalidate_cache)
    def invalidate_cache(self):
        self._loaded.clear()

    @same_docstring_as(Store.list)
    def list(self):
        q = sql.select([self._tables.c.id])
        with self._engine.begin() as conn:
            rows = conn.execute(q)
            ids = [i[0] for i in rows.fetchall()]
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
        # if __debug__:
        #     global _lvl
        #     _lvl += '>'
        #     gc3libs.log.debug("%s Saving %r@%x as %s ...", _lvl, obj, id(obj), id_)

        # build row to insert/update
        fields = {'id': id_}

        with closing(BytesIO()) as dstdata:
            make_pickler(self, dstdata, obj).dump(obj)
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
            except Exception as ex:
                gc3libs.log.warning(
                    "Error saving DB column '%s' of object '%s': %s: %s",
                    column, obj, ex.__class__.__name__, str(ex))

        if __debug__:
            for column in fields:
                if column == 'data':
                    continue
                gc3libs.log.debug(
                    "Writing value '%s' in column '%s' for object '%s'",
                    fields[column], column, obj)

        q = sql.select([self._tables.c.id]).where(self._tables.c.id == id_)
        with self._engine.begin() as conn:
            r = conn.execute(q)
            if not r.fetchone():
                # It's an insert
                q = self._tables.insert().values(**fields)
            else:
                # it's an update
                q = self._tables.update().where(
                        self._tables.c.id == id_).values(**fields)
            conn.execute(q)
        obj.persistent_id = id_
        if hasattr(obj, 'changed'):
            obj.changed = False

        # update cache
        if str(id_) in self._loaded:
            old = self._loaded[str(id_)]
            if old is not obj:
                self._loaded[str(id_)] = obj
                # if __debug__:
                #     gc3libs.log.debug(
                #         "%s Overwriting object %s %r@%x with %r@%x",
                #         _lvl, id_, old, id(old), obj, id(obj))
                #     from traceback import format_stack
                #     gc3libs.log.debug("Traceback:\n%s", ''.join(format_stack()))

        # if __debug__:
        #     gc3libs.log.debug("%s Done saving %r@%x as %s ...", _lvl, obj, id(obj), id_)
        #     if _lvl:
        #         _lvl = _lvl[:-1]

        # return id
        return id_

    @same_docstring_as(Store.load)
    def load(self, id_):
        # if __debug__:
        #     global _lvl
        #     _lvl += '<'
        #     gc3libs.log.debug("%s Store %s: Loading task %s %r ...", _lvl, self, id_, type(id_))

        # return cached copy, if any
        try:
            obj = self._loaded[str(id_)]
            # if __debug__:
            #     if _lvl:
            #         _lvl = _lvl[:-1]
            #     gc3libs.log.debug("%s Store %s: Returning cached object %r@%x as task %s", _lvl, self, obj, id(obj), id_)
            return obj
        except KeyError:
            pass

        # no cached copy, load from disk
        q = sql.select([self._tables.c.data]).where(self._tables.c.id == id_)
        with self._engine.begin() as conn:
            rawdata = conn.execute(q).fetchone()
        if not rawdata:
            raise gc3libs.exceptions.LoadError(
                "Unable to find any object with ID '%s'" % id_)
        obj = make_unpickler(self, BytesIO(rawdata[0])).load()
        super(SqlStore, self)._update_to_latest_schema()
        assert str(id_) not in self._loaded
        self._loaded[str(id_)] = obj
        # if __debug__:
        #     if _lvl:
        #         _lvl = _lvl[:-1]
        #     gc3libs.log.debug("%s Store %s: Done loading task %s as %r@%x.", _lvl, self, id_, obj, id(obj))
        return obj

    @same_docstring_as(Store.remove)
    def remove(self, id_):
        with self._engine.begin() as conn:
            conn.execute(
                self._tables.delete().where(self._tables.c.id == id_))
        try:
            del self._loaded[str(id_)]
        except KeyError:
            pass


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
    gc3libs.log.debug("Building SQL store from URL %s", url)
    if url.scheme == 'sqlite':
        # create parent directories: avoid "OperationalError: unable to open
        # database file None None"
        dir = gc3libs.utils.dirname(url.path)
        if not os.path.exists(dir):
            os.makedirs(dir)
    return SqlStore(url, *args, **extra_args)


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="sql",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
