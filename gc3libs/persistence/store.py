#! /usr/bin/env python
#
"""
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

# stdlib imports
from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
from builtins import object
from abc import ABCMeta, abstractmethod

# GC3Pie imports
import gc3libs
from gc3libs.url import Url
from future.utils import with_metaclass


__docformat__ = 'reStructuredText'


class Store(with_metaclass(ABCMeta, object)):
    """
    Interface for storing and retrieving objects on permanent storage.

    Each `save` operation returns a unique "ID"; each ID is a Python
    string value, which is guaranteed to be temporally unique, i.e.,
    no two `save` operations in the same persistent store can result
    in the same IDs being assigned to different objects.  The "ID" is
    also stored in the instance attribute `_id`.

    Any Python object can stored, provided it meets the following
    conditions:

      * it can be pickled with Python's standard module `pickle`.
      * the instance attribute `persistent_id` is reserved for use by
        the `Store` class: it should not be set or altered by other
        parts of the code.
    """

    def __init__(self, url=None):
        if url and not isinstance(url, Url):
            url = Url(url)
        self.url = url

    def pre_fork(self):
        """
        Make preparations for `fork()`-ing the current process.

        This should close open network connections or any other
        sockets or file descriptors that cannot be used by both the
        parent and child process.

        The default implementation of this method does nothing; as of
        2018-04-10, the only subclass making use of this functionality
        is `SqlStore`:class:, which needs to dispose the SQLAlchemy
        engine and re-create it after forking.
        """
        pass

    def post_fork(self):
        """
        Restore functionality that was suspended in `pre_fork`:meth:

        This method will be called after forking/daemonizing has been
        successfully accomplished.

        The default implementation of this method does nothing.
        """
        pass

    def invalidate_cache(self):
        """
        Clear the loaded objects cache (if any).

        Subsequent `load()`:meth: calls are guaranteed to re-load the
        data directly from the backing store.
        """
        # The default implementation of this method does nothing, since
        # a `Store` subclass may not keep a cache of loaded objects.
        pass

    def list(self, **extra_args):
        """
        Return list of IDs of saved `Job` objects.

        This is an optional method; classes that do not implement it
        should raise a `NotImplementedError` exception.
        """
        raise NotImplementedError(
            "Method `list` not implemented in this class.")

    @abstractmethod
    def load(self, id_):
        """
        Load a saved object given its ID, and return it.
        """
        pass

    def _update_to_latest_schema(self):
        """
        Modify an object in-place to reflect changes in the schema.

        Called immediately after a successful `load()`, just before returning
        the retrieved object to the caller.
        """
        pass

    @abstractmethod
    def remove(self, id_):
        """
        Delete a given object from persistent storage, given its ID.
        """
        pass

    @abstractmethod
    def replace(self, id_, obj):
        """
        Replace the object already saved with the given ID with a copy
        of `obj`.
        """
        pass

    @abstractmethod
    def save(self, obj):
        """
        Save an object, and return an ID.
        """
        pass


# registration mechanism

_registered_store_ctors = {}


def register(scheme, constructor):
    """
    Register `constructor` as the factory corresponding to an URL scheme.

    If a different constructor is already registered for the same
    scheme, it is silently overwritten.

    The registry mapping schemes to constructors is used in the
    `make_store`:func: to create concrete instances of
    `gc3libs.persistence.Store`, given a URI that identifies the kind
    and location of the storage.

    :param str scheme: URL scheme to associate with the given constructor.

    :param callable constructor: A callable returning a `Store`:class:
      instance. Typically, a class constructor.
    """
    global _registered_store_ctors
    assert callable(constructor), (
        "Registering non-callable constructor for scheme "
        "'%s' in `gc3libs.persistence.register`"
        % scheme)
    gc3libs.log.debug(
        "Registering scheme '%s' with the `gc3libs.persistence` registry.",
        scheme)
    _registered_store_ctors[str(scheme)] = constructor


def make_store(uri, *args, **extra_args):
    """
    Factory producing concrete `Store`:class: instances.

    Given a URL and (optionally) initialization arguments, return a
    fully-constructed `Store`:class: instance.

    The only required argument is `uri`; if any other arguments are
    present in the function invocation, they are passed verbatim to
    the constructor associated with the scheme of the given `uri`.

    Example::

      >>> fs1 = make_store('file:///tmp')
      >>> fs1.__class__.__name__
      'FilesystemStore'

    Argument `uri` can also consist of a path name, in which case a
    URL scheme 'file:///' is assumed::

      >>> fs2 = make_store('/tmp')
      >>> fs2.__class__.__name__
      'FilesystemStore'

    """
    if not isinstance(uri, Url):
        uri = Url(uri)

    # since SQLAlchemy allows URIs of the form `db+driver://...`
    # (e.g., `postresql+psycopg://...`) we need to examine the URI
    # scheme only up to the first `+`
    scheme = uri.scheme.split('+')[0]

    try:
        # hard-code schemes that are supported by GC3Pie itself
        if uri.scheme == 'file':
            import gc3libs.persistence.filesystem
            return gc3libs.persistence.filesystem.make_filesystemstore(
                uri, *args, **extra_args)
        elif scheme in [
                # DBs supported in SQLAlchemy core as of version 1.1,
                # see: http://docs.sqlalchemy.org/en/latest/dialects/index.html
                'firebird',
                'mssql',
                'mysql',
                'oracle',
                'postgresql',
                'sqlite',
                'sybase',
        ]:
            import gc3libs.persistence.sql
            return gc3libs.persistence.sql.make_sqlstore(
                uri, *args, **extra_args)
        else:
            try:
                return _registered_store_ctors[
                    uri.scheme](uri, *args, **extra_args)
            except KeyError:
                gc3libs.log.error(
                    "Unknown URL scheme '%s' in"
                    " `gc3libs.persistence.make_store`:"
                    " has never been registered.", uri.scheme)
                raise
    except Exception as err:
        gc3libs.log.error(
            "Error constructing store for URL '%s': %s: %s",
            uri, err.__class__.__name__, err)
        raise

# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="store",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
