#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011-2014, 2018  University of Zurich. All rights reserved.
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
from builtins import str
__docformat__ = 'reStructuredText'

# stdlib imports
import os
import sys
from weakref import WeakValueDictionary

# GC3Pie imports
import gc3libs
import gc3libs.exceptions
from gc3libs.utils import same_docstring_as
from gc3libs.url import Url

from gc3libs.persistence.idfactory import IdFactory
from gc3libs.persistence.serialization import (DEFAULT_PROTOCOL, make_pickler,
                                               make_unpickler)
from gc3libs.persistence.store import Store


# persist objects in a filesystem directory

class FilesystemStore(Store):

    """
    Save and load objects in a given directory.  Uses Python's
    standard `pickle` module to serialize objects onto files.

    All objects are saved as files in the given directory (default:
    `gc3libs.defaults.JOBS_DIR`).  The file name is the object ID.

    If an object contains references to other `Persistable` objects,
    these are saved in the file they would have been saved if the
    `save` method was called on them in the first place, and only an
    'external reference' is saved in the pickled container. This
    ensures that: (1) only one copy of a shared object is ever saved,
    and (2) any shared reference to `Persistable` objects is correctly
    restored when restoring the container.

    The default `idfactory` assigns object IDs by appending a
    sequential number to the class name; see class `Id` for
    details.

    The `protocol` argument specifies the serialization protocol to use,
    if different from `gc3libs.persistence.serialization.DEFAULT_PROTOCOL`.

    Any extra keyword arguments are ignored for compatibility with
    `SqlStore`.
    """

    def __init__(self,
                 directory=gc3libs.defaults.JOBS_DIR,
                 idfactory=IdFactory(),
                 protocol=DEFAULT_PROTOCOL,
                 **extra_args):
        if isinstance(directory, Url):
            super(FilesystemStore, self).__init__(directory)
            directory = directory.path
        else:
            super(FilesystemStore, self).__init__(
                Url(scheme='file', path=os.path.abspath(directory)))
        self._directory = directory

        self.idfactory = idfactory
        self._loaded = WeakValueDictionary()
        self._protocol = protocol

    @same_docstring_as(Store.invalidate_cache)
    def invalidate_cache(self):
        self._loaded.clear()

    @same_docstring_as(Store.list)
    def list(self):
        if not os.path.exists(self._directory):
            return []
        return [id_ for id_ in os.listdir(self._directory)
                if not id_.endswith('.OLD')]

    def _load_from_file(self, path):
        """Auxiliary method for `load`."""
        # gc3libs.log.debug("Loading object from file '%s' ...", path)
        with open(path, 'rb') as src:
            unpickler = make_unpickler(self, src)
            obj = unpickler.load()
            return obj

    @same_docstring_as(Store.load)
    def load(self, id_):
        # return cached copy, if any
        try:
            return self._loaded[str(id_)]
        except KeyError:
            pass

        # no cached copy, load from disk
        filename = os.path.join(self._directory, id_)

        sources = [filename, filename + '.OLD']
        for source in sources:
            if not os.path.exists(source):
                gc3libs.log.debug(
                    "Cannot load object %s from '%s':"
                    " file does not exist", id_, source)
                continue
            try:
                obj = self._load_from_file(source)
                break  # exit `for source in sources` loop ...
            except Exception as ex:
                gc3libs.log.warning(
                    "Failed loading file '%s': %s: %s",
                    filename, ex.__class__.__name__, ex,
                    exc_info=True)
        else:
            # complain loudly
            raise gc3libs.exceptions.LoadError(
                "Failed loading object %s from file(s) %r."
                " (Earlier log lines may provide more details.)"
                % (id_, sources))

        # minimal sanity check
        if not hasattr(obj, 'persistent_id'):
            raise gc3libs.exceptions.LoadError(
                "Invalid format in file '%s':"
                " missing 'persistent_id' attribute"
                % (filename,))
        if str(obj.persistent_id) != str(id_):
            raise gc3libs.exceptions.LoadError(
                "Retrieved persistent ID '%s' %s"
                " does not match given ID '%s' %s"
                % (obj.persistent_id, type(obj.persistent_id),
                   id_, type(id_)))

        # maybe update object after GC3Pie update?
        super(FilesystemStore, self)._update_to_latest_schema()

        # update cache
        assert str(id_) not in self._loaded
        self._loaded[str(id_)] = obj

        return obj

    @same_docstring_as(Store.remove)
    def remove(self, id_):
        filename = os.path.join(self._directory, id_)
        os.remove(filename)
        try:
            del self._loaded[str(id_)]
        except KeyError:
            pass

    @same_docstring_as(Store.replace)
    def replace(self, id_, obj):
        self._save_or_replace(id_, obj)

    @same_docstring_as(Store.save)
    def save(self, obj):
        if not hasattr(obj, 'persistent_id'):
            obj.persistent_id = self.idfactory.new(obj)
        self._save_or_replace(obj.persistent_id, obj)
        return obj.persistent_id

    def _save_or_replace(self, id_, obj):
        """
        Save `obj` into file identified by `id_`; if no such
        destination file exists, create it.  Ensure that the
        destination file is kept intact in case dumping `obj` fails.
        """
        filename = os.path.join(self._directory, id_)
        # gc3libs.log.debug("Storing job '%s' into file '%s'", obj, filename)

        if not os.path.exists(self._directory):
            try:
                os.makedirs(self._directory)
            except Exception as ex:
                # raise same exception but add context message
                gc3libs.log.error("Could not create jobs directory '%s': %s"
                                  % (self._directory, str(ex)))
                raise

        backup = None
        if os.path.exists(filename):
            backup = filename + '.OLD'
            os.rename(filename, backup)

        with open(filename, 'w+b') as tgt:
            try:
                pickler = make_pickler(self, tgt, obj)
                pickler.dump(obj)
            except Exception as err:
                gc3libs.log.error(
                    "Error saving task '%s' to file '%s': %s: %s",
                    obj, filename, err.__class__.__name__, err)
                # move backup file back in place
                if backup is not None:
                    try:
                        os.rename(backup, filename)
                    except:
                        pass  # ignore errors
                raise
            if hasattr(obj, 'changed'):
                obj.changed = False
            # remove backup file, if exists
            try:
                os.remove(backup)
            except:
                pass  # ignore errors
            # update cache
            if id_ in self._loaded:
                old = self._loaded[str(id_)]
                if old is not obj:
                    self._loaded[str(id_)] = obj


def make_filesystemstore(url, *args, **extra_args):
    """
    Return a `FilesystemStore`:class: instance, given a 'file:///' URL
    and optional initialization arguments.

    This function is a bridge between the generic factory functions
    provided by `gc3libs.persistence.make_store`:func: and
    `gc3libs.persistence.register`:func: and the class constructor
    `FilesystemStore`:class.

    Examples::

      >>> fs1 = make_filesystemstore(Url('file:///tmp'))
      >>> fs1.__class__.__name__
      'FilesystemStore'
    """
    assert isinstance(url, Url)
    return FilesystemStore(url.path, *args, **extra_args)


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="filesystem",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
