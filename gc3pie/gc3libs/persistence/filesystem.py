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
__docformat__ = 'reStructuredText'
__version__ = '$Revision$'


import os
import cPickle as pickle
import sys

import gc3libs
import gc3libs.exceptions
from gc3libs.utils import progressive_number, same_docstring_as
import gc3libs.url

from store import Store, Persistable, register
from idfactory import IdFactory


class _PersistentIdToSave(object):
    """This class is needed because:

    * we want to save each `Persistable`:class: object as a separate record

    * we want to use cPickle.

    Check http://docs.python.org/library/pickle.html#pickling-and-unpickling-external-objects

    for details on the differences between `pickle` and `cPickle`
    modules.
    """
    def __init__(self, driver, root):
        self._root = root
        self._driver = driver

    def __call__(self, obj):
        if obj is self._root:
            return None
        elif hasattr(obj, 'persistent_id'):
            return obj.persistent_id
        elif isinstance(obj, Persistable):
            self._driver.save(obj)
            return obj.persistent_id

class _PersistentLoadExternalId(object):
    """This class is needed because:

    * we want to save each `Persistable`:class: object as a separate record

    * we want to use cPickle.

    Check http://docs.python.org/library/pickle.html#pickling-and-unpickling-external-objects

    for details on the differences between `pickle` and `cPickle`
    modules.
    """
    def __init__(self, driver):
        self._driver = driver

    def __call__(self, id_):
        return self._driver.load(id_)

def create_pickler(driver, stream, root, protocol=pickle.HIGHEST_PROTOCOL):
    p = pickle.Pickler(stream, protocol=protocol)
    p.persistent_id = _PersistentIdToSave(driver, root)
    return p

def create_unpickler(driver, stream):
    p = pickle.Unpickler(stream)
    p.persistent_load = _PersistentLoadExternalId(driver)
    return p


## persist objects in a filesystem directory

class FilesystemStore(Store):
    """
    Save and load objects in a given directory.  Uses Python's
    standard `pickle` module to serialize objects onto files.

    All objects are saved as files in the given directory (default:
    `gc3libs.Default.JOBS_DIR`).  The file name is the object ID.

    If an object contains references to other `Persistable` objects,
    these are saved in the file they would have been saved if the
    `save` method was called on them in the first place, and only an
    "external reference" is saved in the pickled container. This
    ensures that: (1) only one copy of a shared object is ever saved,
    and (2) any shared reference to `Persistable` objects is correctly
    restored when restoring the container.

    The default `idfactory` assigns object IDs by appending a
    sequential number to the class name; see class `Id` for
    details.

    The `protocol` argument specifies the pickle protocol to use
    (default: `pickle` protocol 0).  See the `pickle` module
    documentation for details.
    """
    def __init__(self, directory=gc3libs.Default.JOBS_DIR, 
                 idfactory=IdFactory(), protocol=pickle.HIGHEST_PROTOCOL):
        if isinstance(directory, gc3libs.url.Url):
            directory=directory.path
        self._directory = directory
        
        self.idfactory = idfactory
        self._protocol = protocol


    @same_docstring_as(Store.list)
    def list(self):
        if not os.path.exists(self._directory):
            return [ ]
        return [ id_ for id_ in os.listdir(self._directory)
                 if not id_.endswith('.OLD') ]


    def _load_from_file(self, path):
        """Auxiliary method for `load`."""
        src = None
        try:
            src = open(path, 'rb')
            unpickler = create_unpickler(self, src)
            obj = unpickler.load()
            src.close()
            return obj
        except Exception, ex:
            if src is not None:
                try:
                    src.close()
                except:
                    pass # ignore errors
            raise
                
    @same_docstring_as(Store.load)
    def load(self, id_):
        filename = os.path.join(self._directory, id_)
        #gc3libs.log.debug("Loading object from file '%s' ...", filename)

        if not os.path.exists(filename):
            raise gc3libs.exceptions.LoadError("No '%s' file found in directory '%s'" 
                                   % (id_, self._directory))

        # XXX: this should become `with src = ...:` as soon as we stop
        # supporting Python 2.4
        try:
            obj = self._load_from_file(filename)
        except Exception, ex:
            gc3libs.log.warning("Failed loading file '%s': %s: %s",
                                filename, ex.__class__.__name__, str(ex),
                                exc_info=True)
            old_copy = filename + '.OLD'
            if os.path.exists(old_copy):
                gc3libs.log.warning(
                    "Will try loading from backup file '%s' instead...", old_copy)
                try:
                    obj = self._load_from_file(old_copy)
                except Exception, ex:
                    sys.excepthook(* sys.exc_info())
                    raise gc3libs.exceptions.LoadError(
                        "Failed retrieving object from file '%s': %s: %s"
                        % (filename, ex.__class__.__name__, str(ex)))
            else:
                # complain loudly
                raise gc3libs.exceptions.LoadError(
                    "Failed retrieving object from file '%s': %s: %s"
                    % (filename, ex.__class__.__name__, str(ex)))
        if not hasattr(obj, 'persistent_id'):
            raise gc3libs.exceptions.LoadError(
                "Invalid format in file '%s': missing 'persistent_id' attribute"
                % (filename))
        if str(obj.persistent_id) != str(id_):
            raise gc3libs.exceptions.LoadError(
                "Retrieved persistent ID '%s' does not match given ID '%s'" 
                % (obj.persistent_id, id_))
        return obj


    @same_docstring_as(Store.remove)
    def remove(self, id_):
        filename = os.path.join(self._directory, id_)
        os.remove(filename)


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
        #gc3libs.log.debug("Storing job '%s' into file '%s'", obj, filename)

        if not os.path.exists(self._directory):
            try:
                os.makedirs(self._directory)
            except Exception, ex:
                # raise same exception but add context message
                gc3libs.log.error("Could not create jobs directory '%s': %s" 
                                  % (self._directory, str(ex)))
                raise

        backup = None
        if os.path.exists(filename):
            backup = filename + '.OLD'
            os.rename(filename, backup)
        
        # TODO: this should become `with tgt = ...:` as soon as we
        # stop supporting Python 2.4
        tgt = None
        try:
            tgt = open(filename, 'w+b')
            pickler = create_pickler(self, tgt, obj)
            pickler.dump(obj)
            tgt.close()
            try:
                os.remove(backup)
            except:
                pass # ignore errors
        except Exception, ex:
            gc3libs.log.error("Error saving job '%s' to file '%s': %s: %s" 
                              % (obj, filename, ex.__class__.__name__, ex))
            if tgt is not None:
                try:
                    tgt.close()
                except:
                    pass # ignore errors
            if backup is not None:
                try: 
                    os.rename(backup, filename)
                except:
                    pass # ignore errors
            raise


def make_filesystemstore(url, *args, **kw):
    """
    Return a `FilesystemStore`:class: instance, given a 'file:///' URL
    and optional initialization arguments.

    This function is a bridge between the generic factory functions
    provided by `gc3libs.persistence.make_store`:func: and
    `gc3libs.persistence.register`:func: and the class constructor
    `FilesystemStore`:class.

    Examples::

      >>> fs1 = make_filesystemstore(gc3libs.url.Url('file:///tmp'))
      >>> fs1.__class__.__name__
      'FilesystemStore'
    """
    assert isinstance(url, gc3libs.url.Url)
    return FilesystemStore(url.path, *args, **kw)

register('file', make_filesystemstore)


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="filesystem",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
