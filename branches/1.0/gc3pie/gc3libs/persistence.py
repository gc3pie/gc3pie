#! /usr/bin/env python
#
"""
Facade to store and retrieve Job information from permanent storage.
"""
# Copyright (C) 2009-2011 GC3, University of Zurich. All rights reserved.
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
__version__ = '1.0rc3 (SVN $Revision$)'


import operator
import os
import pickle
import sys

import gc3libs
import gc3libs.exceptions
from gc3libs.utils import progressive_number, same_docstring_as



class Store(object):
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
    
    def list(self, **kw):
        """
        Return list of IDs of saved `Job` objects.

        This is an optional method; classes that do not implement it
        should raise a `NotImplementedError` exception.
        """
        raise NotImplementedError("Method `list` not implemented in this class.")

    def remove(self, id_):
        """
        Delete a given object from persistent storage, given its ID.
        """
        raise NotImplementedError("Abstract method 'Store.remove' called"
                                  " -- should have been implemented in a derived class!")

    def replace(self, id_, obj):
        """
        Replace the object already saved with the given ID with a copy of `obj`.
        """
        raise NotImplementedError("Abstract method 'Store.replace' called"
                                  " -- should have been implemented in a derived class!")

    def load(self, id_):
        """
        Load a saved object given its ID, and return it.
        """
        raise NotImplementedError("Abstract method 'Store.load' called"
                                  " -- should have been implemented in a derived class!")

    def save(self, obj):
        """
        Save an object, and return an ID.
        """
        raise NotImplementedError("Abstract method 'Store.save' called"
                                  " -- should have been implemented in a derived class!")



# since the code for `Id` comparison methods is basically the same for
# all methods, we use a decorator-based approach to reduce boilerplate
# code...  Oh, how do I long for LISP macros! :-)
def _Id_make_comparison_function(op):
    """
    Return a function that compares two `Id` objects with the
    passed relational operator. Discards the function being
    decorated.
    """
    def decorate(fn):
        def cmp_fn(self, other):
            # try:
            #     if self._prefix != other._prefix:
            #         raise TypeError("Cannot compare `Id(prefix=%s)` with `Id(prefix=%s)`"
            #                         % (repr(self._prefix), repr(other._prefix)))
            #     return op(self._seqno, other._seqno)
            # except AttributeError:
            #    raise TypeError("`Id` objects can only be compared with other `Id` objects")
            try:
                return op((str(self._prefix), self._seqno), 
                          (str(other._prefix), other._seqno))
            except AttributeError:
                # fall back to safe comparison as `str`
                gc3libs.log.debug("Wrong job ID: comparing '%s' (%s) with '%s' (%s)" 
                                  % (self, type(self), other, type(other)))
                return op(str(self), str(other))
        return cmp_fn
    return decorate

class Id(str):
    """
    An automatically-generated "unique identifier" (a string-like object).
    The unique object identifier has the form "PREFIX.NNN"
    where "NNN" is a decimal number, and "PREFIX" defaults to the
    object class name but can be overridden in the `Id`
    constructor.

    Two object IDs can be compared iff they have the same prefix; in
    which case, the result of the comparison is the same as comparing
    the two sequence numbers.
    """

    def __new__(cls, prefix, seqno):
        """
        Construct a new "unique identifier" instance (a string).
        """
        instance = str.__new__(cls, "%s.%d" % (prefix, seqno))
        instance._seqno = seqno
        instance._prefix = prefix
        return instance
    def __getnewargs__(self):
        return (self._prefix, self._seqno)
    
    # Rich comparison operators, to ensure `Id` is sorted by numerical value
    @_Id_make_comparison_function(operator.gt)
    def __gt__(self, other):
        pass
    @_Id_make_comparison_function(operator.ge)
    def __ge__(self, other):
        pass
    @_Id_make_comparison_function(operator.eq)
    def __eq__(self, other):
        pass
    @_Id_make_comparison_function(operator.ne)
    def __ne__(self, other):
        pass
    @_Id_make_comparison_function(operator.le)
    def __le__(self, other):
        pass
    @_Id_make_comparison_function(operator.lt)
    def __lt__(self, other):
        pass


class IdFactory(object):
    """
    Automatically generate a "unique identifier" (of class `Id`).
    Object identifiers are temporally unique: no identifier will
    (ever) be re-used, even in different invocations of the program.
    """
    def __init__(self, prefix=None, next_id_fn=None, id_class=Id):
        """
        Construct an `IdFactory` instance whose `new` method returns
        objects of class `id_class` (default: `Id`:class:) with the
        given `prefix` string and whose identifier number is computed
        by an invocation of function `next_id_fn`.

        Function `next_id_fn` must conform to the calling syntax and
        behavior of the `gc3libs.utils.progressive_number`:func:
        (which is the one used by default).
        """
        self._prefix = prefix
        if next_id_fn is None:
            self._next_id_fn = progressive_number
        else:
            self._next_id_fn = next_id_fn
        self._idclass = id_class

    def reserve(self, n):
        """
        Pre-allocate `n` IDs.  Successive invocations of the `Id`
        constructor will return one of the pre-allocated, with a
        potential speed gain if many `Id` objects are constructed in a
        loop.
        """
        assert n > 0, "Argument `n` must be a positive integer"
        IdFactory._seqno_pool.extend(self._next_id_fn(n))
    _seqno_pool = [ ]

    def new(self, obj):
        """
        Return a new "unique identifier" instance (a string).
        """
        if self._prefix is None:
            prefix = obj.__class__.__name__
        else:
            prefix = self._prefix
        if len(IdFactory._seqno_pool) > 0:
            seqno = IdFactory._seqno_pool.pop()
        else:
            seqno = self._next_id_fn()
        return self._idclass(prefix, seqno)


class JobIdFactory(IdFactory):
    """
    Override :py:class:`IdFactory` behavior and generate IDs starting with a
    lowercase ``job`` prefix.
    """
    def __init__(self, next_id_fn=None):
        IdFactory.__init__(self, 'job', next_id_fn)



class Persistable(object):
    """
    A mix-in class to mark that an object should be persisted by its ID.

    Any instance of this class is saved as an "external reference"
    when a container holding a reference to it is saved.
    """
    pass


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
        self._directory = directory
        self.idfactory = idfactory
        self._protocol = protocol


    class Pickler(pickle.Pickler):
        """
        Pickle a Python object, saving the `Persistable` instances contained
        in it as external references through the same `FilesystemStore`.
        """
        def __init__(self, parent, file, root_obj):
            pickle.Pickler.__init__(self, file, parent._protocol)
            self._parent = parent
            self._root = root_obj
        def persistent_id(self, obj):
            # see: http://docs.python.org/library/pickle.html#pickling-and-unpickling-external-objects
            if obj is self._root:
                return None
            elif hasattr(obj, 'persistent_id'):
                return obj.persistent_id
            elif isinstance(obj, Persistable):
                # object is persistable, but not saved (yet), so save
                # it now and then return its `persistent_id` as
                # assigned by `save`.
                self._parent.save(obj)
                return obj.persistent_id
            else:
                return None
        # we may need to pickle/unpickle `None`, so define an
        # "impossible object" to be used as a "no argument given"
        # marker (Oh, CL... again!)
        _NoObject = object()
        def dump(self, obj=_NoObject):
            if obj is self._NoObject:
                pickle.Pickler.dump(self, self._root)
            elif isinstance(obj, Persistable):
                self._parent.save(obj)
            else:
                pickle.Pickler.dump(self, obj)

    class Unpickler(pickle.Unpickler):
        def __init__(self, parent, file):
            pickle.Unpickler.__init__(self, file)
            self._parent = parent
        # see: http://docs.python.org/library/pickle.html#pickling-and-unpickling-external-objects
        def persistent_load(self, id_):
            return self._parent.load(id_)


    @same_docstring_as(Store.list)
    def list(self):
        if not os.path.exists(self._directory):
            return [ ]
        return [ id_ for id_ in os.listdir(self._directory)
                 if not id_.endswith('.OLD') ]


    @same_docstring_as(Store.load)
    def load(self, id_):
        filename = os.path.join(self._directory, id_)
        gc3libs.log.debug("Loading object from file '%s' ...", filename)

        if not os.path.exists(filename):
            raise gc3libs.exceptions.LoadError("No '%s' file found in directory '%s'" 
                                   % (id_, self._directory))

        # XXX: this should become `with src = ...:` as soon as we stop
        # supporting Python 2.4
        src = None
        try:
            src = open(filename, 'rb')
            unpickler = FilesystemStore.Unpickler(self, src)
            obj = unpickler.load()
            src.close()
        except Exception, ex:
            if src is not None:
                try:
                    src.close()
                except:
                    pass # ignore errors
            sys.excepthook(* sys.exc_info())
            raise gc3libs.exceptions.LoadError("Failed retrieving object from file '%s': %s: %s"
                                   % (filename, ex.__class__.__name__, str(ex)))
        if not hasattr(obj, 'persistent_id'):
            raise gc3libs.exceptions.LoadError("Invalid format in file '%s': missing 'persistent_id' attribute"
                                   % (filename))
        if str(obj.persistent_id) != str(id_):
            raise gc3libs.exceptions.LoadError("Retrieved persistent ID '%s' does not match given ID '%s'" 
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
        gc3libs.log.debug("Storing job '%s' into file '%s'", obj, filename)

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
            pickler = FilesystemStore.Pickler(self, tgt, obj)
            pickler.dump()
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



## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="persistence",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
