#! /usr/bin/env python
#
"""
Facade to store and retrieve Job information from permanent storage.
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
#
# Includes parts adapted from the ``bzr`` code, which is
# copyright (C) 2005, 2006, 2007, 2008, 2009 Canonical Ltd
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


import cPickle as pickle
import os

import gc3libs
import gc3libs.Default
from gc3libs.Exceptions import JobRetrieveError
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
      * the instance attribute `_id` is reserved for use by the `Store` 
        class: it should not be set or altered by other parts of the code.
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



class _Id(str):
    """
    An automatically-generated "unique job identifier" (a string).
    Job identifiers are temporally unique: no job identifier will
    (ever) be re-used, even in different invocations of the program.
    
    Currently, the unique job identifier has the form "job.XXX" where
    "XXX" is a decimal number.  

    This class provides services for generating temporally unique Job
    IDs, and for comparing/sorting Job IDs based on their progressive
    number.
    """
    def __new__(cls, seqno=None, prefix="job"):
        """
        Construct a new "unique job identifier" instance (a string).
        """
        if seqno is None:
            seqno = progressive_number()
        instance = str.__new__(cls, "%s.%d" % (prefix, seqno))
        instance._seqno = seqno
        instance._prefix = prefix
        return instance
    def __getnewargs__(self):
        return (self._seqno, self._prefix)

    # rich comparison operators, to ensure `_Id` is sorted by numerical value
    def __gt__(self, other):
        try:
            return self._seqno > other._seqno
        except AttributeError:
            raise TypeError("`_Id` objects can only be compared with other `_Id` objects")
    def __ge__(self, other):
        try:
            return self._seqno >= other._seqno
        except AttributeError:
            raise TypeError("`_Id` objects can only be compared with other `_Id` objects")
    def __eq__(self, other):
        try:
            return self._seqno == other._seqno
        except AttributeError:
            raise TypeError("`_Id` objects can only be compared with other `_Id` objects")
    def __ne__(self, other):
        try:
            return self._seqno != other._seqno
        except AttributeError:
            raise TypeError("`_Id` objects can only be compared with other `_Id` objects")
    def __le__(self, other):
        try:
            return self._seqno <= other._seqno
        except AttributeError:
            raise TypeError("`_Id` objects can only be compared with other `_Id` objects")
    def __lt__(self, other):
        try:
            return self._seqno < other._seqno
        except AttributeError:
            raise TypeError("`_Id` objects can only be compared with other `_Id` objects")



class FilesystemStore(Store):
    """
    Save and load objects in a given directory.  Uses Python's
    standard `pickle` module to serialize objects onto files.
    
    All objects are saved as files in the given directory (default:
    `gc3libs.Default.JOBS_DIR`).
    
    The `protocol` argument specifies the pickle protocol to use
    (default: `pickle` protocol 2).  See the `pickle` module
    documentation for details.
    """
    def __init__(self, directory=gc3libs.Default.JOBS_DIR, 
                 idfactory=_Id, protocol=2):
        self._directory = directory
        self._idfactory = idfactory
        self._protocol = protocol


    @same_docstring_as(Store.list)
    def list(self):
        if not os.path.exists(self._directory):
            return [ ]
        return [ id_ for id_ in os.listdir(self._directory)
                 if not id_.endswith('.OLD') ]


    @same_docstring_as(Store.load)
    def load(self, id_):
        filename = os.path.join(self._directory, id_)
        gc3libs.log.debug("Retrieving job from file '%s' ...", filename)

        if not os.path.exists(filename):
            raise JobRetrieveError("No '%s' file found in directory '%s'" 
                                   % (id_, self._directory))

        # XXX: this should become `with src = ...:` as soon as we stop
        # supporting Python 2.4
        src = None
        try:
            src = open(filename, 'rb')
            obj = pickle.load(src)
            src.close()
        except Exception, ex:
            if src is not None:
                try:
                    src.close()
                except:
                    pass # ignore errors
            raise JobRetrieveError("Failed retrieving job from file '%s': %s: %s"
                                   % (filename, ex.__class__.__name__, str(ex)))
        if str(obj._id) != str(id_):
            raise JobRetrieveError("Retrieved Job ID '%s' does not match given Job ID '%s'" 
                                   % (obj._id, id_))
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
        if not hasattr(obj, '_id'):
            obj._id = self._idfactory(prefix=obj.__class__.__name__)
        self._save_or_replace(obj._id, obj)
        return obj._id


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
        
        # XXX: this should become `with tgt = ...:` as soon as we stop
        # supporting Python 2.4
        tgt = None
        try:
            tgt = open(filename, 'w+b')
            pickle.dump(obj, tgt, self._protocol)
            tgt.close()
            if backup is not None:
                os.remove(backup)
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
