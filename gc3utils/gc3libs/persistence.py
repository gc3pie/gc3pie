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
from gc3libs.utils import same_docstring_as



class Store(object):
    """
    Interface for storing and retrieving `Job`s on permanent storage.

    Any object that can stored, provided it can be pickled (with
    Python's standard module `pickle`).
    
    Each `save` operation returns a unique "ID"; each ID is a Python
    string value, which is guaranteed to be temporally unique, i.e.,
    no two `save` operations in the same persistent store can result
    in the same IDs being assigned to different objects.
    """
    
    def list(self, **kw):
        """
        Return list of IDs of saved `Job` objects.

        This is an optional method; classes that do not implement it
        should raise a `NotImplementedError` exception.
        """
        raise NotImplementedError("Method `list` not implemented in this class.")

    def remove(self, id):
        """
        Delete a given object from persistent storage, given its ID.
        """
        raise NotImplementedError("Abstract method 'Store.remove' called"
                                  " -- should have been implemented in a derived class!")

    def replace(self, id, obj):
        """
        Replace the object already saved with the given ID with a copy of `obj`.
        """
        raise NotImplementedError("Abstract method 'Store.replace' called"
                                  " -- should have been implemented in a derived class!")

    def load(self, id):
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



class FilesystemStore(Store):
    """
    Save and load objects in a given directory.  Uses Python's
    standard `pickle` module to serialize objects onto files.

    All objects are saved as files in the given directory (default:
    `gc3libs.Default.JOBS_DIR`).

    The `protocol` argument specifies the pickle protocol to use
    (default: `pickle.HIGHEST_PROTOCOL`).  See the `pickle` module
    documentation for details.
    """
    def __init__(self, directory=gc3libs.Default.JOBS_DIR, 
                 protocol=0): #pickle.HIGHEST_PROTOCOL):
        self._directory = directory
        self._protocol = protocol


    @same_docstring_as(Store.list)
    def list(self):
        if not os.path.exists(self._directory):
            return [ ]
        return [ id for id in os.listdir(self._directory)
                 if not id.endswith('.OLD') ]


    @same_docstring_as(Store.load)
    def load(self, id):
        filename = os.path.join(self._directory, id)
        gc3libs.log.debug("Retrieving job from file '%s' ...", filename)

        if not os.path.exists(filename):
            raise JobRetrieveError("No '%s' file found in directory '%s'" 
                                   % (id, self._directory))

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
        if str(obj.jobid) != str(id):
            raise JobRetrieveError("Retrieved Job ID '%s' does not match given Job ID '%s'" 
                                   % (obj.jobid, id))
        return obj


    @same_docstring_as(Store.remove)
    def remove(self, id):
        filename = os.path.join(self._directory, id)
        os.remove(filename)


    @same_docstring_as(Store.replace)
    def replace(self, id, obj):
        self._save_or_replace(id, obj)


    @same_docstring_as(Store.save)
    def save(self, obj):
        self._save_or_replace(obj.jobid, obj)
        return obj.jobid


    def _save_or_replace(self, id, obj):
        """
        Save `obj` into file identified by `id`; if no such
        destination file exists, create it.  Ensure that the
        destination file is kept intact in case dumping `obj` fails.
        """
        filename = os.path.join(self._directory, id)
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
