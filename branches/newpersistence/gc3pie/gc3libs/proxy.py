#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, GC3, University of Zurich. All rights reserved.
#
# Portions of this file were extracted from
# http://code.activestate.com/recipes/496741-object-proxying/ which is
# copyright Tomer Filiba.
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

from gc3libs import log
from persistence import Store
import time

class ProxyManager(object):
    """ProxyManager should be responsible to decide when a Proxy
    should persist its proxied object and forget it.
    """
    def getattr_called(self, proxy, name):
        """This method is called by a `proxy` everytime the
        __getattribute__ method of the proxy is called with argument `name`."""
        raise NotImplementedError("Abstract method `ProxyManager.getattr_called` called")


class BaseProxy(object):
    """
    This class implements a generic proxy for generic objects.

    The class is took from
    http://code.activestate.com/recipes/496741-object-proxying/ and is
    used as a base for the `Proxy` class which is used to implement
    lazy objects to access persistent tasks.

    To create a BaseProxy object simply type:

    >>> p = BaseProxy(1)
    >>> type(p)
    <class 'gc3libs.proxy.BaseProxy(int)'>
    >>> p+1
    2
    >>> type(p+1)
    <type 'int'>
    
    proxying builtin types can lead to exceptions:

    >>> p + BaseProxy([6, 7]) # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    TypeError: unsupported operand type(s) for +: 'BaseProxy(int)' and 'BaseProxy(list)'

    """
    __slots__ = ["_obj", "__weakref__"]

    def __init__(self, obj):
        object.__setattr__(self, "_obj", obj)
    
    #
    # proxying (special cases)
    #
    def __getattribute__(self, name):
        return getattr(object.__getattribute__(self, "_obj"), name)
    def __delattr__(self, name):
        delattr(object.__getattribute__(self, "_obj"), name)
    def __setattr__(self, name, value):
        setattr(object.__getattribute__(self, "_obj"), name, value)
    
    def __nonzero__(self):
        return bool(object.__getattribute__(self, "_obj"))
    def __str__(self):
        return str(object.__getattribute__(self, "_obj"))
    def __repr__(self):
        return repr(object.__getattribute__(self, "_obj"))
    
    #
    # factories
    #
    _special_names = [
        '__abs__', '__add__', '__and__', '__call__', '__cmp__', '__coerce__', 
        '__contains__', '__delitem__', '__delslice__', '__div__', '__divmod__', 
        '__eq__', '__float__', '__floordiv__', '__ge__', '__getitem__', 
        '__getslice__', '__gt__', '__hash__', '__hex__', '__iadd__', '__iand__',
        '__idiv__', '__idivmod__', '__ifloordiv__', '__ilshift__', '__imod__', 
        '__imul__', '__int__', '__invert__', '__ior__', '__ipow__', '__irshift__', 
        '__isub__', '__iter__', '__itruediv__', '__ixor__', '__le__', '__len__', 
        '__long__', '__lshift__', '__lt__', '__mod__', '__mul__', '__ne__', 
        '__neg__', '__oct__', '__or__', '__pos__', '__pow__', '__radd__', 
        '__rand__', '__rdiv__', '__rdivmod__', '__reduce__', '__reduce_ex__', 
        '__repr__', '__reversed__', '__rfloorfiv__', '__rlshift__', '__rmod__', 
        '__rmul__', '__ror__', '__rpow__', '__rrshift__', '__rshift__', '__rsub__', 
        '__rtruediv__', '__rxor__', '__setitem__', '__setslice__', '__sub__', 
        '__truediv__', '__xor__', 'next',
    ]
    
    @classmethod
    def _create_class_proxy(cls, theclass):
        """creates a proxy for the given class"""
        
        def make_method(name):
            def method(self, *args, **kw):
                return getattr(object.__getattribute__(self, "_obj"), name)(*args, **kw)
            return method
        
        namespace = {}
        for name in cls._special_names:
            if hasattr(theclass, name) and not hasattr(cls, name):
                namespace[name] = make_method(name)
        return type("%s(%s)" % (cls.__name__, theclass.__name__), (cls,), namespace)
    
    def __new__(cls, obj, *args, **kwargs):
        """
        creates an proxy instance referencing `obj`.

        (obj, *args, **kwargs) are passed to this class' __init__, so
        deriving classes can define an __init__ method of their own.
        """
        theclass = cls._create_class_proxy(obj.__class__)
        ins = object.__new__(theclass)
        return ins

class Proxy(BaseProxy):
    """This class is a Proxy which is able to store the proxied object
    in a persistency database `storage`, and retrive it if needed.

    >>> from gc3libs.persistence import persistence_factory
    >>> import tempfile, os
    >>> (fd, tmpname) = tempfile.mkstemp()
    >>> from gc3libs import Task
    >>> p = Proxy(Task('NoTask'), storage=persistence_factory("sqlite://%s" % tmpname))
    >>> p # doctest: +ELLIPSIS
    <gc3libs.Task object at ...>
    >>> object.__getattribute__(p, "_obj") # doctest: +ELLIPSIS
    <gc3libs.Task object at ...>
    >>> p.proxy_saved()
    False
    >>> p.jobname
    'NoTask'

    Let's try to *forget it*:
    
    >>> p.proxy_forget()
    >>> object.__getattribute__(p, "_obj") # doctest: +ELLIPSIS
    >>> p.proxy_saved()
    True

    The object has been *forgotten*, and hopefully saved in the
    persistency. If you try to access an attribute, however, you
    should be able to:
    
    >>> p.jobname
    'NoTask'

    Guess what happen if you don't define a storage?
    
    >>> p = Proxy(Task('NoTask'))
    >>> p # doctest: +ELLIPSIS
    <gc3libs.Task object at ...>
    >>> p.jobname
    'NoTask'
    >>> object.__getattribute__(p, "_obj") # doctest: +ELLIPSIS
    <gc3libs.Task object at ...>
    >>> p.proxy_saved()
    False

    The `proxy_forget()` method will *not* delete the internal
    reference, otherwise we would be unable to retrive the object
    later:
    
    >>> p.proxy_forget()
    >>> object.__getattribute__(p, "_obj") # doctest: +ELLIPSIS
    <gc3libs.Task object at ...>
    >>> p.proxy_saved()
    False
    >>> p.jobname
    'NoTask'

    If you are not able to save the object to the persistent storage
    too, it will not be deleted:

    >>> p = Proxy(Task('NoTask2'), storage=persistence_factory('file:///path/to/non/existent/file'))
    >>> p.proxy_forget()
    >>> object.__getattribute__(p, "_obj") # doctest: +ELLIPSIS
    <gc3libs.Task object at ...>
    >>> p.proxy_saved()
    False
    >>> p.jobname
    'NoTask2'
    
    Clean up tests.
    >>> os.remove(tmpname)    
    """
    
    __slots__ = ["_obj", "_obj_id", "__storage", "__manager", "__last_access", "proxy_forget", "proxy_last_accessed", "proxy_set_storage", "proxy_saved"]
    
    def __init__(self, obj, storage=None, manager=None):
        object.__setattr__(self, "_obj", obj)
        object.__setattr__(self, "__storage", storage)
        object.__setattr__(self, "__manager", manager)
        object.__setattr__(self, "__last_access", -1)
    
    def __getattribute__(self, name):
        if name.startswith('proxy_'):
            return object.__getattribute__(self, name)

        manager = object.__getattribute__(self, "__manager")
        if manager:
            manager.getattr_called(self, name)
        
        obj = object.__getattribute__(self, "_obj")
        object.__setattr__(self, "__last_access", time.time())

        if not obj:
            storage = object.__getattribute__(self, "__storage")
            obj_id = object.__getattribute__(self, "_obj_id")
            if storage and obj_id:
                obj = storage.load(obj_id)
                object.__setattr__(self, "_obj", obj)
        return getattr(obj, name)

    def proxy_forget(self):
        obj = object.__getattribute__(self, "_obj")
        storage = object.__getattribute__(self, "__storage")
        if not obj: 
            return
        if storage:
            try:
                p_id = storage.save(obj)
                object.__setattr__(self, "_obj", None)
                object.__setattr__(self, "_obj_id", p_id)
            except Exception, e:
                log.error("Proxy: Error saving object to persistent storage")
        else:
            log.warning("Proxy: `proxy_forget()` called but no persistent storage has been defined. Aborting *without* deleting proxied object")
            
        # object.__delattr__(self, "_obj")
            
    def proxy_last_accessed(self):
        return object.__getattribute__(self, "__last_access")

    def proxy_set_storage(self, storage):
        object.__setattr__(self, "__storage", storage)

    def proxy_saved(self):
        """Return True if the object attribute is None, False otherwise"""
        return object.__getattribute__(self, "_obj") is None

class MemoryPool(object):
    """This class is used to store a set of Proxy objects but tries to
    keep in memory only a limited amount of them.

    It works with any Proxy object.

    This class is basically a FIFO where at each addiction
    (:meth:`add`) or each time the :meth:`refresh` method is called
    all proxy objects are saved but the last `self.maxobjects`.

    Other than updating the `self.maxobjects` attribute of the class
    you can customize the behavior by subclassing `MemoryPool` and
    overriding the two main methods: :meth:`cmp` and :meth:`keep`.
    """

    def __init__(self, storage, maxobjects=0):
        """
        * `maxobjects`: If maxobjects is >0, then the MemoryPool will
          *never* save more than `maxobjects` objects.
          
          Let's setup a storage:
          >>> import tempfile, os
          >>> from gc3libs.persistence import persistence_factory
          >>> from gc3libs import Task
          >>> (f, tmp) = tempfile.mkstemp()
          >>> store = persistence_factory("sqlite://%s" % tmp)

          MemoryPool is called with this store and the maximum number
          of objects we want to save:

          >>> mempool = MemoryPool(store, maxobjects=10)

          We add 30 Proxy objects to the memory pool (we can add only
          Proxy objects)

          >>> for i in range(30):
          ...     mempool.add(Proxy(Task(str(i))))

          The `refresh` method will remove all the *old* objects. It
          is currently called each time `add` is called, but this may
          change in future.

          >>> mempool.refresh()          
          >>> len([i for i in mempool if i.proxy_saved()])
          20
          >>> len([i for i in mempool if not i.proxy_saved()])
          10
          >>> os.remove(tmp)
          """

        if not isinstance(storage, Store):
            raise TypeError("Invalid storage %s" % type(storage))
        self.__storage = storage
        self._proxies = []
        self.maxobjects=maxobjects

    def add(self, obj):
        """Add `proxy` object to the memory pool."""
        # if obj in self._proxies: return
        if not isinstance(obj, Proxy):
            raise TypeError("Object of type %s not supported by MemoryPool" % type(obj))
        
        obj.proxy_set_storage(self.__storage)
        self._proxies.append(obj)
        if self.maxobjects and len(self._proxies)>self.maxobjects: 
            self.refresh()

    def extend(self, objects):
        """objects is a sequence of objects that will be added to the
        pool, if they are not already there."""
        for obj in objects: 
            self.add(obj)
        
    def remove(self, obj):
        """Remove `porxy` object from the memory pool"""
        self._proxies.remove(obj)

    def refresh(self):
        """Refresh the list of proxies, forget "old" proxies if
        needed"""

        # If policy_function is defined, forget all objects we don't
        # want to remember anymore.
        map(lambda x: not self.keep(x) and x.proxy_forget(), self._proxies)

        if self.maxobjects > 0:
            self._proxies.sort(cmp=lambda x,y: self.cmp(x,y))             
            for i in range(len(self._proxies) - self.maxobjects):
                self._proxies[i].proxy_forget()
        
    def __iter__(self):
        return iter(self._proxies)

    @staticmethod
    def last_accessed(obj1, obj2):
        """Default comparison function used to sort proxies. It uses
        the `last_access` method of the objects to sort them.

        >>> p1 = Proxy("p1")
        >>> p2 = Proxy("p2")
        >>> p1.strip() == "p1"
        True
        >>> p2.strip() == "p2"
        True
        >>> MemoryPool.last_accessed(p1, p2)
        -1

        For the sake of our refresh function, if any "forgetted"
        object is considered *greager* than any non-forgetted object.
        """
        if cmp(obj2.proxy_saved(), obj1.proxy_saved()):
            return cmp(obj2.proxy_saved(), obj1.proxy_saved())
        return cmp(obj1.proxy_last_accessed(), obj2.proxy_last_accessed())
    def cmp(self, x, y):
        """This method is used to inplace sort the list of Proxy
        objects in order to decide which proxies need to be forgotten.

        Default sorting method is based on the access time of any
        attribute of the proxied argument, in order to dump the
        "oldest" jobs.

        You can override this method by subclassing `MemoryPool`, but
        please remember that accessing attributes other than the ones
        stored on the Proxy itself may cause multiple `save` and
        `load` of the same object, thus degrading the overall
        performance.

        Moreover, since this function is called *after* the
        :meth:`keep` method, it is possible that an object already
        *forgotten* by the `keep` method is then loaded again because
        of the comparison.

        Therefore, it's probably safer not to mix `keep` and `cmp`
        methods in your implementation.
        """
        return MemoryPool.last_accessed(x, y)

    def keep(self, obj):
        """This method is used to decide if an object has to be
        forgotten or not.

        By default only least accessed objects are forgotten, thus
        this function returns always True.

        Please note that this method is called before the :meth:`cmp`
        method, and customizing both methods may not be safe. Please
        check the documentation for the :meth:`cmp` method too.
        """
        return True
    
## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="Proxy",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
