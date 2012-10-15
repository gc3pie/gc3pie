#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012, GC3, University of Zurich. All rights reserved.
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
from gc3libs.persistence.store import Persistable

class ProxyManager(object):
    """ProxyManager should be responsible to decide when a Proxy
    should persist its proxied object and forget it.
    """
    def getattr_called(self, proxy, name):
        """This method is called by a `proxy` everytime the
        __getattribute__ method of the proxy is called with argument `name`."""
        raise NotImplementedError("Abstract method `ProxyManager.getattr_called` called")


def create_proxy_class(cls, obj, extra):
    prxy = cls(obj)
    if 'persistent_id' in extra:
        prxy.persistent_id = extra['persistent_id']
    return prxy

class BaseProxy(object):
    """
    This class is took from
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
    # __slots__ = ["_obj", "__weakref__"]
    def __init__(self, obj):
        object.__setattr__(self, "_obj", obj)
    
    #
    # proxying (special cases)
    #
    _reserved_names = ['_obj', '__reduce__', '__reduce_ex__', 'persistent_id']
    def __getattribute__(self, name):
        if name in BaseProxy._reserved_names:
            return object.__getattribute__(self, name)
        else:
            return getattr(object.__getattribute__(self, "_obj"), name)

    def __delattr__(self, name):
        delattr(object.__getattribute__(self, "_obj"), name)

    def __setattr__(self, name, value):
        if name in Proxy._reserved_names:
            object.__setattr__(self, name, value)
        else:
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
        creates an proxy instance referencing `obj`. (obj, *args, **kwargs) are
        passed to this class' __init__, so deriving classes can define an 
        __init__ method of their own.
        note: _class_proxy_cache is unique per deriving class (each deriving
        class must hold its own cache)
        """
        theclass = cls._create_class_proxy(obj.__class__)
        ins = object.__new__(theclass)
        return ins

    def __reduce_ex__(self, proto):
        return ( create_proxy_class,
                 (BaseProxy,
                  object.__getattribute__(self, '_obj'),
                  object.__getattribute__(self, '__dict__') ),
                 )

class Proxy(BaseProxy):
    """This class is a Proxy which is able to store the proxied object
    in a persistency database `storage`, and retrive it if needed.

    >>> from gc3libs.persistence import make_store
    >>> import tempfile, os
    >>> (fd, tmpname) = tempfile.mkstemp()
    >>> from gc3libs import Task
    >>> p = Proxy(Task(jobname='NoTask'), storage=make_store("sqlite://%s" % tmpname))
    >>> p # doctest: +ELLIPSIS
    <gc3libs.Task object at ...>
    >>> object.__getattribute__(p, "_obj") # doctest: +ELLIPSIS
    <gc3libs.Task object at ...>
    >>> p.jobname
    'NoTask'

    Let's try to *forget it*:
    
    >>> p.proxy_forget()
    >>> object.__getattribute__(p, "_obj") # doctest: +ELLIPSIS

    The object has been *forgetted*, and hopefully saved in the
    persistency. If you try to access an attribute, however, you
    should be able to:
    
    >>> p.jobname
    'NoTask'

    Guess what happen if you don't define a storage?
    
    >>> p = Proxy(Task(jobname='NoTask'))
    >>> p # doctest: +ELLIPSIS
    <gc3libs.Task object at ...>
    >>> p.jobname
    'NoTask'
    >>> object.__getattribute__(p, "_obj") # doctest: +ELLIPSIS
    <gc3libs.Task object at ...>

    The `proxy_forget()` method will *not* delete the internal
    reference, otherwise we would be unable to retrive the object
    later:
    
    >>> p.proxy_forget()
    >>> object.__getattribute__(p, "_obj") # doctest: +ELLIPSIS
    <gc3libs.Task object at ...>
    >>> p.jobname
    'NoTask'

    If you are not able to save the object to the persistent storage
    too, it will not be deleted:

    >>> p = Proxy(Task(jobname='NoTask2'), storage=make_store('file:///path/to/non/existent/file'))
    >>> p.proxy_forget()
    >>> object.__getattribute__(p, "_obj") # doctest: +ELLIPSIS
    <gc3libs.Task object at ...>
    >>> p.jobname
    'NoTask2'
    
    Clean up tests.
    >>> os.remove(tmpname)    
    """

    _reserved_names = BaseProxy._reserved_names + [
        "_obj_id", "_storage", "_manager", "proxy_forget", "proxy_set_storage"]
    def __init__(self, obj, storage=None, manager=None):
        object.__setattr__(self, "_obj", obj)
        object.__setattr__(self, "_storage", storage)
        object.__setattr__(self, "_manager", manager)
    
    def __getattribute__(self, name):
        if name in Proxy._reserved_names:
            return object.__getattribute__(self, name)

        manager = object.__getattribute__(self, "_manager")
        if manager:
            manager.getattr_called(self, name)
        
        obj = object.__getattribute__(self, "_obj")

        if not obj:
            storage = object.__getattribute__(self, "_storage")
            obj_id = object.__getattribute__(self, "_obj_id")
            if storage and obj_id:
                obj = storage.load(obj_id)
                object.__setattr__(self, "_obj", obj)
        return getattr(obj, name)

    def proxy_forget(self):
        obj = object.__getattribute__(self, "_obj")
        storage = object.__getattribute__(self, "_storage")
        if storage:
            try:
                p_id = storage.save(obj)
                object.__setattr__(self, "_obj", None)
                object.__setattr__(self, "_obj_id", p_id)
            except Exception, e:
                log.error("Proxy: Error saving object to persistent storage")
        else:
            log.warning("Proxy: `proxy_forget()` called but no persistent storage has been defined. Aborting *without* deleting proxied object")


    def proxy_set_storage(self, storage, overwrite=True):
        """
        Set the persistence store to use.
        """
        if overwrite or not object.__getattribute__(self, '_storage'):
            object.__setattr__(self, '_storage', storage)


    def __reduce_ex__(self, proto):
        return ( create_proxy_class,
                 (Proxy,
                  object.__getattribute__(self, '_obj'),
                  object.__getattribute__(self, '__dict__') ),
                 )

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="proxy",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
