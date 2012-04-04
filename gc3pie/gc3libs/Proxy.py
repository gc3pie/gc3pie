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

class ProxyManager:
    """ProxyManager should be responsible to decide when a Proxy
    should persist its proxied object and forget it.
    """

class BaseProxy(object):
    """
    This class is took from
    http://code.activestate.com/recipes/496741-object-proxying/ and is
    used as a base for the `Proxy` class which is used to implement
    lazy objects to access persistent tasks.

    To create a BaseProxy object simply type:

    >>> p = BaseProxy(1)
    >>> type(p)
    <class 'gc3libs.Proxy.BaseProxy(int)'>
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
            if hasattr(theclass, name):
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
        try:
            cache = cls.__dict__["_class_proxy_cache"]
        except KeyError:
            cls._class_proxy_cache = cache = {}
        try:
            theclass = cache[obj.__class__]
        except KeyError:
            cache[obj.__class__] = theclass = cls._create_class_proxy(obj.__class__)
        ins = object.__new__(theclass)
        theclass.__init__(ins, obj, *args, **kwargs)
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

    Clean up tests.
    >>> os.remove(tmpname)    
    """
    
    __slots__ = ["_obj", "_obj_id", "__storage", "proxy_forget"]
    def __init__(self, obj, storage=None):
        object.__setattr__(self, "_obj", obj)
        object.__setattr__(self, "__storage", storage)
    
    def __getattribute__(self, name):
        if name.startswith('proxy_'):
            return object.__getattribute__(self, name)

        obj = object.__getattribute__(self, "_obj")
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
        if storage:
            p_id = storage.save(obj)
            object.__setattr__(self, "_obj", None)
            object.__setattr__(self, "_obj_id", p_id)
            
        # object.__delattr__(self, "_obj")
        
## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="Proxy",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
