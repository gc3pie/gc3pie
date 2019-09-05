#! /usr/bin/env python
#
"""
Accessors for object attributes and container items.
"""
# Copyright (C) 2011-2012, 2019  University of Zurich.
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
from builtins import object
__docformat__ = 'reStructuredText'


from gc3libs.utils import getattr_nested


# tag object for catching the "no value passed" in `GetAttr` and
# `value_at_index` (cannot use `None` as it's a legit value!)
_none = object()


class GetValue(object):

    """
    Provide easier compositional syntax for `GetAttributeValue` and
    `GetItemValue`.

    Instances of `GetAttributeValue` and `GetItemValue` can be composed by
    passing one as `xform` parameter to the other; however, this
    results in the writing order being the opposite of the composition
    order: for instance, to create an accessor to evaluate `x.a[0]`
    for any Python object `x`, one has to write::

       >>> from gc3libs import Struct
       >>> fn1 = GetItemValue(0, GetAttributeValue('a'))

    The `GetValue` class allows to write accessor expressions the way
    they are normally written in Python::

       >>> GET = GetValue()
       >>> fn2 = GET.a[0]
       >>> x = Struct(a=[21,42], b='foo')
       >>> fn1(x)
       21
       >>> fn2(x)
       21

    The optional `default` argument specifies a value that should be
    used in case the required attribute or item is not found:

       >>> fn3 = GetValue(default='no value found').a[3]
       >>> fn3(x) == 'no value found'
       True

    """
    __slots__ = ('default',)

    def __init__(self, default=_none):
        self.default = default

    def __call__(self, obj):
        # identity accessor
        return obj

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(
                "'%s' object has no attribute '%s'"
                % (self.__class__.__name__, name))
        return GetAttributeValue(
            name, xform=(lambda obj: self(obj)), default=self.default)

    def __getitem__(self, place):
        return GetItemValue(
            place, xform=(lambda obj: self(obj)), default=self.default)

    def ONLY(self, specifier):
        """
        Restrict the action of the accessor expression to members of a certain
        class; return default value otherwise.

        The invocation to `only`:meth: should *always be last*::

            >>> from gc3libs import Struct
            >>> fn = GetValue(default='foo').a[0].ONLY(Struct)
            >>> fn(Struct(a=['bar','baz'])) == 'bar'
            True
            >>> fn(dict(a=['bar','baz'])) == 'foo'
            True

        If it's not last, you will get `AttributeError` like the following:

            >>> fn = GetValue().ONLY(Struct).a[0]
            >>> fn(dict(a=[0,1]))
            Traceback (most recent call last):
              ...
            AttributeError: 'NoneType' object has no attribute 'a'

        """
        return GetOnly(
            specifier, xform=(lambda obj: self(obj)), default=self.default)


GET = GetValue()
"""
Constant identity getter.

Use this for better readability (e.g., `GET[0]` instead of
`GetValue()[0]`).
"""


class GetAttributeValue(GetValue):

    """
    Return an accessor function for the given attribute.

    An instance of `GetAttributeValue` is a callable that, given any
    object, returns the value of its attribute `attr`, whose name is
    specified in the `GetAttributeValue` constructor::

       >>> from gc3libs import Struct
        >>> fn = GetAttributeValue('x')
        >>> a = Struct(x=1, y=2)
        >>> fn(a)
        1

    The accessor raises `AttributeError` if no such attribute
    exists)::

        >>> b = Struct(z=3)
        >>> fn(b)
        Traceback (most recent call last):
           ...
        AttributeError: 'Struct' object has no attribute 'x'

    However, you can specify a default value, in which case the
    default value is returned and no error is raised::

        >>> fn = GetAttributeValue('x', default=42)
        >>> fn(b)
        42
        >>> fn = GetAttributeValue('y', default=None)
        >>> print(fn(b))
        None

    In other words, if `fn = GetAttributeValue('x')`, then `fn(obj)`
    evaluates to `obj.x`.

    If the string `attr` contains any dots, then attribute lookups are
    chained: if `fn = GetAttributeValue('x.y')` then `fn(obj)` evaluates to
    `obj.x.y`::

        >>> fn = GetAttributeValue('x.y')
        >>> a = Struct(x=Struct(y=42))
        >>> fn(a)
        42

    The optional second argument `xform` allows composing the accessor
    with an arbitrary function that is passed an object and should
    return a (possibly different) object whose attributes should be
    looked up.  In other words, if `xform` is specified, then the
    returned accessor function computes `xform(obj).attr` instead of
    `obj.attr`.

    This allows combining `GetAttributeValue` with `GetItemValue`:meth: (which
    see), to access objects in deeply-nested data structures; see
    `GetItemValue`:class: for examples.

    """
    __slots__ = ('attr', 'xform', 'default')

    def __init__(self, attr, xform=(lambda obj: obj), default=_none):
        self.attr = attr
        self.xform = xform
        self.default = default

    def __call__(self, obj):
        try:
            return getattr_nested(self.xform(obj), self.attr)
        except AttributeError:
            if self.default is not _none:
                return self.default
            else:
                raise


class GetItemValue(GetValue):

    """
    Return accessor function for the given item in a sequence.

    An instance of `GetItemValue` is a callable that, given any
    sequence/container object, returns the value of the item at its
    place `idx`::

        >>> fn = GetItemValue(1)
        >>> a = 'abc'
        >>> fn(a) == 'b'
        True
        >>> b = { 1:'x', 2:'y' }
        >>> fn(b) == 'x'
        True

    In other words, if `fn = GetItemValue(x)`, then `fn(obj)` evaluates
    to `obj[x]`.

    Note that the returned function `fn` raises `IndexError` or `KeyError`,
    (depending on the type of sequence/container) if place `idx` does not
    exist::

        >>> fn = GetItemValue(42)
        >>> a = list('abc')
        >>> fn(a)
        Traceback (most recent call last):
           ...
        IndexError: list index out of range
        >>> b = dict(x=1, y=2, z=3)
        >>> fn(b)
        Traceback (most recent call last):
           ...
        KeyError: 42

    However, you can specify a default value, in which case the
    default value is returned and no error is raised::

        >>> fn = GetItemValue(42, default='foo')
        >>> fn(a) == 'foo'
        True
        >>> fn(b) == 'foo'
        True

    The optional second argument `xform` allows composing the accessor
    with an arbitrary function that is passed an object and should
    return a (possibly different) object where the item lookup should
    be performed.  In other words, if `xform` is specified, then the
    returned accessor function computes `xform(obj)[idx]` instead of
    `obj[idx]`.  For example::

        >>> c = 'abc'
        >>> fn = GetItemValue(1, xform=(lambda s: s.upper()))
        >>> fn(c) == 'B'
        True

        >>> c = (('a',1), ('b',2))
        >>> fn = GetItemValue('a', xform=dict)
        >>> fn(c)
        1


    This allows combining `GetItemValue` with `GetAttrValue`:class:
    (which see), to access objects in deeply-nested data structures.
    """
    __slots__ = ('idx', 'xform', 'default')

    def __init__(self, place, xform=(lambda obj: obj), default=_none):
        self.idx = place
        self.xform = xform
        self.default = default

    def __call__(self, obj):
        try:
            return self.xform(obj)[self.idx]
        except (KeyError, IndexError):
            if self.default is not _none:
                return self.default
            else:
                raise


class GetOnly(GetValue):

    """
    Apply accessor function to members of a certain class; return a
    default value otherwise.

    The `GetOnly` accessor performs just like `GetValue`, but is
    effective only on instances of a certain class; if the accessor
    function is passed an instance of a different class, the default
    value is returned::

       >>> from gc3libs import Struct
       >>> fn4 = GetOnly(Struct, default=42)
       >>> isinstance(fn4(Struct(foo='bar')), Struct)
       True
       >>> isinstance(fn4(dict(foo='bar')), dict)
       False
       >>> fn4(dict(foo='bar'))
       42

    If `default` is not specified, then `None` is returned::

       >>> fn5 = GetOnly(Struct)
       >>> repr(fn5(dict(foo='bar')))
       'None'

    """
    __slots__ = ('only', 'xform', 'default')

    def __init__(self, only, xform=(lambda obj: obj), default=_none):
        self.only = only
        self.xform = xform
        self.default = default

    def __call__(self, obj):
        # only apply `xform` if `obj` matches a class in `only`
        if isinstance(obj, self.only):
            return self.xform(obj)
        else:
            if self.default is not _none:
                return self.default
            else:
                return None


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="get",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
