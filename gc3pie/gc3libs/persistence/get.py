#! /usr/bin/env python
#
"""
Accessors for object attributes and container items.
"""
# Copyright (C) 2011-2012, GC3, University of Zurich. All rights reserved.
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


import gc3libs.utils


# tag object for catching the "no value passed" in `GetAttr` and
# `value_at_index` (cannot use `None` as it's a legit value!)
_none = object()


class Get(object):
    """
    Provide easier compositional syntax for `GetAttribute` and `GetItem`.

    Instances of `GetAttribute` and `GetItem` can be composed by
    passing one as `xform` parameter to the other; however, this
    results in the writing order being the opposite of the composition
    order: for instance, to create an accessor to evaluate `x.a[0]`
    for any Python object `x`, one has to write::

       >>> fn1 = GetItem(0, GetAttribute('a'))

    The `Get` class allows this to be reversed, i.e., to reflect more
    naturally the way Python expressions are laid out::

       >>> fn2 = Get().attr('a').item(0)
       >>> x = gc3libs.utils.Struct(a=[21,42], b='foo')
       >>> fn1(x)
       21
       >>> fn2(x)
       21

    """

    def __call__(self, obj):
        return obj

    def attr(self, name, default=_none):
        return GetAttribute(name, xform=(lambda obj: self(obj)), default=default)

    def item(self, place, default=_none):
        return GetItem(place,  xform=(lambda obj: self(obj)), default=default)


GET = Get()
"""
Constant identity getter.

Use this for better readability (e.g., `GET.index(0)` instead of
`Get().index(0)`).
"""


class GetAttribute(Get):
    """
    Return an accessor function for the given attribute.

    An instance of `GetAttribute` is a callable that, given any
    object, returns the value of its attribute `attr`, whose name is
    specified in the `GetAttribute` constructor::

        >>> fn = GetAttribute('x')
        >>> a = gc3libs.utils.Struct(x=1, y=2)
        >>> fn(a)
        1

    The accessor raises `AttributeError` if no such attribute
    exists)::

        >>> b = gc3libs.utils.Struct(z=3)
        >>> fn(b)
        Traceback (most recent call last):
           ...
        AttributeError: 'Struct' object has no attribute 'x'

    However, you can specify a default value, in which case the
    default value is returned and no error is raised::

        >>> fn = GetAttribute('x', default=42)
        >>> fn(b)
        42
        >>> fn = GetAttribute('y', default=None)
        >>> print(fn(b))
        None

    In other words, if `fn = GetAttribute('x')`, then `fn(obj)`
    evaluates to `obj.x`.

    If the string `attr` contains any dots, then attribute lookups are
    chained: if `fn = GetAttribute('x.y')` then `fn(obj)` evaluates to
    `obj.x.y`::

        >>> fn = GetAttribute('x.y')
        >>> a = gc3libs.utils.Struct(x=gc3libs.utils.Struct(y=42))
        >>> fn(a)
        42

    The optional second argument `xform` allows composing the accessor
    with an arbitrary function that is passed an object and should
    return a (possibly different) object whose attributes should be
    looked up.  In other words, if `xform` is specified, then the
    returned accessor function computes `xform(obj).attr` instead of
    `obj.attr`.

    This allows combining `GetAttribute` with `GetItem`:meth: (which
    see), to access objects in deeply-nested data structures; see
    `GetItem`:class: for examples.

    """
    __slots__ = ('attr', 'xform', 'default')

    def __init__(self, attr, xform=(lambda obj: obj), default=_none):
        self.attr = attr
        self.xform = xform
        self.default = default

    def __call__(self, obj):
        try:
            return gc3libs.utils.getattr_nested(self.xform(obj), self.attr)
        except AttributeError:
            if self.default is not _none:
                return self.default
            else:
                raise


class GetItem(Get):
    """
    Return accessor function for the given item in a sequence.

    An instance of `GetItem` is a callable that, given any
    sequence/container object, returns the value of the item at its
    place `idx`::

        >>> fn = GetItem(1)
        >>> a = 'abc'
        >>> fn(a)
        'b'
        >>> b = { 1:'x', 2:'y' }
        >>> fn(b)
        'x'

    In other words, if `fn = GetItem(x)`, then `fn(obj)` evaluates
    to `obj[x]`.

    Note that the returned function `fn` raises `IndexError` or `KeyError`,
    (depending on the type of sequence/container) if place `idx` does not
    exist::

        >>> fn = GetItem(42)
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

        >>> fn = GetItem(42, default='foo')
        >>> fn(a)
        'foo'
        >>> fn(b)
        'foo'

    The optional second argument `xform` allows composing the accessor
    with an arbitrary function that is passed an object and should
    return a (possibly different) object where the item lookup should
    be performed.  In other words, if `xform` is specified, then the
    returned accessor function computes `xform(obj)[idx]` instead of
    `obj[idx]`.  For example::

        >>> c = 'abc'
        >>> fn = GetItem(1, xform=(lambda s: s.upper()))
        >>> fn(c)
        'B'

        >>> c = (('a',1), ('b',2))
        >>> fn = GetItem('a', xform=dict)
        >>> fn(c)
        1


    This allows combining `GetItem` with `GetAttr`:class:
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

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="get",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
