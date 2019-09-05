#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011-2019  University of Zurich.
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
from builtins import object
__docformat__ = 'reStructuredText'

import operator

import gc3libs
from gc3libs.utils import progressive_number


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
            try:
                return op((str(self._prefix), self._seqno),
                          (str(other._prefix), other._seqno))
            except AttributeError:
                # fall back to safe comparison as `str`
                gc3libs.log.debug(
                    "Wrong job ID: comparing '%s' (%s) with '%s' (%s)"
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

    __slots__ = ('_seqno', '_prefix')

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

    def __hash__(self):
        return hash((self._prefix, self._seqno))

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
        self._next_id_fn = next_id_fn or progressive_number
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
    _seqno_pool = []

    def new(self, obj):
        """
        Return a new "unique identifier" instance (a string).
        """
        prefix = self._prefix or obj.__class__.__name__
        if IdFactory._seqno_pool:
            seqno = IdFactory._seqno_pool.pop()
        else:
            seqno = self._next_id_fn()
        return self._idclass(prefix, seqno)


class JobIdFactory(IdFactory):

    """
    Override :class:`IdFactory` behavior and generate IDs starting with a
    lowercase ``job`` prefix.
    """

    def __init__(self, next_id_fn=None):
        IdFactory.__init__(self, 'job', next_id_fn)


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="id",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
