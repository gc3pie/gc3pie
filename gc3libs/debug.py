#! /usr/bin/env python

"""
Tools for debugging GC3Libs based programs.

Part of the code used in this module originally comes from:
  - http://wordaligned.com/articles/echo

"""

# Copyright (C) 2011, 2015, 2019  University of Zurich. All rights reserved.
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

from __future__ import absolute_import, print_function, unicode_literals
from builtins import zip
__docformat__ = 'reStructuredText'


import functools
import inspect

import gc3libs


def name(item):
    """Return an item's name."""
    return item.__name__


def is_classmethod(instancemethod):
    """Determine if an instancemethod is a classmethod."""
    return instancemethod.__self__ is not None


def is_class_private_name(name):
    """Determine if a name is a class private name."""
    # Exclude system defined names such as __init__, __add__ etc
    return name.startswith("__") and not name.endswith("__")


def method_name(method):
    """
    Return a method's name.

    This function returns the name the method is accessed by from
    outside the class (i.e. it prefixes "private" methods appropriately).
    """
    mname = name(method)
    if is_class_private_name(mname):
        mname = "_%s%s" % (name(method.__self__.__class__), mname)
    return mname


def format_arg_value(arg_val):
    """
    Return a string representing a (name, value) pair.

    Example::

      >>> 'x=(1, 2, 3)' == format_arg_value(('x', (1, 2, 3)))
      True
    """
    arg, val = arg_val
    return "%s=%r" % (arg, val)


def trace(fn, log=gc3libs.log.debug):
    """
    Logs calls to a function.

    Returns a decorated version of the input function which "echoes" calls
    made to it by writing out the function's name and the arguments it was
    called with.
    """
    # Unpack function's arg count, arg names, arg defaults
    code = fn.__code__
    argcount = code.co_argcount
    argnames = code.co_varnames[:argcount]
    fn_defaults = fn.__defaults__ or list()
    argdefs = dict(list(zip(argnames[-len(fn_defaults):], fn_defaults)))

    @functools.wraps(fn)
    def wrapped(*v, **k):
        # Collect function arguments by chaining together positional,
        # defaulted, extra positional and keyword arguments.
        positional = [format_arg_value(nm_val)
                      for nm_val in zip(argnames, v)]
        defaulted = [format_arg_value((a, argdefs[a]))
                     for a in argnames[len(v):] if a not in k]
        nameless = [repr(val) for val in v[argcount:]]
        keyword = [format_arg_value((key, val))
                   for key, val in list(k.items())]
        args = positional + defaulted + nameless + keyword
        log("%s(%s)" % (name(fn), ', '.join(args)))
        return fn(*v, **k)
    return wrapped


def trace_instancemethod(cls, method, log=gc3libs.log.debug):
    """
    Change an instancemethod so that calls to it are traced.

    Replacing a classmethod is a little more tricky.
    See: http://www.python.org/doc/current/ref/types.html
    """
    mname = method_name(method)
    # Avoid recursion printing method calls
    never_echo = "__str__", "__repr__",
    if mname in never_echo:
        pass
    elif is_classmethod(method):
        setattr(cls, mname, classmethod(trace(method.__func__, log)))
    else:
        setattr(cls, mname, trace(method, log))


def trace_class(cls, log=gc3libs.log.debug):
    """
    Trace calls to class methods and static functions
    """
    for _, method in inspect.getmembers(cls, inspect.ismethod):
        trace_instancemethod(cls, method, log)
    for _, fn in inspect.getmembers(cls, inspect.isfunction):
        setattr(cls, name(fn), staticmethod(trace(fn, log)))


def trace_module(mod, log=gc3libs.log.debug):
    """
    Trace calls to functions and methods in a module.
    """
    for fname, fn in inspect.getmembers(mod, inspect.isfunction):
        setattr(mod, fname, trace(fn, log))
    for _, cls in inspect.getmembers(mod, inspect.isclass):
        trace_class(cls, log)


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="debug",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
