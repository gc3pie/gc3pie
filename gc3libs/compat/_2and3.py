#! /usr/bin/env python
"""
Compatibility functions during the Py2/Py3 migration phase.
"""
#
# Copyright (C) 2019  University of Zurich.
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

import sys

import gc3libs.exceptions


# flags indicating major version
PY2 = (sys.version_info[0] == 2)
PY3 = (sys.version_info[0] == 3)


def to_filesystem_path(arg):
    """
    Turn a string argument into the internal representation of a
    filesystem path.

    This is needed because of the

    Currently, the implementation of this method only makes sure that
    `arg` is an ASCII-only string and then converts it to a byte
    string or a text string (based on Python's major version).
    """
    # XXX: would it be better to return a pathlib object instead?
    try:
        if PY2:
            return bytes(arg).decode('ascii')
        else:
            if isinstance(arg, bytes):
                return arg.decode('ascii')
            elif isinstance(arg, str):
                return arg
            else:
                raise TypeError(
                    "Can only handle `bytes` or `str` objects,"
                    " but was given `{0}`"
                    .format(type(arg)))
    except UnicodeEncodeError as err:
        raise gc3libs.exceptions.InvalidValue(
            "Use of non-ASCII file names is"
            " not (yet) supported in GC3Pie: {0}: {1}"
            .format(err.__class__.__name__, err))


## run doctests if this module is executed as a script
if __name__ == '__main__':
    import doctest
    doctest.testmod(name='utils',
                    optionflags=doctest.NORMALIZE_WHITESPACE|doctest.ELLIPSIS)
