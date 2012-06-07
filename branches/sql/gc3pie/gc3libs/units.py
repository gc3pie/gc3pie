#! /usr/bin/env python
#
"""
Manipulation of quantities with units attached with automated
conversion among compatible units.

For details and the discussion leading up to this,
see: http://code.google.com/p/gc3pie/issues/detail?id=47
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


import datetime
import re

import unum
unit = unum.Unum.unit

import gc3libs.utils


## memory units
B  = unit('B',         0,  "Byte")

# base-10 multiples
kB = unit('kB', 1000 * B,  "KiloByte")
MB = unit('MB', 1000 * kB, "MegaByte")
GB = unit('GB', 1000 * MB, "GigaByte")
TB = unit('TB', 1000 * GB, "TeraByte")
PB = unit('PB', 1000 * TB, "PetaByte")

# base-2 multiples
KiB = unit("KiB", 1024 * B,   "KiBiByte")
MiB = unit("MiB", 1024 * KiB, "MiBiByte")
GiB = unit("GiB", 1024 * MiB, "GiBiByte")
TiB = unit("TiB", 1024 * GiB, "TiBiByte")
PiB = unit("PiB", 1024 * TiB, "PiBiByte")

## duration units

s = unit('s',         0, "second")
m = unit('min',  60 * s, "minute")
h = unit('hour', 60 * m, "hour")
d = unit('day',  24 * h, "day")

# aliases
seconds = s
mins = m
hours = h
days = d


# collection of all memory units
memory = frozenset((B, kB, MB, GB, TB, PB,
                    KiB, MiB, GiB, TiB, PiB))

# collection of all time duration units
duration = frozenset((s, m, h, d))


## utility functions

_qty_re = re.compile(r'(?P<value>[+-]?([0-9]+(\.[0-9]+)?|\.[0-9]+)(E[+-]?[0-9]+)?)'
                     r'\s*'
                     r'(?P<unit>[a-z]+)?',
                     re.I | re.X)

# map unit names into the units (`Unum` objects) defined in this
# module; this is used for parsing strings, so we allow several
# alternative spellings for the the same physical unit, although
# that would be forbidden by the SI standard...
_units = gc3libs.utils.Struct(
    ## memory units
    B = B,
    # base-10 multiples
    kB = kB,
    MB = MB,
    GB = GB,
    TB = TB,
    PB = PB,
    # base-2 multiples
    KiB = KiB,
    MiB = MiB,
    GiB = GiB,
    TiB = TiB,
    PiB = PiB,
    ## duration units
    s = s,
    sec = s,
    second = s,
    seconds = s,
    m = m,
    min = m,
    mins = m,
    minutes = m,
    h = h,
    hour = h,
    hours = h,
    d = d,
    day = d,
    days = d,
    )

def from_string(val, unit=None, allow=None):
    """
    Parse string `val` and extract amount and measurement unit;
    return a quantity corresponding to those.

    The string to be parsed should consist of a number, followed by a
    unit specification. The number and the unit may be separated by 0
    or more spaces.

      >>> q1 = from_string('7 s')
      >>> q2 = from_string('7s')
      >>> q3 = from_string('-7.0s')
      >>> q1 == q2
      True
      >>> q1 == -q3
      True

    If `val` does not conform to this syntax, a `ValueException` will
    be raised:

      >>> from_string('foo')
      Traceback (most recent call last):
        ...
      ValueError: Cannot parse quantity 'foo'

    Note the amount is always stored as a floating point number:
    
      >>> q1
      7.0 [s]

    If string `val` only specifies an amount (e.g., ``42``) with no
    unit, then the optional argument `unit` must provide a valid unit
    (as a `unum.Unum` instance).

      >>> q3 = from_string('7', s)
      >>> q3 == q2
      True

    Optional argument `allow` restricts the resulting quantity to be a
    multiple of one of the specified units; if it's not, a
    `ValueError` exception is raised.

      >>> from_string('42 GB', allow=[seconds, mins, hours]) 
      Traceback (most recent call last):
        ...
      ValueError: Unit [GB] is not allowed here: only [s],[min],[hour] are.
      
    """
    assert unit is None or isinstance(unit, unum.Unum), \
           "Invalid `unit` argument value for gc3libs.units.quantity"
    match = _qty_re.search(val)
    if not match:
        raise ValueError("Cannot parse quantity '%s'" % val)
    v = float(match.group('value'))
    u = match.group('unit')
    if u is None:
        u = unit
    else:
        if not _units.has_key(u):
            raise ValueError("Unit '%s' is unknown." % u)
        u = _units[u]
    q = v * u 
    if allow is None:
        # no check, we're done
        return q
    else:
        # check that we are compatible with one of the listed units
        for allowed in allow:
            try:
                q.matchUnits(allowed)
                return q
            except unum.IncompatibleUnitsError:
                pass
        raise ValueError("Unit %s is not allowed here: only %s are."
                         % (u.strUnit(), str.join(",", [a.strUnit() for a in allow])))


def from_timedelta(td):
    """
    Return a duration expressing the same time amount as the Python
    `datetime.timedelta` object `td`.
    """
    try:
        # Python 2.7 onwards
        return td.total_seconds() * _units['s']
    except AttributeError:
        return ((td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6) * _units.s


def to_timedelta(duration):
    """
    Convert a duration into a Python `datetime.timedelta` object.

    This is useful to operate on Python's `datetime.time` and
    `datetime.date` objects, which can be added or subtracted to
    `datetime.timedelta`.
    """
    assert duration.matchUnits(_units['s']), \
           "gc3libs.units.to_timedelta: `duration` argument must be convertible to 'seconds'"
    return datetime.timedelta(seconds=duration.asUnit(_units.s).value)


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="units",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
