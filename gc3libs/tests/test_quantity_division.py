#! /usr/bin/env python
#
from __future__ import division

"""
Test that classes from `gc3libs.quantity.Quantity` behave well
also when `from __future__ import division` is in effect.

Se issue #525 for background and possibly more info.
"""
# Copyright (C) 2012, 2013, University of Zurich. All rights reserved.
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


from random import randint

# 3rd party imports
from nose.tools import assert_equal, raises

# GC3Pie imports
from gc3libs.quantity import Duration, Memory


# test definitions

def test_divide_duration1():
    n = randint(1, 100)
    d1 = Duration(2*n, unit=Duration.s)
    d2 = d1 / 2
    assert_equal(d2,   Duration(n, unit=Duration.s))
    assert_equal(2*d2, d1)
    assert_equal(d2*2, d1)

# same, but with a non-base unit
def test_divide_duration2():
    n = randint(1, 100)
    d1 = Duration(2*n, unit=Duration.days)
    d2 = d1 / 2
    assert_equal(d2,   Duration(n, unit=Duration.days))
    assert_equal(2*d2, d1)
    assert_equal(d2*2, d1)


def test_divide_memory1():
    n = randint(1, 100)
    d1 = Memory(2*n, unit=Memory.B)
    d2 = d1 / 2
    assert_equal(d2,   Memory(n, unit=Memory.B))
    assert_equal(2*d2, d1)
    assert_equal(d2*2, d1)


# same, but with a non-base unit
def test_divide_memory2():
    n = randint(1, 100)
    d1 = Memory(2*n, unit=Memory.MB)
    d2 = d1 / 2
    assert_equal(d2,   Memory(n, unit=Memory.MB))
    assert_equal(2*d2, d1)
    assert_equal(d2*2, d1)


# main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()
