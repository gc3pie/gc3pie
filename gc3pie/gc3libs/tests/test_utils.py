#! /usr/bin/env python
#
"""
Test for classes and functions in the `utils` module.
"""
# Copyright (C) 2012, 2013, GC3, University of Zurich. All rights reserved.
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

# stdlib imports
import os
import sys
import unittest

# 3rd party imports
from nose.tools import assert_true, assert_false, assert_equal, raises
from nose.tools import set_trace
from nose.plugins.skip import SkipTest

# GC3Pie imports
import gc3libs.exceptions
import gc3libs.utils


## test definitions

class TestYieldAtNext(object):

    def test_YieldAtNext_yield(self):
        def generator_yield():
            yield 0
        g = gc3libs.utils.YieldAtNext(generator_yield())
        assert_equal(g.next(), 0)

    def test_YieldAtNext_send(self):
        def generator_yield_send():
            val = (yield 1)
            yield val
        g = gc3libs.utils.YieldAtNext(generator_yield_send())
        assert_equal(g.next(), 1)
        result = g.send('a sent value')
        assert_equal(result, None)
        assert_equal(g.next(), 'a sent value')

    def test_YieldAtNext_throw(self):
        def generator_yield_throw():
            try:
                val = (yield 2)
            except RuntimeError:
                yield 'exception caught'
        g = gc3libs.utils.YieldAtNext(generator_yield_throw())
        assert_equal(g.next(), 2)
        result = g.throw(RuntimeError)
        assert_equal(result, None)
        assert_equal(g.next(), 'exception caught')

    @raises(StopIteration)
    def test_YieldAtNext_StopIteration(self):
        def generator_yield():
            yield 3
        g = gc3libs.utils.YieldAtNext(generator_yield())
        assert_equal(g.next(), 3)
        # raises `StopIteration`
        g.next()


## main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()
