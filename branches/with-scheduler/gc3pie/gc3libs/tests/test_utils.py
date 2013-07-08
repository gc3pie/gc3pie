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

# 3rd party imports
from nose.tools import assert_true, assert_false, assert_equal, raises
from nose.tools import set_trace
from nose.plugins.skip import SkipTest

# GC3Pie imports
import gc3libs.exceptions
import gc3libs.utils


## test definitions

def test_YieldAtNext():

    # create a generator that performs all steps in sequence
    def yield_send_throw_seq():
        # yield a value and get one back from `send`
        val = (yield 0)
        # yield that back and get an exception
        try:
            yield val
        except RuntimeError:
            yield 2

    seq = gc3libs.utils.YieldAtNext(yield_send_throw_seq)
    for i, val in enumerate(seq):
        assert_equal(i, val)
        if i == 0:
            seq.send(1)
        if i == 1:
            seq.throw(RuntimeError)
    assert_equal(i, 2)
    assert_equal(val, 2)


## main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()
