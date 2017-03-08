#! /usr/bin/env python
#
"""
Test for classes and functions in the `utils` module.
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


from itertools import izip

# 3rd party imports
import pytest

# GC3Pie imports
import gc3libs.exceptions
import gc3libs.utils


# test definitions

class TestYieldAtNext(object):

    def test_YieldAtNext_yield(self):
        def generator_yield():
            yield 0
        g = gc3libs.utils.YieldAtNext(generator_yield())
        assert g.next() == 0

    def test_YieldAtNext_send(self):
        def generator_yield_send():
            val = (yield 1)
            yield val
        g = gc3libs.utils.YieldAtNext(generator_yield_send())
        assert g.next() == 1
        result = g.send('a sent value')
        assert result == None
        assert g.next() == 'a sent value'

    def test_YieldAtNext_send_iter(self):
        def generator_yield_send():
            val = (yield 0)
            while True:
                val = (yield val)
        g = gc3libs.utils.YieldAtNext(generator_yield_send())
        expected = range(0, 9)
        n = 0
        print ("expecting %d messages" % (len(expected),))
        for msg in g:
            print ("received: %s" % (msg,))
            # check msg
            assert msg == expected[n]
            # send another msg
            n += 1
            if n < len(expected):
                result = g.send(expected[n])
                assert result == None
            else:
                assert n == len(expected)
                print ("%d messages received, no more messages to send" % (n,))
                break

    def test_YieldAtNext_send_many(self):
        def generator_yield_send():
            val = (yield 0)
            while True:
                val = (yield val)
        g = gc3libs.utils.YieldAtNext(generator_yield_send())
        expected = range(1, 10)
        # consume one value to init the generator
        g.next()
        # send all messages
        for msg in expected:
            g.send(msg)
            print ("sent message '%s'" % (msg,))
        # receive them all
        print ("expecting %d messages back" % (len(expected),))
        for msg, expected_msg in izip(g, expected):
            print ("received: %s" % (msg,))
            # check msg
            assert msg == expected_msg

    def test_YieldAtNext_throw(self):
        def generator_yield_throw():
            try:
                # XXX: why do we need val = yield syntax here?
                # val = (yield 2)
                yield 2
            except RuntimeError:
                yield 'exception caught'
        g = gc3libs.utils.YieldAtNext(generator_yield_throw())
        assert g.next() == 2
        result = g.throw(RuntimeError)
        assert result == None
        assert g.next() == 'exception caught'

    def test_YieldAtNext_StopIteration(self):
        def generator_yield():
            yield 3
        g = gc3libs.utils.YieldAtNext(generator_yield())
        assert g.next() == 3
        with pytest.raises(StopIteration):
            g.next()


# main: run tests

if "__main__" == __name__:
    pytest.main(["-v", __file__])
