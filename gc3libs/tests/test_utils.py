#! /usr/bin/env python
#
"""
Test for classes and functions in the `utils` module.
"""
# Copyright (C) 2012, 2013, 2019,  University of Zurich. All rights reserved.
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
from future import standard_library
standard_library.install_aliases()
from builtins import zip
from builtins import next
from builtins import range
from builtins import object

# 3rd party imports
import mock
import pytest

# GC3Pie imports
import gc3libs.exceptions
from gc3libs.quantity import Duration, Memory
import gc3libs.utils


__docformat__ = 'reStructuredText'


# test definitions

def test_get_linux_memcg_limit_no_proc_self_cgroup():
    with mock.patch('gc3libs.utils.open') as mo:
        mo.side_effect = OSError(2, 'No such file or directory', '/proc/self/cgroup')
        limit = gc3libs.utils.get_linux_memcg_limit()
        mo.assert_called_once_with('/proc/self/cgroup', 'r')
        assert limit is None


def test_get_linux_memcg_limit_no_memcg():
    mo = mock.mock_open(read_data='*** no memcg ***')
    with mock.patch('gc3libs.utils.open', mo):
        limit = gc3libs.utils.get_linux_memcg_limit()
        mo.assert_called_once_with('/proc/self/cgroup', 'r')
        assert limit is None


def test_get_linux_memcg_limit_with_memcg_limit():
    def fake_open(path, mode):
        from io import StringIO
        from contextlib import closing
        if path == '/proc/self/cgroup':
            return closing(StringIO('2:memory:/test'))
        elif path == '/sys/fs/cgroup/memory/test/memory.soft_limit_in_bytes':
            raise OSError(2, 'No such file or directory',
                          '/sys/fs/cgroup/memory/test/memory.soft_limit_in_bytes')
        elif path == '/sys/fs/cgroup/memory/test/memory.limit_in_bytes':
            return closing(StringIO('42'))
        else:
            raise AssertionError(
                "Unexpected call to open({0!r}, {1!r})"
                .format(path, mode))
    with mock.patch('gc3libs.utils.open') as mo:
        mo.side_effect = fake_open
        limit = gc3libs.utils.get_linux_memcg_limit()
        mo.assert_has_calls([
            mock.call('/proc/self/cgroup', 'r'),
            mock.call('/sys/fs/cgroup/memory/test/memory.soft_limit_in_bytes', 'r'),
            mock.call('/sys/fs/cgroup/memory/test/memory.limit_in_bytes', 'r'),
        ])
        assert limit == Memory(42, Memory.B)


def test_parse_linux_proc_limits():
    # snapshot of `/proc/self/limits` taken on Linux 4.4.0-87-generic (Ubuntu 16.04)
    data = """\
Limit                     Soft Limit           Hard Limit           Units
Max cpu time              unlimited            unlimited            seconds
Max file size             unlimited            unlimited            bytes
Max data size             unlimited            unlimited            bytes
Max stack size            8388608              unlimited            bytes
Max core file size        0                    unlimited            bytes
Max resident set          unlimited            unlimited            bytes
Max processes             30442                30442                processes
Max open files            1024                 1048576              files
Max locked memory         65536                65536                bytes
Max address space         unlimited            unlimited            bytes
Max file locks            unlimited            unlimited            locks
Max pending signals       30442                30442                signals
Max msgqueue size         819200               819200               bytes
Max nice priority         0                    0
Max realtime priority     0                    0
Max realtime timeout      unlimited            unlimited            us
"""
    soft, hard = gc3libs.utils.parse_linux_proc_limits(data)
    # soft limits
    assert soft['max_realtime_timeout'] == None
    assert soft['max_realtime_priority'] == 0
    assert soft['max_nice_priority'] == 0
    assert soft['max_msgqueue_size'] == Memory(819200, unit=Memory.B)
    assert soft['max_pending_signals'] == 30442
    assert soft['max_file_locks'] == None
    assert soft['max_address_space'] == None
    assert soft['max_locked_memory'] == Memory(65536, unit=Memory.B)
    assert soft['max_open_files'] == 1024
    assert soft['max_processes'] == 30442
    assert soft['max_resident_set'] == None
    assert soft['max_core_file_size'] == Memory(0, unit=Memory.B)
    assert soft['max_stack_size'] == Memory(8388608, unit=Memory.B)
    assert soft['max_data_size'] == None
    assert soft['max_file_size'] == None
    assert soft['max_cpu_time'] == None
    # hard limits
    assert hard['max_realtime_timeout'] == None
    assert hard['max_realtime_priority'] == 0
    assert hard['max_nice_priority'] == 0
    assert hard['max_msgqueue_size'] == Memory(819200, unit=Memory.B)
    assert hard['max_pending_signals'] == 30442
    assert hard['max_file_locks'] == None
    assert hard['max_address_space'] == None
    assert hard['max_locked_memory'] == Memory(65536, unit=Memory.B)
    assert hard['max_open_files'] == 1048576
    assert hard['max_processes'] == 30442
    assert hard['max_resident_set'] == None
    assert hard['max_core_file_size'] == None
    assert hard['max_stack_size'] == None
    assert hard['max_data_size'] == None
    assert hard['max_file_size'] == None
    assert hard['max_cpu_time'] == None


class TestYieldAtNext(object):

    def test_YieldAtNext_yield(self):
        def generator_yield():
            yield 0
        g = gc3libs.utils.YieldAtNext(generator_yield())
        assert next(g) == 0

    def test_YieldAtNext_send(self):
        def generator_yield_send():
            val = (yield 1)
            yield val
        g = gc3libs.utils.YieldAtNext(generator_yield_send())
        assert next(g) == 1
        result = g.send('a sent value')
        assert result == None
        assert next(g) == 'a sent value'

    def test_YieldAtNext_send_iter(self):
        def generator_yield_send():
            val = (yield 0)
            while True:
                val = (yield val)
        g = gc3libs.utils.YieldAtNext(generator_yield_send())
        expected = list(range(0, 9))
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
        expected = list(range(1, 10))
        # consume one value to init the generator
        next(g)
        # send all messages
        for msg in expected:
            g.send(msg)
            print ("sent message '%s'" % (msg,))
        # receive them all
        print ("expecting %d messages back" % (len(expected),))
        for msg, expected_msg in zip(g, expected):
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
        assert next(g) == 2
        result = g.throw(RuntimeError)
        assert result == None
        assert next(g) == 'exception caught'

    def test_YieldAtNext_StopIteration(self):
        def generator_yield():
            yield 3
        g = gc3libs.utils.YieldAtNext(generator_yield())
        assert next(g) == 3
        with pytest.raises(StopIteration):
            next(g)


# main: run tests

if "__main__" == __name__:
    pytest.main(["-v", __file__])
