#! /usr/bin/env python
#
"""
Unit tests for the EC2 backend.
"""
# Copyright (C) 2011-2013, GC3, University of Zurich. All rights reserved.
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
import shutil
import sys
import tempfile

# 3rd party imports
from nose.tools import assert_equal, assert_false, assert_true

# local imports
from gc3libs.backends.ec2 import VMPool, EC2Lrms
from gc3libs.session import Session


class _MockVM(object):
    def __init__(self, id, **extra):
        self.id = id
        for k,v in extra.iteritems():
            setattr(self, k, v)


class TestVMPool(object):

    # XXX: the `get*` methods are not tested here (yet) as they
    # require mocking the `boto` library!

    def setup(self):
        # an empty VMPool
        self.pool0 = VMPool('pool0', None)
        # VMpool with 1 VM
        self.pool1 = VMPool('pool1', None)
        self.vm1 = _MockVM('a')
        self.pool1.add_vm(self.vm1)
        # VMpool with 2 VM
        self.pool2 = VMPool('pool2', None)
        self.vm2 = _MockVM('b')
        self.pool2.add_vm(self.vm1)
        self.pool2.add_vm(self.vm2)

        # for persistence tests
        self.tmpdir = tempfile.mkdtemp()
        self.sess = Session(self.tmpdir)

    def teardown(self):
        shutil.rmtree(self.tmpdir)

    def test_empty_vmpool(self):
        assert_equal(self.pool0._vm_ids, set([]))
        assert_equal(self.pool0._vm_cache, {})

    def test_vmpool_one_vm(self):
        assert_equal(self.pool1._vm_ids, set(['a']))
        assert_equal(self.pool1._vm_cache, {'a': self.vm1})

    def test_vmpool_two_vms(self):
        assert_equal(self.pool2._vm_ids, set(['a', 'b']))
        assert_equal(self.pool2._vm_cache, {'a': self.vm1, 'b': self.vm2})

    def test_repr(self):
        assert_equal(repr(self.pool0), "set([])")
        assert_equal(repr(self.pool1), "set(['a'])")
        assert_equal(repr(self.pool2), "set(['a', 'b'])")

    def test_str(self):
        assert_equal(str(self.pool0), "VMPool('pool0') : set([])")
        assert_equal(str(self.pool1), "VMPool('pool1') : set(['a'])")
        assert_equal(str(self.pool2), "VMPool('pool2') : set(['a', 'b'])")

    def test_save_then_load_empty_vmpool(self):
        saved_id = self.sess.add(self.pool0)
        loaded = self.sess.load(saved_id)
        assert_equal(loaded.persistent_id, self.pool0.persistent_id)
        assert_equal(loaded.conn,          None)
        assert_equal(loaded._vm_cache,     {})
        assert_equal(loaded._vm_ids,       self.pool0._vm_ids)

    def test_save_then_load_nonempty_vmpool(self):
        saved_id = self.sess.add(self.pool2)
        loaded = self.sess.load(saved_id)

        assert_equal(loaded.persistent_id, self.pool2.persistent_id)
        # the list of VM ids should have been persisted ...
        assert_equal(loaded._vm_ids,       self.pool2._vm_ids)
        # ... but the VM cache is now empty
        assert_equal(loaded._vm_cache,     {})
        # ...and so is the connection
        assert_equal(loaded.conn,          None)

    def test_add_remove(self):
        VM_ID = 'x'
        vm = _MockVM(VM_ID)
        for pool in self.pool0, self.pool1, self.pool2:
            L = len(pool)
            # test add
            pool.add_vm(vm)
            assert_true(VM_ID in pool)
            assert_equal(len(pool), L+1)
            # test remove
            pool.remove_vm(VM_ID)
            assert_false(VM_ID in pool)
            assert_equal(len(pool), L)

    def test_add_delete(self):
        VM_ID = 'x'
        vm = _MockVM(VM_ID)
        for pool in self.pool0, self.pool1, self.pool2:
            L = len(pool)
            # test add
            pool.add_vm(vm)
            assert_true(VM_ID in pool)
            assert_equal(len(pool), L+1)
            # test delete
            del pool[VM_ID]
            assert_false(VM_ID in pool)
            assert_equal(len(pool), L)

    def test_len(self):
        for l, pool in enumerate([self.pool0, self.pool1, self.pool2]):
            assert_equal(len(pool), l)

    def test_iter(self):
        """Check that `VMPool.__iter__` iterates over VM IDs."""
        for n, pool in enumerate([self.pool0, self.pool1, self.pool2]):
            assert_equal(list(iter(pool)), list('ab'[:n]))


if "__main__" == __name__:
    import nose
    nose.runmodule()
