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
from nose.tools import assert_equal, assert_false, assert_true, raises

# local imports
from gc3libs.backends.ec2 import VMPool, EC2Lrms
import gc3libs.exceptions


class _MockVM(object):
    def __init__(self, id, **extra):
        self.id = id
        for k,v in extra.iteritems():
            setattr(self, k, v)


class TestVMPool(object):

    # XXX: the `get*` methods are not tested here (yet) as they
    # require mocking the `boto` library!

    def setup(self):
        # for persistence tests
        self.tmpdir = tempfile.mkdtemp()

        # an empty VMPool
        self.pool0 = VMPool(self.tmpdir + '/pool0', None)
        # VMpool with 1 VM
        self.pool1 = VMPool(self.tmpdir + '/pool1', None)
        self.vm1 = _MockVM('a')
        self.pool1.add_vm(self.vm1)
        # VMpool with 2 VM
        self.pool2 = VMPool(self.tmpdir + '/pool2', None)
        self.vm2 = _MockVM('b')
        self.pool2.add_vm(self.vm1)
        self.pool2.add_vm(self.vm2)

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

    def test_get_vm_in_cache(self):
        assert_equal(self.vm1, self.pool1['a'])

        assert_equal(self.vm1, self.pool2['a'])
        assert_equal(self.vm2, self.pool2['b'])

    @raises(gc3libs.exceptions.UnrecoverableError)
    def test_get_vm_not_in_cache_and_no_connection(self):
        # clone pool2 from disk copy
        pool = VMPool(self.pool2.path, None)
        assert_equal(self.vm1, pool['a'])
        assert_equal(self.vm2, pool['b'])

    def test_get_all_vms(self):
        all_vms1 = self.pool1.get_all_vms()
        assert self.vm1 in all_vms1

        all_vms2 = self.pool2.get_all_vms()
        assert self.vm1 in all_vms2
        assert self.vm2 in all_vms2

    def test_lookup_is_get_vm(self):
        assert_equal(self.pool1.get_vm('a'), self.pool1['a'])

        assert_equal(self.pool2.get_vm('a'), self.pool2['a'])
        assert_equal(self.pool2.get_vm('b'), self.pool2['b'])

    def test_reload(self):
        # simulate stopping program by deleting object and cloning a
        # copy from disk
        for pool in self.pool0, self.pool1, self.pool2:
            ids = pool._vm_ids
            path = pool.path
            del pool
            pool = VMPool(path, None)
            assert_equal(pool._vm_ids, ids)
            assert pool._vm_ids is not ids

    def test_save_then_load_empty_vmpool(self):
        self.pool0.save()

        pool = VMPool(self.pool0.path, None)
        assert_equal(pool.name,      self.pool0.name)
        assert_equal(pool.conn,      None)
        assert_equal(pool._vm_cache, {})
        assert_equal(pool._vm_ids,   self.pool0._vm_ids)

    def test_save_then_load_nonempty_vmpool(self):
        self.pool2.save()

        pool = VMPool(self.pool2.path, None)
        assert_equal(pool.name,      self.pool2.name)
        # the list of VM ids should have been persisted ...
        assert_equal(pool._vm_ids,   self.pool2._vm_ids)
        # ... but the VM cache is now empty
        assert_equal(pool._vm_cache, {})
        # ...and so is the connection
        assert_equal(pool.conn,      None)

    def test_update(self):
        # clone pool2
        pool = VMPool(self.pool2.path, None)
        vm3 = _MockVM('c')
        pool.add_vm(vm3)
        self.pool2.update()
        assert 'c' in self.pool2
        # cannot test the following without a connection:
        #assert_equal(self.pool2['c'], vm3)

    def test_update_remove(self):
        # clone pool2
        pool = VMPool(self.pool2.path, None)
        pool.remove_vm('b')
        self.pool2.update(remove=True)
        assert 'b' not in self.pool2
        assert 'a' in self.pool2


if "__main__" == __name__:
    import nose
    nose.runmodule()
