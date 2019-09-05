#! /usr/bin/env python
#
"""
Unit tests for the EC2 backend.
"""
# Copyright (C) 2011-2013, 2019  University of Zurich. All rights reserved.
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
from builtins import str
from builtins import object
__docformat__ = 'reStructuredText'

# stdlib imports
import shutil
import tempfile

# 3rd party imports
import pytest

# The EC2 backend might not be installed (it's currently marked as
# optional in `setup.py`), so skip these tests altogether if there is any error
boto = pytest.importorskip("boto")

# local imports
from gc3libs.backends.ec2 import VMPool
import gc3libs.exceptions


class _MockVM(object):

    def __init__(self, id, **extra):
        self.id = id
        for k, v in extra.items():
            setattr(self, k, v)


class TestVMPool(object):

    # XXX: the `get*` methods are not tested here (yet) as they
    # require mocking the `boto` library!

    @pytest.fixture(autouse=True)
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

        yield

        shutil.rmtree(self.tmpdir)

    def test_empty_vmpool(self):
        assert self.pool0._vm_ids == set([])
        assert self.pool0._vm_cache == {}

    def test_vmpool_one_vm(self):
        assert self.pool1._vm_ids == set(['a'])
        assert self.pool1._vm_cache == {'a': self.vm1}

    def test_vmpool_two_vms(self):
        assert self.pool2._vm_ids == set(['a', 'b'])
        assert self.pool2._vm_cache == {'a': self.vm1, 'b': self.vm2}

    def test_repr(self):
        # representation of empty sets differs in Py2 and Py3 ...
        assert repr(self.pool0) in ["set([])", "set()"]
        # ... also representation of unicode strings differs on Py2 and Py3
        assert repr(self.pool1) in ["{'a'}", "set(['a'])", "set([u'a'])"]
        # ...and also sets do not have predictable representation
        assert repr(self.pool2) in [
            # Py2
            "set(['a', 'b'])",
            "set(['b', 'a'])",
            "set([u'a', u'b'])",
            "set([u'b', u'a'])",
            # Py3
            "{'a', 'b'}",
            "{'b', 'a'}",
        ]


    def test_str(self):
        # representation of empty sets differs in Py2 and Py3 ...
        assert str(self.pool0) in [
            "VMPool('pool0') : set()",
            "VMPool('pool0') : set([])",
        ]
        # representation of unicode strings differs on Py2 and Py3
        assert str(self.pool1) in [
            "VMPool('pool1') : {'a'}",
            "VMPool('pool1') : set(['a'])",
            "VMPool('pool1') : set([u'a'])",
        ]
        # also sets do not have predictable representation
        assert str(self.pool2) in [
            # Py2
            "VMPool('pool2') : set(['a', 'b'])",
            "VMPool('pool2') : set(['b', 'a'])",
            "VMPool('pool2') : set([u'a', u'b'])",
            "VMPool('pool2') : set([u'b', u'a'])",
            # Py3
            "VMPool('pool2') : {'a', 'b'}",
            "VMPool('pool2') : {'b', 'a'}",
        ]

    def test_add_remove(self):
        VM_ID = 'x'
        vm = _MockVM(VM_ID)
        for pool in self.pool0, self.pool1, self.pool2:
            L = len(pool)
            # test add
            pool.add_vm(vm)
            assert VM_ID in pool
            assert len(pool) == L + 1
            # test remove
            pool.remove_vm(VM_ID)
            assert not VM_ID in pool
            assert len(pool) == L

    def test_add_delete(self):
        VM_ID = 'x'
        vm = _MockVM(VM_ID)
        for pool in self.pool0, self.pool1, self.pool2:
            L = len(pool)
            # test add
            pool.add_vm(vm)
            assert VM_ID in pool
            assert len(pool) == L + 1
            # test delete
            del pool[VM_ID]
            assert not VM_ID in pool
            assert len(pool) == L

    def test_len(self):
        for l, pool in enumerate([self.pool0, self.pool1, self.pool2]):
            assert len(pool) == l

    def test_iter(self):
        """Check that `VMPool.__iter__` iterates over VM IDs."""
        for n, pool in enumerate([self.pool0, self.pool1, self.pool2]):
            assert list(iter(pool)) in (
                list('ab'[:n]),
                list('ba'[:n]),
            )

    def test_get_vm_in_cache(self):
        assert self.vm1 == self.pool1['a']

        assert self.vm1 == self.pool2['a']
        assert self.vm2 == self.pool2['b']

    def test_get_vm_not_in_cache_and_no_connection(self):
        # clone pool2 from disk copy
        pool = VMPool(self.pool2.path, None)
        with pytest.raises(gc3libs.exceptions.UnrecoverableError):
            # pylint: disable=pointless-statement
            pool['a']

    def test_get_all_vms(self):
        all_vms1 = self.pool1.get_all_vms()
        assert self.vm1 in all_vms1

        all_vms2 = self.pool2.get_all_vms()
        assert self.vm1 in all_vms2
        assert self.vm2 in all_vms2

    def test_lookup_is_get_vm(self):
        assert self.pool1.get_vm('a') == self.pool1['a']

        assert self.pool2.get_vm('a') == self.pool2['a']
        assert self.pool2.get_vm('b') == self.pool2['b']

    def test_reload(self):
        # simulate stopping program by deleting object and cloning a
        # copy from disk
        for pool in self.pool0, self.pool1, self.pool2:
            ids = pool._vm_ids
            path = pool.path
            del pool
            pool = VMPool(path, None)
            assert pool._vm_ids == ids
            assert pool._vm_ids is not ids

    def test_save_then_load_empty_vmpool(self):
        self.pool0.save()

        pool = VMPool(self.pool0.path, None)
        assert pool.name == self.pool0.name
        assert pool.conn == None
        assert pool._vm_cache == {}
        assert pool._vm_ids == self.pool0._vm_ids

    def test_save_then_load_nonempty_vmpool(self):
        self.pool2.save()

        pool = VMPool(self.pool2.path, None)
        assert pool.name == self.pool2.name
        # the list of VM ids should have been persisted ...
        assert pool._vm_ids == self.pool2._vm_ids
        # ... but the VM cache is now empty
        assert pool._vm_cache == {}
        # ...and so is the connection
        assert pool.conn == None

    @pytest.mark.skip("This test must be rewritten")
    def test_update(self):
        # clone pool2
        pool = VMPool(self.pool2.path, None)
        vm3 = _MockVM('c')
        pool.add_vm(vm3)
        self.pool2.update()
        assert 'c' in self.pool2
        # cannot test the following without a connection:
        # assert_equal(self.pool2['c'], vm3)

    def test_update_remove(self):
        # clone pool2
        pool = VMPool(self.pool2.path, None)
        pool.remove_vm('b')
        self.pool2.update(remove=True)
        assert 'b' not in self.pool2
        assert 'a' in self.pool2


if "__main__" == __name__:
    pytest.main(["-v", __file__])
