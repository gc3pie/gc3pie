#! /usr/bin/env python

"""
"""

# Copyright (C) 2012-2013  University of Zurich. All rights reserved.
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

from __future__ import absolute_import, print_function, unicode_literals
from builtins import object
__docformat__ = 'reStructuredText'

import os

import gc3libs
from gc3libs.exceptions import UnrecoverableError


class InstanceNotFound(UnrecoverableError):

    """Specified instance was not found"""


class VMPool(object):

    """
    Persistable container for a list of VM objects.

    Holds a list of all VM IDs of inserted VMs, and a cache of the
    actual VM objects. If information about a VM is requested, which
    is not currently in the cache, a request is made to the cloud
    provider API (through the `conn` object passed to the constructor)
    to get that information.

    The `VMPool`:class: looks like a mixture of the `set` and `dict`
    interfaces:

    * VMs are added to the container using the `add_vm` method::

        | >>> vmpool.add_vm(vm1)

      (There is no dictionary-like ``D[x]=y`` setter syntax, though,
      as that would require spelling out the VM ID.)

    * VMs can be removed via the `remove_vm` method or the `del`
      syntax; in both cases it's the VM *ID* that must be passed::

       | >>> vmpool.remove_vm(vm1)

       | >>> del vmpool[vm1]

    * Iterating over a `VMPool`:class: instance returns the VM IDs.

    * Other sequence methods work as expected: the VM info can be
      accessed with the usual ``[]`` lookup syntax from its ID, the
      ``len()`` of a `VMPool`:class: object is the total number of VM
      IDs registered, etc..

    `VMPool`:class: objects can be persisted using the
    `gc3libs.persistence`:module: framework.  Note however that the VM
    cache will be empty upon loading a `VMPool` instance from
    persistent storage.
    """

    def __init__(self, path, connection):
        # remove trailing `/` so that we can use the last path
        # component as a name
        if path.endswith('/'):
            self.path = path[:-1]
        else:
            self.path = path
        self.name = os.path.basename(self.path)

        if os.path.isdir(path):
            self.load()
        else:
            os.makedirs(self.path)
            self._vm_ids = set()

        self.conn = connection
        self._vm_cache = {}
        self.changed = False

    def __delitem__(self, vm_id):
        """
        x.__delitem__(self, vm_id) <==> x.remove_vm(vm_id)
        """
        return self.remove_vm(vm_id)

    def __getitem__(self, vm_id):
        """
        x.__getitem__(vm_id) <==> x.get_vm(vm_id)
        """
        return self.get_vm(vm_id)

    def __getstate__(self):
        # only save path and list of IDs, the rest can be
        # reconstructed from these two (see `__setstate__`)
        return dict(
            path=self.path,
            _vm_ids=self._vm_ids,
        )

    def __iter__(self):
        """
        Iterate over the list of known VM ids.
        """
        # We need to create a new list because the _vm_ids set may be
        # updated during iteration.
        return iter(list(self._vm_ids))

    def __len__(self):
        return len(self._vm_ids)

    def __repr__(self):
        return self._vm_ids.__repr__()

    def __setstate__(self, state):
        self.path = state['path']
        self.name = os.path.basename(self.path)
        self.conn = None
        self._vm_cache = {}
        self._vm_ids = state['_vm_ids']

    def __str__(self):
        return "VMPool('%s') : %s" % (self.name, self._vm_ids)

    def add_vm(self, vm, cache=True):
        """
        Add a VM object to the list of VMs.
        """
        if not hasattr(vm, 'preferred_ip'):
            vm.preferred_ip = ''
        gc3libs.utils.write_contents(
            os.path.join(self.path, vm.id), vm.preferred_ip)
        self._vm_ids.add(vm.id)
        if cache:
            self._vm_cache[vm.id] = vm
        self.changed = True

    def remove_vm(self, vm_id):
        """
        Remove VM with id `vm_id` from the list of known VMs. No
        connection to the EC2 endpoint is performed.
        """
        if os.path.exists(os.path.join(self.path, vm_id)):
            try:
                os.remove(os.path.join(self.path, vm_id))
            except OSError as err:
                if err.errno == 2:  # ENOENT, "No such file or directory"
                    # ignore - some other process might have removed it
                    pass
                else:
                    raise
        if vm_id in self._vm_ids:
            self._vm_ids.remove(vm_id)
        if vm_id in self._vm_cache:
            del self._vm_cache[vm_id]
        self.changed = True

    def _get_instance(self, vm_id):
        """
        Each cloud provider should implement this method, that must accept
        a VM id and return an instance object.
        """
        raise NotImplementedError(
            "Abstract method `VMPool._get_instance()` called "
            "- this should have been defined in a derived class.")

    def get_vm(self, vm_id, force_reload=False):
        """
        Return the VM object with id `vm_id`.

        If it is found in the local cache, that object is
        returned. Otherwise a new VM object is searched for in the EC2
        endpoint.
        """
        # return cached info, if any
        if not force_reload and vm_id in self._vm_cache:
            return self._vm_cache[vm_id]

        # XXX: should this be an `assert` instead?
        if not self.conn:
            raise UnrecoverableError(
                "No connection set for `VMPool('%s')`" % self.path)

        vm = self._get_instance(vm_id)
        if not hasattr(vm, 'preferred_ip'):
            # read from file
            vm.preferred_ip = gc3libs.utils.read_contents(
                os.path.join(self.path, vm.id))
        self._vm_cache[vm_id] = vm
        if vm_id not in self._vm_ids:
            self._vm_ids.add(vm_id)
            self.changed = True
        return vm

    def get_all_vms(self):
        """
        Return list of all known VMs.
        """
        vms = []
        for vm_id in self._vm_ids:
            try:
                vms.append(self.get_vm(vm_id))
            except UnrecoverableError as ex:
                gc3libs.log.warning(
                    "Cloud resource `%s`: ignoring error while trying to "
                    "get information on VM wiht id `%s`: %s"
                    % (self.name, vm_id, ex))
        return vms

    def load(self):
        """Populate list of VM IDs from the data saved on disk."""
        self._vm_ids = set([
            entry for entry in os.listdir(self.path)
            if not entry.startswith('.')
        ])

    def save(self):
        """Ensure all VM IDs will be found by the next `load()` call."""
        for vm_id in self._vm_ids:
            gc3libs.utils.write_contents(os.path.join(self.path, vm_id),
                                         self.get_vm(vm_id).preferred_ip)

    def update(self, remove=False):
        """
        Synchronize list of VM IDs with contents of disk storage.

        If optional argument `remove` is true, then remove VMs whose
        ID is no longer present in the on-disk storage.
        """
        ids_on_disk = set([
            entry for entry in os.listdir(self.path)
            if not entry.startswith('.')
        ])
        added = ids_on_disk - self._vm_ids
        for vm_id in added:
            self._vm_ids.add(vm_id)
        self.save()
        if remove:
            removed = self._vm_ids - ids_on_disk
            for vm_id in removed:
                self.remove_vm(vm_id)


# main: run tests
if "__main__" == __name__:
    import doctest
    doctest.testmod(name="vmpool",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
