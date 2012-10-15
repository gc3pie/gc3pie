#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012, GC3, University of Zurich. All rights reserved.
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

import pickle
import tempfile
import shutil
import os

from gc3libs import Task, Application
from gc3libs.proxy import BaseProxy, Proxy
from gc3libs.persistence import make_store
import gc3libs.exceptions

from nose.tools import assert_equal, raises

def _base_proxy_basic_tests(obj, proxycls=BaseProxy):
    """Basic persistency tests with `pickle`"""
    prxy1 = proxycls(obj)
    assert isinstance(prxy1, proxycls)
    s = pickle.dumps(prxy1)
    prxy2 = pickle.loads(s)
    assert isinstance(prxy2, proxycls)
    assert isinstance(prxy2, obj.__class__)

def _base_proxy_persistence_test(obj, proxycls=BaseProxy):
    """Basic persistency tests with `gc3libs.persistence` subsystem"""
    tmpdir = tempfile.mkdtemp()
    store = make_store(tmpdir)
    prxy1 = proxycls(obj)
    try:
        oid = store.save(prxy1)
        prxy2 = store.load(oid)
    finally:
        shutil.rmtree(tmpdir)

def test_base_proxy_with_builtins():
    _base_proxy_basic_tests(1)
    assert_equal(BaseProxy(1) + 1, 2)
    _base_proxy_persistence_test(1)

    _base_proxy_basic_tests(1, Proxy)
    assert_equal(Proxy(1) + 1, 2)
    _base_proxy_persistence_test(1, Proxy)

def test_base_proxy_with_task():
    _base_proxy_basic_tests(Task())
    _base_proxy_persistence_test(Task())
    _base_proxy_basic_tests(Task(), Proxy)
    _base_proxy_persistence_test(Task(), Proxy)

def test_base_proxy_with_app():
    _base_proxy_basic_tests(Application([], [], [], ''))
    _base_proxy_persistence_test(Application([], [], [], ''))
    _base_proxy_basic_tests(Application([], [], [], ''), Proxy)
    _base_proxy_persistence_test(Application([], [], [], ''), Proxy)


class test_proxy_class(object):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.store = make_store(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_forget(self):
        app = Application([], [], [], '')
        prxy = Proxy(app, storage=self.store)
        assert prxy._obj is app
        prxy.proxy_forget()
        assert prxy._obj is None

    def test_set_storage(self):
        app = Application([], [], [], '', jobname='App')
        prxy = Proxy(app)

        # Forgetting an object will not delete the object if no store
        # has been set.
        assert prxy._obj is app
        prxy.proxy_forget()
        assert prxy._obj is app
        # Set the external storage to use to save the object.
        prxy.proxy_set_storage(self.store)
        prxy.proxy_forget()
        # Now, the object reference is *empty*
        assert prxy._obj is None
        # however, the `Proxy` class can load it again if needed.
        assert_equal(prxy.jobname, 'App')

    def test_set_storage_overwrite_arg(self):
        app = Application([], [], [], '')
        prxy = Proxy(app, storage="FakeStorage")

        # Do not overwrite the storage
        prxy.proxy_set_storage(self.store, overwrite=False)
        assert_equal(prxy._storage, "FakeStorage")

        # Overwrite the storage
        prxy.proxy_set_storage(self.store, overwrite=True)
        assert_equal(prxy._storage, self.store)

    def test_persistent_id_consistency(self):
        app = Application([], [], [], '', jobname='App')
        prxy = Proxy(app, storage=self.store)

        # Force saving of the object
        prxy.proxy_forget()
        # ...and reload it
        prxy.jobname
        
        # When an object is saved, a new attribute `persistent_id` is
        # set
        aid = prxy._obj.persistent_id
        # This persistent_id is also stored into the Proxy instance.
        assert_equal(aid, prxy._obj_id)

    def test_proxy_and_application_saved_separately(self):
        """Test that `Proxy` and `Application` object are saved separately."""
        # When saving an `Application` and a `Proxy`, two different
        # objects will be saved.
        app = Application([], [], [], '', jobname='App')
        prxy = Proxy(app)
        pid = self.store.save(prxy)
        assert_equal(len(os.listdir(self.tmpdir)), 2)

        # We should be able to load the proxied object as well
        aid = prxy._obj.persistent_id
        app2 = self.store.load(aid)
        assert isinstance(app2, Application)
        assert not isinstance(app2, Proxy)
        assert_equal(app2.jobname, app.jobname)

    def test_storage_set_automatically_by_persistence(self):
        app = Application([], [], [], '', jobname='App')
        prxy = Proxy(app)
        self.store.save(prxy)
        assert prxy._storage is not None


## main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()
