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
        


## main: run tests

if "__main__" == __name__:
    import nose
    nose.runmodule()
