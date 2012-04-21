#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, GC3, University of Zurich. All rights reserved.
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

from gc3libs import Task, Application, configure_logger
from gc3libs.Proxy import Proxy, MemoryPool
from gc3libs.persistence import persistence_factory
import tempfile, os

ma = {'executable': '/bin/true',
      'arguments': [],
      'inputs': [],
      'outputs': [],
      'output_dir': '/tmp',
      'requested_cores': 1,          
      }

class TestTask(Proxy):
    def __init__(self, name, grid=None, storage=None, manager=None, **kw):
        obj = Task(name, grid=grid, **kw)
        Proxy.__init__(self, obj, storage=storage, manager=manager)

def test_proxy_no_storage():
    t = TestTask('NoTask')
    assert isinstance(t, Task)


def test_proxy_storage():
    from gc3libs.persistence import persistence_factory
    import tempfile, os
    (f, tmp) = tempfile.mkstemp()
    
    fb = persistence_factory("sqlite://%s" % tmp)
    try:
        t = TestTask('NoTask', storage=fb)
    
        assert t.jobname == 'NoTask'
    
        t.proxy_forget()
        assert object.__getattribute__(t, "_obj") is None
        assert t.jobname == 'NoTask'
        assert isinstance(object.__getattribute__(t, "_obj") , Task)
    finally:
        os.remove(tmp)


def test_proxy_storage_wrong_storage():
    from gc3libs.persistence import persistence_factory
    
    fb = persistence_factory("file:///path/to/non/existing/storage")
    t = TestTask('NoTask', storage=fb)
    assert t.jobname == 'NoTask'
    
    t.proxy_forget()
    assert object.__getattribute__(t, "_obj") is not None
    assert t.jobname == 'NoTask'
    assert isinstance(object.__getattribute__(t, "_obj") , Task)


class MyClass:
    @staticmethod
    def get_me_wrong():
        return "wrong"

class MyPClass(Proxy):
    def __init__(self, x, **kw):
        obj = MyClass()
        Proxy.__init__(self, obj, **kw)

def test_staticmethod():
    t = Proxy(MyClass())
    assert t.get_me_wrong() == "wrong"
    # t = MyPClass() questo da' un errore!!!
    # assert t.get_me_wrong() == "wrong"

def test_application():
    app = Proxy(Application('bash', [], [], [], '/tmp'))
    app.proxy_forget()

def test_memory_pool():
    """Test MemoryPool class basic behavior"""
    (f, tmp) = tempfile.mkstemp()    
    store = persistence_factory("sqlite://%s" % tmp)
    nobjects = 10
    mempool = MemoryPool(store, maxobjects=nobjects)

    objects = []
    for i in range(40):
        obj = TestTask(str(i))
        assert obj.jobname == str(i) # let's call a getattr, to be
                                     # sure it will be properly
                                     # ordered
        objects.append(obj)
    try:
        mempool.extend(objects)
        mempool.refresh()
        # First all-maxobjs should NOT be there
        for i in objects[:len(objects)-nobjects]:
            assert object.__getattribute__(i, '_obj') is None
        # All remaining maxobjs should be there
        for i in objects[nobjects:]:
            #assert object.__getattribute__(i, '_obj') is not None
            i is None

    finally:
        os.remove(tmp)

if "__main__" == __name__:
    import logging
    loglevel = logging.INFO
    configure_logger(loglevel, 'test_proxies')

    test_proxy_no_storage()
    test_proxy_storage()
    test_proxy_storage_wrong_storage()
    test_staticmethod()
    test_memory_pool()
