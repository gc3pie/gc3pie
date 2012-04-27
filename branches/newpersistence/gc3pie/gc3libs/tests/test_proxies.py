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
from gc3libs.proxy import Proxy, MemoryPool
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
    for i in range(25):
        obj = Proxy(Task(str(i)))
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

class MyMemoryPoolKeep(MemoryPool):
    """Class used to test customization of the keep method of the
    MemoryPool class."""
    
    def keep(self, obj):
        """Keep all odd name, dump even name"""
        return int(obj.jobname) % 2 == 1


def test_custom_memorypool_keep():
    """Test customization of MemoryPool class"""
    (f, tmp) = tempfile.mkstemp()    
    store = persistence_factory("sqlite://%s" % tmp)

    mempool = MyMemoryPoolKeep(store)

    for i in range(20):
        obj = TestTask(str(i))
        mempool.add(obj)
    try:
        saved = []
        mempool.refresh()
        for i in mempool:
            if not i.proxy_saved():
                saved.append(i)
                assert int(i.jobname) % 2 == 1
        assert len(saved) == 10


        # Now let's try to set maxobjects to 5. After calling
        # refresh() we should see only 5 object saved.
        mempool.maxobjects = 5
        mempool.refresh()
        objects = [i for i in mempool if not i.proxy_saved()]
        assert len(objects) == 5

    finally:
        os.remove(tmp)

class MyMemoryPoolCmp(MemoryPool):
    """This class will sort objects by name: evens before odds, from
    lesser to greater
    """
    
    def cmp(self, x, y):
        if hasattr(x, 'jobname'):
            x = x.jobname
        if hasattr(y, 'jobname'):
            y = y.jobname
        x,y = int(x), int(y)
        # Check if one of them is even and the other is odd
        if x%2 != y %2:
            if x%2 == 0: return -1
            else:        return 1
        return cmp(x,y)
        
def test_autotest_memoypoolcmp():
    # I need to test this method as well, so that I'll make no
    # mistakes :)

    # The idea is that even numbers will be lesser than odd
    # numbers. So:

    m = MyMemoryPoolCmp(persistence_factory('file:///tmp'))
    assert m.cmp(1,2) == 1
    assert m.cmp(2,1) == -1
    assert m.cmp(2,2) == 0
    assert m.cmp(2,4) == -1
    assert m.cmp(4,2) == 1
    assert m.cmp(1,3) == -1
    
def test_custom_memorypool_cmp():
    """Test customization of MemoryPool class"""
    (f, tmp) = tempfile.mkstemp()    
    store = persistence_factory("sqlite://%s" % tmp)

    mempool = MyMemoryPoolCmp(store, maxobjects=10)

    for i in range(20):
        obj = TestTask(str(i))
        mempool.add(obj)
    try:
        saved = []
        mempool.refresh()
        for i in mempool:
            if not i.proxy_saved():
                saved.append(i)
        assert len(saved) == 10


        # Now let's try to set maxobjects to 5. After calling
        # refresh() we should see only 5 object saved.
        mempool.maxobjects = 5
        mempool.refresh()
        objects = [i for i in mempool if not i.proxy_saved()]
        assert len(objects) == 5

    finally:
        os.remove(tmp)

    

if "__main__" == __name__:
    import logging
    loglevel = logging.INFO
    configure_logger(loglevel, 'test_proxies')

    test_functions = [i for i in sorted(locals()) if i.startswith('test_')]
    for i in test_functions:
        try:
            func = "%s()" % i
            print "Testing %s" % func,
            eval(func)
            print "OK"
        except Exception, e:
            print "ERROR"
            print "Error in function %s" % func
            print "Exception %s: %s" % (repr(e), e)
            print
