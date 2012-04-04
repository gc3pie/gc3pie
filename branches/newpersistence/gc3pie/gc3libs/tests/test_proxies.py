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

from gc3libs import Task, configure_logger
from gc3libs.Proxy import Proxy


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
    t = TestTask('NoTask', storage=fb)
    assert t.jobname == 'NoTask'
    
    t.proxy_forget()
    assert object.__getattribute__(t, "_obj") is None
    assert t.jobname == 'NoTask'
    assert isinstance(object.__getattribute__(t, "_obj") , Task)
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

    
## main: run tests

if "__main__" == __name__:
    import logging
    loglevel = logging.INFO
    configure_logger(loglevel, 'test_proxies')

    test_proxy_no_storage()
    test_proxy_storage()
    test_proxy_storage_wrong_storage()
