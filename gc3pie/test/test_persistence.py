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
"""Test for persistency backend(s)"""

__docformat__ = 'reStructuredText'
__version__ = '$Revision$'

from gc3libs.url import Url
from gc3libs.persistence import FilesystemStore
from gc3libs.sql_persistence import SQL
import gc3libs.exceptions as gc3ex

import os

class MyObj:
    def __init__(self, x):
        self.x = x

def _generic_persistency_test(driver, obj):
    id = driver.save(obj)
    del obj
    obj = driver.load(id)
    assert obj.x == 'GC3'

    driver.remove(id)
    try:
        obj = driver.load(id)
        assert "Object %s should NOT be found" % id
    except gc3ex.LoadError:
        pass
    except Exception, e:
        raise e

def test_file_persistency():
    path = Url('/tmp')
    fs = FilesystemStore(path.path)
    obj = MyObj('GC3')

    _generic_persistency_test(fs, obj)


def test_sql_persistency():
    path = Url('sqlite:///tmp/antani.db')
    db = SQL(path)
    obj = MyObj('GC3')

    _generic_persistency_test(db, obj)
    os.remove('/tmp/antani.db')
    
if __name__ == "__main__":
    # fix pickle error
    from test_persistence import MyObj
    test_file_persistency()
    test_sql_persistency()
