#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011  University of Zurich. All rights reserved.
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
from __future__ import absolute_import, print_function, unicode_literals
from builtins import range
from builtins import object
__docformat__ = 'reStructuredText'

import pytest

from gc3libs.persistence.idfactory import IdFactory


class DummyObject(object):
    pass


def test_new_item():
    idfactory = IdFactory()
    ids = []
    dummy = DummyObject()
    for i in range(10):
        ids.append(idfactory.new(dummy))
    assert len(ids) == len(set(ids))

    # reserve is tested only in order to check if we get an error calling
    # it...
    idfactory.reserve(5)

@pytest.mark.skip("Code currently bugged, cfr. issue #608")
def test_custom_next_id():
    class next_id(object):

        def __init__(self):
            self.curid = -1

        def __call__(self):
            self.curid += 1
            return self.curid

    idfactory = IdFactory(next_id_fn=next_id())

    ids = []
    dummy = DummyObject()
    for i in range(10):
        ids.append(idfactory.new(dummy))

    assert len(ids) == len(set(ids))

    for i in range(len(ids)):
        assert ids[i] == "DummyObject.%d" % i


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="test_idfactory",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
