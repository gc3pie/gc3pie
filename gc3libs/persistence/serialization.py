#! /usr/bin/env python
#
"""
Generic object serialization (using Python's *pickle*/*cPickle* modules).

See the documentation for Python's standard `*pickle* and *cPickle*
modules`__ for more details.

.. __: http://docs.python.org/library/pickle.html

"""
# Copyright (C) 2011-2012, 2018  University of Zurich. All rights reserved.
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
__docformat__ = 'reStructuredText'


import cPickle as pickle

from gc3libs.persistence.store import Persistable


DEFAULT_PROTOCOL = pickle.HIGHEST_PROTOCOL


def make_pickler(driver, stream, root, protocol=pickle.HIGHEST_PROTOCOL):
    p = pickle.Pickler(stream, protocol=protocol)
    p.persistent_id = _PersistentIdToSave(driver, root)
    return p


def make_unpickler(driver, stream):
    p = pickle.Unpickler(stream)
    p.persistent_load = _PersistentLoadExternalId(driver)
    return p


class _PersistentIdToSave(object):

    """Used internally to provide `persistent_id` support to *cPickle*.

    This class is needed because:

    * we want to save each `Persistable`:class: object as a separate record,
    * we want to use the *cPickle* module for performance reasons.

    Check the `documentation of Python's *pickle* module`__ for
    details on the differences between *pickle* and *cPickle* modules.

    .. __: http://goo.gl/CCknrT

    """

    def __init__(self, driver, root):
        self._root = root
        self._driver = driver

    def __call__(self, obj):
        if obj is self._root:
            return None
        elif isinstance(obj, Persistable):
            if (not hasattr(obj, 'persistent_id')
                or getattr(obj, 'changed', True)):
                self._driver.save(obj)
            return obj.persistent_id


class _PersistentLoadExternalId(object):

    """Used internally to provide `persistent_id` support to *cPickle*.

    This class is needed because:

    * we want to save each `Persistable`:class: object as a separate record,
    * we want to use the *cPickle* module for performance reasons.

    Check the `documentation of Python's *pickle* module`__ for
    details on the differences between *pickle* and *cPickle* modules.

    .. __: http://goo.gl/CCknrT

    """

    def __init__(self, driver):
        self._driver = driver

    def __call__(self, id_):
        return self._driver.load(id_)
