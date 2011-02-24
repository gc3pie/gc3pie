#! /usr/bin/env python
"""
Support for running a generic application with the GC3Libs.
"""
# Copyright (C) 2009-2011 GC3, University of Zurich. All rights reserved.
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
__version__ = '1.0rc1 (SVN $Revision$)'


import gc3libs.exceptions


## registry of applications
__registered_apps = { }

def get(tag, *args, **kwargs):
    """
    Return an instance of the specific application class associated
    with `tag`.  Example:

      >>> app = get('gamess')
      >>> isinstance(app, GamessApplication)
      True

    The returned object is always an instance of a sub-class of
    `Application`::

      >>> isinstance(app, Application)
      True
    """
    # FIXME: allow registration of 3rd party app classes
    try:
        return __registered_apps[tag](*args, **kwargs)
    except KeyError:
        raise gc3libs.exceptions.UnknownApplication("Application '%s' is not unknown to the gc3libs library." % tag)


def register(application_class, tag):
    """
    Register an application class with name `tag`.
    After registration, application factories can be retrieved
    by tag name with ``gc3libs.application.get('tag')``.
    """
    __registered_apps[tag] = application_class



## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
