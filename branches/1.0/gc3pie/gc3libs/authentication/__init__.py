#! /usr/bin/env python
"""
Authentication support for the GC3Libs.
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
__version__ = '1.0rc4 (SVN $Revision$)'

import sys

import gc3libs.exceptions

class Auth(object):
    types = {}
    def __init__(self, _auth_dict, auto_enable):
        self.auto_enable = auto_enable
        self.__auths = { }
        self._auth_dict = _auth_dict
        self._auth_type = { }
        for auth_name, auth_params in self._auth_dict.items():
            self._auth_type[auth_name] = Auth.types[auth_params['type']]

    def get(self, auth_name):
        if not self.__auths.has_key(auth_name):
            try:
                a =  self._auth_type[auth_name](** self._auth_dict[auth_name])
            except (AssertionError, AttributeError), ex:
                a = gc3libs.exceptions.ConfigurationError("Missing required configuration parameters"
                                       " in auth section '%s': %s" % (auth_name, str(ex)))
        else:
            a = self.__auths[auth_name]


        if isinstance(a, Exception):
            self.__auths[auth_name] = a
            raise a

        if not a.check():
            if self.auto_enable:
                    try:
                        a.enable()
                    except gc3libs.exceptions.RecoverableAuthError, x:
                        raise
                    except gc3libs.exceptions.UnrecoverableAuthError, x:
                        gc3libs.log.debug("Got exception while enabling auth '%s',"
                                          " will remember for next invocations:"
                                          " %s: %s" % (auth_name, x.__class__.__name__, x))
                        a = x
            else:
                a = gc3libs.exceptions.UnrecoverableAuthError("No valid credentials of type '%s'"
                                                " and `auto_enable` not set." % auth_name)

        self.__auths[auth_name] = a
        return a

    @staticmethod
    def register(auth_type, ctor):
        Auth.types[auth_type] = ctor


class NoneAuth(object):
    """Auth proxy to use when no auth is needed."""
    def __init__(self, **auth):
        try:
            # test validity
            assert auth['type'] == 'none',\
                "Configuration error. Unknown type: %s. Valid type: none" \
                % auth.type
            self.__dict__.update(auth)
        except AssertionError, x:
            raise gc3libs.exceptions.ConfigurationError('Erroneous configuration parameter: %s' % str(x))

    def is_valid(self):
        return True
    
    def check(self):
        gc3libs.log.debug('Checking auth: none')
        return True

    def enable(self):
        return True

Auth.register('none', NoneAuth)
# register additional auth types
# FIXME: it would be nice to have some kind of auto-discovery instead
import gc3libs.authentication.grid
import gc3libs.authentication.ssh

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
