#! /usr/bin/env python
"""
Authentication support for the GC3Libs.
"""
# Copyright (C) 2009-2015  University of Zurich. All rights reserved.
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
from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
from builtins import object
__docformat__ = 'reStructuredText'

import gc3libs.exceptions


class Auth(object):

    """
    A mish-mash of authorization functions.

    This class actually serves the purposes of:

    - a registry of authorization 'types', mapping internally-assigned
      names to Python classes;
    - storage for the configuration information (which can be
      arbitrary, but should probably be read off a configuration
      file);
    - a factory, returning a 'SomeAuth' object through which clients
      can deal with actual authorization issues (like checking if the
      authorization credentials are valid and getting/renewing them).
    - a cache, that tries to avoid expensive re-initializations of
      `Auth` objects by allowing only one live instance per type, and
      returning it when requested.

    .. admonition:: FIXME

      There are several problems with this approach:

      - the configuration is assumed *static* and cannot be changed after
        the `Auth` instance is constructed.
      - there is no communication between the client class and the
        `Auth` classes.
      - there is no control over the lifetime of the cache; at a
        minimum, it should be settable per-auth-type.
      - I'm unsure whether the mapping of 'type names' (as in the
        `type=...` keyword in the config file) to Python classes
        belongs in a generic factory method or in the configuration
        file reader.  (Probably the former, so the code here would
        actually be right.)
      - The whole `auto_enable` stuff really belongs to the user-interface
        part, which is also hard-coded in the auth classes, and should not be.
    """
    types = {}

    def __init__(self, config, auto_enable):
        self.auto_enable = auto_enable
        self.__auths = {}
        self._config = config
        self._ctors = {}
        for auth_name, auth_params in self._config.items():
            self._ctors[auth_name] = Auth.types[auth_params['type']]

    def add_params(self, **params):
        """
        Add the specified keyword arguments as initialization
        parameters to all the configured auth classes.

        Parameters that have already been specified are silently
        overwritten.
        """
        for auth_name, auth_params in self._config.items():
            auth_params.update(params)

    def get(self, auth_name, **kwargs):
        """
        Return an instance of the `Auth` class corresponding to the
        given `auth_name`, or raise an exception if instanciating the
        same class has given an unrecoverable exception in past calls.

        Additional keyword arguments are passed unchanged to the class
        constructor and can override values specified at configuration time.

        Instances are remembered for the lifetime of the program; if
        an instance of the given class is already present in the
        cache, that one is returned; otherwise, an instance is
        contructed with the given parameters.

        .. caution::

          The `params` keyword arguments are only used if a new
          instance is constructed and are silently ignored if the
          cached instance is returned.

        """
        if auth_name not in self.__auths:
            try:
                params = self._config[auth_name].copy()
                params.update(kwargs)
                a = self._ctors[auth_name](**dict(params))
            except KeyError as err:
                a = gc3libs.exceptions.ConfigurationError(
                    "Unknown auth section %s" % (str(err),))
            except (AssertionError, AttributeError) as ex:
                a = gc3libs.exceptions.ConfigurationError(
                    "Missing required configuration parameters"
                    " in auth section '%s': %s" % (auth_name, str(ex)))
        else:
            a = self.__auths[auth_name]

        if isinstance(a, Exception):
            if isinstance(a, gc3libs.exceptions.UnrecoverableError):
                self.__auths[auth_name] = a
            raise a

        if not a.check():
            if self.auto_enable:
                try:
                    a.enable()
                except gc3libs.exceptions.RecoverableAuthError as x:
                    raise
                except gc3libs.exceptions.UnrecoverableAuthError as x:
                    gc3libs.log.debug(
                        "Got exception while enabling auth '%s',"
                        " will remember for next invocations:"
                        " %s: %s" % (auth_name, x.__class__.__name__, x))
                    a = x
            else:
                a = gc3libs.exceptions.UnrecoverableAuthError(
                    "No valid credentials of type '%s'"
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
            assert auth['type'] == 'none', (
                "Configuration error. Unknown type: %s. Valid type: none"
                % auth.type)
        except AssertionError as x:
            raise gc3libs.exceptions.ConfigurationError(
                'Erroneous configuration parameter: %s' % str(x))

    def is_valid(self):
        return True

    def check(self):
        return True

    def enable(self):
        return True

Auth.register('none', NoneAuth)
# register additional auth types
# FIXME: it would be nice to have some kind of auto-discovery instead
import gc3libs.authentication.ssh
import gc3libs.authentication.ec2
import gc3libs.authentication.openstack

# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
