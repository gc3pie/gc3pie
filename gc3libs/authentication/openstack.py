#! /usr/bin/env python

"""
"""

# Copyright (C) 2012-2014  University of Zurich. All rights reserved.
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

from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
from builtins import object
__docformat__ = 'reStructuredText'

import os

import gc3libs
from gc3libs.authentication import Auth
import gc3libs.exceptions


class OpenStackAuth(object):

    def __init__(self, **auth):
        try:
            # test validity
            assert auth['type'] == 'openstack', \
                "Configuration error. Unknown type; %s. " \
                "Valid type: openstack" \
                % auth.type
        except AssertionError as x:
            raise gc3libs.exceptions.ConfigurationError(
                'Erroneous configuration parameter: %s' % str(x))

        try:
            for (key, var) in (
                    # list of mandatory env vars taken from:
                    # http://docs.openstack.org/user-guide/common/cli_set_environment_variables_using_openstack_rc.html
                    ('os_auth_url',     'OS_AUTH_URL'),
                    ('os_password',     'OS_PASSWORD'),
                    ('os_project_name', 'OS_TENANT_NAME'),
                    ('os_username',     'OS_USERNAME'),
            ):
                if key not in auth:
                    auth[key] = os.getenv(var)
                    assert auth[key], (
                        "Missing mandatory configuration parameter for {name} auth:"
                        " either define the `{key}` configuration key,"
                        " or set the `{var}` environmental variable."
                        .format(name=("'" + auth['name'] + "'"), key=key, var=var,))
                    # Strip quotes from os_* in case someone put
                    # it in the configuration file
                    auth[key] = auth[key].strip('"').strip("'")
        except AssertionError as x:
            raise gc3libs.exceptions.ConfigurationError(str(x))

        self.__dict__.update(auth)

    def check(self):
        return True

    def enable(self):
        return True


Auth.register('openstack', OpenStackAuth)

# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="openstack",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
