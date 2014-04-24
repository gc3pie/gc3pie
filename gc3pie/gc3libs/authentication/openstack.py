#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012-2014, GC3, University of Zurich. All rights reserved.
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

import os

import gc3libs
from gc3libs.authentication import Auth
import gc3libs.exceptions


class OpenStackAuth(object):
    def __init__(self, **auth):
        try:
            # test validity
            assert auth['type'] == 'openstack', \
                "Configuration error. Unknown type; %s. Valid type: openstack" \
                % auth.type
        except AssertionError, x:
            raise gc3libs.exceptions.ConfigurationError(
                'Erroneous configuration parameter: %s' % str(x))

        try:
            for (key, var) in (
                    ('os_username', 'OS_USERNAME'),
                    ('os_password', 'OS_PASSWORD'),
                    ('os_project_name', 'OS_TENANT_NAME'),
                    ):
                if key not in auth:
                    auth[key] = os.getenv(var)
                    assert auth[key], \
                        "Configuration error. Missing mandatory "\
                        "`%s` key" % key
                    # Strip quotes from os_* in case someone put
                    # it in the configuration file
                    auth[key] = auth[key].strip('"').strip("'")
        except AssertionError, x:
            raise gc3libs.exceptions.ConfigurationError(
                'Erroneous configuration parameter: %s' % str(x))

        self.__dict__.update(auth)


    def check(self):
        gc3libs.log.debug('Checking auth: OpenStackAuth')
        return True

    def enable(self):
        return True


Auth.register('openstack', OpenStackAuth)

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="openstack",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
