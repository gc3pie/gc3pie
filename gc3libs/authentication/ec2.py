#! /usr/bin/env python

"""
"""

# Copyright (C) 2012  University of Zurich. All rights reserved.
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


class EC2Auth(object):

    def __init__(self, **auth):
        self.__dict__.update(auth)
        try:
            # test validity
            assert auth['type'] == 'ec2', \
                "Configuration error. Unknown type; %s. Valid type: ec2" \
                % auth.type
        except AssertionError as x:
            raise gc3libs.exceptions.ConfigurationError(
                'Erroneous configuration parameter: %s' % str(x))

        try:
            if 'ec2_access_key' not in auth:
                auth['ec2_access_key'] = os.getenv('EC2_ACCESS_KEY')
                assert auth['ec2_access_key'], \
                    "Configuration error. Missing mandatory " \
                    "`ec2_access_key` key"
            if 'ec2_secret_key' not in auth:
                auth['ec2_secret_key'] = os.getenv('EC2_SECRET_KEY')
                assert auth['ec2_secret_key'], \
                    "Configuration error. Missing mandatory " \
                    "`ec2_secret_key` key"

        except AssertionError as x:
            raise gc3libs.exceptions.ConfigurationError(
                'Erroneous configuration parameter: %s' % str(x))

        # Strip quotes from ec2_*_key in case someone put it in the
        # configuration file
        auth['ec2_secret_key'] = auth['ec2_secret_key'].strip('"').strip("'")
        auth['ec2_access_key'] = auth['ec2_access_key'].strip('"').strip("'")

    def check(self):
        return True

    def enable(self):
        return True


Auth.register('ec2', EC2Auth)

# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="ec2",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
