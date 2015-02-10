#! /usr/bin/env python
#
"""
Authentication support for accessing resources through the SSH protocol.
"""
# Copyright (C) 2009-2011, 2015 S3IT, Zentrale Informatik, University of Zurich. All rights reserved.
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
__version__ = 'development version (SVN $Revision$)'

import gc3libs
from gc3libs.authentication import Auth
import gc3libs.exceptions


class SshAuth(object):

    def __init__(self, **auth):

        assert auth['type'] == 'ssh'

        try:
            auth['username']
        except KeyError as err:
            raise gc3libs.exceptions.ConfigurationError(
                "Missing `username` in SSH auth section.")

        try:
            if 'port' in auth:
                auth['port'] = int(auth['port'])
            else:
                auth['port'] = gc3libs.Default.SSH_PORT
        except (ValueError, TypeError) as err:
            raise gc3libs.exceptions.ConfigurationError(
                "Invalid `port` setting in SSH auth section.")

        # everything else is just stored as-is
        self.__dict__.update(auth)

    def check(self):
        return True

    def enable(self):
        return True

Auth.register('ssh', SshAuth)


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="ssh",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
