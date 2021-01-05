#! /usr/bin/env python

"""
Authentication support for accessing resources through the SSH protocol.
"""

# Copyright (C) 2009-2011, 2015, 2019  University of Zurich. All rights reserved.
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
from builtins import object
__docformat__ = 'reStructuredText'


import io

import paramiko

import gc3libs
import gc3libs.defaults
from gc3libs.authentication import Auth
import gc3libs.exceptions


class SshAuth(object):

    def __init__(self,
                 # inherited from the generic `Auth` class
                 type,
                 # SSH-specific arguments
                 username,
                 keyfile=None,
                 pkey=None,
                 port=None,
                 ssh_config=None,
                 timeout=None,
                 # extra arguments, if any
                 **extra):

        assert type == 'ssh'

        # this is required
        self.username = username

        # this can be set to None if no value is provided
        # (no sensible default)
        self.keyfile = keyfile

        self.pkey = pkey
        if ssh_config is not None:
            self.ssh_config = ssh_config
        else:
            self.ssh_config = gc3libs.defaults.SSH_CONFIG_FILE

        # these need type conversion; if no value is supplied, use
        # `None` as doing otherwise would override settings from the
        # SSH config file in the `SshTransport` constructor.
        try:
            self.port = None if port is None else int(port)
        except (ValueError, TypeError) as err:
            raise gc3libs.exceptions.ConfigurationError(
                "Invalid `port` setting in SSH auth section.")
        try:
            self.timeout = None if timeout is None else float(timeout)
        except (ValueError, TypeError) as err:
            raise gc3libs.exceptions.ConfigurationError(
                "Invalid `timeout` setting in SSH auth section.")

        # everything else is just stored as-is
        self.__dict__.update(extra)

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
