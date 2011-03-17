#! /usr/bin/env python
#
"""
Authentication support for accessing resources through the SSH protocol.
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
__version__ = '1.0rc3 (SVN $Revision$)'

import gc3libs
from gc3libs.authentication import Auth
import gc3libs.exceptions

class SshAuth(object):
    def __init__(self, **auth):
        
        try:
            # test validity
            assert auth['type'] == 'ssh',\
                "Configuration error. Unknown type: %s. Valid type: ssh" \
                % auth.type
            auth['username']
            self.__dict__.update(auth)
        except AssertionError, x:
            raise gc3libs.exceptions.ConfigurationError('Erroneous configuration parameter: %s' % str(x))

    def check(self):
        gc3libs.log.debug('Checking auth: ssh')
        return True

    def enable(self):
        return True

Auth.register('ssh', SshAuth)


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="ssh",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
