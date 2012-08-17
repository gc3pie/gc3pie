#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, GC3, University of Zurich. All rights reserved.
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

from gc3libs.backends.transport import LocalTransport

class FakeTransport(LocalTransport):
    """This class allows you to override responses to specific commands.

    The `execute_command` of `FakeTransport` instead of executing a
    command will look into the `self.expected_answer` dictionary and
    find a proper triple `(retcode, output, error)`. If not found, the
    command will be executed by the parent (`LocalTransport`)

    To use this class you need to fill the `self.expected_answer` with
    commands (only the *binary*, not the whole command line) as keys,
    and triples (retcode, output, error) as values. For example:

    >>> t = FakeTransport()
    >>> t.expected_answer['echo'] = (0, 'GC3pie is wonderful', '')
    >>> t.execute_command("echo 'GC3pie is wonderful'")
    (0, 'GC3pie is wonderful', '')
    
    """
    def __init__(self, expected_answer={}):
        LocalTransport.__init__(self)        
        self.expected_answer = expected_answer
    
    def execute_command(self, command):
        """parse the command and return fake output and error codes
        depending on the current suppose status of the job"""
        commands = []
        
        # There isn't an easy way to split a complex pipeline like
        # "cmd1 ; cmd2 && cmd3 || cmd4"
        
        # We do split the command line, however, but it will work only
        # in a simple case. Therefore you have to remember that
        # complex command lines will probably not be handled correctly
        # by this method.
        
        _ = command.split(';')
        for i in _:
            commands.extend(i.strip().split('&&'))
        
        for cmd in commands:
            args = cmd.strip().split()
            if args[0] in self.expected_answer:
                return self.expected_answer[args[0]]
            else:
                continue
        return LocalTransport.execute_command(self, command)


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="faketransport",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
