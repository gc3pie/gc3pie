#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, 2014, 2015, University of Zurich. All rights reserved.
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


import re

from gc3libs import log
from gc3libs.backends.transport import LocalTransport


class FakeTransport(LocalTransport):

    """
    This class allows you to override responses to specific commands.

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

    FIXME: The whole logic in this class is flawed.  GC3Pie does many
    layers of quoting and wrapping commands, to remove which we would
    need a complete and correct ``sh`` parser (no, the `shlex` module
    is not enough).  We should be passing argv-style arrays to the
    `Transport.execute_command` module and check that instead; see
    https://code.google.com/p/gc3pie/issues/detail?id=285&q=argv
    """

    def __init__(self, expected_answer={}):
        LocalTransport.__init__(self)
        self.expected_answer = expected_answer

    _COMMAND_RE = re.compile(
        # match start of the line or any shell metacharacter that can
        # introduce a new command (XXX: this is quite approximate,
        # e.g. ``)`` will only be a command-introducing syntax in the
        # ``case ... esac`` body).
        r"(^|;|&|`|\(|\))"
        # optional white space
        r" \s*"
        # optional quotes, including doubly-nested quotes
        r" ('|'\\''" r'|")?'
        # the actual command (XXX: strictly speaking this is incorrect
        # as ``foo"bar"`` is a valid shell word that evaluates to
        # ``foobar`` but again there's no way we can really parse
        # ``sh`` syntax with regexps...)
        " (?P<cmd>[a-z0-9_+-]+)",
        re.VERBOSE | re.IGNORECASE)

    def execute_command(self, cmdline):
        """
        Scan the given command-line and return a predefined result if
        *any* word in command position matches one of the keys in the
        `expected_answer` argument to the class constructor.

        Note that the parsing of command-line is based on regular
        expressions and is thus only an approximation at ``sh``
        syntax.  It will *certainly* fail on some command-lines, but
        there is no way around this short of writing a complete ``sh``
        parser just for this function.  (And no, Python's module
        `shlex` will not do the job -- been there, done that.)
        """

        log.debug("scanning command-line <<<%s>>>", cmdline)

        for match in self._COMMAND_RE.finditer(cmdline):
            cmd = match.group("cmd")
            if cmd in self.expected_answer:
                return self.expected_answer[cmd]

        # if everything else failed, do run the command-line ...
        return LocalTransport.execute_command(self, cmdline)


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="faketransport",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
