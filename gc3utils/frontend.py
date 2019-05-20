#! /usr/bin/env python
#
"""
This is the main entry point for command `gc3utils`:command: -- a
simple command-line frontend to distributed resources

This is a generic front-end code; actual implementation of commands
can be found in `gc3utils.commands`:mod:
"""
# Copyright (C) 2009-2012, 2019  University of Zurich. All rights reserved.
#
# Includes parts adapted from the ``bzr`` code, which is
# copyright (C) 2005, 2006, 2007, 2008, 2009 Canonical Ltd
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


# stdlib imports
from __future__ import absolute_import, print_function, unicode_literals

import os
import os.path
import sys


def main():
    """
    Generic front-end function to invoke the commands in `gc3utils/commands.py`
    """

    # program name
    PROG = os.path.basename(sys.argv[0])

    # use docstrings for providing help to users,
    # so complain if they were removed by excessive optimization
    if __doc__ is None:
        sys.stderr.write(
            "%s does not support python -OO; aborting now.\n" % PROG)
        sys.exit(2)

    # ensure we run on a supported Python version
    NEED_VERS = (2, 4)
    try:
        version_info = sys.version_info
    except AttributeError:
        version_info = 1, 5  # 1.5 or older
    REINVOKE = "__GC3UTILS_REINVOKE"
    KNOWN_PYTHONS = ('python2.6', 'python2.5', 'python2.4')
    if version_info < NEED_VERS:
        if REINVOKE not in os.environ:
            # mutating os.environ doesn't work in old Pythons
            os.putenv(REINVOKE, "1")
            for python in KNOWN_PYTHONS:
                try:
                    os.execvp(python, [python] + sys.argv)
                except OSError:
                    pass
        sys.stderr.write("%s: error: cannot find a suitable python interpreter"
                         " (need %d.%d or later)" % (PROG, NEED_VERS))
        return 1
    if hasattr(os, "unsetenv"):
        os.unsetenv(REINVOKE)

    # ensure locale is set to "" (C, POSIX); otherwise parsing messages from
    # commands we invoke might fail because they speak, e.g., German.
    #
    if sys.platform == 'darwin':
        # jameinel says this hack is to force python to honor the LANG setting,
        # even on Darwin.  Otherwise it is apparently hardcoded to Mac-Roman,
        # which is incorrect for the normal Terminal.app which wants UTF-8.
        #
        # "It might be that I should be setting the "system locale"
        # somewhere else on the system, rather than setting
        # LANG=en_US.UTF-8 in .bashrc.  Switching to 'posix' and
        # setting LANG worked for me."
        #
        # So we can remove this if someone works out the right way to tell Mac
        # Python which encoding to use.  -- mbp 20080703
        sys.platform = 'posix'
        try:
            import locale
        finally:
            sys.platform = 'darwin'
    else:
        import locale

    try:
        locale.setlocale(locale.LC_ALL, '')
    except locale.Error as e:
        sys.stderr.write(
            '%s: WARNING: %s\n'
            '  could not set the application locale.\n'
            '  This might cause problems with some commands.\n'
            '  To investigate the issue, look at the output\n'
            '  of the locale(1p) tool available on POSIX systems.\n'
            % (PROG, e))

    if PROG == 'gc3utils':
        # the real command name is the first non-option argument
        for arg in sys.argv[1:]:
            if not arg.startswith('-'):
                if arg.startswith('g'):
                    PROG = arg
                else:
                    PROG = 'g' + arg
                del sys.argv[0]  # so that PROG == sys.argv[0]
                break

        # no command name found, print usage text and exit
        if PROG == 'gc3utils':
            sys.stderr.write("""Usage: gc3utils COMMAND [options]

Command `gc3utils` is a unified front-end to computing resources.
You can get more help on a specific sub-command by typing::

  gc3utils COMMAND --help

where command is one of these:
""")
            import gc3utils.commands
            for cmd in [sym[5:] for sym in dir(gc3utils.commands)
                        if sym.startswith("cmd_")]:
                sys.stderr.write('  ' + cmd + '\n')
            return 1

    # find command as function in the `commands.py` module
    PROG.replace('-', '_')
    import gc3utils.commands
    try:
        cmd = getattr(gc3utils.commands, 'cmd_' + PROG)
    except AttributeError:
        sys.stderr.write(
            "Cannot find command '%s' in gc3utils; aborting now.\n" % PROG)
        return 1
    rc = cmd().run()  # (*sys.argv[1:])
    return rc
