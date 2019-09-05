#! /usr/bin/env python
#
"""
Unit tests for `_Machine`:class: and derived classes
(from module `gc3libs.backends.shellcmd`).
"""
# Copyright (C) 2011-2015, 2019  University of Zurich. All rights reserved.
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
__docformat__ = 'reStructuredText'


import os

from mock import Mock
import pytest

from gc3libs.backends.shellcmd import _LinuxMachine


@pytest.fixture(autouse=True)
def transport():
    transport = Mock()
    yield transport


def test_list_process_tree_empty(transport):
    mach = _LinuxMachine(transport)

    transport.execute_command.return_value = (
        # exit code
        0,
        # stdout
        '',
        # stderr
        '',
    )

    pids = mach.list_process_tree()
    assert pids == []


def test_list_process_tree_full(transport):
    r"""
    Check that the output relative to this process tree is correct
    (process tree as displayed by `ps f`)::

         PID  PPID CMD
        2361  1466 /sbin/upstart --user
        2431  2361  \_ upstart-udev-bridge --daemon --user
        2663  2361  \_ gpg-agent --homedir /home/rmurri/.gnupg --use-standard-socket --daemon
        2679  2361  \_ /usr/lib/at-spi2-core/at-spi-bus-launcher
        2684  2679  |   \_ /usr/bin/dbus-daemon --config-file=/etc/at-spi2/accessibility.conf --nofork --print-address 3
        2725  2361  \_ /usr/bin/lxsession -s Lubuntu -e LXDE
        2736  2725  |   \_ lxpanel --profile Lubuntu
    """
    mach = _LinuxMachine(transport)

    transport.execute_command.return_value = (
        # exit code
        0,
        # stdout
        '''\
2361  1466
2431  2361
2663  2361
2679  2361
2684  2679
2725  2361
2736  2725
        ''',
        # stderr
        '',
    )

    pids = mach.list_process_tree("2361")
    assert pids == [
              # PID   PPID CMD
        "2361", # 2361  1466 /sbin/upstart --user
        "2431", # 2431  2361  \_ upstart-udev-bridge --daemon --user
        "2663", # 2663  2361  \_ gpg-agent --homedir /home/rmurri/.gnupg --use-standard-socket --daemon
        "2679", # 2679  2361  \_ /usr/lib/at-spi2-core/at-spi-bus-launcher
        "2725", # 2684  2679  |   \_ /usr/bin/dbus-daemon --config-file=/etc/at-spi2/accessibility.conf --nofork --print-address 3
        "2684", # 2725  2361  \_ /usr/bin/lxsession -s Lubuntu -e LXDE
        "2736", # 2736  2725  |   \_ lxpanel --profile Lubuntu
    ]


if __name__ == "__main__":
    pytest.main(["-v", __file__])
