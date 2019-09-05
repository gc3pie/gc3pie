#! /usr/bin/env python

"""
Specialized support for TURBOMOLE.
"""

# Copyright (C) 2009-2012  University of Zurich. All rights reserved.
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
__docformat__ = 'reStructuredText'


import gc3libs
import gc3libs.application
import os
import os.path
from pkg_resources import Requirement, resource_filename


class TurbomoleApplication(gc3libs.Application):

    """
    Run TURBOMOLE's `program` on the given `control` file.  Any
    additional arguments are considered additional filenames to input
    files (e.g., the ``coord`` file) and copied to the execution
    directory.

    :param str program: Name of the TURBOMOLE's program to run (e.g., ``ridft``)

    :param str control: Path to a file in TURBOMOLE's ``control`` format.

    :param others: Path(s) to additional input files.
    """

    application_name = 'turbomole'

    def __init__(self, program, control, *others, **extra_args):
        src_wrapper_sh = resource_filename(
            Requirement.parse("gc3pie"), "gc3libs/etc/turbomole.sh")

        inputs = {
            src_wrapper_sh: 'turbomole.sh',
            control: 'control',
        }
        for path in others:
            inputs[path] = os.path.basename(path)

        self.program = program

        # set defaults for keyword arguments
        extra_args.setdefault('join', True)
        extra_args.setdefault('stdout', program + '.log')
        extra_args.setdefault('output_dir', None)

        gc3libs.Application.__init__(
            self,
            arguments=["./turbomole.sh", program],
            inputs=inputs,
            outputs=gc3libs.ANY_OUTPUT,
            **extra_args
        )

    def terminated(self):
        output_filename = os.path.join(self.output_dir, self.program + '.out')
        if not os.path.exists(output_filename):
            self.execution.exitcode = 1  # FAIL
            return
        ok = self.program + " ended normally\n"
        output_file = open(output_filename, 'r')
        output_file.seek(-len(ok), os.SEEK_END)
        if ok != output_file.read():
            self.execution.exitcode = 1  # FAIL
            return
        self.execution.exitcode = 0  # SUCCESS
        return


class TurbomoleDefineApplication(gc3libs.Application):

    """
    Run TURBOMOLE's 'define' with the given `define_in` file as input,
    then run `program` on the `control` file produced.

    Any additional arguments are considered additional filenames to
    input files and copied to the execution directory.

    :param str program: Name of the TURBOMOLE's program to run
      (e.g., ``ridft``)

    :param str define_in: Path to a file containing keystrokes
      to pass as input to the 'define' program.

    :param str coord: Path to a file containing the molecule coordinates
      in TURBOMOLE's format.

    :param others: Path(s) to additional input files.
    """

    application_name = 'turbomole_define'

    def __init__(self, program, define_in, coord, *others, **extra_args):
        src_wrapper_sh = resource_filename(
            Requirement.parse("gc3pie"), "gc3libs/etc/turbomole.sh")

        inputs = {
            src_wrapper_sh: 'turbomole.sh',
            define_in: 'define.in',
            coord: 'coord',
        }
        for path in others:
            inputs[path] = os.path.basename(path)

        self.program = program

        # set defaults for keyword arguments
        extra_args.setdefault('join', True)
        extra_args.setdefault('stdout', program + '.log')
        extra_args.setdefault('output_dir', None)

        gc3libs.Application.__init__(
            self,
            arguments=["./turbomole.sh", program],
            inputs=inputs,
            outputs=gc3libs.ANY_OUTPUT,
            **extra_args
        )

    def terminated(self):
        output_filename = os.path.join(self.output_dir, self.program + '.out')
        if not os.path.exists(output_filename):
            self.execution.exitcode = 1  # FAIL
            return
        ok = self.program + " ended normally\n"
        output_file = open(output_filename, 'r')
        output_file.seek(-len(ok), os.SEEK_END)
        if ok != output_file.read():
            self.execution.exitcode = 1  # FAIL
            return
        self.execution.exitcode = 0  # SUCCESS
        return


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="square",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
