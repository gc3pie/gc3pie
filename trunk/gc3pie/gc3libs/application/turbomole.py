#! /usr/bin/env python
#
"""
Specialized support for TurboMol.
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
__version__ = 'development version (SVN $Revision$)'


import gc3libs
import gc3libs.application
from gc3libs.exceptions import *
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
    def __init__(self, program, control, *others, **kw):
        src_wrapper_sh = resource_filename(
            Requirement.parse("gc3pie"), "gc3libs/etc/turbomole.sh")

        inputs = {
            src_wrapper_sh:'turbomole.sh',
            control:'control',
            }
        for path in others:
            inputs[path] = os.path.basename(path)

        # set defaults for keyword arguments
        kw.setdefault('join', True)
        kw.setdefault('stdout', program + '.log')
        kw.setdefault('output_dir', None)
        
        # hard-coded list, according to Andreas' description
        outputs = [
            'control',
            'coord',
            'mos',
            'energy',
            'basis',
            'auxbasis',
            ]

        gc3libs.Application.__init__(
            self,
            executable = "./turbomole.sh",
            arguments = [ program ],
            inputs = inputs,
            outputs = outputs,
            **kw
            )


class TurbomoleDefineApplication(gc3libs.Application):
    """
    Run TURBOMOLE's 'define' with the given `define_in` file as input,
    then run `program` on the `control` file produced.

    Any additional arguments are considered additional filenames to
    input files and copied to the execution directory.

    :param str program: Name of the TURBOMOLE's program to run (e.g., ``ridft``)

    :param str define_in: Path to a file containing keystrokes to pass
    as input to the 'define' program.

    :param str coord: Path to a file containing the molecule
    coordinates in TURBOMOLE's format.

    :param others: Path(s) to additional input files.
    """
    def __init__(self, program, define_in, coord, *others, **kw):
        src_wrapper_sh = resource_filename(
            Requirement.parse("gc3pie"), "gc3libs/etc/turbomole.sh")

        inputs = {
            src_wrapper_sh:'turbomole.sh',
            define_in:'define.in',
            coord:'coord',
            }
        for path in others:
            inputs[path] = os.path.basename(path)

        # hard-coded list, according to Andreas' description
        outputs = [
            'control',
            'coord',
            'mos',
            'energy',
            'basis',
            'auxbasis',
            ]

        # set defaults for keyword arguments
        kw.setdefault('join', True)
        kw.setdefault('stdout', program + '.log')
        kw.setdefault('output_dir', None)
        
        gc3libs.Application.__init__(
            self,
            executable = "./turbomole.sh",
            arguments = [ program ],
            inputs = inputs,
            outputs = outputs,
            **kw
            )



gc3libs.application.register(TurbomoleApplication, 'turbomole')
gc3libs.application.register(TurbomoleDefineApplication, 'turbomole_define')


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="square",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
