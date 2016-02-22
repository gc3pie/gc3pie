#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012-2013, GC3, University of Zurich. All rights reserved.
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



import gc3libs
from gc3libs.cmdline import SessionBasedScript
from gc3libs.quantity import MiB, GiB
import os

class GBeastApp(gc3libs.Application):
    def __init__(self, beast, jarfile, dirname, fname, ncores, **extra):
        infile = os.path.join(dirname, fname)

        extra['output_dir'] = fname[:-4] + '.d'
        extra['requested_cores'] = ncores
        extra['requested_memory'] = ncores*4*GiB

        args = ['java',
                '-Xmx%dm' % extra['requested_memory'].amount(MiB),
                '-jar', jarfile,
                '-threads', ncores,
                '-beagle_instances', ncores,
                fname]

        gc3libs.Application.__init__(
            self,
            arguments = args,
            inputs = [infile],
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'stdout.txt',
            stderr = 'stderr.txt',
            **extra)

class GBeastScript(SessionBasedScript):
    """Script to parallelize execution of BEAST over multiple input files. Takes a directory containing XML files, parse
    """
    version = '1.0'
    def setup_options(self):
        self.add_param('-b', '--beast', choices=['beast1', 'beast2'],
                       help='Beast version to run')
        self.add_param('--beast1', default='/apps/BEASTv1.8.2/lib/beast.jar', help='Path to BEAST v1 jar file')
        self.add_param('--beast2', default='/apps/BEASTv2.3.2/lib/beast.jar', help='Path to BEAST v2 jar file')
        self.add_param('path', help='Directory containing XML input files for beast')
        self.add_param('--cores', default=1, type=int, help="Amount of cores to use. Default: %(default)s")

    def parse_args(self):
        if not os.path.isdir(self.params.path):
            raise gc3libs.exceptions.InvalidUsage("%s is not a directory", self.params.path)

    def new_tasks(self, extra):
        tasks = []
        for fname in os.listdir(self.params.path):
            if fname.endswith('.xml'):
                beast = self.params.beast
                if self.params.beast:
                    beast = self.params.beast
                elif 'BEAST1' in fname:
                    beast = 'beast1'
                elif 'BEAST2' in fname:
                    beast = 'beast2'
                else:
                    gc3libs.error("Unable to guess which version of BEAST you want to run. Skipping file %s" % fname)
                    continue
                jarfile = self.params.beast1 if beast == 'beast1' else self.params.beast2
                tasks.append(GBeastApp(beast,
                                       jarfile,
                                       self.params.path,
                                       fname,
                                       self.params.cores,
                                       **extra.copy()))
        return tasks

## main

if "__main__" == __name__:
    from gbeast import GBeastScript
    GBeastScript().run()
