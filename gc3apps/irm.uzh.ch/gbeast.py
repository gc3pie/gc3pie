#! /usr/bin/env python
#
"""
Run the BEAST or BEAST-2 programs from a prepared tree of input files.
"""
# Copyright (C) 2012-2016  University of Zurich. All rights reserved.
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


import os


import gc3libs
from gc3libs.cmdline import SessionBasedScript
from gc3libs.quantity import MiB, GB


def find_files(dirname, suffix=None):
    """
    Return a list of (full paths to) files in the given directory.

    If `suffix` is given, restrict to files with that ending.
    """
    result = []
    for entry in os.listdir(dirname):
        path = os.path.join(dirname, entry)
        if os.path.isfile(path):
            if suffix and not entry.endswith(suffix):
                continue
            result.append(path)
    return result


class GBeastApp(gc3libs.Application):
    def __init__(self, jarfile, dirname, ncores, **extra):
        extra['output_dir'] = os.path.basename(dirname)
        extra['requested_cores'] = ncores
        extra['requested_memory'] = ncores*4*GB

        inputs = find_files(dirname)

        xmls = [path for path in inputs if path.endswith('.xml')]

        treefiles = [path for path in inputs if path.endswith('.trees')]
        if treefiles:
            # Natasha's config places seed in .log/.trees file name
            treename, _ = treefiles[0].split('.')
            parts = treename.split('_')
            self.seed = int(parts[-1])
            self.resume = True
        else:
            self.seed = None
            self.resume = False

        # build command-line
        args = ['java',
                '-Xmx{mem_mb:d}m'.format(mem_mb=extra['requested_memory'].amount(MiB)),
                '-jar', jarfile,
                '-threads', ncores,
                '-beagle_instances', ncores]
        if self.resume:
            args += ['-resume']
        if self.seed:
            args += ['-seed', self.seed]
            args += [xml]

        gc3libs.Application.__init__(
            self,
            arguments = args,
            inputs = [xmls[0]],
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'stdout.txt',
            stderr = 'stderr.txt',
            **extra)

        
class GBeastScript(SessionBasedScript):
    """
    Script to parallelize execution of BEAST over multiple input files.
    Takes a list of directories containing XML files and processes them all.
    If a `.trees` file is already present in the directory, BEAST is passed
    the ``-resume`` option to append to existing files instead of starting over.
    """
    version = '1.1'
    def setup_options(self):
        self.add_param('-b', '--beast', choices=['beast1', 'beast2'],
                       help='Beast version to run')
        self.add_param('--beast1', default='/apps/BEASTv1.8.2/lib/beast.jar', help='Path to BEAST v1 jar file')
        self.add_param('--beast2', default='/apps/BEASTv2.3.2/lib/beast.jar', help='Path to BEAST v2 jar file')
        self.add_param('path', nargs='+', help='Directory containing XML input files for beast')
        self.add_param('--cores', default=1, type=int, help="Amount of cores to use. Default: %(default)s")

    def parse_args(self):
        if not os.path.isdir(self.params.path):
            raise gc3libs.exceptions.InvalidUsage(
                "{0} is not a directory".format(self.params.path))

    def new_tasks(self, extra):
        tasks = []
        for dirname in self.params.path:
            if self.params.beast:
                beast = self.params.beast
            else:
                xmls = find_files(dirname, '.xml')
                if len(xmls) > 1:
                    self.log.error(
                        "More than 1 `.xml` file in directory `{dirname}`: {xmls!r}"
                        " Skipping it."
                        .format(**locals()))
                    continue
                xml = xmls[0]
                if 'BEAST1' in xml:
                    beast = 'beast1'
                elif 'BEAST2' in xml:
                    beast = 'beast2'
                else:
                    gc3libs.error(
                        "Unable to guess which version of BEAST you want to run."
                        " Skipping file `{xml}`"
                        .format(xml=xml)
                    continue
            jarfile = (self.params.beast1 if beast == 'beast1' else self.params.beast2)
            tasks.append(GBeastApp(jarfile,
                                   dirname,
                                   self.params.cores,
                                   **extra.copy()))
        return tasks

## main

if "__main__" == __name__:
    from gbeast import GBeastScript
    GBeastScript().run()
