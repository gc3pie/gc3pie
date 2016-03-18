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
from gc3libs.cmdline import SessionBasedDaemon
import os
from gc3libs.quantity import GiB, MiB

class GBeastApp(gc3libs.Application):
    def __init__(self, beast, jarfile, path, ncores, **extra):
        self.fname = os.path.basename(path)
        self.infile = path
        self.extra = extra
        extra['output_dir'] = self.fname[:-4] + '.d'
        extra['requested_cores'] = ncores
        extra['requested_memory'] = ncores*4000*MiB
        extra['jobname'] = "GBeastApp.%s" % self.fname
        args = ['java',
                '-Xmx%dm' % extra['requested_memory'].amount(MiB),
                '-jar', jarfile,
                '-threads', ncores,
                '-beagle_instances', ncores,
                self.fname]

        gc3libs.Application.__init__(
            self,
            arguments = args,
            inputs = [self.infile],
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'stdout.txt',
            stderr = 'stderr.txt',
            **extra)

class GBeastDaemon(SessionBasedDaemon):
    """This script gets a directory as only argument. For each file in the
    directory will run a GBeastApp application.

    It also check, through inotify, if new files are added to the
    directory, in that case, a new application is added to the
    session.

    """
    version = '0.1'

    def setup_options(self):
        """Script to parallelize execution of BEAST over multiple input
        files. Takes a directory containing XML files, parse"""

        self.add_param('--beast1', default='/apps/BEASTv1.8.2/lib/beast.jar', help='Path to BEAST v1 jar file')
        self.add_param('--beast2', default='/apps/BEASTv2.3.2/lib/beast.jar', help='Path to BEAST v2 jar file')
        self.add_param('--cores', default=1, type=int, help="Amount of cores to use. Default: %(default)s")


    def new_tasks(self, extra, epath=None, emask=0):
        # Scan of the directory is performed in `before_main_loop`,
        # here we only need to ensure the fake always-running task is
        # created.

        if not epath or not epath.endswith('.xml'):
            gc3libs.log.info("Ignoring file %s since it doesn't end in .xml" % epath)
            return []

        beast = 'beast1'
        if 'BEAST2' in epath:
            beast = 'beast2'
        else:
            gc3libs.log.warning("Unable to guess which version of BEAST you want to run for file %s. Assuming BEAST v1" % epath)

        jarfile = self.params.beast1 if beast == 'beast1' else self.params.beast2
        try:
            # We need to load from a previously saved job extra
            # arguments, otherwise we don't know where to get
            # them.

            # FIXME: What should we do when a file is *updated*?
            # If you touch an existing file, this will trigger
            # the creation of a new instance. You could check if
            # fname is not in
            #
            #    [app.path for app in self.session if 'path' in app]
            #
            # but it might not be what you want. Maybe a command
            # line option?

            app = GBeastApp(beast,
                            jarfile,
                            epath,
                            self.params.cores,
                            **extra.copy())
            gc3libs.log.info("Added new application to session for file %s", epath)
            return [app]
        except Exception as ex:
            gc3libs.log.error("Error while adding application for file %s: %s", epath, ex)
        return []

if "__main__" == __name__:
    from gbeastdaemon import GBeastDaemon
    GBeastDaemon().run()
