#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012-2013,  University of Zurich. All rights reserved.
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


import gc3libs
from gc3libs.cmdline import SessionBasedDaemon
import os
from gc3libs.quantity import GiB, MiB

import inotifyx

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
    """This daemon script will continuosly check one or more `inbox`
    directories and, for each *new* XML file created in it, will create
    a new job running BEAST on it.

    By default it will run BEAST v1.8, but if the filename contains
    the string 'BEAST2' it will use BEAST v2 instead. Path to the
    correct Beast jar can be specified throught option `--beast1` and
    `--beast2`.

    """
    version = '0.1'

    def setup_options(self):
        self.add_param('--beast1', default='/apps/BEASTv1.8.2/lib/beast.jar', help='Path to BEAST v1 jar file')
        self.add_param('--beast2', default='/apps/BEASTv2.3.2/lib/beast.jar', help='Path to BEAST v2 jar file')
        self.add_param('--cores', default=1, type=int, help="Amount of cores to use. Default: %(default)s")


    def new_tasks(self, extra, epath=None, emask=0):
        # Scan of the directory is performed in `before_main_loop`,
        # here we only need to ensure the fake always-running task is
        # created.

        if not epath:
            # The script has just started, don't do anything yet, as
            # we will always wait for inotify for new files.
        elif epath.endswith('.xml'):
            gc3libs.log.info("Ignoring file `%s` since it doesn't end in .xml" % epath)
            return []

        beast = 'beast1'
        if 'BEAST2' in epath:
            beast = 'beast2'
        else:
            gc3libs.log.warning("Unable to guess which version of BEAST you"
                                " want to run for file %s. Assuming BEAST v1" % epath)

        jarfile = self.params.beast1 if beast == 'beast1' else self.params.beast2
        try:
            # We need to load from a previously saved job extra
            # arguments, otherwise we don't know where to get
            # them.

            # Check if the file is being processed already
            if epath in [app.path for app in self.session if 'path' in app]:
                gc3libs.log.warning(
                    "Ignoring file `%s`, as it's been processed already."
                    " You can re-process it by renaming the file or using"
                    " `gresub`" % epath)
                return []

            if emask == inotifyx.IN_CLOSE_WRITE:
                app = GBeastApp(beast,
                                jarfile,
                                epath,
                                self.params.cores,
                                **extra.copy())
                gc3libs.log.info("Added new application to session for file %s", epath)
            else:
                gc3libs.log.debug("Ignoring notify mask `%s` for file `%s`" % (emask, epath))
            return [app]
        except Exception as ex:
            gc3libs.log.error("Error while adding application for file %s: %s", epath, ex)
        return []

if "__main__" == __name__:
    from gbeastdaemon import GBeastDaemon
    GBeastDaemon().run()
