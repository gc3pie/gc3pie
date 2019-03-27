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
__version__ = '$Revision$'

from __future__ import absolute_import, print_function
import gc3libs
from gc3libs.cmdline import SessionBasedScript
import os
import inotifyx
from gc3libs.quantity import GiB, MiB

BEAST1_COMMAND = "java -Xmx{requested_memory}m -jar {jar} -working -overwrite -threads {requested_cores} -beagle_instances {requested_cores} {input_xml}"
BEAST24_COMMAND = "java -Xmx{requested_memory}m -jar {jar} -working -overwrite -threads {requested_cores} -instances {requested_cores} -beagle {input_xml}"

## main: run tests

class GRunningApp(gc3libs.Task):
    """Fake application. Its state is always RUNNING: it's a cheap way to
    run a "daemon" gc3pie application.

    It's also used to save the **extra keyword arguments from one
    instance to the other, by saving it in the gc3pie session as task
    attribute.

    """
    def __init__(self, **extra):
        self.extra = extra
        gc3libs.Task.__init__(self, **extra)

class GBeastApp(gc3libs.Application):
    def __init__(self, beast, jarfile, dirname, fname, ncores, **extra):
        infile = os.path.join(dirname, fname)
        self.fname = fname
        self.extra = extra
        extra['output_dir'] = fname[:-4] + '.d'
        extra['requested_cores'] = ncores
        extra['requested_memory'] = ncores*4*GiB

        if beast == 'beast2.4':
            args = BEAST24_COMMAND.format(requested_memory=extra['requested_memory'].amount(MiB),
                                           jar=jarfile,
                                           requested_cores=ncores,
                                           input_xml=fname)
        else:
            args = BEAST1_COMMAND.format(requested_memory=extra['requested_memory'].amount(MiB),
                                         jar=jarfile,
                                         requested_cores=ncores,
                                         input_xml=fname)

        gc3libs.Application.__init__(
            self,
            arguments = args,
            inputs = [infile],
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'stdout.txt',
            stderr = 'stderr.txt',
            **extra)

class GBeastScript(SessionBasedScript):
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

        self.add_param('-b', '--beast', choices=['beast1', 'beast2'],
                       help='Beast version to run')
        self.add_param('--beast1', default='/apps/BEASTv1.8.2/lib/beast.jar', help='Path to BEAST v1 jar file')
        self.add_param('--beast2', default='/apps/BEASTv2.3.2/lib/beast.jar', help='Path to BEAST v2 jar file')
        self.add_param('--beast2_4', default='/apps/BEASTv2.4.7/beast/lib/beast.jar', help='Path to BEAST v2.4 jar file')
        self.add_param('--cores', default=1, type=int, help="Amount of cores to use. Default: %(default)s")
        self.add_param('path', help='Path to directory to watch for new input files')

    def parse_args(self):
        # Fails if path is not a directory.
        if not os.path.isdir(self.params.path):
            raise gc3libs.exceptions.InvalidUsage("%s is not a directory", self.params.path)

        # We start the inotify as soon as possible, so that if a new
        # file is created between the execution of this function and
        # the every_main_loop() call (which is called *after* the main
        # loop), we don't lose the newly created file.
        self.ifd = inotifyx.init()
        self.iwatch = inotifyx.add_watch(self.ifd, self.params.path, inotifyx.IN_CLOSE_WRITE)

    def add_new_application(self, fname):
        if fname.endswith('.xml') and not fname.startswith('._'):
            beast = 'beast1'
            jarfile = self.params.beast1
            if 'BEAST24' in fname:
                beast = 'beast2.4'
                jarfile = self.params.beast2_4
            elif 'BEAST2' in fname:
                beast = 'beast2'
                jarfile = self.params.beast2
            else:
                gc3libs.log.warning("Unable to guess which version of BEAST you want to run for file %s. Assuming BEAST v1" % fname)

            try:
                # We need to load from a previously saved job extra
                # arguments, otherwise we don't know where to get
                # them.
                extra = self.session.tasks.values()[0].extra

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
                                self.params.path,
                                fname,
                                self.params.cores,
                                **extra.copy())
                ### FIXME: do we really need to add the app to the
                ### controller *and* to the session?
                self._controller.add(app)
                self.session.add(app)
                gc3libs.log.info("Added new application to session for file %s", fname)
                return app
            except Exception as ex:
                gc3libs.log.error("Error while adding application for file %s: %s", fname, ex)

    def every_main_loop(self):
        # Check if any new file has been created. The 5 seconds
        # timeout allows us to continue the execution if no new file
        # is created.
        events = inotifyx.get_events(self.ifd, 5)
        for event in events:
            fname = event.name
            self.add_new_application(fname)

    def before_main_loop(self):
        # If the script is started on an already existing session, we
        # have to ensure that there is already an app for each file in
        # the directory, otherwise we create a new app.

        working_files = [app.fname for app in self.session if 'fname' in app]
        extra = self.session.tasks.values()[0].extra
        for fname in os.listdir(self.params.path):
            if fname not in working_files:
                self.add_new_application(fname)

    def new_tasks(self, extra):
        # Scan of the directory is performed in `before_main_loop`,
        # here we only need to ensure the fake always-running task is
        # created.
        return [GRunningApp(**extra)]

if "__main__" == __name__:
    from gbeastnotify import GBeastScript
    GBeastScript().run()
