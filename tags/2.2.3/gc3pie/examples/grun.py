#! /usr/bin/env python
#
"""
This simple SessionBasedScript allows you to run a generic command and
will allow you to execute it in a sequential or in a parallel task
collection.

This is mainly used for testing and didactic purpouses, don't use it
on a production environment!
"""
# Copyright (C) 2012-2014, GC3, University of Zurich. All rights reserved.
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


import os
import os.path
import shlex

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run
from gc3libs.cmdline import SessionBasedScript, nonnegative_int
from gc3libs.workflow import TaskCollection
from gc3libs.workflow import ParallelTaskCollection, SequentialTaskCollection

class GRunApplication(Application):
    """
    An `Application` wrapper which will execute the arguments as a
    shell script command. This application will also check if some of
    the argument is a file, and in that case it will add it to the
    files to upload as input.
    """
    def __init__(self, arguments, **extra_args):
        # Fix path of the executable
        inputs = extra_args.get('inputs', [])
        shellargs = []
        for arg in arguments:
            shellargs.extend(shlex.split(arg))
            argpath = os.path.expandvars(os.path.expanduser(arg))
            if os.path.exists(argpath):
                inputs.append(argpath)
        Application.__init__(self,
                             arguments = ["sh", "-c", str.join(' ', shellargs)],
                             inputs = inputs,
                             outputs = gc3libs.ANY_OUTPUT,
                             stdout = "stdout.txt",
                             stderr = "stderr.txt",
                             **extra_args)

class GRunScript(SessionBasedScript):
    """
    Simple script to run an application or script. It also allow to
    run it multiple times, in parallel or in a serial.

    Mainly used for testing purposes.
    """
    version = '1.1'
    def setup_options(self):
        """Add options specific to this session-based script."""
        self.add_param('--parallel', metavar="COUNT",
                       action="store", default=0, type=nonnegative_int,
                       help='Execute the command line this many times in parallel')
        self.add_param('--sequential', metavar="COUNT",
                       action="store", default=0, type=nonnegative_int,
                       help='Execute the command line this many times in a sequence')

    def parse_args(self):
        if not self.params.parallel and not self.params.sequential:
            raise RuntimeError("You must specify either --parallel or --sequential.")
        if self.params.parallel and self.params.sequential:
            raise RuntimeError("You can either use --parallel or --sequential, not both")

    def new_tasks(self, extra):
        appextra = extra.copy()
        del appextra['output_dir']

        if self.params.parallel:
            task = ParallelTaskCollection(
                [GRunApplication(
                        self.params.args,
                        jobname='GRunApplication.%d' % i,
                        output_dir='GRunApplication.%d.d' % i,
                        **appextra)
                 for i in range(self.params.parallel)], **extra)

        elif self.params.sequential:
            task = SequentialTaskCollection(
                [GRunApplication(
                        self.params.args,
                        jobname='GRunApplication.%d' % i,
                        output_dir='GRunApplication.%d.d' % i,
                        **appextra)
                 for i in range(self.params.sequential)], **extra)

        else:
            task = GRunApplication(self.params.args, **extra)

        return [task]

    def after_main_loop(self):
        print ""
        tasks = self.session.tasks.values()
        for app in tasks:
            if isinstance(app, TaskCollection):
                tasks.extend(app.tasks)
            if not isinstance(app, Application):
                continue
            print "==========================================="
            print "Application     %s" % app.jobname
            print "  state:        %s" % app.execution.state
            print "  command line: %s" % str.join(" ", app.arguments)
            print "  return code:  %s" % app.execution._exitcode
            print "  output dir:   %s" % app.output_dir

## main: run tests

if "__main__" == __name__:
    import grun
    grun.GRunScript().run()
