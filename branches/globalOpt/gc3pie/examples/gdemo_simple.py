#!/usr/bin/env python
# -*- coding: utf-8 -*-#
# @(#)gdemo_simple.py
#
#
# Copyright (C) 2009-2012 GC3, University of Zurich. All rights reserved.
#
#
#  This program is free software; you can redistribute it and/or modify it
#  under the terms of the GNU General Public License as published by the
#  Free Software Foundation; either version 2 of the License, or (at your
#  option) any later version.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
"""This is a basic script to demonstrate the basics of GC3Pie.

This simple script will create an instance of a `GdemoSimpleApp` class
which inherits from `Application`:class and which basically run the
command `/bin/hostname`.

It will create an instance of `Core`:class and will submit the
`GdemoSimpleApp` instance, check its status until the application is
terminated and then fetch the output.

You can specify the resource you want to use by passing its name as
command line argument.
"""

# stdlib imports
import logging
import sys
import time

# GC3Pie imports
import gc3libs
import gc3libs.config
import gc3libs.core


# Configure logging. This is not really necessary but will avoid some
# boring errors from the logging subsystem. Moreover, it's important
# to set it at least at ERROR level, otherwise some errors from your
# code could be silently ignored.
loglevel = logging.DEBUG
gc3libs.configure_logger(loglevel, "gdemo")

class GdemoSimpleApp(gc3libs.Application):
    """This simple application will run /bin/hostname on the remove
    host, and retrive the output in a file named `stdout.txt` into a
    directory `mygc3job` inside the current directory."""
    def __init__(self):
        gc3libs.Application.__init__(
            self,
            arguments = ['/bin/hostname'], # mandatory
            inputs = [],                  # mandatory
            outputs = [],                 # mandatory
            output_dir = "./mygc3job",    # mandatory
            stdout = "stdout.txt",)


# create an instance of GdemoSimpleApp
app = GdemoSimpleApp()

# create an instance of Core. Read configuration from your default
# configuration file
cfg = gc3libs.config.Configuration(*gc3libs.Default.CONFIG_FILE_LOCATIONS,
                                   **{'auto_enable_auth': True})
core = gc3libs.core.Core(cfg)

# in case you want to select a specific resource, call
# `Core.select_resource(...)`
if len(sys.argv)>1:
    core.select_resource(sys.argv[1])

# Submit your application.
core.submit(app)

# After submssion, you have to check the application for its state:
print  "Job id: %s" % app.execution.lrms_jobid

# Periodically check the status of your application.
while app.execution.state in [ gc3libs.Run.State.SUBMITTED,
                               gc3libs.Run.State.RUNNING,
                               ]:
    try:
        print "Job in status %s " % app.execution.state
        time.sleep(5)
        # This call will contact the resource(s) and get the current
        # job state
        core.update_job_state(app)
        sys.stdout.write("[ %s ]\r" % app.execution.state)
        sys.stdout.flush()
    except:
        raise

print "Job is now in state %s. Fetching output." % app.execution.state

# You can specify a different `download_dir` option if you want to
# override the value used in the GdemoSimpleApp initialization
# (app.output_dir).

# By default overwrite is False. If the output directory exists, it
# will be renamed by appending a unique numerical suffix in the form
# of output_dir.~N~ with N the first available number.
core.fetch_output(app, overwrite=False)

print "Done. Results are in %s" % app.output_dir
