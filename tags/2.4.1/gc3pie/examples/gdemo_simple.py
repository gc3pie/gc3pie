#!/usr/bin/env python
# -*- coding: utf-8 -*-#
# @(#)gdemo_simple.py
#
#
# Copyright (C) 2009-2013 S3IT, Zentrale Informatik, University of Zurich. All rights reserved.
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
"""
This is a basic script to demonstrate the basics of GC3Pie.

This simple script will create an instance of a `GdemoSimpleApp` class
which inherits from `Application`:class and which basically run the
command `/bin/hostname`.

It will create an instance of `Engine`:class and will submit the
`GdemoSimpleApp` instance, check its status until the application is
terminated and then fetch the output.

You can specify the resource you want to use by passing its name as
command line argument.
"""

# stdlib imports
import sys
import time

# GC3Pie imports
import gc3libs

######################
# OPTIONAL - LOGGING #
######################
# Configure logging. This is not really necessary but will avoid some
# boring errors from the logging subsystem. Moreover, it's important
# to set it at least at ERROR level, otherwise some errors from your
# code could be silently ignored.
#
# import logging
# loglevel = logging.DEBUG
# gc3libs.configure_logger(loglevel, "gdemo")

class GdemoSimpleApp(gc3libs.Application):
    """
    This simple application will run `/bin/hostname`:file: on the remote host,
    and retrieve the output in a file named `stdout.txt`:file: into a
    directory `GdemoSimpleApp_output`:file: inside the current directory.
    """
    def __init__(self):
        gc3libs.Application.__init__(
            self,
            # the following arguments are mandatory:
            arguments = ["/bin/hostname"],
            inputs = [],
            outputs = [],
            output_dir = "./GdemoSimpleApp_output",
            # the rest is optional and has reasonable defaults:
            stdout = "stdout.txt",)

# Create an instance of GdemoSimpleApp
app = GdemoSimpleApp()

# Create an instance of `Engine` using the configuration file present
# in your home directory.
engine = gc3libs.create_engine()

# Add your application to the engine. This will NOT submit your
# application yet, but will make the engine awere *aware* of the
# application.
engine.add(app)

# in case you want to select a specific resource, call
# `Engine.select_resource(<resource_name>)`
if len(sys.argv)>1:
    engine.select_resource(sys.argv[1])

# Periodically check the status of your application.
while app.execution.state != gc3libs.Run.State.TERMINATED:
    print "Job in status %s " % app.execution.state
    # `Engine.progress()` will do the GC3Pie magic:
    # submit new jobs, update status of submitted jobs, get
    # results of terminating jobs etc...
    engine.progress()

    # Wait a few seconds...
    time.sleep(1)

print "Job is now terminated."
print "The output of the application is in `%s`." %  app.output_dir
