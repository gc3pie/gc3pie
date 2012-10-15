#!/usr/bin/env python
# -*- coding: utf-8 -*-#
# @(#)exercise_A_3.py
"""

Exercise A (3)

work on the source for `exercise_A_2.py`:

* modify the script so that it runs 10 copies of the Application
  class, and prints statistics about CPU models (how many unique
  `model name` strings)

"""
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

# stdlib imports
import logging
import sys
import time
import os

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
            arguments = ['/bin/cat', '/proc/cpuinfo'], # mandatory
            inputs = [],                  # mandatory
            outputs = [],                 # mandatory
            output_dir = "./mygc3job",    # mandatory
            stdout = "stdout.txt",)

    def terminated(self):
        """
        Called when the job state transitions to `TERMINATED`, i.e.,
        the job has finished execution (with whatever exit status, see
        `returncode`) and the final output has been retrieved.

        The location where the final output has been stored is
        available in attribute `self.output_dir`.

        The default implementation does nothing, override in derived
        classes to implement additional behavior.
        """
        fname = os.path.join(self.output_dir, 'stdout.txt')
        fd = open(fname)
        self.model_name = 'UNKNOWN'
        for line in fd:
            if line.startswith('model name'):
                self.model_name = line.split(':', 1)[1].strip()
                break
        fd.close()

# create an instance of GdemoSimpleApp
applications = [GdemoSimpleApp() for i in range(10)]

# create an instance of Core. Read configuration from your default
# configuration file
cfg = gc3libs.config.Configuration(*gc3libs.Default.CONFIG_FILE_LOCATIONS,
                                   **{'auto_enable_auth': True})
core = gc3libs.core.Core(cfg)

# in case you want to select a specific resource, call
# `Core.select_resource(...)`
if len(sys.argv)>1:
    core.select_resource(sys.argv[1])

# This dictionary will contain a  mapping "model name" => "occurrencies"
model_names = dict()

# Loop for each application, so that the library will not complain
# about `max_cores` reached.
for app in applications:
    # Submit the application
    core.submit(app)
    
    # Periodically check the status of the application.
    while app.execution.state in [ gc3libs.Run.State.NEW,
                                   gc3libs.Run.State.SUBMITTED,
                                   gc3libs.Run.State.RUNNING,
                                   ]:
        try:
            print "Job in status %s " % app.execution.state
            time.sleep(1)
            # This call will contact the resource(s) and get the current
            # job state
            core.update_job_state(app)
            sys.stdout.write("[ %s ]\r" % app.execution.state)
            sys.stdout.flush()
        except:
            raise

    # Application is done. Fetching output so the `terminated()`
    # method is claled.
    print "Job is completed. Fetching the output."
    core.fetch_output(app, overwrite=False)
    if app.model_name not in model_names:
        model_names[app.model_name] = 1
    else:
        model_names[app.model_name] += 1
        

for (k,v) in model_names.iteritems():
    print "Model name: %s (%d)" % (k,v)
