#!/usr/bin/env python
"""
This is an example script to demonstrate the basics of GC3Pie.

You can find more examples at:

  http://gc3pie.googlecode.com/svn/branches/2.0/examples/
"""

# stdlib imports
import logging
import sys
import time

# GC3Pie imports
import gc3libs
import gc3libs.config
import gc3libs.core


# Configure logging. Without this, you won't see any messages from GC3Pie.
# Possible levels are: DEBUG, INFO, WARNING, ERROR
loglevel = logging.DEBUG
gc3libs.configure_logger(loglevel, "gdemo")


class SquareApplication(gc3libs.Application):
    """Compute the square of `x`, using a remote job."""
    def __init__(self, x):
        self.to_square = x
        gc3libs.Application.__init__(
            self,
            arguments=['expr', x, '*', x],
            inputs=[],
            outputs=[],
            output_dir="./squares.d",
            stdout="stdout.txt",
            join=True)


# assume the number to square is given on the command-line
x = int(sys.argv[1])

# Read configuration from the default configuration file, and create
# an instance of `Core`.
cfg = gc3libs.config.Configuration(
    *gc3libs.Default.CONFIG_FILE_LOCATIONS,
    auto_enable_auth=True)
core = gc3libs.core.Core(cfg)

# create an instance of SquareApplication and submit it
app = SquareApplication(x)
core.submit(app)

# After submssion, you have to check the application for its state:
# if state is NEW, then submission failed
if app.execution.state == gc3libs.Run.State.NEW:
    print ("Failed submitting application, check log for errors.")
    sys.exit(1)
else:
    print ("SquareApplication successfully submitted,"
           " remote job ID is: %s" % app.execution.lrms_jobid)

# Periodically check the status of your application.
while app.execution.state in [ gc3libs.Run.State.SUBMITTED,
                               gc3libs.Run.State.RUNNING,
                               ]:
    time.sleep(5)
    # This call will contact the resource(s) and get the current
    # job state
    core.update_job_state(app)
    print "Job state is now %s " % app.execution.state

print ("Fetching job output...")

# You can specify a different `download_dir` option if you want to
# override the value used in the GdemoSimpleApp initialization
# (app.output_dir).

# By default overwrite is False. If the output directory exists, it
# will be renamed by appending a unique numerical suffix in the form
# of output_dir.~N~ with N the first available number.
core.fetch_output(app, overwrite=False)


# after calling, `fetch_output` the job is in TERMINATED state and
# GC3Pie won't act on it any more
print ("Job state is now %s." % app.execution.state)


print "Done. Results are in %s" % app.output_dir
