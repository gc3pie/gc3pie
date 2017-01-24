#! /usr/bin/env python

"""
Exercise 6.B: Modify the grayscaling script ex2c (or the code it
depends upon) so that, when a ``GrayscaleApp`` task has terminated
execution, it prints:

* whether the program has been killed by a signal, and the signal number;
* whether the program has terminated by exiting, and the exit code.
"""

import os
from os.path import abspath, basename
import sys

from gc3libs import Application, log
from gc3libs.cmdline import SessionBasedScript
from gc3libs.quantity import GB


if __name__ == '__main__':
    from ex6b import GrayscaleScript
    GrayscaleScript().run()


class GrayscaleScript(SessionBasedScript):
    """
    Convert images to grayscale.
    """
    def __init__(self):
        super(GrayscaleScript, self).__init__(version='1.0')
    def new_tasks(self, extra):
        # since `self.params.args` is already a list of file names,
        # just iterate over it to build the list of apps to run...
        apps_to_run = []
        for input_file in self.params.args:
            input_file = abspath(input_file)
            apps_to_run.append(GrayscaleApp(input_file))
        return apps_to_run


class GrayscaleApp(Application):
    """Convert a single image file to grayscale."""
    def __init__(self, img):
        inp = basename(img)
        out = "gray-" + inp
        Application.__init__(
            self,
            arguments=[
                "convert", inp, "-colorspace", "gray", out],
            inputs=[img],
            outputs=[out],
            output_dir=("gray-" + inp + ".d"),
            stdout="stdout.txt",
            stderr="stderr.txt",
            # this is needed to circumvent GC3Pie issue #559
            requested_memory=1*GB)
    def terminated(self):
        if self.execution.signal != 0:
            log.info("Task %s killed by signal %d", self, self.execution.signal)
        else:
            # self.execution.signal == 0, hence normal termination
            if self.execution.exitcode == 0:
                log.info("Task %s exited successfully!", self)
            else:
                log.info("Task %s exited with error code %d", self, self.execution.exitcode)
