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
    from ex6b import OomScript
    OomScript().run()


class OomScript(SessionBasedScript):
    """
    Convert images to grayscale.
    """
    def __init__(self):
        super(OomScript, self).__init__(version='1.0')
    def new_tasks(self, extra):
        # since `self.params.args` is already a list of file names,
        # just iterate over it to build the list of apps to run...
        apps_to_run = []
        for input_file in self.params.args:
            input_file = abspath(input_file)
            apps_to_run.append(MatlabApp('downloads/ra.m'))
        return apps_to_run


class MatlabApp(Application):
    """Run a MATLAB source file."""
    application_name = 'matlab'

    def __init__(self, code_file_path):
        code_file_name = basename(code_file_path)
        code_func_name = code_file_name[:-len('.m')]  # remove `.m` extension
        Application.__init__(
            self,
            arguments=["matlab", "-nodesktop", "-nojvm", "-r", code_func_name],
            inputs=[code_file_path],
            outputs=[],
            output_dir=("matlab.out.d"),
            stdout="matlab.log",
            stderr="matlab.log",
            # this is needed to circumvent GC3Pie issue #559
            requested_memory=1*GB)
    def terminated(self):
        err_file_path = os.path.join(self.output_dir, self.stderr)
        with open(err_file_path, 'r') as err_file:
            errors = err_file.read()
            if 'Out of memory' in errors or 'exceeds maximum array size' in errors:
                self.execution.exitcode = 11
        # verbosely notify user
        if self.execution.signal != 0:
            log.info("Task %s killed by signal %d", self, self.execution.signal)
        else:
            # self.execution.signal == 0, hence normal termination
            if self.execution.exitcode == 0:
                log.info("Task %s exited successfully!", self)
            else:
                log.info("Task %s exited with error code %d", self, self.execution.exitcode)
