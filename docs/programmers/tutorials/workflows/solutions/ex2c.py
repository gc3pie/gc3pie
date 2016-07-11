#! /usr/bin/env python

import os
from os.path import abspath, basename
import sys

from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript
from gc3libs.quantity import GB


if __name__ == '__main__':
    from ex2c import AScript
    AScript().run()


class AScript(SessionBasedScript):
    """
    Minimal workflow scaffolding.
    """
    def __init__(self):
        super(AScript, self).__init__(version='1.0')
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
            output_dir="grayscale.d",
            stdout="stdout.txt",
            stderr="stderr.txt",
            # this is needed to circumvent GC3Pie issue #559, see
            # <https://github.com/uzh/gc3pie/issues/559>
            requested_memory=1*GB)
