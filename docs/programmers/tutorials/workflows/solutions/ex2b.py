#! /usr/bin/env python

import os
from os.path import abspath, basename
import sys

from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript


if __name__ == '__main__':
    from ex2b import GrayscalingScript
    GrayscalingScript().run()


# alternatively, you can just copy+paste
# the code for `GrayscaleApp` here
from grayscale_app import GrayscaleApp


class GrayscalingScript(SessionBasedScript):
    """
    Convert an image to grayscale.
    """
    def __init__(self):
        super(GrayscalingScript, self).__init__(version='1.0')
    def new_tasks(self, extra):
        input_file = abspath(self.params.args[0])
        apps_to_run = [ GrayscaleApp(input_file) ]
        return apps_to_run
