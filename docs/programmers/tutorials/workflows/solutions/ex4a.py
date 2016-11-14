#! /usr/bin/env python

"""
Exercise 4.A: Write a colorize.py script to apply
this colorization process to a set of grayscale images.

The colorize.py script shall be invoked like this::

  $ python colorize.py c1 c2 c3 img1 [img2 ...]

where c1, c2, c3 are color names and img1, img2 are
image files.

Each image shall be processed in a separate colorization task.
"""

import os
from os.path import abspath, basename
import sys

from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript


if __name__ == '__main__':
    from ex4a import ColorizeScript
    ColorizeScript().run()


class ColorizeScript(SessionBasedScript):
    """
    Colorize multiple images.
    """
    def __init__(self):
        super(ColorizeScript, self).__init__(version='1.0')
    def setup_args(self):
        self.add_param('colors', nargs=3,   help="Three colors")
        self.add_param('images', nargs='+', help="Images to colorize")
    def new_tasks(self, extra):
        col1, col2, col3 = self.params.colors
        apps_to_run = []
        for input_file in self.params.images:
            input_file = abspath(input_file)
            apps_to_run.append(ColorizeApp(input_file, col1, col2, col3))
        return apps_to_run


from gc3libs.quantity import GB

class ColorizeApp(Application):
    """Add colors to a grayscale image."""
    def __init__(self, img, col1, col2, col3):
        inp = basename(img)
        out = "color-" + inp
        Application.__init__(
            self,
            arguments=[
                "convert", inp,
                "(", "xc:"+col1,  "xc:"+col2, "xc:"+col3, "+append", ")", "-clut",
                out],
            inputs=[img],
            outputs=[out],
            # need to use a different output dir per set of
            # construction params, otherwise the output of one task
            # will be overwritten by another task's output ...
            output_dir="colorized-" + inp + ".d",
            stdout="stdout.txt",
            stderr="stderr.txt",
            # required for running on the cloud, see GC3Pie issue #559
            requested_memory=1*GB)
