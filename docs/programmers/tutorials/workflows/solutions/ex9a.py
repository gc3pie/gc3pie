#! /usr/bin/env python

"""
Exercise 9.A: Write a ``ParallelTaskCollection``
class ``RandomlyColorize`` that is initialized with two
parameters: an image file name img and a number N.

The ``RandomlyColorize`` collection then consists of N
instances of the ``ColorizeApp``, each initialized with
the same image file name ``img`` and three random
colors.
"""

import os
from os.path import abspath, basename
import sys

from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript, positive_int
from gc3libs.quantity import GB
from gc3libs.workflow import ParallelTaskCollection


if __name__ == '__main__':
    from ex9a import RandomlyColorizeScript
    RandomlyColorizeScript().run()


class RandomlyColorizeScript(SessionBasedScript):
    """
    Colorize an image with multiple random colors.
    """
    def __init__(self):
        super(RandomlyColorizeScript, self).__init__(version='1.0')
    def setup_args(self):
        self.add_param('repeat', type=positive_int, help="Number of distinct colorizations per image")
        self.add_param('images', nargs='+', help="Images to colorize")
    def new_tasks(self, extra):
        col1, col2, col3 = self.params.colors
        apps_to_run = []
        for input_file in self.params.images:
            input_file = abspath(input_file)
            apps_to_run.append(RandomlyColorize(input_file, self.params.repeat))
        return apps_to_run


from random import randint

def random_color():
    r = randint(0, 255)
    g = randint(0, 255)
    b = randint(0, 255)
    color = ("xc:#%02x%02x%02x" % (r, g, b))
    return color


class RandomlyColorize(ParallelTaskCollection):
    """
    """
    def __init__(self, img, N):
        apps = []
        for n in range(N):
            col1 = random_color()
            col2 = random_color()
            col3 = random_color()
            output_dir = ("colorized-{name}-{nr}.d".format(name=basename(img), nr=n))
            apps.append(ColorizeApp(img, col1, col2, col3, output_dir))
        ParallelTaskCollection.__init__(self, apps)


class ColorizeApp(Application):
    """Add colors to a grayscale image."""
    def __init__(self, img, col1, col2, col3, output_dir):
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
            output_dir=output_dir,
            stdout="stdout.txt",
            stderr="stderr.txt",
            # required for running on the cloud, see GC3Pie issue #559
            requested_memory=1*GB)
