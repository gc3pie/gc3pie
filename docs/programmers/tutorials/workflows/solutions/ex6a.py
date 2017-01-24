#! /usr/bin/env python

"""
Exercise 6.A: In the ``colorize.py`` script from Exercise 4.A,
modify the ColorizeApp application to move the output picture file
into directory ``/home/ubuntu/pictures``.  You might need to store the
output file name to have it available when the application has
terminated running.
"""

import os
from os.path import abspath, basename, exists, join
import sys

from gc3libs import Application, log
from gc3libs.cmdline import SessionBasedScript


if __name__ == '__main__':
    from ex6a import ColorizeScript
    ColorizeScript().run()


class ColorizeScript(SessionBasedScript):
    """
    Colorize multiple images and collect results
    into directory ``./pictures``
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


from shutil import move

from gc3libs.quantity import GB

class ColorizeApp(Application):
    """Add colors to a grayscale image."""
    def __init__(self, img, col1, col2, col3):
        inp = basename(img)
        # need to save this for later reference in ``terminated()``
        self.output_file_name = "color-" + inp
        Application.__init__(
            self,
            arguments=[
                "convert", inp,
                "(", "xc:"+col1,  "xc:"+col2, "xc:"+col3, "+append", ")", "-clut",
                self.output_file_name],
            inputs=[img],
            outputs=[self.output_file_name],
            output_dir="colorized-" + inp + ".d",
            stdout="stdout.txt",
            stderr="stderr.txt",
            # required for running on the cloud, see GC3Pie issue #559
            requested_memory=1*GB)
    def terminated(self):
        # full path to output file on local filesystem
        output_file = join(self.output_dir, self.output_file_name)
        # if the output file is not there, log an error and exit
        if not exists(output_file):
            log.error("Expected output file `%s` from %s does not exists!",
                      output_file, self)
            return
        # ensure destination directory exists
        if not exists('pictures'):
            os.mkdir('pictures')
        # the trailing slash ensures `shutil.move` raises an error if
        # the destination exists but is not a directory
        move(output_file, 'pictures/')
