#! /usr/bin/env python

import os
from os.path import basename
import sys

from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript


if __name__ == '__main__':
    from ex2b import AScript
    AScript().run()


class AScript(SessionBasedScript):
    """
    Minimal workflow scaffolding.
    """
    def __init__(self):
      super(AScript, self).__init__(version='1.0')
    def new_tasks(self, extra):
      input_file = self.params.args[0]
      app = GrayscaleApp(input_file)
      return [app]


class GrayscaleApp(Application):
  """Convert an image file to grayscale."""
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
      stderr="stderr.txt")
