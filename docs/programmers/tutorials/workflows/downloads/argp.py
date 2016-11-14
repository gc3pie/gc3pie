#! /usr/bin/env python

import os
import sys

from gc3libs.cmdline import SessionBasedScript

if __name__ == '__main__':
    from argp import AScript
    AScript().run()

class AScript(SessionBasedScript):
    """
    Minimal workflow scaffolding.
    """
    def __init__(self):
      super(AScript, self).__init__(version='1.0')
    def new_tasks(self, extra):
      return []
    def setup_args(self):
      # this argument is a string (default type)
      self.add_param('input',  type=str,   help="Input file")
      # the `radius` argument is an integer
      self.add_param('radius', type=int,   help="Convolution radius")
      # the `sigma` argument is a floating-point number
      self.add_param('sigma',  type=float, help="Threshold")
