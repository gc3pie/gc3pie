#! /usr/bin/env python

import os
from os.path import abspath, basename
import sys
import csv

from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript

def parse(input_csv):

    input_parameters = []
    
    with open(input_csv,'rb') as fd:
        reader = csv.reader(fd)
        for line in reader:
            input_parameters.append(line)

    return input_parameters

            
if __name__ == '__main__':
    from ex2a import AScript
    AScript().run()

# alternatively, you can just copy+paste
# the code for `GrayscaleApp` here
from gasset import GassetApplication
    
class AScript(SessionBasedScript):
    """
    Minimal workflow scaffolding.
    """
    def __init__(self):
      super(AScript, self).__init__(version='1.0')
    def new_tasks(self, extra):
        input_params = parse(self.params.args[0])[0]
        matlab_script = abspath(self.params.args[1])
        app = GassetApplication(input_params,matlab_script)
        return [app]

