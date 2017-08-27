#! /usr/bin/env python

import os
import sys

from gc3libs.cmdline import SessionBasedScript

if __name__ == '__main__':
    from ex2a import AScript
    AScript().run()

class AScript(SessionBasedScript):
    """
    Minimal workflow scaffolding.
    """
    def __init__(self):
        super(AScript, self).__init__(version='1.0')
    def new_tasks(self, extra):
        apps_to_run = [ ]
        return apps_to_run
