#! /usr/bin/env python
#
#   gcrypto.py -- Front-end script for submitting multiple Crypto jobs to SMSCG.
"""
Front-end script for submitting multiple Crypto jobs to SMSCG.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gcodeml --help`` for program usage instructions.
"""
__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
"""
__author__ = 'Pascal Jermini <Pascal.Jermini@epfl.ch>'
__docformat__ = 'reStructuredText'


import fnmatch
import logging
import os
import os.path
import re
import sys

import gc3libs
from gc3libs.cmdline import SessionBasedScript
from gc3libs.application.crypto import CryptoApplication

## the script itself

class GCryptoScript(SessionBasedScript):
    """
    Scan the specified INPUTDIR directories recursively for '.in' files,
    and submit a Crypto job for each input file found; job progress is
    monitored and, when a job is done, its output files are retrieved back
    into the same directory where the '.in' file is (this can be
    overridden with the '-o' option).
    
    The `gcrypto` command keeps a record of jobs (submitted, executed and
    pending) in a session file (set name with the '-s' option); at each
    invocation of the command, the status of all recorded jobs is updated,
    output from finished jobs is collected, and a summary table of all
    known jobs is printed.  New jobs are added to the session if new input
    files are added to the command line.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; `gcrypto` will delay submission
    of newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = CryptoApplication,
            input_filename_pattern = '*.in'
            )

# run it
if __name__ == '__main__':
    GCryptoScript().run()
