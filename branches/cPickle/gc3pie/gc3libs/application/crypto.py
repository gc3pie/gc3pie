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
from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.application
from gc3libs.exceptions import *

class CryptoApplication(gc3libs.Application):
    """
    Run a Crypto job 
    """
    
    def __init__(self, inp, **kw):
        # Get the list of input files to send to the grid

        #inputs = self.gatherInputList()
        arguments = self.read_inp(inp)

        # Append # cores
        if kw.has_key('requested_cores'):
            arguments.append(kw['requested_cores'])
        else:
            # default 1 core
            arguments.append('1')

        # append reference to archive containing static input files
        if not kw.has_key('input_directives'):
            # set a default
            kw['input_directives'] = 'input_files.tgz'
            
        arguments.append(kw['input_directives'])

        src_crypto_bin = resource_filename(Requirement.parse("gc3pie"), 
                                           "gc3libs/etc/gnfs-cmd")

        inputs = {kw['input_directives']:"input_files.tgz", src_crypto_bin:"gnfs-cmd" }

        kw['tags'] = [ 'TEST/CRYPTO-1.0' ]

        gc3libs.Application.__init__(
            self,
            executable =  os.path.basename(src_crypto_bin),
            arguments = arguments, 
            inputs = inputs,
            # outputs = [ '@output.files' ],
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'gcrypto.log',
            join=True,
            **kw
            )

    def read_inp(self, inFile):
        # format of expected input file
        # 1 line with two numbers separated by blank space:
        # 2200000400 100
        # should be treated as two separate arguments
        try:
            f = open(inFile, 'r')
            line = f.readline().strip()
            f.close()
            args = line.split(" ")
            return args
        except IOError:
            gc3libs.log.error("Argument file '%s' not found" % inFile)

    def terminated(self):
        """
        Checks whether M*.gz files have been created
        Checks 'done' pattern in stdout
        
        The exit status of the whole job is set to one of these values:

        *  0 -- all files processed successfully
        *  1 -- some files were *not* processed successfully
        *  2 -- no files processed successfully
        * 127 -- the ``codeml`` application did not run at all.
         
        """
        
        gc3libs.log.debug('Application terminated. postprocessing with execution.signal [%d]' % self.execution.exitcode)
        # if self.execution.signal == 125:
        #     # submission failed, job did not run at all
        #     self.execution.exitcode = 127
        #     return

        # total = len(self.outputs)
        # # form full-path to the output files
        # outputs = [ os.path.join(download_dir, filename) 
        #             for filename in fnmatch.filter(os.listdir(download_dir), '*.mlc') ]
        # if len(outputs) == 0:
        #     # no output retrieved, did ``codeml`` run at all?
        #     self.execution.exitcode = 127
        #     return
        # # count the number of successfully processed files
        # failed = 0
        # for mlc in outputs:
        #     output_file = open(mlc, 'r')
        #     last_line = output_file.readlines()[-1]
        #     output_file.close()
        #     if not last_line.startswith('Time used: '):
        #         failed += 1
        # # set exit code and informational message
        # if failed == 0:
        #     self.execution.exitcode = 0
        #     self.info = "All files processed successfully, output downloaded to '%s'" % download_dir
        # elif failed < total:
        #     self.execution.exitcode = 1
        #     self.info = "Some files *not* processed successfully, output downloaded to '%s'" % download_dir
        # else:
        #     self.execution.exitcode = 2
        #     self.info = "No files processed successfully, output downloaded to '%s'" % download_dir
        # self.execution.exitcode = self.execution.signal
        return


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="square",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
