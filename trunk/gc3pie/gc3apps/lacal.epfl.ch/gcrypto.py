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
__author__ = 'sergio.maffiolett@gc3.uzh.ch'
__docformat__ = 'reStructuredText'


import fnmatch
import logging
import os
import os.path
import re
import sys
from pkg_resources import Requirement, resource_filename

import gc3libs
from gc3libs.cmdline import SessionBasedScript, existing_file
# from gc3libs.application.crypto import CryptoApplication
from gc3libs import Application, Run, Task, RetryableTask
import gc3libs.exceptions
import gc3libs.application
# from gc3libs.exceptions import *

if __name__ == "__main__":
    import gcrypto

class CryptoApplication(gc3libs.Application):
    """
    Run a Crypto job 
    """
    
    def __init__(self, start_range, step, input_files_archive, output, **kw):

        # set some execution defaults...
        kw.setdefault('requested_cores', 4)
        kw.setdefault('requested_architecture', Run.Arch.X86_64)
        kw.setdefault('requested_walltime', 1)
        # XXX: check whehter this is necessary
        kw.setdefault('output_dir', output)

        arguments = []
        arguments.append(start_range)
        arguments.append(step)
        arguments.append(kw['requested_cores'])
        arguments.append("input.tgz")

        src_crypto_bin = resource_filename(Requirement.parse("gc3pie"), 
                                           "gc3libs/etc/gnfs-cmd")

        inputs = {input_files_archive:"input.tgz", src_crypto_bin:"gnfs-cmd" }

        kw['tags'] = [ 'TEST/CRYPTO-1.0' ]

        gc3libs.Application.__init__(
            self,
            executable =  os.path.basename(src_crypto_bin),
            executables = ["input.tgz"],
            arguments = arguments, 
            inputs = inputs,
            # outputs = [ '@output.files' ],
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'gcrypto.log',
            join=True,
            **kw
            )

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

        # checks if in output_dir M*.gz files have been created.
        # Resubmit otherwise
        # os.listdir(self.output_dir)

        gc3libs.log.debug('Application terminated. postprocessing with execution.signal [%d]' % self.execution.exitcode)
        return


gc3libs.application.register(CryptoApplication, 'crypto')


class CryptoTask(RetryableTask, gc3libs.utils.Struct):

    def __init__(self, start_range, step, input_files_archive, output, **kw):
        RetryableTask.__init__(
            self,
            # task name
            # os.path.basename(start_range),
            str(start_range),
            # actual computational job
            CryptoApplication(start_range, step, input_files_archive, output, **kw),
            # keyword arguments
            **kw)

    def retry(self):
        return False

## the script itself

class GCryptoScript(SessionBasedScript):
    """
    gnfs-cmd begin length nth
    does computations for the range from begin to begin+length.
    The following ranges are of interest: 800M-1200M and 2100M-2400M.
    If begin is in the first one then the job takes about 4GB (it is
    probably possible to squeeze this below 2GB; if there are lots of
    machines with 2GB we can try this).
    If begin is in the second range, an 8-core-job takes about 6GB (or
    a 4-core-job takes 5.3GB).
    The run time of a job is roughly proportionally to length, but
    jobs in the range 2100M-2400M are faster than jobs in 800M-1200M.

    Takes as input four arguments:
    1. Initial value of the range (e.g. 800000000)
    2. steps (ot final value of the range) (e.g. 1200000000)
    3. increment (1000)
    4. inputfile archive location (e.g. lfc://lfc.smscg.ch/crypto/lacal/input.tgz)
    grypto 800000000 1200000000 1000
    will produce 400000 jobs
    job progress is
    monitored and, when a job is done,
    output is retrieved back to submitting host in a folder structure
    organized by 1.+increment*actual_step

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


    def setup_args(self):
        """
        Set up command-line argument parsing.

        The default command line parsing considers every argument as
        an (input) path name; processing of the given path names is
        done in `parse_args`:meth:
        """
        self.add_param('args', nargs='*', metavar='START_RANGE, END_RANGE, STEP', 
                  help="Like in a for loop, define:"
                       "Start range, "
                       "end range, "
                       "increment "
                       )
        

    def setup_options(self):
        self.add_param("-i", "--input_files", metavar="PATH",
                       action="store", dest="input_files_archive",
                       default="input.tgz", 
                       help="Reference to input_file archive."
                       "By default, a file named 'input.tgz' will "
                       "be searched in the current directory.")

    def parse_args(self):
         """
         Checks that self.params.args contains the three required arguments:
         1. Start range
         2. End range
         3. Step
         """
         if len(self.params.args) != 3:
             raise gc3libs.exceptions.InvalidUsage("Wrong number of input parameters. Got %d" % len(self.params.args))
         self.range_start = self.params.args[0]
         self.range_stop = self.params.args[1]
         self.range_step = self.params.args[2]


    def new_tasks(self, extra):
        for param in range(int(self.range_start), int(self.range_stop), int(self.range_step)):
            yield (
                # job name
                # gc3libs.utils.basename_sans(param),
                str(param),
                # task constructor
                gcrypto.CryptoTask,
                [ # parameters passed to the constructor, see `CryptoTask.__init__`
                    str(param), # Initial range
                    self.range_step, # step
                    self.params.input_files_archive, # path to input.tgz
                    self.params.output, # output folder
                    ],
                # extra keyword arguments passed to the constructor,
                # see `GeotopTask.__init__`
                extra.copy()
                )




    def __new_tasks(self, extra):
        inputs = self._search_for_input_files(self.params.args)

        self.log.info('Input search yeld [%d] files' % len(inputs))

        for path in inputs:
            kwargs = extra.copy()
            kwargs.setdefault('input_directives', self.params.directives)

            if self.instances_per_file > 1:
                for seqno in range(1, 1+self.instances_per_file, self.instances_per_job):
                    if self.instances_per_job > 1:
                        yield ("%s.%d--%s" % (gc3libs.utils.basename_sans(path),
                                              seqno, 
                                              min(seqno + self.instances_per_job - 1,
                                                  self.instances_per_file)),
                               self.application, [path], extra.copy())
                    else:
                        yield ("%s.%d" % (gc3libs.utils.basename_sans(path), seqno),
                               self.application, [path], kwargs)
            else:
                self.log.debug('Yelinding new task with input [%s]' % path)
                yield (gc3libs.utils.basename_sans(path),
                       self.application, [path], kwargs)

 

# run it
if __name__ == '__main__':
    GCryptoScript().run()
