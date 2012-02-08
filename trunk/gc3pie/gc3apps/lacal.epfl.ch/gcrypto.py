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
  2012-01-29:
    * Moved CryptoApplication from gc3libs.application

    * Restruxtured main script due to excessive size of initial
    jobs. SessionBaseScript generate a single SequentialTask.
    SequentialTask generated as many ParallelTasks as the whole range divided
    by the number of simultaneous active jobs.

    * Each ParallelTask lauches 'max_running' CryptoApplications
"""
__author__ = 'sergio.maffiolett@gc3.uzh.ch'
__docformat__ = 'reStructuredText'


import fnmatch
import logging
import os
import os.path
import sys
from pkg_resources import Requirement, resource_filename

import gc3libs
from gc3libs.cmdline import SessionBasedScript, existing_file
from gc3libs import Application, Run, Task, RetryableTask
import gc3libs.exceptions
import gc3libs.application
from gc3libs.dag import SequentialTaskCollection, ParallelTaskCollection, ChunkedParameterSweep

if __name__ == "__main__":
    import gcrypto_sequentialWF


class CryptoApplication(gc3libs.Application):
    """
    Run a Crypto job
    CryptoApplication(param, step, input_files_archive, output_folder, **kw)
    """
    
    def __init__(self, start_range, step, input_files_archive, output, **kw):

        # set some execution defaults...
        kw.setdefault('requested_cores', 4)
        kw.setdefault('requested_architecture', Run.Arch.X86_64)
        kw.setdefault('requested_walltime', 2)

        # XXX: check whehter this is necessary
        kw.setdefault('output_dir', os.path.join(output,str(int(start_range) + int(step))))

        arguments = []
        arguments.append(start_range)
        arguments.append(step)
        arguments.append(kw['requested_cores'])
        arguments.append("input.tgz")

        name = str(start_range + step)
        kw.setdefault('jobname', name)

        src_crypto_bin = resource_filename(Requirement.parse("gc3pie"), 
                                           "gc3libs/etc/gnfs-cmd")

        inputs = {input_files_archive:"input.tgz", src_crypto_bin:"gnfs-cmd" }

        # XXX: this will be changed once RTE will be validated
        # will use APPS/CRYPTO/LACAL-1.0
        kw['tags'] = [ 'TEST/CRYPTO-1.0' ]

        gc3libs.Application.__init__(
            self,
            executable =  os.path.basename(src_crypto_bin),
            executables = ["input.tgz"],
            arguments = arguments, 
            inputs = inputs,
            outputs = [ '@output.list' ],
            # outputs = gc3libs.ANY_OUTPUT,
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

        gc3libs.log.debug('Application terminated. postprocessing with execution.exicode [%d]' % self.execution.exitcode)
        return


gc3libs.application.register(CryptoApplication, 'crypto')

class CryptoChunkedParameterSweep(ChunkedParameterSweep):
    """
    provided the beginning of the range 'begin_range',
    the end of the range 'end_range',
    the step size of each job 'step',
    CryptoChunkedParameterSweep creates a chunked_size of CryptoApplication
    executed in parallel. Every update cycle it will check how many new
    CryptoApplication will have to be created
    (each of the launching in parallel
    DEFAULT_PARALLEL_RANGE_INCREMENT CryptoApplications)
    as the following rule:
    [ (end-range - begin_range) / step ] / DEFAULT_PARALLEL_RANGE_INCREMENT
    """
    
    def __init__(self, start_range, stop_range, step, max_running, input_files_archive, output_folder, grid=None, **kw):

        self.parameter_count_increment = int(step) * int(max_running)
        self.input_files_archive = input_files_archive
        self.output_folder = output_folder

        ChunkedParameterSweep.__init__(self, kw['jobname'], start_range, stop_range, step, max_running, grid)
        
    def new_task(self, param, **kw):
        """
        trust param value and create a new CryptoApplication
        from param to param+step.
        Ending condition is determined by ChunkedParameterSweep
        """

        return CryptoApplication(param, self.step, self.input_files_archive, self.output_folder, **kw)
        
## the script itself

class GCryptoScript(SessionBasedScript):
    """
    crypto execution pattern:
    $ gnfs-cmd begin length nth
    does computations for a range: begin to begin+length.
    nth is the number of threads spwaned.
    The following ranges are of interest: 800M-1200M and 2100M-2400M.

    gcrytpo pilot script takes as input three arguments:
    1. Initial value of the range (e.g. 800000000)
    2. steps (ot final value of the range) (e.g. 1200000000)
    3. increment (1000)

    e.g. grypto 800000000 1200000000 1000
    will produce 400000 jobs
    job progress is monitored and, when a job is done,
    output is retrieved back to submitting host in a folder structure
    organized by 'Initial value'+(increment*actual_step)

    The `gcrypto` command keeps a record of jobs (submitted, executed and
    pending) in a session file (set name with the '-s' option); at each
    invocation of the command, the status of all recorded jobs is updated,
    output from finished jobs is collected, and a summary table of all
    known jobs is printed.  New jobs are added to the session if new input
    files are added to the command line.

    inputfile archive location (e.g. lfc://lfc.smscg.ch/crypto/lacal/input.tgz)
    can be specified with the '-i' option. Otherwise a default filename
    'input.tgz' will be searched in current directory.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; `gcrypto` will delay submission
    of newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = CryptoApplication
            # input_filename_pattern = '*' # we do not need this, is 
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
        self.range_start = int(self.params.args[0])
        self.range_stop = int(self.params.args[1])
        self.range_step = int(self.params.args[2])

    def new_tasks(self, extra):
        yield (
            "LACAL_"+str(self.range_start), # jobname
            CryptoChunkedParameterSweep,
            [ # parameters passed to the constructor, see `CryptoSequence.__init__`
                self.range_start, # Initial range
                self.range_stop, # End range
                self.range_step, # step
                self.params.max_running, # increment of each ParallelTask
                self.params.input_files_archive, # path to input.tgz
                self.params.output, # output folder
                ],
            extra.copy()
            )

# run it
if __name__ == '__main__':
    GCryptoScript().run()
