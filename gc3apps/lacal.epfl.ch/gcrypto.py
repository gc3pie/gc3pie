#! /usr/bin/env python
#
#   gcrypto.py -- Front-end script for submitting multiple Crypto jobs to SMSCG.
"""
Front-end script for submitting multiple ``gnfs-cmd`` jobs to SMSCG.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gcrypto --help`` for program usage instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2012-01-29:
    * Moved CryptoApplication from gc3libs.application

    * Restructured main script due to excessive size of initial
    jobs. SessionBaseScript generate a single SequentialTask.
    SequentialTask generated as many ParallelTasks as the whole range divided
    by the number of simultaneous active jobs.

    * Each ParallelTask lauches 'max_running' CryptoApplications
"""
__author__ = 'sergio.maffiolett@gc3.uzh.ch'
__docformat__ = 'reStructuredText'



# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gcrypto
    gcrypto.GCryptoScript().run()


# stdlib imports
import fnmatch
import logging
import os
import os.path
import sys
from pkg_resources import Requirement, resource_filename

# GC3Pie interface
import gc3libs
from gc3libs.cmdline import SessionBasedScript, existing_file, positive_int, nonnegative_int
from gc3libs import Application, Run, Task
import gc3libs.exceptions
import gc3libs.application
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection, ChunkedParameterSweep, RetryableTask

DEFAULT_INPUTFILE_LOCATION="srm://dpm.lhep.unibe.ch/dpm/lhep.unibe.ch/home/crypto/lacal_input_files.tgz"
DEFAULT_GNFS_LOCATION="srm://dpm.lhep.unibe.ch/dpm/lhep.unibe.ch/home/crypto/gnfs-cmd_20120406"

class CryptoApplication(gc3libs.Application):
    """
    Represent a ``gnfs-cmd`` job that examines the range `start` to `start+extent`.

    LACAL's ``gnfs-cmd`` invocation::

      $ gnfs-cmd begin length nth

    performs computations for a range: *begin* to *begin+length*,
    and *nth* is the number of threads spwaned.

    The following ranges are of interest: 800M-1200M and 2100M-2400M.

    CryptoApplication(param, step, input_files_archive, output_folder, **extra_args)
    """

    def __init__(self, start, extent, gnfs_location, input_files_archive, output, **extra_args):

        gnfs_executable_name = os.path.basename(gnfs_location)

        # # set some execution defaults...
        extra_args.setdefault('requested_cores', 4)
        extra_args.setdefault('requested_architecture', Run.Arch.X86_64)
        extra_args['jobname'] = "LACAL_%s" % str(start + extent)
        extra_args['output_dir'] = os.path.join(extra_args['output_dir'], str(start + extent))
        extra_args['tags'] = [ 'APPS/CRYPTO/LACAL-1.0' ]
        extra_args['executables'] = ['./gnfs-cmd']
        extra_args['requested_memory'] = Memory(
            int(extra_args['requested_memory'].amount() / float(extra_args['requested_cores'])),
            unit=extra_args['requested_memory'].unit)

        gc3libs.Application.__init__(
            self,

            arguments = [ "./gnfs-cmd", start, extent, extra_args['requested_cores'], "input.tgz" ],
            inputs = {
                input_files_archive:"input.tgz",
                gnfs_location:"./gnfs-cmd",
                },
            outputs = [ '@output.list' ],
            # outputs = gc3libs.ANY_OUTPUT,
            stdout = 'gcrypto.log',
            join=True,
            **extra_args
            )

    def terminated(self):
        """
        Checks whether the ``M*.gz`` files have been created.

        The exit status of the whole job is set to one of these values:

        *  0 -- all files processed successfully
        *  1 -- some files were *not* processed successfully
        *  2 -- no files processed successfully
        * 127 -- the ``gnfs-cmd`` application did not run at all.

        """
        # XXX: need to gather more info on how to post-process.
        # for the moment do nothing and report job's exit status

        if self.execution.exitcode:
            gc3libs.log.debug(
                'Application terminated. postprocessing with execution.exicode %d',
                self.execution.exitcode)
        else:
            gc3libs.log.debug(
                'Application terminated. No exitcode available')

        if self.execution.signal == 123:
            # XXX: this is fragile as it does not really applies to all
            # DataStaging errors.
            # Assume Data staging problem at the beginning of the job
            # resubmit
            self.execution.returncode = (0, 99)


class CryptoTask(RetryableTask):
    """
    Run ``gnfs-cmd`` on a given range
    """
    def __init__(self, start, extent, gnfs_location, input_files_archive, output, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            CryptoApplication(start, extent, gnfs_location, input_files_archive, output, **extra_args),
            # XXX: should decide which policy to use here for max_retries
            max_retries = 2,
            # keyword arguments
            **extra_args)

    def retry(self):
        """
        Resubmit a cryto application instance iff it exited with code 99.

        *Note:* There is currently no upper limit on the number of
        resubmissions!
        """
        if self.task.execution.exitcode == 99:
            return True
        else:
            return False


class CryptoChunkedParameterSweep(ChunkedParameterSweep):
    """
    Provided the beginning of the range `range_start`, the end of the
    range `range_end`, the slice size of each job `slice`,
    `CryptoChunkedParameterSweep` creates `chunk_size`
    `CryptoApplication`s to be executed in parallel.

    Every update cycle it will check how many new CryptoApplication
    will have to be created (each of the launching in parallel
    DEFAULT_PARALLEL_RANGE_INCREMENT CryptoApplications) as the
    following rule: [ (end-range - begin_range) / step ] /
    DEFAULT_PARALLEL_RANGE_INCREMENT
    """


    def __init__(self, range_start, range_end, slice, chunk_size,
                 input_files_archive, gnfs_location, output_folder, **extra_args):

        # remember for later
        self.range_start = range_start
        self.range_end = range_end
        self.parameter_count_increment = slice * chunk_size
        self.input_files_archive = input_files_archive
        self.gnfs_location = gnfs_location
        self.output_folder = output_folder
        self.extra_args = extra_args

        ChunkedParameterSweep.__init__(
            self, range_start, range_end, slice, chunk_size, **self.extra_args)

    def new_task(self, param, **extra_args):
        """
        Create a new `CryptoApplication` for computing the range
        `param` to `param+self.parameter_count_increment`.
        """
        return CryptoTask(param, self.step, self.gnfs_location, self.input_files_archive, self.output_folder, **self.extra_args.copy())




## the script itself

class GCryptoScript(SessionBasedScript):
    # this will be display as the scripts' `--help` text
    """
Like a `for`-loop, the ``gcrypto`` driver script takes as input
three mandatory arguments:

1. RANGE_START: initial value of the range (e.g., 800000000)
2. RANGE_END: final value of the range (e.g., 1200000000)
3. SLICE: extent of the range that will be examined by a single job (e.g., 1000)

For example::

  gcrypto 800000000 1200000000 1000

will produce 400000 jobs; the first job will perform calculations
on the range 800000000 to 800000000+1000, the 2nd one will do the
range 800001000 to 800002000, and so on.

Inputfile archive location (e.g. lfc://lfc.smscg.ch/crypto/lacal/input.tgz)
can be specified with the '-i' option. Otherwise a default filename
'input.tgz' will be searched in current directory.

Job progress is monitored and, when a job is done,
output is retrieved back to submitting host in folders named:
'range_start + (slice * actual step)'

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
            stats_only_for = CryptoApplication,
            )


    def setup_args(self):
        """
        Set up command-line argument parsing.

        The default command line parsing considers every argument as
        an (input) path name; processing of the given path names is
        done in `parse_args`:meth:
        """

        # self.add_param('args',
        #                nargs='*',
        #                metavar=
        #                """
        #                [range_start] [range_end] [slice],
        #                help=[range_start]: Positive integer value of the range start.
        #                [range_end]: Positive integer value of the range end.
        #                [slice]: Positive integer value of the increment.
        #                """
        #                )


        self.add_param('range_start', type=nonnegative_int,
                  help="Non-negative integer value of the range start.")
        self.add_param('range_end', type=positive_int,
                  help="Positive integer value of the range end.")
        self.add_param('slice', type=positive_int,
                  help="Positive integer value of the increment.")

    def parse_args(self):
        # XXX: why is this necessary ? shouldn't add_params of 'args' handle this ?
        # check on the use of nargs and type.
        # if len(self.params.args) != 3:
        #     raise ValueError("gcrypto takes exaclty 3 arguments (%d are given)" % len(self.params.args))
        # self.params.range_start = int(self.params.args[0])
        # self.params.range_end = int(self.params.args[1])
        # self.params.slice = int(self.params.args[2])

        if self.params.range_end <= self.params.range_start:
            # Failed
            raise ValueError("End range cannot be smaller than Start range. Start range %d. End range %d" % (self.params.range_start, self.params.range_end))

    def setup_options(self):
        self.add_param("-i", "--input-files", metavar="PATH",
                       action="store", dest="input_files_archive",
                       default=DEFAULT_INPUTFILE_LOCATION,
                       help="Path to the input files archive."
                       " By default, the preloaded input archive available on"
                       " SMSCG Storage Element will be used: "
                       " %s" % DEFAULT_INPUTFILE_LOCATION)
        self.add_param("-g", "--gnfs-cmd", metavar="PATH",
                       action="store", dest="gnfs_location",
                       default=DEFAULT_GNFS_LOCATION,
                       help="Path to the executable script (gnfs-cmd)"
                       " By default, the preloaded gnfs-cmd available on"
                       " SMSCG Storage Element will be used: "
                       " %s" % DEFAULT_GNFS_LOCATION)


    def new_tasks(self, extra):
        yield (
            "%s-%s" % (str(self.params.range_start),str(self.params.range_end)), # jobname
            CryptoChunkedParameterSweep,
            [ # parameters passed to the constructor, see `CryptoSequence.__init__`
                self.params.range_start,
                self.params.range_end,
                self.params.slice,
                self.params.max_running, # increment of each ParallelTask
                self.params.input_files_archive, # path to input.tgz
                self.params.gnfs_location, # path to gnfs-cmd
                self.params.output, # output folder
                ],
            extra.copy()
            )


    def before_main_loop(self):
        """
        Ensure each instance of `ChunkedParameterSweep` has
        `chunk_size` set to the maximum allowed number of jobs.
        """
        for task in self.session:
            assert isinstance(task, CryptoChunkedParameterSweep)
            task.chunk_size = self.params.max_running
