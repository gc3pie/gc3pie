#! /usr/bin/env python
#
#   gmhc_coev.py -- Front-end script for submitting multiple `MHC_coev` jobs to SMSCG.
#
#   Copyright (C) 2011-2012  University of Zurich. All rights reserved.
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
"""
Front-end script for submitting multiple `MHC_coev` jobs to SMSCG.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gmhc_coev --help`` for program usage instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2011-07-22:
    * Use the ``APPS/BIO/MHC_COEV-040711`` run time tag to select execution sites.
  2011-06-15:
    * Initial release, forked off the ``ggamess`` sources.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == '__main__':
    import gmhc_coev
    gmhc_coev.GMhcCoevScript().run()


# std module imports
import csv
import glob
import math
import os
import re
import shutil
import sys
import time

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript
from collections import defaultdict
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import SequentialTaskCollection


## auxilirary functions

def _increase(value, inflection, ratio=2, increment=2):
    """
    Return increased `value`.

    The new value is `value` multiplied by `ratio` if the product is
    less than `inflection`; otherwise, it's `value` plus `increment`.
    In other words, values grow exponentially until the inflection
    point, after which they grow linearly.
    """
    if value > inflection:
        return (ratio * value)
    else:
        return (value + increment)


## custom application class

class GMhcCoevApplication(Application):
    """
    Custom class to wrap the execution of a single step of the
    ``MHC_coev_*`` program by A. B. Wilson and collaborators.
    """

    application_name = 'mhc_coev'

    def __init__(self,
                 N, p_mut_coeff, choose_or_rand, sick_or_not, off_v_last,
                 output_dir, latest_work=None, executable=None, **extra_args):
        extra_args.setdefault('requested_memory', 1*GB)
        extra_args.setdefault('requested_cores', 1)
        extra_args.setdefault('requested_architecture', Run.Arch.X86_64)
        # command-line parameters to pass to the MHC_coev_* program
        self.N = N
        self.p_mut_coeff = p_mut_coeff
        self.choose_or_rand = choose_or_rand
        self.sick_or_not = sick_or_not
        self.off_v_last = off_v_last
        if executable is not None:
            # use the specified executable
            executable_name = './' + os.path.basename(executable)
            inputs = { executable:os.path.basename(executable) }
        else:
            # use the default one provided by the RTE
            executable_name = '/$MHC_COEV'
            inputs = { }
        if latest_work is not None:
            inputs[latest_work] = 'latest_work.mat'
        Application.__init__(self, [
                                 executable_name,
                                 # use `single_run_time` as the
                                 # maximum allowed time before saving
                                 # the MatLab workspace, but allow 5
                                 # minutes for I/O before the job is
                                 # killed forcibly by the batch system
                                 extra_args.get('requested_walltime').amount(minutes) - 5,
                                 # rest of `MHC_coev` args as passed
                                 N, p_mut_coeff, choose_or_rand, sick_or_not, off_v_last,
                             ],
                             inputs = inputs,
                             outputs = gc3libs.ANY_OUTPUT,
                             output_dir = output_dir,
                             stdout = 'matlab.log',
                             stderr = 'matlab.err',
                             tags = [ 'TEST/MHC_COEV-040711ML2012' ],
                             **extra_args)


class GMhcCoevTask(SequentialTaskCollection):
    """
    Custom class to wrap the execution of the ``MHC_coev_*` program
    by A. B. Wilson and collaborators.

    Execution continues in steps of predefined duration until a
    specified number of generations have been computed and saved.  All
    the output files ever produced are collected in the path specified
    by the `output_dir` parameter to `__init__`.

    """

    def __init__(self, single_run_duration, generations_to_do,
                 N, p_mut_coeff, choose_or_rand, sick_or_not, off_v_last,
                 output_dir, executable=None, **extra_args):

        """Create a new task running an ``MHC_coev`` binary.

        Each binary is expected to run for `single_run_duration`
        minutes and dump its state in file ``latest_work.mat`` if it's
        not finished. This task will continue re-submitting the same
        executable together with the saved workspace until
        `generations_to_do` generations have been computed.

        :param single_run_duration:
          Duration of a single step (as a `gc3libs.quantity.Duration`:class: value)

        :param int generations_to_do:
          Count of generations that ``MHC_coev`` should simulate.

        :param              N: Passed unchanged to the MHC_coev program.
        :param    p_mut_coeff: Passed unchanged to the MHC_coev program.
        :param choose_or_rand: Passed unchanged to the MHC_coev program.
        :param    sick_or_not: Passed unchanged to the MHC_coev program.
        :param     off_v_last: Passed unchanged to the MHC_coev program.

        :param str output_dir:
          Path to a directory where output files
          from all runs should be collected.

        :param str executable:
          Path to the ``MHC_coev`` executable binary, or `None`
          (default) to specify that the default version available on
          the execution site should be used.

        """
        # remember values for later use
        self.executable = executable
        self.output_dir = output_dir
        # allow 5 extra minutes for final saving the MatLab workspace
        self.single_run_duration = single_run_duration + 5*minutes
        self.generations_to_do = generations_to_do
        self.p_mut_coeff = p_mut_coeff
        self.N = N
        self.choose_or_rand = choose_or_rand
        self.sick_or_not = sick_or_not
        self.off_v_last = off_v_last
        self.extra = extra_args

        self.generations_done = 0

        # make up a sensibel job name if there isn't one
        if 'jobname' in extra_args:
            self.jobname = extra_args['jobname']
        else:
            if self.executable is None:
                self.jobname = ('MHC_coev.%s.%s.%s.%s.%s'
                                % (N, p_mut_coeff, choose_or_rand, sick_or_not, off_v_last))
            else:
                os.path.basename(self.executable)

        # remove potentially conflicting kyword arguments
        extra_args.pop('output_dir', None)
        extra_args.pop('requested_walltime', None)

        # create initial task and register it
        initial_task = GMhcCoevApplication(
            N, p_mut_coeff, choose_or_rand, sick_or_not, off_v_last,
            output_dir = os.path.join(output_dir, 'tmp'),
            executable = self.executable,
            requested_walltime = self.single_run_duration,
            **extra_args)
        SequentialTaskCollection.__init__(self, [initial_task])


    # regular expression for extracting the generation no. from an output file name
    GENERATIONS_FILENAME_RE = re.compile(r'(?P<generation_no>[0-9]+)gen\.mat$')

    def next(self, done):
        """
        Analyze the retrieved output and decide whether to submit
        another run or not, depending on whether there is a
        ``latest_work.mat`` file.
        """
        task_output_dir = self.tasks[done].output_dir
        if not os.path.exists(task_output_dir):
            # no output, resubmit immediately
            return done
        stdout_filename = self.tasks[done].stdout
        stderr_filename = self.tasks[done].stderr
        last_run = self.tasks[done].execution
        exclude = [
            os.path.basename(self.tasks[done].arguments[0]),
            stdout_filename,
            stderr_filename,
            ]
        # move files one level up, except the ones listed in
        # `exclude`, and read the contents of STDOUT/STDERR into
        # `job_out` and `job_err`
        stdout_contents = ''
        stderr_contents = ''
        generation_files_count = 0
        for entry in os.listdir(task_output_dir):
            src_entry = os.path.join(task_output_dir, entry)
            # concatenate all output files together
            if entry == stdout_filename:
                gc3libs.utils.cat(src_entry, output=os.path.join(self.output_dir, entry), append=True)
                stdout_contents = gc3libs.utils.read_contents(src_entry)
            if entry == stderr_filename:
                gc3libs.utils.cat(src_entry, output=os.path.join(self.output_dir, entry), append=True)
                stderr_contents = gc3libs.utils.read_contents(src_entry)
            if entry in exclude or (entry.startswith('script.') and entry.endswith('.sh')):
                # delete entry and continue with next one
                os.remove(src_entry)
                continue
            # if `entry` is a generation output file, get the
            # generation no. and update the generation count
            match = GMhcCoevTask.GENERATIONS_FILENAME_RE.search(entry)
            if match:
                generation_files_count += 1
                generation_no = int(match.group('generation_no'))
                self.generations_done = max(self.generations_done, generation_no)
            # now really move file one level up
            dest_entry = os.path.join(self.output_dir, entry)
            if os.path.exists(dest_entry):
                # backup with numerical suffix
                gc3libs.utils.backup(dest_entry)
            os.rename(os.path.join(task_output_dir, entry), dest_entry)
        os.removedirs(task_output_dir)

        # scan for common failures and take correction measures
        report_filename = os.path.join(self.output_dir, stdout_filename)
        if os.path.exists(report_filename):
            report = open(report_filename, 'a')
        else:
            report = open(report_filename, 'w')
        report.write(
            "gmhc_coev: About last job: duration=%s, exitcode=%d, max_used_memory=%s\n"
            % (last_run.duration, last_run.exitcode, last_run.max_used_memory))


        # 0. did the job run at all?
        if last_run.duration < 5*minutes:
            report.write(
                "gmhc_coev: Job %s did not run at all,"
                " will try resubmitting to a different resource.\n"
                % (self,))
            # resubmit
            return done

        # 1. out of memory?
        elif (# generic memory error
            (last_run.max_used_memory >= self.extra['requested_memory'])
            # MATLAB-specific error condition
            or (last_run.exitcode == 255
                and ('Out of memory.' in stderr_contents
                     or 'MATLAB:nomem' in stderr_contents))):
            self.extra['requested_memory'] = _increase(self.extra['requested_memory'], 8*GB, increment=2*GB) # start growing linearly at 8GB
            report.write(
                "gmhc_coev: Possible out-of-memory condition detected in job %s,"
                " will request %s for next run.\n"
                % (self.tasks[done], self.requested_memory,))

        # 2. not enough time to compute even one generation?
        elif (generation_files_count == 0
              # be sure that the job has run for at least the expected time, +/- 30 minutes
              and (last_run.duration > (self.single_run_duration - 30*minutes))):
            self.single_run_duration = _increase(self.single_run_duration, 24*hours,
                                                 increase=4*hours)
            report.write(
                "gmhc_coev: Job %s could not complete 1 generation in the allotted time,"
                " will request %s for next run.\n"
                % (self.tasks[done], self.single_run_duration,))

        # if a `latest_work.mat` file exists, then we need
        # more time to compute the required number of generations
        latest_work = os.path.join(self.output_dir, 'latest_work.mat')
        if not os.path.exists(latest_work):
            latest_work = None
            report.write(
                "gmhc_coev: No `latest_work.mat` file was produced,"
                " next job will start from k=1.\n")
        if self.generations_done < self.generations_to_do:
            gc3libs.log.debug("Computed %s generations, %s to do; submitting another job."
                              % (self.generations_done, self.generations_to_do))
            report.write("gmhc_coev:"
                         " Computed %s generations, %s to do; submitting another job.\n"
                         % (self.generations_done, self.generations_to_do))
            report.close()
            self.add(
                GMhcCoevApplication(self.N, self.p_mut_coeff, self.choose_or_rand, self.sick_or_not, self.off_v_last,
                                    output_dir = os.path.join(self.output_dir, 'tmp'),
                                    latest_work = latest_work,
                                    executable = self.executable,
                                    requested_walltime = self.single_run_duration,
                                    **self.extra))
            return Run.State.RUNNING
        else:
            gc3libs.log.debug("Computed %s generations, no more to do."
                              % (self.generations_done))
            report.write("gmhc_coev:"
                         " Computed %s generations, no more to do.\n"
                         % (self.generations_done,))
            report.close()
            self.execution.returncode = self.tasks[done].execution.returncode
            return Run.State.TERMINATED


## main script class

class GMhcCoevScript(SessionBasedScript):
    """
Scan the specified INPUT directories recursively for executable files
whose name starts with the string ``MHC_coev``, and submit a job for
each file found; job progress is monitored and, when a job is done,
its ``.sav`` output files are retrieved back into the same directory
where the executable file is (this can be overridden with the ``-o``
option).

The ``gmhc_coev`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``gmhc_coev`` will delay submission of
newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            input_filename_pattern = 'MHC_coev_*',
            application = GMhcCoevTask,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GMhcCoevTask,
            )

    def setup_options(self):
        self.add_param("-G", "--generations", metavar="NUM",
                       dest="generations", type=int, default=3000,
                       help="Compute NUM generations (default: 3000).")
        self.add_param("-x", "--executable", metavar="PATH",
                       dest="executable", default=None,
                       help="Path to the MHC_coev_* executable file.")


    def make_directory_path(self, pathspec, jobname):
        # XXX: Work around SessionBasedScript.process_args() that
        # apppends the string ``NAME`` to the directory path.
        # This is really ugly, but the whole `output_dir` thing needs to
        # be re-thought from the beginning...
        if pathspec.endswith('/NAME'):
            return pathspec[:-len('/NAME')]
        else:
            return pathspec


    def new_tasks(self, extra):
        # how many iterations are we already computing (per parameter set)?
        iters = defaultdict(lambda: 0)
        for task in self.session:
            name, instance = task.jobname.split('#')
            iters[name] = max(iters[name], int(instance))

        for path in self.params.args:
            if path.endswith('.csv'):
                try:
                    inputfile = open(path, 'r')
                except (OSError, IOError), ex:
                    self.log.warning("Cannot open input file '%s': %s: %s",
                                     path, ex.__class__.__name__, str(ex))
                try:
                    # the `csv.sniff()` function is confused by blank and comment lines,
                    # so we need to filter the input to build a correct sample
                    sample_lines = [ ]
                    while len(sample_lines) < 5:
                        line = inputfile.readline()
                        # exit at end of file
                        if line == '':
                            break
                        # ignore comment lines as they confuse `csv.sniff`
                        if line.startswith('#') or line.strip() == '':
                            continue
                        sample_lines.append(line)
                    csv_dialect = csv.Sniffer().sniff(str.join('', sample_lines))
                    self.log.debug("Detected CSV delimiter '%s'", csv_dialect.delimiter)
                except csv.Error:
                    # in case of any auto-detection failure, fall back to the default
                    self.log.warning("Could not determine field delimiter in file '%s',"
                                     " assuming it's a comma character (',').",
                                     path)
                    csv_dialect = 'excel'
                inputfile.seek(0)
                for lineno, row in enumerate(csv.reader(inputfile, csv_dialect)):
                    # ignore blank and comment lines (those that start with '#')
                    if len(row) == 0 or row[0].startswith('#'):
                        continue
                    try:
                        (iterno, N_str, p_mut_coeff_str, choose_or_rand_str, sick_or_not_str, off_v_last_str) = row
                    except ValueError:
                        self.log.error("Wrong format in line %d of file '%s':"
                                       " need 6 comma-separated values, but only got %d ('%s')."
                                       " Ignoring input line, fix it and re-run.",
                                       lineno+1, path, len(row), str.join(',', (str(x) for x in row)))
                        continue # with next `row`
                    # extract parameter values
                    try:
                        iterno = int(iterno)
                        N = GMhcCoevScript._parse_N(N_str)
                        p_mut_coeff = GMhcCoevScript._parse_p_mut_coeff(p_mut_coeff_str)
                        choose_or_rand = GMhcCoevScript._parse_choose_or_rand(choose_or_rand_str)
                        sick_or_not = GMhcCoevScript._parse_sick_or_not(sick_or_not_str)
                        off_v_last = GMhcCoevScript._parse_off_v_last(off_v_last_str)
                    except ValueError, ex:
                        self.log.warning("Ignoring line '%s' in input file '%s': %s",
                                         str.join(',', row), path, str(ex))
                        continue
                    basename = ('MHC_coev_' +
                                GMhcCoevScript._params_to_str(
                                    N, p_mut_coeff, choose_or_rand,
                                    sick_or_not, off_v_last))

                    # prepare job(s) to submit
                    if (iterno > iters[basename]):
                        self.log.info(
                                "Requested %d iterations for %s: %d already in session, preparing %d more",
                                iterno, basename, iters[basename], iterno - iters[basename])
                        for iter in range(iters[basename]+1, iterno+1):
                            kwargs = extra.copy()
                            base_output_dir = kwargs.pop('output_dir', self.params.output)
                            yield GMhcCoevTask(
                                self.params.walltime,    # single_run_duration
                                self.params.generations,
                                N,
                                p_mut_coeff,
                                choose_or_rand,
                                sick_or_not,
                                off_v_last,
                                executable=self.params.executable,
                                jobname=('%s#%d' % (basename, iter)),
                                output_dir = os.path.join(
                                    base_output_dir,
                                    ("N%d" % N),    # population size
                                    basename,       # params,
                                    str(iter),      # this iteration nr.
                                    ),
                                **kwargs)

            else:
                self.log.error("Ignoring input file '%s': not a CSV file.", path)


    ##
    ## INTERNAL METHODS
    ##


    _P_MUT_COEFF_RE = re.compile(r'((?P<num>[0-9]+)x)?10min(?P<exp>[0-9]+)')

    @staticmethod
    def _parse_p_mut_coeff(p_mut_coeff_str):
        try:
            return float(p_mut_coeff_str)
        except ValueError:
            match = GMhcCoevScript._P_MUT_COEFF_RE.match(p_mut_coeff_str)
            if not match:
                raise ValueError("Cannot parse P_MUT_COEFF expression '%s'" % p_mut_coeff_str)
            return float(match.group('num')) * 10.0**(-int(match.group('exp')))

    _N_RE = re.compile(r'N(?P<N>[0-9]+)')

    @staticmethod
    def _parse_N(N_str):
        try:
            return int(N_str)
        except ValueError:
            match = GMhcCoevScript._N_RE.match(N_str)
            if not match:
                raise ValueError("Cannot parse N expression '%s'" % N_str)
            return int(match.group('N'))

    @staticmethod
    def _parse_choose_or_rand(choose_or_rand_str):
        if choose_or_rand_str == "RM":
            return 1
        elif choose_or_rand_str == "DMAM":
            return 2
        elif choose_or_rand_str == "DMSSGD":
            return 3
        else:
            raise ValueError("Cannot parse CHOOSE_OR_RAND expression '%s'" % choose_or_rand_str)

    @staticmethod
    def _parse_sick_or_not(sick_or_not):
        if sick_or_not == "pat_on":
            return 1
        elif sick_or_not == "pat_off":
            return 0
        else:
            raise ValueError("Cannot parse SICK_OR_NOT expression '%s'" % sick_or_not)

    @staticmethod
    def _parse_off_v_last(off_v_last):
        try:
            return float(off_v_last)
        except ValueError:
            if not off_v_last.startswith("offval_"):
                raise ValueError("Cannot parse OFF_V_LAST expression '%s'" % off_v_last)
            off_v_last = off_v_last[7:]
            if off_v_last.startswith("0"):
                return float("0." + off_v_last[1:])
            elif off_v_last.startswith("1"):
                return 1.0
            else:
                raise ValueError("Cannot parse OFF_V_LAST expression '%s'" % off_v_last)

    @staticmethod
    def _string_to_params(s):
        """
        Return a tuple (N, p_mut_coeff, choose_or_rand, sick_or_not, off_v_last)
        obtained by parsing the string `s`.
        """
        p_mut_coeff, N, choose_or_rand, sick_or_not, off_v_last = s.split('__')

        return (GMhcCoevScript._parse_N(N),
                GMhcCoevScript._parse_p_mut_coeff(p_mut_coeff),
                GMhcCoevScript._parse_choose_or_rand(choose_or_rand),
                GMhcCoevScript._parse_sick_or_not(sick_or_not),
                GMhcCoevScript._parse_off_v_last(off_v_last))

    @staticmethod
    def _N_to_str(N):
        return ("N%d" % N)

    @staticmethod
    def _p_mut_coeff_to_str(p_mut_coeff):
        """
        Print the floating-point number `p_mut_coeff` as ``1x10min3``
        instead of the usual ``1e-3``.

        Examples::

          >>> _p_mut_coeff_to_str(0.005)
          '5x10min3'
          >>> _p_mut_coeff_to_str(10)
          '1x10plus1'
          >>> _p_mut_coeff_to_str(0.1234)
          '1.234x10min1'

        """
        exponent = math.log10(p_mut_coeff)
        mantissa = p_mut_coeff * (10 ** math.ceil(-exponent))
        # format output string
        if (mantissa == int(mantissa)):
            # no fractional part
            result = str(int(mantissa))
        else:
            result = str(mantissa)
        if exponent != 0:
            if exponent < 0:
                result += ('x10min%d' % (-int(exponent),))
            else:
                result += ('x10plus%d' % (int(exponent),))
        return result

    @staticmethod
    def _choose_or_rand_to_str(choose_or_rand):
        if choose_or_rand == 1:
            return "RM"
        elif choose_or_rand == 2:
            return "DMAM"
        elif choose_or_rand == 3:
            return "DMSSGD"
        else:
            raise ValueError("Valid values for `choose_or_rand` are: 1,2,3;"
                             " got '%s' instead" % choose_or_rand)

    @staticmethod
    def _sick_or_not_to_str(sick_or_not):
        if sick_or_not:
            return "pat_on"
        else:
            return "pat_off"

    @staticmethod
    def _off_v_last_to_str(off_v_last):
        if off_v_last < 0.0 or off_v_last > 1.0:
            raise ValueError("Parameter `off_v_last` must be in range 0.0 to 1.0;"
                             " got `%s`instead." % off_v_last)
        if off_v_last == 1.0:
            return "offval_1"
        else:
            # off_v_last == 0.xxxx

            # XXX: Python switches to scientific notation for
            # N<0.00001; the funny %-hack below ensures that the
            # dot+digits notation is used instead; starting Python
            # 2.6, the `format` function is available and should be
            # preferred:: off_v_last_str = format(off_v_last, 'f')
            off_v_last_str = ("%.16f" % off_v_last).rstrip('0')
            return ('offval_0' + off_v_last_str[2:])

    @staticmethod
    def _params_to_str(N, p_mut_coeff, choose_or_rand, sick_or_not, off_v_last):
        return str.join('__', [
            GMhcCoevScript._p_mut_coeff_to_str(p_mut_coeff),
            GMhcCoevScript._N_to_str(N),
            GMhcCoevScript._choose_or_rand_to_str(choose_or_rand),
            GMhcCoevScript._sick_or_not_to_str(sick_or_not),
            GMhcCoevScript._off_v_last_to_str(off_v_last),
            ])
