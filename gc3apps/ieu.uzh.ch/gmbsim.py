#! /usr/bin/env python
#
#   gmbsim.py -- GC3Pie front-end for running the
#   "sim_run_dclone_design_test.R" by Mollie Brooks
#
#   Copyright (C) 2014, 2015  University of Zurich. All rights reserved.
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
Front-end script for running multiple `sim_run_dclone_design_test` instances.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gmbsim --help`` for program usage instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2014-03-03:
    * Initial release, forked off the ``gmhc_coev`` sources.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'
__version__ = '1.0.0'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == '__main__':
    import gmbsim
    gmbsim.GmbsimScript().run()


# std module imports
import csv
from itertools import product
import os
import re
import sys
import time
from pkg_resources import Requirement, resource_filename

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, existing_file
from collections import defaultdict
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask


## utilities

def repr_as_R(val):
    if isinstance(val, (list, tuple)):
        return ('c(' + str.join(',', [repr_as_R(v) for v in val]) + ')')
    elif isinstance(val, (str, unicode)):
        return val
    else:
        return repr(val)

def grep_not_matching(it, ignore=re.compile(r'^\s*(#.*)?$')):
    """
    Return items in `it` that do *not* match the `ignore` regexp.

    First argument `it` should be an iterator over string values.
    Second optional argument is a regexp that will be searched for
    in every value: values that match will be skipped.
    """
    while True:
        item = next(it)
        if not ignore.search(item):
            yield item


## custom application class

class GmbsimApplication(Application):
    """
    Custom class to wrap the execution of the ``sim_run_dclone_design_test``
    code by M. Brooks.
    """

    application_name = 'sim_run_dclone_design_test'

    def __init__(self,
                 scriptfile,   # path to the R script to run
                 datafiles,    # additional files to upload
                 days_of_the_week, # list of up to 7 int
                 sampling_exp, # list of 3 float
                 isolation_exp,# list of 4 int
                 detection_exp,# list of 7 float
                 nb, # see `R2jags::jags` param `n.burnin`
                 ni, # see `R2jags::jags` param `n.iter`
                 nt, # see `R2jags::jags` param `n.thin`
                 **extra_args):
        # use a wrapper script to drive remote run
        wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                       "gc3libs/etc/run_R.sh")
        inputs = { wrapper_sh:os.path.basename(wrapper_sh) }
        # upload R script
        exename = os.path.basename(scriptfile)
        run_name = './' + exename
        inputs[scriptfile] = exename
        # upload additional files
        for path in datafiles:
            inputs[path] = os.path.basename(path)
        # save command-line params
        self.days_of_the_week = days_of_the_week
        self.sampling_exp = sampling_exp
        self.isolation_exp = isolation_exp
        self.detection_exp = detection_exp
        self.nb = nb
        self.ni = ni
        self.nt = nt
        # provide defaults for envelope requests
        extra_args.setdefault('requested_cores',        4)
        extra_args.setdefault('requested_memory',       1*GB)
        extra_args.setdefault('requested_architecture', Run.Arch.X86_64)
        extra_args.setdefault('requested_walltime',     12*hours)
        # chain into `Application` superclass ctor
        Application.__init__(
            self,
            arguments=( #['/bin/echo'] +
                        ['./' + os.path.basename(wrapper_sh), run_name]
                        + [ repr_as_R(val) for val in (
                            days_of_the_week,
                            sampling_exp,
                            isolation_exp,
                            detection_exp,
                            nb,
                            ni,
                            nt,
                            extra_args['requested_cores'], # nc
                        )]),
            inputs = inputs,
            outputs = [
                'dclone_design_test_fits.Rdata',
                self.application_name + '.log',
            ],
            stdout = self.application_name + '.log',
            join=True,
            **extra_args)


## main script class

class GmbsimScript(SessionBasedScript):
    """
Read the specified INPUT ``.csv`` files and submit jobs according
to the content of those files.

The ``gmbsim`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``gmbsim`` will delay submission of
newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            input_filename_pattern = '*.csv',
            application = GmbsimApplication,
            # only display stats for the top-level policy objects
            #stats_only_for = GmbsimApplication,
            )


    def setup_options(self):
        self.add_param("-b", "-nb", "--burn-in", metavar="NUM",
                       dest="nb", default=1,
                       help="Execute NUM iterations for JAGS burn-in.")
        self.add_param("-i", "-ni", "--iter", metavar="NUM",
                       dest="ni", default=3,
                       help="Execute NUM JAGS main iterations.")
        self.add_param("-t", "-nt", "--thin", "--thinning", metavar="NUM",
                       dest="nt", default=1,
                       help="Execute NUM iterations for JAGS thinning.")
        # change default for the core/memory/walltime options
        self.actions['memory_per_core'].default = 1*Memory.GB
        self.actions['wctime'].default = '2 hours'
        self.actions['ncores'].default = 4



    def setup_args(self):
        self.add_param('replicates', type=int,
                       help="Number of replicates to run,"
                       " for each combination of experiment parameters.")
        self.add_param('sampling', type=existing_file,
                       help="File containing sampling experiment definition,"
                       " one per line.")
        self.add_param('isolation', type=existing_file,
                       help="File containing isolation experiment definition,"
                       " one per line.")
        self.add_param('detection', type=existing_file,
                       help="File containing detection experiment definition,"
                       " one per line.")
        self.add_param("scriptfile", type=existing_file, default=None,
                       help="Path to the `sim_run_dclone_design_test` script source.")
        self.add_param("datafiles", nargs='*', type=existing_file, default=[],
                       help="Additional data files to upload, e.g., `weird_dates_sdur.R`.")


    def make_directory_path(self, pathspec, jobname):
        # XXX: Work around SessionBasedScript.process_args() that
        # apppends the string ``NAME`` to the directory path.
        # This is really ugly, but the whole `output_dir` thing needs to
        # be re-thought from the beginning...
        if pathspec.endswith('/NAME'):
            return pathspec[:-len('/NAME')]
        else:
            return pathspec


    @staticmethod
    def read_param_file(filename):
        with open(filename, 'r') as stream:
            csvdata = csv.reader(grep_not_matching(stream))
            return [ tuple(item.replace(' ','') for item in row)
                     for row in csvdata ]


    def new_tasks(self, extra):
        dates_and_sampling_exps = self.read_param_file(self.params.sampling)
        isolation_exps = self.read_param_file(self.params.isolation)
        detection_exps = self.read_param_file(self.params.detection)

        for date_and_sampling, isolation, detection in product(dates_and_sampling_exps, isolation_exps, detection_exps):
            dates = date_and_sampling[0]
            sampling = date_and_sampling[1:]
            extra_args = extra.copy()
            # identify job uniquely -- replace forward slashes as this
            # will be part of a directory/file name
            basename = (str
                        .join('_', [GmbsimApplication.application_name]
                              + [ (param +'='+ repr_as_R(locals()[param]))
                                  for param in ('dates', 'sampling',
                                                'isolation', 'detection') ])
                        .replace('/', 'over'))

            # prepare job(s) to submit
            already = len([ task for task in self.session
                            if ((task.dates, task.sampling, task.isolation, task.detection)
                                == (dates, sampling, isolation, detection)) ])
            base_output_dir = extra_args.pop('output_dir', self.params.output)
            for n in range(already, self.params.replicates):
                jobname=('%s#%d' % (basename, n+1))
                yield GmbsimApplication(
                    scriptfile=self.params.scriptfile,
                    datafiles=self.params.datafiles,
                    days_of_the_week=dates,
                    sampling_exp=sampling,
                    isolation_exp=isolation,
                    detection_exp=detection,
                    nb=self.params.nb,
                    ni=self.params.ni,
                    nt=self.params.nt,
                    jobname=jobname,
                    output_dir=os.path.join(base_output_dir, jobname),
                    **extra_args)
