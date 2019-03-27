#! /usr/bin/env python
#
#   gmphili.py -- Parallel runs of M. Th. Philipp's "stability" pipeline
#
#   Copyright (C) 2016  University of Zurich. All rights reserved.
#
#   This program is free software: you can redistribute it and/or modify it
#   under the terms of the GNU General Public License as published by the Free
#   Software Foundation, either version 3 of the License, or (at your option)
#   any later version.
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
"""

from __future__ import absolute_import, print_function
import csv
import os

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, existing_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gmphili
    gmphili.GMphiliScript().run()


__version__ = '1.0'
# summary of user-visible changes
__changelog__ = """
  2016-09-21:
  * Initial version
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'


class GMphiliScript(SessionBasedScript):
    """
    Read a `.csv` file and invoke the 'stability' R code once per each row.

    The CSV input file is expected to have 5 columns: 'dgp', 'learner',
    'cldist', 'dim', 'n'. Each 'stability' job will be called with these 5
    arguments, in the order they are given in the input file.

    The ``gmphili`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gmphili`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version=__version__, # module version == script version
            application=GMphiliApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for=GMphiliApplication,
        )


    def setup_args(self):
        self.add_param(
            'scenarios', nargs='+', type=existing_file,
            help=(
                "CSV file(s) listing program invocation parameters."
                " It is expected to have the following 5 columns:"
                " 'dgp', 'learner', 'cldist', 'dim', 'n'."
            ))


    def new_tasks(self, extra):
        tasks = []
        for scenario in self.params.scenarios:
            with open(scenario, 'r') as input_file:
                header, dialect = _csv_features(input_file)
                input_csv = csv.reader(input_file, dialect)
                # skip header row
                if header:
                    input_csv.next()
                for row in input_csv:
                    extra_args = extra.copy()
                    self.log.info("Adding simulation with parameters %s", row)
                    tasks.append(GMphiliApplication(*row, **extra_args))

        return tasks


class GMphiliApplication(Application):
    """
    Run M. Philipp's `runsim.R` pipeline.
    """

    def __init__(self, dgp, learner, cldist, dim, n, **extra_args):
        # save execution parameters
        self.dgp = dgp
        self.learner = learner
        self.cldist = cldist
        self.dim = dim
        self.n = n
        # provide defaults
        extra_args.setdefault('requested_cores', 1)
        # initialize GC3Pie Application object
        super(GMphiliApplication, self).__init__(
            # command-line to run
            [
                # GC3Pie wrapper script
                './run_R.sh',
                # actual R code invocation
                'runsim', dgp, learner, cldist, dim, n,
                # how many cores to use for parallel code
                extra_args.get('requested_cores', 1),
            ],
            # input files
            inputs=[
                resource_filename(Requirement.parse('gc3pie'),
                                  'gc3libs/etc/run_R.sh'),
                'stability_v0.1-4.R',
                'runsim.R',
            ],
            # output files
            outputs=[
                'siminfo.rda',
                'simres.rda'
            ],
            stdout='runsim.log',
            stderr='runsim.log',
            # output dir, etc.
            **extra_args
        )


def _csv_features(input_file, sample_size=1024):
    """
    Guess dialect and other features of a CSV file from its initial contents.
    Return a tuple consisting of:

    * ``header``: a boolean flag indicating whether the first row is a header
      line;
    * ``dialect``: a `csv.Dialect` instance, or ``None`` if auto-detection of
      dialect failed.

    Reads the first `sample_size` bytes from the file and analyzes them, then
    rewinds the file to the beginning. Expects the passed file-like object to
    be seekable, and the stream to be positioned at the beginning of the CSV
    content.

    This is just a convenient interface for the `Sniffer`:class: in Python's
    standard `csv` module.
    """
    startpos = input_file.tell()
    detect = csv.Sniffer()
    sample = input_file.read(sample_size)
    dialect = detect.sniff(sample)
    header = detect.has_header(sample)
    input_file.seek(startpos, 0)  ## rewind
    return header, dialect
