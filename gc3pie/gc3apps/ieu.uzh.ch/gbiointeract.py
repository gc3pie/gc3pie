#! /usr/bin/env python
#
#   gbiointeract.py -- Front-end script for submitting multiple
#   `biointeract` jobs.
#
#   Copyright (C) 2013 GC3, University of Zurich
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
Front-end script for submitting multiple `biointeract` jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gbiointeract --help`` for program usage instructions.
"""

__version__ = 'development version (SVN $Revision$)'
__author__ = 'Antonio Messina <antonio.s.messina@gmail.com>'
__docformat__ = 'reStructuredText'

import decimal
import itertools
import os
import tempfile

import gc3libs
from gc3libs import Application, Run
from gc3libs.cmdline import SessionBasedScript
from gc3libs.workflow import ChunkedParameterSweep
from gc3libs.utils import write_contents

def arange(start, stop=None, step=1, precision=None):
    """arange generates a set of Decimal values over the
    range [start, stop) with step size step

    arange([start,] stop, [step [,precision]])

    Courtesy of Nisan Haramati:
    http://code.activestate.com/recipes/66472-frange-a-range-function-with-float-increments/#c14
    """

    if stop is None:
        for x in xrange(int(ceil(start))):
            yield x
    else:
        # find precision
        if precision is not None:
            decimal.getcontext().prec = precision
        # convert values to decimals
        start = decimal.Decimal(start)
        stop = decimal.Decimal(stop)
        step = decimal.Decimal(step)
        # create a generator expression for the index values
        indices = (
            i for i in xrange(
                0,
                ((stop-start)/step).to_integral_value()
            )
        )
        # yield results
        for i in indices:
            yield float(start + step*i)


class GBiointeractApplication(Application):
    """
    Class to wrap execution of a single `biointeract` program.
    """

    application_name = "gbiointeract"

    def __init__(self,
                 executable,
                 cell_diffusion,
                 public_good_diffusion,
                 public_good_durability,
                 death_rate,
                 **extra_args):
        extra_args.setdefault('requested_cores', 1)

        arguments = ['./'+os.path.basename(executable),
                     '-c', str(float(cell_diffusion)),
                     '-p', str(float(public_good_diffusion)),
                     '-d', str(int(public_good_durability)),
                     '-x', str(float(death_rate)),
                     '-D', 'data']

        extra_args['jobname'] = "GBiointeract_cdiff:" + \
        "%f_pgdiff:%f_dur:%f_deathrate:%f" % (cell_diffusion,
                                              public_good_diffusion,
                                              public_good_durability, death_rate)
        if 'output_dir' in extra_args:
            extra_args['output_dir'] = os.path.join(
                os.path.dirname(extra_args['output_dir']),
                extra_args['jobname'])

        executable_script = """#!/bin/sh

# Create data directory
mkdir data

# Run the script

%s

# Compress all output files
gzip data/*.txt
""" % str.join(' ', arguments)
        try:
            (fd, self.tmp_filename) = tempfile.mkstemp(prefix='c3pie-gbiointeract')
            write_contents(self.tmp_filename, executable_script)
            os.chmod(self.tmp_filename, 0755)
        except Exception, ex:
            gc3libs.log.debug("Error creating execution script."
                              "Error type: %s. Message: %s" % (type(ex), ex.message))
            raise

        data_files = [
            'data/allele_nonproducer.txt.gz',
            'data/allele_producer.txt.gz',
            'data/doubling_size_time.txt.gz',
            'data/draw.txt.gz',
            'data/final_numbers.txt.gz',
            'data/final_values.txt.gz',
            'data/initial_values.txt.gz',
            'data/log.txt.gz',
            'data/plot_values.txt.gz',
            'data/public_goods.txt.gz',
            'data/statistics.txt.gz',
            'data/warnings.txt.gz',
            ]

        Application.__init__(self,
                             ['./gbiointeract.sh'],
                             [executable, (self.tmp_filename, 'gbiointeract.sh')],        # inputs
                             data_files,  # outputs
                             stdout="gbiointeract.out",
                             stderr="gbiointeract.err",
                             **extra_args)

        def terminated(self):
            """
            Remove temporary script file
            """
            try:
                os.remove(self.tmp_filename)
            except Exception, ex:
                gc3libs.log.error("Failed removing temporary file %s. " % self.tmp_filename +
                              "Error type %s. Message %s" % (type(ex), str(ex)))

class GBiointeractTaskCollection(ChunkedParameterSweep):
    def __init__(self,
                 executable,
                 cell_diffusion_range,
                 public_good_diffusion_range,
                 public_good_durability_range,
                 death_rate_range,
                 chunk_size,
                 **extra_args):
        if not chunk_size:
            chunk_size = step
        self.executable = executable
        self.extra_args = extra_args

        self.combinations = list(itertools.product(
                 cell_diffusion_range,
                 public_good_durability_range,
                 public_good_diffusion_range,
                 death_rate_range))
        self.curr = 0

        ChunkedParameterSweep.__init__(self,
                                       1,
                                       len(self.combinations)+1, # it has to be at least 2
                                       1,
                                       chunk_size,
                                       **extra_args)

    def new_task(self, param, **extra):
        (cd, d, pg, dr) = self.combinations[self.curr]
        self.curr += 1

        return GBiointeractApplication(
            self.executable,
            cd, d, pg, dr,
            **self.extra_args)


class GBiointeractScript(SessionBasedScript):
    """
    Run multiple instances of `biointeract` witha a combination of
    supplied parameters.
    """
    version = __version__
    application_name = 'gbiointeract'

    def _parse_range(self, string):
        """
        Parse a string in the form N[:END:STEP] and returns an iterator.

        Raises a ValueError if the string does not match
        """
        params = string.split(':')
        if len(params) not in (1, 3):
            raise ValueError("Invalid syntax for range `%s'" % string)

        if len(params) == 3:
            return arange(*string.split(':'))
        else:
            return iter((float(params[0]),))

    def setup_options(self):
        self.add_param("-c", "--cell-diffusion",
                       help="In the form N[:END:STEP]. If only `N` is "
                       "supplied, will use only that value, otherwise "
                       "will use all the values in the range from `N` "
                       "to `END` (exclusive) using `STEP` increments")
        self.add_param("-p", "--public-good-diffusion",
                       help="In the form N[:END:STEP]. If only `N` is "
                       "supplied, will use only that value, otherwise "
                       "will use all the values in the range from `N` "
                       "to `END` (exclusive) using `STEP` increments")
        self.add_param("-d", "--public-good-durability",
                       help="In the form N[:END:STEP]. If only `N` is "
                       "supplied, will use only that value, otherwise "
                       "will use all the values in the range from `N` "
                       "to `END` (exclusive) using `STEP` increments")
        self.add_param("-x", "--death-rate",
                       help="In the form N[:END:STEP]. If only `N` is "
                       "supplied, will use only that value, otherwise "
                       "will use all the values in the range from `N` "
                       "to `END` (exclusive) using `STEP` increments")
        self.add_param('--chunk-size', default=10, type=int,
                       help="How many jobs to submit at the "
                       "same time. Default: %(default)s")

    def setup_args(self):
        self.add_param("executable", nargs="?", default='biointeract',
                       metavar='EXECUTABLE',
                       help="Path to the biointeract executable.")

    def parse_args(self):
        if self.params.cell_diffusion:
            self.params.cell_diffusion_range = self._parse_range(
                self.params.cell_diffusion)

        if self.params.public_good_diffusion:
            self.params.public_good_diffusion_range = self._parse_range(
                self.params.public_good_diffusion)

        if self.params.public_good_durability:
            self.params.public_good_durability_range = self._parse_range(
                self.params.public_good_durability)

        if self.params.death_rate:
            self.params.death_rate_range = self._parse_range(
                self.params.death_rate)

        self.params.executable = os.path.abspath(self.params.executable)

    def new_tasks(self, extra):
        # Check that the required parameters have been passed to the script
        if not self.params.cell_diffusion:
            raise gc3libs.exceptions.InvalidUsage("-c, --cell-diffusion argument is required")

        if not self.params.public_good_diffusion:
            raise gc3libs.exceptions.InvalidUsage("-p, --public-good-diffusion argument is required")

        if not self.params.public_good_durability:
            raise gc3libs.exceptions.InvalidUsage("-d, --public-good-durability argument is required")

        if not self.params.death_rate:
            raise gc3libs.exceptions.InvalidUsage("-x, --death-rate argument is required")

        # Check that the `biointeract` executable exists.
        if not os.path.exists(self.params.executable):
            raise gc3libs.exceptions.InvalidUsage("Biointeract binary file `%s` does not exists. Please, re-run specifying a valid path to the biointeract program." % self.params.executable)
        elif not os.path.isfile(self.params.executable):
            raise gc3libs.exceptions.InvalidUsage("Invalid biointeract binary `%s`. Please, re-run specifying a valid path to the biointeract program." % self.params.executable)

        return [GBiointeractTaskCollection(
            self.params.executable,
            self.params.cell_diffusion_range,
            self.params.public_good_diffusion_range,
            self.params.public_good_durability_range,
            self.params.death_rate_range,
            self.params.chunk_size,
            **extra.copy()
            )]


if __name__ == '__main__':
    import gbiointeract
    gbiointeract.GBiointeractScript().run()
