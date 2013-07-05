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

import gc3libs
from gc3libs import Application, Run
from gc3libs.cmdline import SessionBasedScript
from gc3libs.workflow import ChunkedParameterSweep


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

    application_name = "biointeract"

    def __init__(self,
                 executable,
                 cell_diffusion,
                 public_good_diffusion,
                 durability,
                 death_rate,
                 **extra_args):
        extra_args.setdefault('requested_cores', 1)

        arguments = [executable,
                     '-c', cell_diffusion,
                     '-p', public_good_diffusion,
                     '-d', durability,
                     '-x', death_rate]

        extra_args['jobname'] = "GBiointeract_cdiff:" + \
        "%f_pgdiff:%f_dur:%f_deathrate:%f" % (cell_diffusion,
                                              public_good_diffusion,
                                              durability, death_rate)
        extra_args['output_dir'] = extra_args['jobname']

        Application.__init__(self,
                             arguments,
                             [executable],        # inputs
                             gc3libs.ANY_OUTPUT,  # outputs
                             stdout="gbiointeract.out",
                             stderr="gbiointeract.err",
                             **extra_args)


class GBiointeractTaskCollection(ChunkedParameterSweep):
    def __init__(self,
                 executable,
                 cell_diffusion_range,
                 public_good_diffusion_range,
                 durability_range,
                 death_rate_range,
                 chunk_size=100,
                 step=10,
                 **extra_args):

        self.executable = executable
        self.extra_args = extra_args

        self.combinations = itertools.product(
                 cell_diffusion_range,
                 durability_range,
                 public_good_diffusion_range,
                 death_rate_range)
        self.next = self.combinations.next()

        ChunkedParameterSweep.__init__(self,
                                       1,
                                       2*(chunk_size+step) , # fake value
                                       chunk_size,
                                       step,
                                       **extra_args)

    def new_task(self, param, **extra):
        (cd, d, pg, dr) = self.next
        try:
            self.next = self.combinations.next()
            self.max_value += self.step*self.chunk_size
        except StopIteration:
            self.max_value = -1

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
        self.add_param("-c", "--cell-diffusion", required=True,
                       help="In the form N[:END:STEP]. If only `N` is "
                       "supplied, will use only that value, otherwise "
                       "will use all the values in the range from `N` "
                       "to `END` (inclusive) using `STEP` increments")
        self.add_param("-p", "--public-good-diffusion", required=True,
                       help="In the form N[:END:STEP]. If only `N` is "
                       "supplied, will use only that value, otherwise "
                       "will use all the values in the range from `N` "
                       "to `END` (inclusive) using `STEP` increments")
        self.add_param("-d", "--durability", required=True,
                       help="In the form N[:END:STEP]. If only `N` is "
                       "supplied, will use only that value, otherwise "
                       "will use all the values in the range from `N` "
                       "to `END` (inclusive) using `STEP` increments")
        self.add_param("-x", "--death-rate", required=True,
                       help="In the form N[:END:STEP]. If only `N` is "
                       "supplied, will use only that value, otherwise "
                       "will use all the values in the range from `N` "
                       "to `END` (inclusive) using `STEP` increments")

    def setup_args(self):
        self.add_param("executable", nargs="?", default='biointeract',
                       metavar='EXECUTABLE',
                       help="Path to the biointeract executable.")

    def parse_args(self):
        self.params.cell_diffusion_range = self._parse_range(
            self.params.cell_diffusion)
        self.params.public_good_diffusion_range = self._parse_range(
            self.params.public_good_diffusion)
        self.params.durability_range = self._parse_range(
            self.params.durability)
        self.params.death_rate_range = self._parse_range(
            self.params.death_rate)

        if not os.path.isfile(self.params.executable):
            raise ValueError("Invalid executable file `%s`" % self.params.executable)

    def new_tasks(self, extra):
        return [GBiointeractTaskCollection(
            self.params.executable,
            self.params.cell_diffusion_range,
            self.params.public_good_diffusion_range,
            self.params.durability_range,
            self.params.death_rate_range,
            **extra.copy()
            )]


if __name__ == '__main__':
    import gbiointeract
    gbiointeract.GBiointeractScript().run()
