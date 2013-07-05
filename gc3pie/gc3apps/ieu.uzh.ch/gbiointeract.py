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

import gc3libs
from gc3libs import Application, Run
from gc3libs.cmdline import SessionBasedScript
from gc3libs.workflow import ParallelTaskCollection


def arange(start, stop=None, step=1, precision=None):
    """arange generates a set of Decimal values over the
    range [start, stop) with step size step

    drange([start,] stop, [step [,precision]])

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
                 cell_diffusion,
                 public_good_diffusion,
                 durability,
                 death_rate,
                 **extra_args):
        extra_args.setdefault('requested_cores', 1)

        Application.__init__(self,
                             arguments,
                             inputs,
                             outputs,
                             **extra_args)


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

    def parse_args(self):
        self.params.cell_diffusion_range = self._parse_range(
            self.params.cell_diffusion)
        self.params.public_good_diffusion_range = self._parse_range(
            self.params.public_good_diffusion)
        self.params.durability_range = self._parse_range(
            self.params.durability)
        self.params.death_rate_range = self._parse_range(
            self.params.death_rate)

    def new_tasks(self, extra):
        for cell_diffusion in self.params.cell_diffusion_range:
            for pgood_diffusion in self.params.public_good_diffusion_range:
                for durability in self.params.durability_range:
                    for death_rate in self.params.death_rate_range:
                        yield GBiointeractApplication(
                            cell_diffusion,
                            pgood_diffusion,
                            durability,
                            death_rate,
                            **extra.copy())


if __name__ == '__main__':
    import gbiointeract
    gbiointeract.GBiointeractScript().run()
