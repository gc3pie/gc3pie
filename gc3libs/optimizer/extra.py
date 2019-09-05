#! /usr/bin/env python

"""
Collection of tools to supplement optimization algorithm
:class:`optimizer.EvolutionaryAlgorithm <gc3libs.optimizer.EvolutionaryAlgorithm>`.

Include a list of desired tools in param `after_update_opt_state` of
:class:`optimizer.EvolutionaryAlgorithm <gc3libs.optimizer.EvolutionaryAlgorithm>`.
"""

# Copyright (C) 2011, 2012, 2013  University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import, print_function, unicode_literals
from builtins import object
__author__ = 'Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
__docformat__ = 'reStructuredText'


import os
import sys
import logging

log = logging.getLogger("gc3.gc3libs")


class plot_population(object):

    '''Plot the 2-dimensional population of an
       :class:`gc3libs.optimizer.EvolutionaryAlgorithm` instance.
       If the population is not 2-d an error message appears and no
       plot is created.

       :param str figure_dir: Path to the directory where plots should be stored.
                              Directory will be created if non-existent.
    '''

    def __init__(self, figure_dir):
        if not os.path.isdir(figure_dir):
            os.mkdir(figure_dir)
        self.figure_dir = figure_dir

    def __call__(self, algo):
        '''Plot population for `algo`.

           :param str algo: Instance of :class:`gc3libs.optimizer.EvolutionaryAlgorithm`.
        '''
        log.debug('entering plot_population.__call__')
        pop = algo.pop
        if not algo.dim == 2:
            algo.logger.critical(
                'plot_population is implemented only for algo.dim = 2')
            return
        import matplotlib
        matplotlib.use('SVG')
        import matplotlib.pyplot as plt
        x = pop[:, 0]
        y = pop[:, 1]
        # determine bounds
        x0_min = algo.pop[:, 0].min()
        x1_min = algo.pop[:, 1].min()
        x0_max = algo.pop[:, 0].max()
        x1_max = algo.pop[:, 1].max()

        # Determine plotting area
        x_dif = x0_max - x0_min
        y_dif = x1_max - x1_min
        scaleFac = 0.3
        x0_min_plot = x0_min - scaleFac * x_dif
        x0_max_plot = x0_max + scaleFac * x_dif
        x1_min_plot = x1_min - scaleFac * y_dif
        x1_max_plot = x1_max + scaleFac * y_dif

        # make plot
        fig = plt.figure()
        ax = fig.add_subplot(111)

        ax.scatter(x, y)
        # x box constraints
        ax.plot([x0_min, x0_min], [x1_min_plot, x1_max_plot])
        ax.plot([x0_max, x0_max], [x1_min_plot, x1_max_plot])
        ax.plot([x0_min_plot, x0_max_plot], [x1_min, x1_min])
        ax.plot([x0_min_plot, x0_max_plot], [x1_max, x1_max])

        # all other linear constraints
        # c_x_min_plot = algo.filter_fn.linearConstr(x_min_plot)
        # c_xmax = algo.filter_fn.linearConstr(xmax)
        # for ixC in range(len(c_x_min_plot)):
        # ax.plot([x_min_plot, xmax], [c_x_min_plot[ixC], c_xmax[ixC]])
        ax.axis(xmin=x0_min_plot, xmax=x0_max_plot,
                ymin=x1_min_plot, ymax=x1_max_plot)

        ax.set_xlabel('x')
        ax.set_ylabel('y')

        ax.set_title('Best: x %s, f(x) %f' % (algo.best_x, algo.best_y))

        fig.savefig(os.path.join(self.figure_dir, 'pop%d' % (algo.cur_iter)))


def print_stats(algo, output=sys.stdout):
    '''Print summary statistics for `algo`.

       :param str algo: Instance of :class:`gc3libs.optimizer.EvolutionaryAlgorithm`.
       :param output: Output stream.
    '''
    output.write(
        'Iteration: %d,  x: %s f(x): %f\n' %
        (algo.cur_iter, algo.best_x, algo.best_y))


def log_stats(algo, logger=logging.getLogger()):
    '''Log summary statistics for `algo`.

       :param str algo: Instance of :class:`gc3libs.optimizer.EvolutionaryAlgorithm`.
    '''
    logger.info('Iteration: %d,  x: %s f(x): %f',
                algo.cur_iter, algo.best_x, algo.best_y)
