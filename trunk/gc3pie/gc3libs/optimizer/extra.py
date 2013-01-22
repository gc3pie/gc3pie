#! /usr/bin/env python
#
"""
Differential Evolution Optimizer
This code is an adaptation of the following MATLAB code: http://www.icsi.berkeley.edu/~storn/DeMat.zip
Please refer to this web site for more information: http://www.icsi.berkeley.edu/~storn/code.html#deb1
"""
# Copyright (C) 2011, 2012, 2013 University of Zurich. All rights reserved.
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__version__ = '$Revision$'
__author__ = 'Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
__docformat__ = 'reStructuredText'

import logging
import sys

def plot_population(algo):
    pop = algo.pop
    if not self.dim == 2:
        self.logger.critical('plot_population is implemented only for self.dim = 2')
    import matplotlib
    matplotlib.use('SVG')
    import matplotlib.pyplot as plt
    x = pop[:, 0]
    y = pop[:, 1]
    # determine bounds
    xDif = self.upper_bds[0] - self.lower_bds[0]
    yDif = self.upper_bds[1] - self.lower_bds[1]
    scaleFac = 0.3
    xmin = self.lower_bds[0] - scaleFac * xDif
    xmax = self.upper_bds[0] + scaleFac * xDif
    ymin = self.lower_bds[1] - scaleFac * yDif
    ymax = self.upper_bds[1] + scaleFac * yDif

    # make plot
    fig = plt.figure()
    ax = fig.add_subplot(111)

    ax.scatter(x, y)
    # x box constraints
    ax.plot([self.lower_bds[0], self.lower_bds[0]], [ymin, ymax])
    ax.plot([self.upper_bds[0], self.upper_bds[0]], [ymin, ymax])
    # all other linear constraints
    c_xmin = self.filter_fn.linearConstr(xmin)
    c_xmax = self.filter_fn.linearConstr(xmax)
    for ixC in range(len(c_xmin)):
        ax.plot([xmin, xmax], [c_xmin[ixC], c_xmax[ixC]])
    ax.axis(xmin = xmin, xmax = xmax,
            ymin = ymin, ymax = ymax)
    ax.set_xlabel('x')
    ax.set_ylabel('y')
    ax.set_title('Best: x %s, f(x) %f' % (self.best_x, self.best_y))

    figure_dir = os.path.join(os.getcwd(), 'dif_evo_figs')
    fig.savefig(os.path.join(figure_dir, 'pop%d' % (self.cur_iter)))
    
def print_stats(algo, output=sys.stdout):
    output.write('Iteration: %d,  x: %s f(x): %f\n',
                 algo.cur_iter, algo.best_x, algo.best_y)

def log_stats(algo, logger=logging.getLogger()):
    logger.info('Iteration: %d,  x: %s f(x): %f',
                algo.cur_iter, algo.best_x, algo.best_y)
