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
import os
import sys

import numpy as np

from gc3libs.optimizer import EvolutionaryAlgorithm

np.set_printoptions(linewidth = 300, precision = 8, suppress = True)

from gc3libs.optimizer import draw_population, populate


# Variable changes from matlab implementation
# I_D -> dim
# I_NP -> pop_size
# FM_popold -> pop_old
# FVr_bestmem -> best
# FVr_bestmemit -> best_cur_iter
# I_nfeval -> n_fun_evals
# I_cur_iter -> cur_iter
# F_weight -> de_step_size
# F_CR -> prob_crossover
# I_itermax -> itermax
# F_VTR -> y_conv_crit
# I_strategy -> de_strategy
# I_plotting -> plotting
# lower_bds
# upper_bds
# dx_conv_crit
# verbosity
# FM_pm1 -> pm1
# FM_pm2 -> pm2 population matrix (pm)
# FM_pm3 -> pm3
# FM_pm4 -> pm4
# FM_pm5 -> pm5
# FM_bm  -> bm best member matrix
# FM_ui  -> ui ??
# FM_mui -> mui # mask for intermediate population
# FM_mpo -> mpo # mask for old population
# FVr_rot -> rot  # rotating index array (size I_NP)
# FVr_rotd -> rotd  # rotating index array (size I_D)
# FVr_rt -> rt  # another rotating index array
# FVr_rtd -> rtd # rotating ininstalldex array for exponential crossover
# FVr_a1 -> a1 # index array
# FVr_a2 -> a2 # index array
# FVr_a3 -> a3 # index array
# FVr_a4 -> a4 # index array
# FVr_a5 -> a5 # index array
# FVr_ind -> ind # index pointer array
# I_best_index -> best_index
# S_vals -> vals
# S_bestval -> bestval
# S_bestvalit -> bestvalit

class DifferentialEvolutionAlgorithm(EvolutionaryAlgorithm):
    '''
    :class:`DifferentialEvolutionAlgorithm` explicitly allows for
    an another process to control the optimization. The methods
    `de_opt` and `iterate` are left unspecified and the outside
    process can instead directly call the methods that are called by
    `de_opt` and `iterate` (see code for
    `DifferentialEvolutionSequential`) when needed.  An example of how
    :class:`DifferentialEvolutionAlgorithm` can be used is found in
    `GlobalOptimizer` located in `optimizer/__init__.py`.

    '''

    def __init__(self, initial_pop, dim,
                 de_step_size = 0.85, prob_crossover = 1.0, itermax = 100,
                 dx_conv_crit = None, y_conv_crit = None,
                 de_strategy = 'DE_rand', filter_fn=None, logger=None, seed=None):
        '''
        Arguments:
        `dim` -- Dimensionality of the problem.
        `de_step_size` -- Differential Evolution step size.
        `prob_crossover` -- Probability new population draws will replace old members.
        `itermax` -- Maximum # of iterations.
        `dx_conv_crit` -- Abort optimization if all population members are within a certain distance to each other.
        `y_conv_crit` -- Terminate opitimization when target function has reached a certain value.
        `de_strategy` -- Specify a certain Differential Evolution strategy from the list above. String input e.g. DE_rand_either_or_algorithm.
        `filter_fn` -- Optional function that implements nonlinear constraints.
        `logger` -- Configured logger to use.
        `seed` -- Seed to initialize NumPy's random number generator
        '''

        # Check input variables
        assert 0.0 <= prob_crossover <= 1.0, "prob_crossover should be from interval [0,1]"
        assert len(initial_pop) >= 5, "DifferentialEvolution requires at least 5 vectors in the population!"

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('gc3.gc3libs')

        # save parameters
        self.dim = dim
        self.de_step_size = de_step_size
        self.prob_crossover = prob_crossover
        self.itermax = itermax
        self.y_conv_crit = y_conv_crit
        self.de_strategy = de_strategy
        self.dx_conv_crit = dx_conv_crit
        self.pop_size = len(initial_pop)

        if not filter_fn:
            self.filter_fn = self._default_filter_fn
        else:
            self.filter_fn = filter_fn

        self.new_pop = initial_pop # self.enforce_constr_re_sample(initial_pop)

        # Initialize variables that needed for state retention.
        self.best = np.zeros( self.dim )                       # best population member ever
        self.best_iter = np.zeros( self.dim )                  # best population member in iteration

        # set initial value for iteration count
        self.cur_iter = 0

        # initialize NumPy's RNG
        np.random.seed(seed)


    def _default_filter_fn(self, x):
        return np.array([ True ] * self.pop_size)


    def has_converged(self):
        converged = False
        # Check convergence
        if self.cur_iter > self.itermax:
            converged = True
            self.logger.info('Exiting difEvo. cur_iter >self.itermax ')
        if self.bestval < self.y_conv_crit:
            converged = True
            self.logger.info('converged self.bestval < self.y_conv_crit')
        if self.has_dx_converged():
            converged = True
            self.logger.info('converged self.population_converged(self.pop)')
        return converged


    def has_dx_converged(self):
        '''
        Check if all population members are within the given dx limits.
        '''
        dxs = np.abs(self.pop[:, :] - self.pop[0, :])
        return (dxs <= self.dx_conv_crit).all()


    def update_opt_state(self, new_vals):
        '''
        Stores set of function values corresponding to the current
        population, then updates optimizer state in many ways:

        * update the `.best*` variables accordingly;
        * merges the two populations (old and new), keeping only the members with lower corresponding value;
        * advances iteration count.
        '''

        self.logger.debug('entering update_opt_state')

        if self.cur_iter == 0:
            self.pop = self.new_pop.copy()
            self.vals = np.array(new_vals)
            # determine bestmemit and bestvalit for random draw.
            self.best_index = np.argmin(self.vals)
            self.bestval = self.vals[self.best_index].copy()
            self.best_iter = self.new_pop[self.best_index, :].copy()
            # the following only applies in the 1st iteration
            self.bestvalit = self.bestval.copy()
            self.best = self.best_iter.copy()

        else:
            new_vals = np.array(new_vals)
            best_index = np.argmin(new_vals)
            if new_vals[best_index] < self.bestval:
                self.bestval   = new_vals[best_index].copy()                    # new best value
                self.best = self.new_pop[best_index, :].copy()                 # new best parameter vector ever

            # merge the two populations, keeping the member with lowest corresponding value
            for k in range(self.pop_size):
                if new_vals[k] < self.vals[k]:
                    self.pop[k,:] = self.new_pop[k, :].copy()                    # replace old vector with new one (for new iteration)
                    self.vals[k]   = new_vals[k].copy()                      # save value in "cost array"

            self.best_iter = self.best.copy()       # freeze the best member of this iteration for the coming
                                                     # iteration. This is needed for some of the strategies.

        self.logger.debug('new values %s', new_vals)
        self.logger.debug('best value %s', self.bestval)

        self.after_update_opt_state()

        self.cur_iter += 1

        return

    def evolve(self):
        return populate(
            create_fn=lambda : evolve_fn(self.pop, self.prob_crossover, self.de_step_size, self.dim, self.best_iter, self.de_strategy),
            filter_fn=self.filter_fn
        )

    #def evolve(self):
        #modified_pop = evolve_fn(self.pop, self.prob_crossover, self.de_step_size, self.dim, self.best_iter, self.de_strategy)
        ## re-evolve if some members do not fullfill fiter_fn
        #ctr = 0
        #max_n_resample = 100
        #pop_valid_orig = self.filter_fn(modified_pop)
        #n_invalid_orig = (pop_valid_orig == False).sum()
        #fillin_pop = self.pop[~pop_valid_orig]
        #total_filled = 0
        #while total_filled < n_invalid_orig and ctr < max_n_resample:
            #reevolved_pop = evolve_fn(self.pop, self.prob_crossover, self.de_step_size, self.dim, self.best_iter, self.de_strategy)
            #pop_valid = self.filter_fn(reevolved_pop)
            #n_pop_valid = (pop_valid == True).sum()
            #new_total_filled = min(total_filled + n_pop_valid, len(fillin_pop))
            #fillin_pop[total_filled:new_total_filled] = reevolved_pop[pop_valid]
            #total_filled = new_total_filled
        #if total_filled < n_invalid_orig:
            #self.logger.warning(
                #"%d population members are invalid even after re-sampling %d times."
                #"  You might want to increase `max_n_resample`.",
                #(n_invalid_orig - total_filled), max_n_resample)
        #modified_pop[~pop_valid_orig] = fillin_pop
        #return modified_pop


    # Adjustments for pickling
    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
#        return state
        return None

    def __setstate__(self, state):
        self.__dict__ = state

    def after_update_opt_state(self):
        '''
        Hook method called at the end of update_opt_state to implement plotting or printing stats.
        Override in subclass.
        '''
        self.print_stats()

    def print_stats(self):
        self.logger.info('Iteration: %d,  x: %s f(x): %f',
                         self.cur_iter, self.best, self.bestval)


def evolve_fn(population, prob_crossover, de_step_size, dim, best_iter, de_strategy):
    """
    Return new population, evolved according to `de_strategy`.
    """

    pop_size = len(population)

    # BJ: Need to add +1 in definition of ind otherwise there is one zero index that leaves creates no shuffling.
    ind = np.random.permutation(4) + 1   # index pointer array.
    rot  = np.arange(0, pop_size, 1)     # rotating index array (size pop_size)
    rt   = np.zeros(pop_size)            # another rotating index array
    ## index arrays
    a1  = np.random.permutation(pop_size)   # shuffle locations of vectors
    a2  = a1[ ( rot + ind[0] ) % pop_size ] # rotate vector locations by ind[0] positions
    a3  = a2[ ( rot + ind[1] ) % pop_size ]
    a4  = a3[ ( rot + ind[2] ) % pop_size ]
    a5  = a4[ ( rot + ind[3] ) % pop_size ]

    pm1 = population[a1, :] # shuffled population matrix 1
    pm2 = population[a2, :] # shuffled population matrix 2
    pm3 = population[a3, :] # shuffled population matrix 3
    pm4 = population[a4, :] # shuffled population matrix 4
    pm5 = population[a5, :] # shuffled population matrix 5

    ## "best member" matrix
    bm    = np.zeros( (pop_size, dim) )   # initialize FVr_bestmember  matrix
    for k in range(pop_size):                              # population filled with the best member
        bm[k,:] = best_iter                       # of the last iteration

    ## mask for intermediate population
    mui = np.random.random_sample( (pop_size, dim ) ) < prob_crossover  # all random numbers < prob_crossover are 1, 0 otherwise

    #----Insert this if you want exponential crossover.----------------
    #rotd = np.arange(0, dim, 1) # rotating index array (size dim)
    #rtd  = np.zeros(dim)        # rotating index array for exponential crossover
    #mui = sort(mui')            # transpose, collect 1's in each column
    #for k  = 1:pop_size
    #  n = floor(rand*dim)
    #  if (n > 0)
    #     rtd     = rem(rotd+n,dim)
    #     mui(:,k) = mui(rtd+1,k) #rotate column k by n
    #  end
    #end
    #mui = mui'                       # transpose back
    #----End: exponential crossover------------------------------------

    ## inverse mask to mui (mpo + mui == <vector of 1's>)
    mpo = mui < 0.5

    if ( de_strategy == 'DE_rand' ):
        #origin = pm3
        ui = pm3 + de_step_size * ( pm1 - pm2 )   # differential variation
        ui = population * mpo + ui * mui       # crossover
    elif (de_strategy == 'DE_local_to_best'):
        #origin = population
        ui = population + de_step_size * ( bm - population ) + de_step_size * ( pm1 - pm2 )
        ui = population * mpo + ui * mui
    elif (de_strategy == 'DE_best_with_jitter'):
        #origin = bm
        ui = bm + ( pm1 - pm2 ) * ( (1 - 0.9999 ) * np.random.random_sample( (pop_size, dim ) ) + de_step_size )
        ui = population * mpo + ui * mui
    elif (de_strategy == 'DE_rand_with_per_vector_dither'):
        #origin = pm3
        f1 = ( ( 1 - de_step_size ) * np.random.random_sample( (pop_size, 1 ) ) + de_step_size)
        for k in range(dim):
            pm5[:,k] = f1
        ui = pm3 + (pm1 - pm2) * pm5    # differential variation
        ui = population * mpo + ui * mui     # crossover
    elif (de_strategy == 'DE_rand_with_per_generation_dither'):
        #origin = pm3
        f1 = ( ( 1 - de_step_size ) * np.random.random_sample() + de_step_size )
        ui = pm3 + ( pm1 - pm2 ) * f1         # differential variation
        ui = population * mpo + ui * mui   # crossover
    elif ( de_strategy == 'DE_rand_either_or_algorithm' ):
        #origin = pm3
        if (np.random.random_sample() < 0.5):                               # Pmu = 0.5
            ui = pm3 + de_step_size * ( pm1 - pm2 )# differential variation
        else:                                           # use F-K-Rule: K = 0.5(F+1)
            ui = pm3 + 0.5 * ( de_step_size + 1.0 ) * ( pm1 + pm2 - 2 * pm3 )
            ui = population * mpo + ui * mui     # crossover

    return ui


class DifferentialEvolutionSequential(DifferentialEvolutionAlgorithm):
    '''
    Differential Evolution Optimizer class.

    Inputs:
        # de_strategy    1 --> DE_rand:
        #                      the classical version of DE.
        #                2 --> DE_local_to_best:
        #                      a version which has been used by quite a number
        #                      of scientists. Attempts a balance between robustness
        #                      and fast convergence.
        #                3 --> DE_best_with_jitter:
        #                      taylored for small population sizes and fast convergence.
        #                      Dimensionality should not be too high.
        #                4 --> DE_rand_with_per_vector_dither:
        #                      Classical DE with dither to become even more robust.
        #                5 --> DE_rand_with_per_generation_dither:
        #                      Classical DE with dither to become even more robust.
        #                      Choosing de_step_size = 0.3 is a good start here.
        #                6 --> DE_rand_either_or_algorithm:
        #                      Alternates between differential mutation and three-point-
        #                      recombination.


    1) Target function that takes x and generates f(x)
    2) filter_fn function that takes x and generates constraint function values c(x) >= 0.
    '''
    def __init__(self, initial_pop, dim, target_fn, de_step_size = 0.85,
                 prob_crossover = 1.0, itermax = 100,
                 dx_conv_crit = None, y_conv_crit = None,
                 de_strategy = 'DE_rand', filter_fn=None, logger=None, seed=None):
        '''
        In addition to initialization parameters of
        :class:`DifferentialEvolutionAlgorithm` (which see), there is
        one more:

        `target_fn` -- Function to evaluate a population and return the corresponding values.
        '''

        DifferentialEvolutionAlgorithm.__init__(
            self, initial_pop, dim,
            de_step_size, prob_crossover, itermax,
            dx_conv_crit, y_conv_crit,
            de_strategy, filter_fn, logger, seed)
        self.target_fn = target_fn


    def de_opt(self):
        '''
        Perform global optimization.
        '''
        self.logger.debug('entering de_opt')
        has_converged = False
        while not has_converged:
            has_converged = self.iterate()
        self.logger.debug('exiting ' + __name__)


    def iterate(self):
        if self.cur_iter == 0:
            self.pop = self.new_pop.copy()
        elif self.cur_iter > 0:
            self.new_pop = self.evolve()

        # EVALUATE TARGET #
        vals = self.target_fn(self.new_pop)
        if __debug__:
            self.logger.debug('x -> f(x)')
            for x, fx in zip(self.new_pop, vals):
                self.logger.debug('%s -> %s' % (x.tolist(), fx))
        self.update_opt_state(vals)
        # create output
        self.print_stats()

        return self.has_converged()


class DifferentialEvolutionWithPlotting(DifferentialEvolutionSequential):
    def after_update_opt_state(self):
        '''
        Hook method called at the end of update_opt_state to implement plotting or printing stats.
        Override in subclass.
        '''
        self.print_stats()
        self.plot_population(self.pop)

    def plot_population(self, pop):
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
        ax.set_title('Best: x %s, f(x) %f' % (self.best, self.bestval))

        figure_dir = os.path.join(os.getcwd(), 'dif_evo_figs')
        fig.savefig(os.path.join(figure_dir, 'pop%d' % (self.cur_iter)))
