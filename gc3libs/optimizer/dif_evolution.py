#! /usr/bin/env python

r"""
This module implements a global optimization algorithm called Differential
Evolution.

Consider the following optimization problem: :math:`min ~ f(\mathbf{x}) ~~ s.t.
~~ \mathbf{x} \\in D`, where :math:`D \\in \mathbb{R}^d` and :math:`f: D
\mapsto \mathbb{R}`. Class :class:`DifferentialEvolutionAlgorithm
<gc3libs.optimizer.dif_evolution.DifferentialEvolutionAlgorithm>` solves this
optimization problem using the differential evolution algorithm. No further
assumptions on the function :math:`f` are needed. Thus it can be non-convex,
noisy etc.

The domain :math:`D` is implicitly specified by passing the function
:func:`filtern_fn` to :class:`DifferentialEvolutionAlgorithm`.

Some information related to Differential Evolution can be found in the following papers:

1) Tvrdik 2008: http://www.proceedings2008.imcsit.org/pliks/95.pdf
2) Fleetwood: http://www.maths.uq.edu.au/MASCOS/Multi-Agent04/Fleetwood.pdf
3) Piyasatian: http://www-personal.une.edu.au/~jvanderw/DE_1.pdf

`~gc3libs.optimizer.dif_evolution.DifferentialEvolutionAlgorithm.evolve_fn`:func: is an adaptation of the following MATLAB code:
http://www.icsi.berkeley.edu/~storn/DeMat.zip hosted on http://www.icsi.berkeley.edu/~storn/code.html#deb1.
"""

# Copyright (C) 2011, 2012, 2013, 2019  University of Zurich. All rights reserved.
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
from builtins import range
__author__ = 'Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
__docformat__ = 'reStructuredText'

import numpy as np

from gc3libs.optimizer import EvolutionaryAlgorithm, populate
from gc3libs.utils import Enum

# in code one can use:
#  `strategies.DE_rand`
# or:
#  'DE_rand'
strategies = Enum(
    'DE_rand',
    'DE_local_to_best',
    'DE_best_with_jitter',
    'DE_rand_with_per_vector_dither',
    'DE_rand_with_per_generation_dither',
    'DE_rand_either_or_algorithm'
)


class DifferentialEvolutionAlgorithm(EvolutionaryAlgorithm):

    '''Differential Evolution Algorithm class.
    :class:`DifferentialEvolutionAlgorithm` explicitly allows for an another
    process to control the optimization. Driver classes can be found
    in `gc3libs.optimizer.drivers.py`:mod:.

    :param initial_pop: Initial population for the optimization.
      Value can be any sequence that can be passed to the `np.array()` constructor.
    :param str de_strategy: e.g. DE_rand_either_or_algorithm. Allowed are:
    :param float `de_step_size`: Differential Evolution step size.
    :param float `prob_crossover`: Probability new population draws will replace old members.
    :param bool exp_cross: Set True to use exponential crossover.
    :param int `itermax`: Maximum # of iterations.
    :param float `dx_conv_crit`: Abort optimization if all population members are within a certain distance to each other.
    :param float `y_conv_crit`: Declare convergence when the target function is below a `y_conv_crit`.
    :param fun `in_domain`: Optional function that implements nonlinear constraints.
    :param float `seed`: Seed to initialize NumPy's random number generator.
    :param obj `logger`: Configured logger to use.
    :param `after_update_opt_state`: List of Functions that are called at the end of
                `DifferentialEvolutionAlgorithm.after_update_opt_state`:meth:. Use this list
                to provide problem-specific printing and plotting routines. Examples can be found
                in `gc3libs.optimizer.extra`:mod:.

    The `de_strategy` value must be chosen from the
    `dif_evolution.strategies` enumeration.  Allowed values are
    (description of the strategies taken from http://www.icsi.berkeley.edu/~storn/DeMat.zip):

    1. ``'DE_rand'``: The classical version of DE.
    2. ``'DE_local_to_best'``: A version which has been used by quite a number of
            scientists. Attempts a balance between robustness # and fast convergence.
    3. ``'DE_best_with_jitter'``: Taylored for small population sizes and fast
            convergence. Dimensionality should not be too high.
    4. ``'DE_rand_with_per_vector_dither'``: Classical DE with dither to become even more robust.
    5. ``'DE_rand_with_per_generation_dither'``: Classical DE with dither to become even more robust.
                                           Choosing de_step_size = 0.3 is a good start here.
    6. ``'DE_rand_either_or_algorithm'``: Alternates between differential mutation and three-point- recombination.

    '''

    def __init__(self, initial_pop,
                 # DE-specific parameters
                 de_strategy='DE_rand', de_step_size=0.85, prob_crossover=1.0, exp_cross=False,
                 # converge-related parameters
                 itermax=100, dx_conv_crit=None, y_conv_crit=None,
                 # misc
                 in_domain=None, seed=None, logger=None, after_update_opt_state=[]):

        # Check input variables
        assert 0.0 <= prob_crossover <= 1.0, "prob_crossover should be from interval [0,1]"
        assert len(
            initial_pop) >= 5, "DifferentialEvolution requires at least 5 vectors in the population!"

        # initialize base class
        EvolutionaryAlgorithm.__init__(
            self,
            initial_pop,
            itermax, dx_conv_crit, y_conv_crit,
            logger, after_update_opt_state
        )
        # save parameters
        self.de_step_size = de_step_size
        self.prob_crossover = prob_crossover
        self.exp_cross = exp_cross
        self.de_strategy = de_strategy

        if not in_domain:
            self.in_domain = self._default_in_domain
        else:
            self.in_domain = in_domain

        # initialize NumPy's RNG
        np.random.seed(seed)

    def _default_in_domain(self, x):
        return np.array([True] * self.pop_size)

    def select(self, new_pop, new_vals):
        '''
        Perform a one-on-one battle by index, keeping the member
        with lowest corresponding value.
        '''
        ix_superior = new_vals < self.vals
        self.pop[ix_superior, :] = new_pop[ix_superior, :].copy()
        self.vals[ix_superior] = new_vals[ix_superior].copy()

    def evolve(self):
        '''
        Generates a new population fullfilling `in_domain`.

        :rtype: list of population members
        '''
        return populate(
            create_fn=(
                lambda: DifferentialEvolutionAlgorithm.evolve_fn(
                    self.pop,
                    self.prob_crossover,
                    self.de_step_size,
                    self.dim,
                    self.best_x,
                    self.de_strategy,
                    self.exp_cross)),
            in_domain=self.in_domain)

    @staticmethod
    def evolve_fn(
            population,
            prob_crossover,
            de_step_size,
            dim,
            best_iter,
            de_strategy,
            exp_cross):
        """
        Return new population, evolved according to `de_strategy`.

        :param population: Population generating offspring from.
        :param prob_crossover: Probability new population draws will replace old members.
        :param de_step_size: Differential Evolution step size.
        :param dim: Dimension of each population member.
        :param best_iter: Best population member of the current population.
        :param de_strategy: Differential Evolution strategy. See :class:`DifferentialEvolutionAlgorithm`.
        :param exp_cross bool: Set True to use exponential crossover.
        """

        assert de_strategy in strategies

        pop_size = len(population)

        # BJ: Need to add +1 in definition of ind otherwise there is one zero
        # index that leaves creates no shuffling.
        # index pointer array. e.g. [2, 1, 4, 3]
        ind = np.random.permutation(4) + 1
        rot = np.arange(
            0,
            pop_size,
            1)     # rotating index array (size pop_size)
        rt = np.zeros(pop_size)            # another rotating index array
        # index arrays
        a1 = np.random.permutation(pop_size)   # shuffle locations of vectors
        a2 = a1[
            (rot + ind[0]) %
            pop_size]  # rotate vector locations by ind[0] positions
        a3 = a2[(rot + ind[1]) % pop_size]
        a4 = a3[(rot + ind[2]) % pop_size]
        a5 = a4[(rot + ind[3]) % pop_size]

        pm1 = population[a1, :]  # shuffled population matrix 1
        pm2 = population[a2, :]  # shuffled population matrix 2
        pm3 = population[a3, :]  # shuffled population matrix 3
        pm4 = population[a4, :]  # shuffled population matrix 4
        pm5 = population[a5, :]  # shuffled population matrix 5

        # "best member" matrix
        bm = np.zeros((pop_size, dim))   # initialize FVr_bestmember  matrix
        # population filled with the best member
        for k in range(pop_size):
            bm[k, :] = best_iter                       # of the last iteration

        # mask for intermediate population
        # all random numbers < prob_crossover are 1, 0 otherwise
        mui = np.random.random_sample((pop_size, dim)) < prob_crossover

        if exp_cross:
            # rotating index array, i.e. [0, 1, 2, ..., dim]
            rotd = np.arange(dim)
            # rotating index array for exponential crossover
            rtd = np.zeros(dim, dtype=np.int)
            # Prepare intermediate population for indexing.
            mui = np.sort(mui.transpose(), axis=0)
            # Columns are pop members. Put all False indices in the first rows.
            for k in range(pop_size):
                n = int(np.floor(np.random.rand() * dim))
                if n > 0:
                    # Build actual rotation vector.
                    rtd = (rotd + n) % dim
                    # e.g. dim = 6, n = 2 -> [3,2,1,0,1,2]
                    # Rotate indices for kth population member by n.
                    mui[:, k] = mui[rtd, k]
            mui = mui.transpose()

        # inverse mask to mui (mpo + mui == <vector of 1's>)
        mpo = mui < 0.5

        if (de_strategy == 'DE_rand'):
            #origin = pm3
            ui = pm3 + de_step_size * (pm1 - pm2)   # differential variation
            ui = population * mpo + ui * mui          # crossover
        elif (de_strategy == 'DE_local_to_best'):
            #origin = population
            ui = population + de_step_size * \
                (bm - population) + de_step_size * (pm1 - pm2)
            ui = population * mpo + ui * mui
        elif (de_strategy == 'DE_best_with_jitter'):
            #origin = bm
            ui = bm + (pm1 - pm2) * ((1 - 0.9999) * \
                       np.random.random_sample((pop_size, dim)) + de_step_size)
            ui = population * mpo + ui * mui
        elif (de_strategy == 'DE_rand_with_per_vector_dither'):
            #origin = pm3
            f1 = (
                (1 -
                 de_step_size) *
                np.random.random_sample(
                    (pop_size,
                     1)) +
                de_step_size)
            for k in range(dim):
                pm5[:, k] = f1
            ui = pm3 + (pm1 - pm2) * pm5    # differential variation
            ui = population * mpo + ui * mui     # crossover
        elif (de_strategy == 'DE_rand_with_per_generation_dither'):
            #origin = pm3
            f1 = (
                (1 -
                 de_step_size) *
                np.random.random_sample() +
                de_step_size)
            ui = pm3 + (pm1 - pm2) * f1         # differential variation
            ui = population * mpo + ui * mui   # crossover
        elif (de_strategy == 'DE_rand_either_or_algorithm'):
            #origin = pm3
            # Pmu = 0.5
            if (np.random.random_sample() < 0.5):
                ui = pm3 + de_step_size * (pm1 - pm2)  # differential variation
            # use F-K-Rule: K = 0.5(F+1)
            else:
                ui = pm3 + 0.5 * (de_step_size + 1.0) * (pm1 + pm2 - 2 * pm3)
                ui = population * mpo + ui * mui     # crossover

        return ui

    # Adjustments for pickling
    def __getstate__(self):
        state = self.__dict__.copy()
        if 'logger' in list(state.keys()):
            del state['logger']
#        return state
        return None

    def __setstate__(self, state):
        self.__dict__ = state


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
# I_best_index -> best_ix
# S_vals -> vals
# S_bestval -> best_y
# S_bestvalit -> best_y_iter
# best -> best_x
# best_iter -> best_x_iter
