# Original source from http://cci.lbl.gov/cctbx_sources/scitbx/differential_evolution.py
# broader web site http://www.icsi.berkeley.edu/~storn/code.html#pyth

'''
*** Copyright Notice ***

cctbx Copyright (c) 2006, The Regents of the University of
California, through Lawrence Berkeley National Laboratory (subject to
receipt of any required approvals from the U.S. Dept. of Energy).  All
rights reserved.

If you have questions about your rights to use or distribute this
software, please contact Berkeley Lab's Technology Transfer Department
at  TTD@lbl.gov referring to "cctbx (LBNL Ref CR-1726)"

This software package includes code written by others which may be
governed by separate license agreements.  Please refer to the associated
licenses for further details.

NOTICE.  This software was developed under funding from the U.S.
Department of Energy.  As such, the U.S. Government has been granted for
itself and others acting on its behalf a paid-up, nonexclusive,
irrevocable, worldwide license in the Software to reproduce, prepare
derivative works, and perform publicly and display publicly.  Beginning
five (5) years after the date permission to assert copyright is obtained
from the U.S. Department of Energy, and subject to any subsequent five
(5) year renewals, the U.S. Government is granted for itself and others
acting on its behalf a paid-up, nonexclusive, irrevocable, worldwide
license in the Software to reproduce, prepare derivative works,
distribute copies to the public, perform publicly and display publicly,
and to permit others to do so.
'''

#from scitbx.array_family import flex
#from stdlib import random
from __future__ import absolute_import, print_function
import numpy as np
class differential_evolution_optimizer(object):
  """
This is a python implementation of differential evolution
It assumes an evaluator class is passed in that has the following
functionality
data members:
  n              :: The number of parameters
  domain         :: a  list [(low,high)]*n
                   with approximate upper and lower limits for each parameter
  x              :: a place holder for a final solution

  also a function called 'target' is needed.
  This function should take a parameter vector as input and return a the function to be minimized.

  The code below was implemented on the basis of the following sources of information:
  1. http://www.icsi.berkeley.edu/~storn/code.html
  2. http://www.daimi.au.dk/~krink/fec05/articles/JV_ComparativeStudy_CEC04.pdf
  3. http://ocw.mit.edu/NR/rdonlyres/Sloan-School-of-Management/15-099Fall2003/A40397B9-E8FB-4B45-A41B-D1F69218901F/0/ses2_storn_price.pdf


  The developers of the differential evolution method have this advice:
  (taken from ref. 1)

If you are going to optimize your own objective function with DE, you may try the
following classical settings for the input file first: Choose method e.g. DE/rand/1/bin,
set the number of parents NP to 10 times the number of parameters, select weighting
factor F=0.8, and crossover constant CR=0.9. It has been found recently that selecting
F from the interval [0.5, 1.0] randomly for each generation or for each difference
vector, a technique called dither, improves convergence behaviour significantly,
especially for noisy objective functions. It has also been found that setting CR to a
low value, e.g. CR=0.2 helps optimizing separable functions since it fosters the search
along the coordinate axes. On the contrary this choice is not effective if parameter
dependence is encountered, something which is frequently occuring in real-world optimization
problems rather than artificial test functions. So for parameter dependence the choice of
CR=0.9 is more appropriate. Another interesting empirical finding is that rasing NP above,
say, 40 does not substantially improve the convergence, independent of the number of
parameters. It is worthwhile to experiment with these suggestions. Make sure that you
initialize your parameter vectors by exploiting their full numerical range, i.e. if a
parameter is allowed to exhibit values in the range [-100, 100] it's a good idea to pick
the initial values from this range instead of unnecessarily restricting diversity.

Keep in mind that different problems often require different settings for NP, F and CR
(have a look into the different papers to get a feeling for the settings). If you still
get misconvergence you might want to try a different method. We mostly use DE/rand/1/... or DE/best/1/... .
The crossover method is not so important although Ken Price claims that binomial is never
worse than exponential. In case of misconvergence also check your choice of objective
function. There might be a better one to describe your problem. Any knowledge that you
have about the problem should be worked into the objective function. A good objective
function can make all the difference.

Note: NP is called population size in the routine below.)
Note: [0.5,1.0] dither is the default behavior unless f is set to a value other then None.

  """

  def __init__(self,
               evaluator,
               population_size=50,
               f=None,   # de stepsize
               cr=0.9,   # looks like this is the inverse of the crossover prob, i.e staying prob.
               eps=1e-2,
               n_cross=1,
               max_iter=10000,
               monitor_cycle=200,
               out=None,
               show_progress=False,
               show_progress_nth_cycle=1,
               insert_solution_vector=None,
               dither_constant=0.4):
    
    # Set running variables
    self.dither=dither_constant
    self.show_progress=show_progress
    self.show_progress_nth_cycle=show_progress_nth_cycle
    self.evaluator = evaluator
    self.population_size = population_size
    self.f = f
    self.cr = cr
    self.n_cross = n_cross
    self.max_iter = max_iter
    self.monitor_cycle = monitor_cycle
    self.vector_length = evaluator.n
    self.eps = eps
    self.population = []
    self.seeded = False
    if insert_solution_vector is not None:
      assert len( insert_solution_vector )==self.vector_length
      self.seeded = insert_solution_vector
    for ii in xrange(self.population_size):
      self.population.append( np.zeros( (self.vector_length ) ) )
    
    # Fill the scores vector with arbitrary high values
    self.scores = np.zeros( (self.population_size) )
    self.scores.fill(1000)

    # Call optimizer
    self.optimize()
    
    # Get the best result back
    self.best_score = self.scores.min()
    self.best_vector = self.population[ self.scores.argmin() ]
    self.evaluator.x = self.best_vector
    
    # Final call to show_progress to report convergence to user. 
    if self.show_progress:
      self.evaluator.print_status(
        self.scores.min(),
        self.scores.mean(),
        self.population[ self.scores.argmin() ],
        'Final')


  def optimize(self):
    # initialise the population please
    self.make_random_population()
    # score the population please
    self.score_population()
    converged = False
    monitor_score = self.scores.min()
    self.count = 0
    while not converged:
      self.evolve()
      location = self.scores.argmin()
      if self.show_progress:
        if self.count%self.show_progress_nth_cycle==0:
          # make here a call to a custom print_status function in the evaluator function
          # the function signature should be (min_target, mean_target, best vector)
          self.evaluator.print_status(
            self.scores.min(),
            self.scores.mean(),
            self.population[ self.scores.argmin() ],
            self.count)

      self.count += 1
      
      # check for convergence

      ## First criteria: 
      #if self.count%self.monitor_cycle==0:
        ## Check if the minimum has decreased more than self.eps. Otherwise stop.
        #if (monitor_score - self.scores.min() ) < self.eps:
          #converged = True
        #else:
          #monitor_score = self.scores.min()

      ## Second criteria
      #rd = self.scores.mean() - self.scores.min() 
      #rd = rd*rd/self.scores.min()*self.scores.min() + self.eps 
      #if ( rd < self.eps ):
        #converged = True

      ## Third criteria
      #if self.count>=self.max_iter:
        #converged =True

      # Fourth criteria
      if self.scores.min() < self.eps:
        converged = True


  def make_random_population(self):
    for ii in xrange(self.vector_length):
      delta  = self.evaluator.domain[ii][1]-self.evaluator.domain[ii][0]
      offset = self.evaluator.domain[ii][0]
      random_values = np.random.random_sample(self.population_size)
      random_values = random_values*delta+offset
      # now please place these values ni the proper places in the
      # vectors of the population we generated
      for vector, item in zip(self.population, random_values):
        vector[ii] = item
    if self.seeded is not False:
      self.population[0] = self.seeded

  def score_population(self):
    vectors = []
    for vector in self.population:
      vectors.append(vector)
    self.scores = self.evaluator.target(vectors)

  def evolve(self):
    # Build collection of input vectors
    test_vectors = []
    for ii in xrange(self.population_size):
      permut = np.random.permutation(self.population_size - 1)
      # make parent indices (each i1, i2, i3 is a scalar)
      i1=permut[0]
      if (i1>=ii):
        i1+=1
      i2=permut[1]
      if (i2>=ii):
        i2+=1
      i3=permut[2]
      if (i3>=ii):
        i3+=1
      # pick out one population member with the scalar. 
      x1 = self.population[ i1 ]
      x2 = self.population[ i2 ]
      x3 = self.population[ i3 ]

      if self.f is None:
        use_f = np.random.random_sample()/2.0 + 0.5
      else:
        use_f = self.f

      vi = x1 + use_f*(x2-x3)
      # prepare the offspring vector please
      permut = np.random.permutation(self.vector_length)
      test_vector = self.population[ii].copy()
      # first the parameters that sure cross over
      for jj in xrange( self.vector_length  ):
        if (jj<self.n_cross):
          test_vector[ permut[jj] ] = vi[ permut[jj] ]
        else:
          if (rnd[jj]>self.cr):
            test_vector[ permut[jj] ] = vi[ permut[jj] ]
      test_vectors.append(test_vector) 

    # get the score please
    test_scores = self.evaluator.target( test_vectors )
    
    # check if the score is lower
    for ii in xrange(self.population_size):
      if test_scores[ii] < self.scores[ii] :
        self.scores[ii] = test_scores[ii]
        self.population[ii] = test_vectors[ii]


  def show_population(self):
    print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    for vec in self.population:
      print list(vec)
    print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++"


class test_function(object):
  def __init__(self):
    self.x = None
    self.n = 9
    self.domain = [ (-100,100) ]*self.n
    self.optimizer =  differential_evolution_optimizer(self,population_size=100,n_cross=5)
    assert self.x*self.x.sum() < 1e-5

  def target(self, vector):
    tmp = vector.copy()
    result = ( np.cos(tmp * 10) + self.n + 1 ).sum() * ( (tmp)*(tmp) ).sum()
    return result


class test_rosenbrock_function(object):
  def __init__(self, dim=5):
    self.x = None
    self.n = 2*dim
    self.dim = dim
    self.domain = [ (-10,10) ]*self.n
    self.optimizer =  differential_evolution_optimizer(self, population_size = min(self.n*10,40), 
                                                       n_cross = self.n,
                                                       cr = 0.9, eps = 1e-12, 
                                                       show_progress = True)
    for x in self.x:
      assert abs(x-1.0)<1e-2


  def target(self, vectors):
    results = np.array([])
    for vector in vectors:
      # build parallel job. 
      tmp = vector.copy()
      x_vec = vector[0:self.dim]
      y_vec = vector[self.dim:]
      result=0
      for x,y in zip(x_vec,y_vec):
        # x is all the odds, y is all the evens
        # -> vector is a stack of self.dim odd entries and self.dim even entries
        result += 100.0 * ( (y - x * x ) ** 2.0 ) + ( 1 - x ) ** 2.0
      #print list(x_vec), list(y_vec), result
    # print(result)
      # For other problems. Need algorithm to analyze output and compute results accordingly. 
      # Run parallel job. Check when done. When done analyze result and append to results. 
      results = np.append(results, result)
    return results

  def print_status(self, mins,means,vector,txt):
    print txt,mins, means, list(vector)


def run():
  np.random.seed(0)
  test_rosenbrock_function(2)
  print "OK"


if __name__ == "__main__":
  run()