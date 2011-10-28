#!/usr/bin/env python

import os
import numpy as np
import scipy.optimize
import scipy.interpolate as si
import numpy.polynomial.chebyshev
import matplotlib
#matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab

from pymods.support.support import wrapLogger

# Set up logger
logger = wrapLogger(loggerName = 'costlyOptimizationLogger', streamVerb = 'DEBUG', logFile = os.path.join(os.getcwd(), 'costlyOpt.log'))

np.seterr(all='raise')

class costlyOptimization(object):
  '''
    Simple optimizer with the goal to minimize (expensive) function evaluations. 
  '''
  def __init__(self, paras):
    logger.debug('initializing new instance of costlyOptimization')
    self.plotting = paras['plotting']
    self.x = np.array([])
    self.fx = np.array([])
    self.convCrit = paras['convCrit']
    self.target_fx = paras['target_fx']
    self.converged = False
    
    
  def updateInterpolationPoints(self, x, fx):
    if not x in self.x:
      self.x  = np.append(self.x, np.asarray(x))
    else: 
      logger.critical('x = %s already in self.x = %s' % (x, self.x))
    self.fx = np.append(self.fx, np.asarray(fx))
    indices = np.argsort(self.x)
    self.x = self.x[indices]
    self.fx = self.fx[indices]
    bestIndex = np.argmin(self._computeNormedDistance(self.fx))
    self.best_x = self.x[bestIndex]
    self.best_fx  = self.fx[bestIndex]
    
  def checkConvergence(self):
    distance = self._computeNormedDistance(self.best_fx)
    if distance < self.convCrit: 
      self.converged = True
    else: 
      self.converged = False
    return self.converged
    
  def updateApproximation(self):
    logger.debug('updating approximation')
    self.gx = si.lagrange(self.x, self.fx)
    logger.debug('done updating approximation')
    
  def generateNewGuess(self):
    x0 = self.best_x
    def target(x):
      distance = self.gx(x) - self.target_fx
      return distance
    logger.debug('generating new guess: ')
    logger.debug('current x points: %s' % self.x)
    logger.debug('current fx points: %s' % self.fx)
    distance = self.fx - self.target_fx
    if np.all(distance > 0) or np.all(distance < 0):
      logger.critical('the initial points %s do not contain the zero')  
    try: 
      logger.debug('trying brentq')
      #xhat = scipy.optimize.newton_krylov(target, x0)
      xhat = scipy.optimize.brentq(f = target, a = np.min(self.x), b = np.max(self.x))
      fxhat = self.gx(xhat)
    except ValueError: 
      logger.debug('brentq failed, trying naive method')
      xGrid = np.linspace(np.min(self.x), np.max(self.x), 1000)
      fxGrid = self.gx(xGrid)
      bestIndex = np.argmin(self._computeNormedDistance(fxGrid))
      xhat = xGrid[bestIndex]
      fxhat = fxGrid[bestIndex]
    logger.debug('sending back optimal value given gx: xhat = %s fxhat = %s' % (xhat, fxhat))
    if xhat in self.x:
      logger.critical('xhat = %s already in self.x = %s' % (x, self.x))
      os._exit(1)
    return np.array([xhat])
  
  def _computeNormedDistance(self, fx):
    return np.abs(fx - self.target_fx)



if __name__ == '__main__':
  a = np.linspace(0.1, 2, 10)
  b = -np.log(a)
  c = si.lagrange(a, b)
  plt.plot(a, c(a))
  plt.plot(a, b)
  #plt.show()
  
  
  fun = lambda x: -np.log(x)
  x   = np.array([0.01, 5])
  
  xhat = np.array([10])
  iter = 0
  while np.abs(fun(xhat)) > 1.e-4:
    print 'iter %d' % iter
    fx  = fun(x)
    gx  = si.lagrange(x, fx)
    x0 = xhat
    xhat = scipy.optimize.newton_krylov(gx, x0)
    print xhat
    x = np.append(x, xhat)
    iter += 1
    
  print 'done'


