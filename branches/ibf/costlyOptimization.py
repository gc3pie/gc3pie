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
from iwdInterpolationNew import iwdInterpolation

# Set up logger
logger = wrapLogger(loggerName = 'costlyOptimizationLogger', streamVerb = 'DEBUG', logFile = os.path.join(os.getcwd(), 'costlyOpt.log'))

np.seterr(all='raise')

class costlyOptimization(object):
  '''
    Simple optimizer with the goal to minimize (expensive) function evaluations. 
    x are para combos
  '''
  def __init__(self, paras):
    logger.debug('initializing new instance of costlyOptimization')
    self.xVars         = paras['xVars']
    self.xInitialGuess = paras['xInitialParaCombo']
    self.targetVar     = paras['targetVar']
    self.target_fx     = np.array(paras['target_fx'])
    self.convCrit      = paras['convCrit'] 
    self.converged     = False
    self.makePlots          = True

    self.x             = self.xInitialGuess
    self.fx            = np.array([])
    #np.empty( ( 0, len(self.x)) )
    #np.array([[]])    
    
  def updateInterpolationPoints(self, x, fx):
    for paraCombo in x:
      if not paraCombo in self.x:
        self.x  = np.append(self.x, np.array([paraCombo]), 0)
      else: 
        logger.critical('x = %s already in self.x = %s' % (x, self.x))
    self.fx = np.append(self.fx, fx)
    bestIndex = np.argmin(self._computeNormedDistance(self.fx))
    self.best_x = self.x[bestIndex]
    self.best_fx  = self.fx[bestIndex]
    
  def checkConvergence(self):
    logger.debug('checking convergence...')
    distance = self._computeNormedDistance(self.best_fx)
    if np.all(distance < self.convCrit):
      logger.debug('converged at fx %s with target %s to precision %s' % (self.best_fx, self.target_fx, self.convCrit))
      self.converged = True
    else: 
      logger.debug('not converged at fx %s with target %s to precision %s' % (self.best_fx, self.target_fx, self.convCrit))
      self.converged = False
    return self.converged
    
  def updateApproximation(self):
    logger.debug('updating approximation')
    if len(self.x[0]) == 1: # we are dealing with the one-dimensional case
      self.gx = si.lagrange(self.x[:, 0], self.fx)
    #self.gx = iwdInterpolation(self.x, self.fx)
    logger.debug('done updating approximation')
    
  def generateNewGuess(self):
    def target(x):
      distance = self.gx(x) - self.target_fx
      return distance[0]
    logger.debug('generating new guess: ')
    logger.debug('current x points: %s' % self.x)
    logger.debug('current fx points: %s' % self.fx)
    if np.all(self.fx > 0) or np.all(self.fx < 0):
      logger.critical('the initial points %s do not contain the zero')  
    try: 
      raise ValueError
      #logger.debug('trying brentq')
      #x0 = self.best_x
      #xhat = scipy.optimize.newton_krylov(target, x0)
      ##xhat = scipy.optimize.brentq(f = target, a = np.min(self.x), b = np.max(self.x))
      #fxhat = self.gx(xhat)
    except ValueError: 
      logger.debug('brentq failed, trying naive method')
##      min_x = np.min(self.x[:, 0])
##      max_x = np.max(self.x[:, 0])
##      min_y = np.min(self.x[:, 1])
##      max_y = np.max(self.x[:, 1])
##      xGrid = np.linspace(0.5, 1.3, 100)
##      YGrid = np.linspace(-1., 2, 100)
##      X, Y = np.meshgrid(xGrid, YGrid)
##      xMat = zip(X.flatten(), Y.flatten())
      xMat = np.linspace(-2, 2, 1.e6)
      fxGrid = self.gx(xMat)
      if self.makePlots:
          # make plot
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(xMat, fxGrid)
        figureFile = 'lagrangeApprox.eps'
        fig.savefig(figureFile)
        os.system('chmod 660 ' + figureFile)
      bestIndex = np.argmin(self._computeNormedDistance(fxGrid))
      xhat = xMat[bestIndex]
      fxhat = fxGrid[bestIndex]
    logger.debug('sending back optimal value given gx: xhat = %s fxhat = %s' % (xhat, fxhat))
    if xhat in self.x:
      logger.critical('xhat = %s already in self.x = %s' % (xhat, self.x))
      os._exit(1)
    return np.array([[xhat]])
  
  def _computeNormedDistance(self, fx):
    fx = np.asanyarray(fx)
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


