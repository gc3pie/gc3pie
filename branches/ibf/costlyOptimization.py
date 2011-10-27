#!/usr/bin/env python

import numpy as np
import scipy.optimize
import scipy.interpolate as si
import numpy.polynomial.chebyshev
import matplotlib
#matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab



class costlyOptimization(object):
  def __init__(self, paras):
    self.plotting = paras['plotting']
    self.x = np.array([])
    self.fx = np.array([])
    self.convCrit = paras['convCrit']
    self.target_fx = paras['target_fx']
    self.converged = False
    
    
  def updateInterpolationPoints(self, x, fx):
    self.x  = np.append(self.x, np.asarray(x))
    self.fx = np.append(self.fx, np.asarray(fx))
    indices = np.argsort(self.x)
    self.x = self.x[indices]
    self.fx = self.fx[indices]
    bestIndex = np.argmin(self.__computeNormedDistance(self.fx))
    self.best_x = self.x[bestIndex]
    self.best_fx  = self.fx[bestIndex]
    
  def checkConvergence(self):
    distance = self.__computeNormedDistance(self.best_fx)
    if distance < self.convCrit: 
      self.converged = True
    else: 
      self.converged = False
    return self.converged
    
  def updateApproximation(self):
    self.gx = si.lagrange(self.x, self.fx)
    
  def generateNewGuess(self):
    x0 = self.best_x
    def target(x):
      distance = self.__computeNormedDistance(self.gx(x))
      return distance
    xhat = scipy.optimize.newton_krylov(target, x0)
    return np.array([xhat])
  
  def __computeNormedDistance(self, fx):
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


