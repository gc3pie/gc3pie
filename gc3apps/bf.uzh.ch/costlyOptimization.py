#!/usr/bin/env python

'''
  Optimization routine for idrisk paper. 
  Uses polynomial to approximate the (curretnly one dim) function and find the root by evaluating a large # of points. 
  Currently the values are normalized with ln. We want to reduce curvature in iBar, our main target variable. This could become a variable normalization function to be more general. 
  Plots can be created to track the optimization progress. 
'''


from __future__ import absolute_import, print_function
import os
import numpy as np
import scipy.optimize
import scipy.interpolate as si
import numpy.polynomial.chebyshev
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab

from pymods.support.support import wrapLogger


np.seterr(all='raise')

class costlyOptimization(object):
  '''
    Simple optimizer with the goal to minimize (expensive) function evaluations. 
    x are para combos
  '''
  def __init__(self, paras):
    self.jobname         = paras['jobname']
    self.xVars           = paras['xVars']
    self.xInitialGuess   = paras['xInitialParaCombo'] # np.array
    self.targetVars      = paras['targetVars']
    self.target_fx       = np.array(paras['target_fx'])
    self.convCrit        = paras['convCrit'] 
    self.converged       = False
    self.makePlots       = paras['plotting']
    self.nUpdates        = 0
    self.optimFolder     = paras['optimFolder']

    self.x               = self.xInitialGuess
    self.fx              = np.array([])

    self.nIntpoints_max  = 4
    self.maxDist         = 10 # maximal distance from second best point
    self.protected_ix    = False
    self.optimRange      = (-1.,0.)
    self.plotRange       = (-1.,0.)

    # Set up logger
    self.logger = wrapLogger(loggerName = 'costlyOptimizationLogger' + self.optimFolder, streamVerb = 'DEBUG', 
                        logFile = os.path.join(self.optimFolder, 'costlyOpt.log'))
    self.logger.debug('initialized new instance of costlyOptimization for jobname = %s' % paras['jobname'])

    
  def updateInterpolationPoints(self, x, fx):
    self.logger.debug('')
    self.logger.debug('entering updateInterpolationPoints')
    self.logger.debug('self.x = \n%s'  % self.x)
    self.logger.debug('self.fx = \n%s' % self.fx)
    self.logger.debug('x = \n%s' % x)
    self.logger.debug('fx = \n%s' % fx)
    if self.nUpdates == 0:
      self.logger.debug('check if fx has more than one value: ')
      self.logger.debug('np.nonzero = %s' % np.nonzero(fx))
      self.logger.debug('np.nonzero(fx)[0] = %s' % np.nonzero(fx)[0])
      numFxVals = len(np.nonzero(fx)[0])
      if not numFxVals > 1:
        self.logger.debug('numFxVals = %s. Sending back failure code. ' % numFxVals)
        return 1
      else: 
        self.logger.debug('numFxVals = %s. Storing values. ' % numFxVals)
      self.logger.debug('storing initial fx values %s corresponding to inital guess \n%s' % (fx, self.xInitialGuess))
      ixParaCombo = 0
      indicesToDelete = []
      for paraCombo,paraComboFx in zip(x, fx):
        # if paraCombo has no paraComboFx return value eliminate it from xInitialGuess
        if not paraComboFx:
          self.logger.debug('adding paraCombo = %s (element = %s) to indicesToDelete because of missing paraComboFx' % (paraCombo, ixParaCombo))
          indicesToDelete.append(ixParaCombo)
        else:
          self.fx = np.append(self.fx, paraComboFx)
        ixParaCombo += 1
      self.logger.debug('self.x before deletion = %s' % self.x)
      self.logger.debug('indicesToDelete = %s' % indicesToDelete)
      self.x = np.delete(self.x, indicesToDelete, axis = 0) # axis = 0 is necessary to keep m*n array
      self.logger.debug('self.x after deletion = %s\n' % self.x)
    else:
      self.logger.debug('check if the only new guess failed. If so terminate to avoid infinite loop')
      if len(fx) == 1:
        if not fx[0]:
          self.logger.debug('Evaluation of new guess at %s failed. Terminating para combo. ' % x)
          return 1
      self.logger.debug('nUpdates > 0.. proceding to append paraCombo')
      for paraCombo,paraComboFx in zip(x, fx):
        if not paraComboFx:
          self.logger.debug('could not evaluate paraCombo = %s because of missing paraComboFx' % paraCombo)
          continue
        self.logger.debug('checking if paracombo %s already exists' % paraCombo)
        if not paraCombo in self.x:
          self.logger.debug('appending para combo %s' % paraCombo)
          self.x  = np.append(self.x, np.array([paraCombo]), 0)
          self.fx = np.append(self.fx, paraComboFx)
        else:
          self.logger.critical('CRITICAL: x = %s already in self.x = %s' % (x, self.x))
    normedDistances = self._computeNormedDistance(self.fx)
    sortedIndicesDistances = np.argsort(normedDistances)
    self.sorted_x = self.x[sortedIndicesDistances]
    self.sorted_fx = self.fx[sortedIndicesDistances]
    self.sortedNormedDistances = normedDistances[sortedIndicesDistances]
    bestIndex = np.argmin(self._computeNormedDistance(self.fx))
    self.best_x = self.x[bestIndex]
    self.best_fx  = self.fx[bestIndex]
    self.logger.debug('done updating interpolation points. self.x = \n%s, \nself.fx = \n%s' % (self.x, self.fx))
    self.logger.debug('best points: best_x = \n%s, \nbest_fx = \n%s' % (self.best_x, self.best_fx))
    nAboveTarget = np.sum(self.sorted_fx - self.target_fx > 0)
    nBelowTarget = np.sum(self.sorted_fx - self.target_fx < 0)
    self.logger.debug('nAboveTarget = %s' % nAboveTarget)
    self.logger.debug('nBelowTarget = %s' % nBelowTarget)
    if nBelowTarget == 0:
      self.logger.critical('nBelowTarget is 0. Abandoning para combo. ')
      return 1
    if nAboveTarget == 0:
      self.logger.critical('nAboveTarget is 0. Abandoning para combo. ')
      return 1
    nAboveTargetInt = np.sum(self.sorted_fx[:self.nIntpoints_max] - self.target_fx > 0)
    nBelowTargetInt = np.sum(self.sorted_fx[:self.nIntpoints_max] - self.target_fx < 0)
    self.logger.debug('nAboveTargetInt = %s' % nAboveTargetInt)
    self.logger.debug('nBelowTargetInt = %s' % nBelowTargetInt)
    if nAboveTargetInt == 0:
      self.logger.debug('nAboveTargetInt == 0. Setting up protected_ix')
      aboveTarget = self.sorted_fx - self.target_fx > 0
      self.protected_ix = np.nonzero(aboveTarget)[0][0] # finds the first occurance of nonzero element in sorted array
    elif nBelowTarget == 0:
      self.logger.debug('nBelowTargetInt == 0. Setting up protected_ix')
      belowTarget = self.sorted_fx - self.target_fx < 0
      self.protected_ix = np.nonzero(belowTarget)[0][0]
    else:
      self.logger.debug('no need to protect any x. setting self.protected_ix = False')
      self.protected_ix = False
    self.logger.debug('protected_ix is = %s' % self.protected_ix)
    self.nUpdates += 1
    if self.nUpdates > 1 and self.makePlots:
      self.plot()
    self.logger.debug('self.x = \n%s'  % self.x)
    self.logger.debug('self.fx = \n%s' % self.fx)
    self.logger.debug('done updateInterpolationPoints')
    self.logger.debug('')
    return 0
    
  def checkConvergence(self):
    self.logger.debug('')
    self.logger.debug('entering checkConvergence...')
    distance = self._computeNormedDistance(self.best_fx)
    if np.all(distance < self.convCrit):
      self.logger.debug('SUCCESS: converged at fx %s with target %s to precision %s' % (self.best_fx, self.target_fx, self.convCrit))
      self.converged = True
    else: 
      self.logger.debug('not converged at fx %s with target %s to precision %s' % (self.best_fx, self.target_fx, self.convCrit))
      self.converged = False
    self.logger.debug('exiting convergence')
    self.logger.debug('')
    return self.converged
    
  def updateApproximation(self):
    self.logger.debug('')
    self.logger.debug('entering updateApproximation')
    self.logger.debug('self.x = \n%s'  % self.x)
    self.logger.debug('self.fx = \n%s' % self.fx)
    if len(self.x[0]) == 1: # we are dealing with the one-dimensional case
      self.logger.debug('self.x[0] == 1')
      xIn = self.sorted_x.copy()
      fIn = self.sorted_fx.copy()
      withinDistance = ( self.sortedNormedDistances[1] / self.sortedNormedDistances ) < 10
      withinDistance[0] = True # need at least two values
      withinDistance[1] = True # 
      if self.protected_ix:
        self.logger.debug('protected exists')
        try: 
          lenProtected_ix = len(self.protected_ix)
        except:
          lenProtected_ix = 1
        actual_nIntpoints_max = self.nIntpoints_max - lenProtected_ix
        withinDistance[self.protected_ix] = True # protected_ix must always be carried over
      else:
        self.logger.debug('protected doesnt exist')
        actual_nIntpoints_max = self.nIntpoints_max
      actual_nIntpoints = np.min([actual_nIntpoints_max, len(xIn)])
      self.logger.debug('actual_nIntpoints = %s' % actual_nIntpoints)
      self.logger.debug('len(xIn) = %s' % len(xIn))
      withinMaxIntpoints = np.array([ True ] * actual_nIntpoints + [ False ] * ( len(xIn) - actual_nIntpoints ) )
      withinMaxIntpoints[self.protected_ix] = True
      self.logger.debug('withinMaxIntpoints = %s' % withinMaxIntpoints)
      self.logger.debug('withinDistance = %s' % withinDistance)
      self.intPoints_ix = withinMaxIntpoints * withinDistance
      xIn = xIn[self.intPoints_ix]
      fIn = fIn[self.intPoints_ix]
      self.logger.debug('after nIntpoints  xIn = %s \n' % xIn)
      self.logger.debug('after nIntpoints  fIn = %s \n' % fIn)
      self.logger.debug('transforming fIn:')
      fIn = np.log((1. - self.target_fx) + fIn)
      self.logger.debug('sending xIn = %s to lagrange\n' % xIn)
      self.logger.debug('sending fIn = %s to lagrange\n' % fIn)
      ### create approximation ###
      self.gx = si.lagrange(xIn[:, 0], fIn)
      ### -------------------- ###
    else:
      self.logger.debug('multi-dimensional case not implemented... exiting')
      os._exit(1)
    if self.makePlots:
      self.plot()
    self.logger.debug('done updating approximation')
    self.logger.debug('')
    
  def generateNewGuess(self):
    def target(x):
      distance = self.gx(x) - self.target_fx
      return distance[0]
    self.logger.debug('')
    self.logger.debug('entering generateNewGuess')
    self.logger.debug('generating new guess for job %s in iteration %s: ' % (self.jobname, self.nUpdates + 1))
    self.logger.debug('current x points: \n%s' % self.x)
    self.logger.debug('current fx points: \n%s' % self.fx)
    if np.all(self.fx - self.target_fx > 0) or np.all(self.fx - self.target_fx < 0):
      self.logger.critical('the initial points %s with fx = \n%s do not contain the zero' % (self.xInitialGuess, self.fx))
    self.logger.debug('Evaluating polynomial to find zero. ')
    xMat = np.linspace(self.optimRange[0], self.optimRange[1], 1.e6)
    fxGrid = np.exp(self.gx(xMat)) - (1. - self.target_fx)
    normedDistances        = self._computeNormedDistance(fxGrid)
    sortedIndicesDistances = np.argsort(normedDistances)
    sortedDistances        = normedDistances[sortedIndicesDistances]
    self.logger.debug('sortedDistances = \n%s' % sortedDistances)
    xMatSorted             = xMat[sortedIndicesDistances]
    fxGridSorted           = fxGrid[sortedIndicesDistances]
    self.logger.debug('xMatSorted = \n%s' % xMatSorted)
    self.logger.debug('fxGridSorted = \n%s' % fxGridSorted)
    topCandidatesBool      = np.abs(sortedDistances - sortedDistances[0]) < 1.e-4
    topCandidates          = np.nonzero(topCandidatesBool)[0]
    if np.sum(topCandidatesBool) > 1: 
      self.logger.debug('More than one zero. Picking the one closest to intial guesses: ')
      xhats = xMatSorted[topCandidatesBool]
      self.logger.debug('Zeros of polynomial are: \n%s' % xhats)
      avgIntpoints = np.mean(self.sorted_x[self.intPoints_ix])
      bestIndex = topCandidates[np.argmin(np.abs(avgIntpoints - xhats))]
    else:
      self.logger.debug('Only one best index. No need to pick one out of many. Easy case...')
      bestIndex = sortedIndicesDistances[0]
    self.logger.debug('bestIndex is %s' % bestIndex)
    xhat = xMatSorted[bestIndex]
    fxhat = fxGridSorted[bestIndex]
    self.logger.debug('sending back optimal value given gx: xhat = %s fxhat = %s' % (xhat, fxhat))
    if xhat in self.x:
      self.logger.critical('xhat = %s already in self.x = %s' % (xhat, self.x))
      os._exit(1)
    self.logger.debug('done generateNewGuess')
    self.logger.debug('')
    return np.array([[xhat]])
  
  def _computeNormedDistance(self, fx):
    fx = np.asanyarray(fx)
    return np.abs(fx - self.target_fx)    

  def plot(self):
    # make plot
    self.logger.debug('making plot')
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.grid()
    # plot approximation
    xVals = np.linspace(self.plotRange[0], self.plotRange[1], 1.e3)
    try: 
      fxVals = self.gx(xVals)
    except: 
      self.logger.debug('couldnt create plot. self.gx not defined')
      return
    # plot polynomial
    ax.plot(xVals, fxVals)
    # plot actual values (non standardized)
    ax.plot(self.x, self.fx, marker = 'o', markersize = 2, linestyle = '')
    ax.plot(self.sorted_x[self.intPoints_ix], self.sorted_fx[self.intPoints_ix], marker = 'o', markersize = 2, linestyle = '', color = 'r')
    # xVals = np.linspace(-1., 0., 1.e2)
    # fxVals = np.log((1. - self.target_fx) + fIn)
    figureFile = os.path.join(self.optimFolder, 'lagrangeApprox_' + str(self.nUpdates) + '.eps')
    fig.savefig(figureFile)
    os.system('chmod 660 ' + figureFile)
    self.logger.debug('done making plot')


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


