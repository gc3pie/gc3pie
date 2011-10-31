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



logger = wrapLogger(loggerName = 'costlyOptimizationLogger', streamVerb = 'DEBUG', logFile = os.path.join(os.getcwd(), 'costlyOpt.log'))

np.seterr(all='raise')

class iwdInterpolation(object):
    '''
    Inverse weighted density interpolation
    '''
    def __init__(self, x, fx):
        logger.debug('initializing new instance of iwdInterpolation')
        self.x = x
        self.fx = fx

    def __call__(self, x0):
        if np.array(x0).ndim == 1:
            x0 = np.array([x0])
        gxList = []
        for x0Ele in x0:
            if x0Ele in self.x:
                gx = self.fx[ np.all(x0Ele == self.x, 1) ]
            else:
                dVec = self.d(x0Ele, self.x)
                gx = np.sum( dVec * self.fx ) / np.sum(dVec)
            gxList.append(gx)
        return np.array(gxList)

    def d(self, x1, x2):
        distanceVectors = x1 - x2
        distanceNorms = np.array([ np.linalg.norm(distanceVector) for distanceVector in distanceVectors ])
        d = 1.0 / distanceNorms
        return d

if __name__ == '__main__':            
    fun = lambda x: np.sum(np.log(x))
    x1  = np.array([1, 2])
    x2  = np.array([3, 5])
    fx1 = fun(x1);
    fx2 = fun(x2);

    x   = np.array([x1, x2])
    fx  = np.array([fx1, fx2])

    print x
    print fx

    iwd = iwdInterpolation(x,fx)

    print 'evaluation:'
    print iwd([1,2])
    print iwd([[2,4],[2,3.5]])
 
    print 'exact value' 
    print fun([2,4])
    
    print 'done'

