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
        sum_f0 = 0
        sum_d = 0
        for ix in range(0,len(self.x)):
            dist = self.d(x0, self.x[ix])
            sum_f0 = sum_f0 + dist * self.fx[ix]
            sum_d = sum_d + dist
        sum_f0 = sum_f0 / sum_d
        return sum_f0

    def d(self, x1, x2):
        return 1.0 / np.linalg.norm(x1 - x2)

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
    print iwd([2,4])
 
    print 'exact value' 
    print fun([2,4])
    
    print 'done'

