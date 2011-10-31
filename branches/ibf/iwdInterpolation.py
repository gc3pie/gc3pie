#!/usr/bin/env python

import os
import numpy as np
import scipy.optimize
import scipy.interpolate as si
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import pylab as p

from pymods.support.support import wrapLogger
from convexHull import *


logger = wrapLogger(loggerName = 'iwdInterpolationLogger', streamVerb = 'DEBUG', logFile = os.path.join(os.getcwd(), 'iwdInterpolation.log'))

np.seterr(all='raise')

class iwdInterpolation(object):
    '''
    Inverse weighted density interpolation
    '''
    def __init__(self, xMat, fx):
        logger.debug('initializing new instance of iwdInterpolation')
        self.center = xMat.mean(1)
        angles = n.apply_along_axis(angle_to_point, 0, xMat, self.center)
        self.angles = angles[angles.argsort()]
        self.xMat = xMat[:,angles.argsort()]

        self.x = [x[0] for x in zip(self.xMat.transpose())]
        self.fx = fx
        self.nPoints = len(x)

        self.hullMat = convex_hull(self.xMat, False)
        self.hullAngles = n.apply_along_axis(angle_to_point, 0, self.hullMat, self.center)
        self.hull = [x[0] for x in zip(self.hullMat.transpose())]
        self.hullPoints = len(self.hull)
#        p.show()

        logger.debug('self.x       %s' %self.x)
        logger.debug('self.fx      %s' %self.fx)
        logger.debug('self.xMat    %s' %self.xMat)
        logger.debug('center       %s' %self.center)
        logger.debug('hull         %s' %self.hull)
        logger.debug('hullPoints   %s' %self.hullPoints)
        logger.debug('hullAngles   %s' %self.hullAngles)
#        self.plot()
        
    def __call__(self, x0):
        isInside = self.check_is_inside(x0)
        logger.debug('isInside: %s' % isInside)

        if isInside:
            sum_f0 = 0
            sum_d = 0
            for ix in range(0,len(self.x)):
                dist = self.d(x0, self.x[ix])
                if np.abs(dist) < 1e-14:
                    return self.fx[ix]
                sum_f0 = sum_f0 + dist * self.fx[ix]
                sum_d = sum_d + dist
                sum_f0 = sum_f0 / sum_d
            return sum_f0
        else:
            #determine two closest points of the hull
            angle_x0 = angle_to_point(x0, self.center)        
            logger.debug('angle_x0: %s' % angle_x0)
            pos = [i for i,x in enumerate(angle_x0 > self.hullAngles) if x == False]
            if len(pos) == 0:
                pos = 0
            else:
                pos = pos[0]

            if pos >= 0:
                hullPointA = self.hull[pos-1]
            else:
                hullPointA = self.hull[self.nPoints-1]
            logger.debug('hullPointA: %s' % hullPointA)
            hullPointB = self.hull[pos]
            logger.debug('hullPointB: %s' % hullPointB)

            projected_x0 = project_point_to_line_segment(hullPointA, hullPointB, x0)
            logger.debug('projected_x0: %s' % projected_x0)
            self.plot()
            p.plot(x0[0], x0[1], 'ko')
            p.plot(projected_x0[0], projected_x0[1], 'ro')
            
            q1 = projected_x0 - 1e-14 * (x0 - projected_x0) # be safely inside
            logger.debug('q1: %s' % q1)
            dx = 1e-6
            q2 = q1 - dx * (x0 - q1)
            logger.debug('q2: %s' % q2)
            
            f_q1 = self.__call__(q1)
            f_q2 = self.__call__(q2)
            fx0 = f_q1 + np.linalg.norm(x0 - q1) / np.linalg.norm(q1 - q2) * (f_q1 - f_q2)

            return fx0

    def d(self, x1, x2):
        return 1.0 / np.linalg.norm(x1 - x2)

    def plot(self):
        p.clf()
        
        p.plot(self.xMat[0], self.xMat[1], 'rx')
        p.plot(self.hullMat[0], self.hullMat[1], 'go')
        p.plot((self.center[0],),(self.center[1],),'bo')
        
#        p.show()
    
    def check_is_inside(self,x0):
        logger.debug('entering is inside')
        logger.debug('x0: %s' %x0)
        angle_x0 = angle_to_point(x0, self.center)        
        logger.debug('angle_x0: %s' % angle_x0)
#        print angle_x0 > self.angles
        pos = [i for i,x in enumerate(angle_x0 > self.hullAngles) if x == False]
        logger.debug('pos: %s' % pos)
        if len(pos) == 0:
            pos = 0
        else:
            pos = pos[0]

        if pos >= 0:
            A_prev_next = area_of_triangle(self.center, self.hull[pos-1], self.hull[pos])
            A_prev_x0 = area_of_triangle(self.center, self.hull[pos-1], x0)
        else:
            A_prev_next = area_of_triangle(self.center, self.hull[self.hullPoints-1], self.x[pos])
            A_prev_x0 = area_of_triangle(self.center, self.hull[self.hullPoints-1], x0)
        A_next_x0 = area_of_triangle(self.center, self.hull[pos], x0)

        if A_prev_next > A_prev_x0 + A_next_x0:
            isInside = True
        else:
            isInside = False
        logger.debug('exiting is inside')
        return isInside
        


if __name__ == '__main__':   
    np.random.seed(123)
    nPoints = 7
    
    fun = lambda x: np.sum(np.log(x))

    xMat = np.random.random_sample((2,nPoints))
    xCoord = [x[0] for x in zip(xMat.transpose())]
    fx = np.zeros(nPoints)

    for ix in range(0, nPoints):
        fx[ix] = fun(xCoord[ix])

    print fx 

    iwd = iwdInterpolation(xMat,fx)

    print 'evaluation:'
    print iwd([1,0.6])
 
    print 'exact value' 
    print fun([1,0.6])

    print 'evaluation:'
    print iwd([0.5,0.4])
 
    print 'exact value' 
    print fun([0.5,0.4])

    p.show()
    print 'done'

