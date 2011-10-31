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
import convexHull


logger = wrapLogger(loggerName = 'iwdInterpolationLogger', streamVerb = 'DEBUG', logFile = os.path.join(os.getcwd(), 'iwdInterpolation.log'))

np.seterr(all='raise')

class iwdInterpolation(object):
    '''
    Inverse weighted density interpolation
    '''
    def __init__(self, xMat, fx, normExp = 4, dx = 1e-3, makePlot=False):
        logger.debug('initializing new instance of iwdInterpolation')
        self.center = xMat.mean(0)
        angles = convexHull._angle_to_points(xMat, self.center)
        sortIndices = angles.argsort()
        self.angles = angles[sortIndices]
        self.xMat = xMat[sortIndices, :]
        self.fx = fx[sortIndices]
        self.normExp = normExp
        self.dx = dx
        self.makePlot = makePlot

        self.hullMat = convexHull.convex_hull(self.xMat.T, False).T
        self.hullAngles = convexHull._angle_to_points(self.hullMat, self.center)
        self.hullPoints = len(self.hullMat)

        if self.makePlot:
            self.plot()
        
    def __call__(self, x0):        
        if np.array(x0).ndim == 1:
            x0 = np.array([x0])
        gxList = []
        for x0Ele in x0:
            isInside = self.check_is_inside(x0Ele)
            logger.debug('isInside: %s' % isInside)
            if isInside:
                if x0Ele in self.xMat:
                    gx = self.fx[ np.all(x0Ele == self.xMat, 1) ]
                else:
                    dVec = self.d(x0Ele, self.xMat)
                    gx = np.sum( dVec * self.fx ) / np.sum(dVec)
                gxList.append(gx)
            else:
                hullPointPrev, hullPointNext = self.closest_hull_points(x0Ele)
                
                projected_x0 = convexHull.project_point_to_line_segment(hullPointPrev, hullPointNext, x0Ele)[0]
                logger.debug('projected_x0: %s' % projected_x0)
                
                if self.makePlot:
                    p.plot(x0Ele[0], x0Ele[1], 'ko')
                    p.plot(projected_x0[0], projected_x0[1], 'ro')
                
                q1 = projected_x0 - 1e-14 * (x0Ele - projected_x0) #be safely inside
                logger.debug('q1: %s' % q1)
                # logger.debug('x0Ele - q1 %s' % (x0Ele - q1))
                # logger.debug('norm %s' % np.linalg.norm(x0Ele - q1) )
                q2 = q1 - self.dx * (x0Ele - q1) / np.linalg.norm(x0Ele - q1) 
                logger.debug('q2: %s' % q2)
                
                f_q1 = self.__call__(q1)
                f_q2 = self.__call__(q2)
                logger.debug('f_q1: %s' % f_q1)
                logger.debug('f_q2: %s' % f_q2)
                df = (f_q1 - f_q2) / np.linalg.norm(q1 - q2)
                logger.debug('df: %s' % df)
                fx0 = f_q1 + np.linalg.norm(x0Ele - q1) * df
                logger.debug('fx0: %s' % fx0)
                gxList.append(fx0[0])
        return np.array(gxList)     

    def closest_hull_points(self, x0Ele):
        ''' finds the previous and next point on the hull'''
        angle_x0 = convexHull._angle_to_point(x0Ele, self.center)        
        logger.debug('angle_x0: %s' % angle_x0)
        pos = [i for i,x in enumerate(angle_x0 > self.hullAngles) if x == False]
        if len(pos) == 0:
            pos = 0
        else:
            pos = pos[0]
    
        if pos >= 0:
            hullPointPrev = self.hullMat[pos-1]
        else:
            hullPointPrev = self.hullMat[self.hullPoints-1]
        logger.debug('hullPointPrev: %s' % hullPointPrev)
        hullPointNext = self.hullMat[pos]
        logger.debug('hullPointNext: %s' % hullPointNext)
        return hullPointPrev, hullPointNext

    def d(self, x1, x2):
        distanceVectors = x1 - x2
        distanceNorms = np.array([ np.linalg.norm(distanceVector) for distanceVector in distanceVectors ])
        d = 1.0 / distanceNorms**self.normExp
        return d

    def plot(self):
        p.clf()
        
        p.plot(self.xMat[:, 0], self.xMat[:, 1], 'rx')
        p.plot(self.hullMat[:, 0], self.hullMat[:, 1], 'g+')
        p.plot((self.center[0],),(self.center[1],),'bo')
        
#        p.show()
    
    def check_is_inside(self, x0):
        logger.debug('entering is inside')
        logger.debug('x0: %s' %x0)

        hullPointPrev, hullPointNext = self.closest_hull_points(x0)
        A_prev_next = convexHull.area_of_triangle(self.center, hullPointPrev, hullPointNext)
        A_prev_x0 = convexHull.area_of_triangle(self.center, hullPointPrev, x0)
        A_next_x0 = convexHull.area_of_triangle(self.center, hullPointNext, x0)

        if A_prev_next > A_prev_x0 + A_next_x0:
            isInside = True
        else:
            isInside = False
        logger.debug('exiting is inside')
        return isInside
        


if __name__ == '__main__':   
    np.random.seed(123)
    nPoints = 20
    makePlot = False
    normExp = 4
    dx = 1e-7
    
    def fun(x):
        x = np.asarray(x)
        if x.ndim == 1:
            x = np.array([x])
        return np.array([ np.sum(np.log(xEle+1)) for xEle in x ])
#        return np.array([ np.sum(xEle) for xEle in x ])
        

    xMat = np.random.random_sample((nPoints, 2))
    fx = fun(xMat)

    print fx 

    iwd = iwdInterpolation(xMat, fx, normExp, dx, makePlot)

    print 'evaluation with point outside:'
    testVec = np.array([1,0.6])
    print iwd.check_is_inside(testVec)
    print iwd(testVec)
 
    print 'exact value' 
    print fun(testVec)

    exit()
    if makePlot:
        p.show()

    print 'evaluation with point inside:'
    testVec = np.array([0.5, 0.6])
    print iwd.check_is_inside(testVec)
    print iwd(testVec)

    print 'exact value' 
    print fun(testVec)

    print 'done'

