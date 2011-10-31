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


logger = wrapLogger(loggerName = 'iwdInterpolationLogger', streamVerb = 'INFO', logFile = os.path.join(os.getcwd(), 'iwdInterpolation.log'))

np.seterr(all='raise')

class iwdInterpolation(object):
    '''
    Inverse weighted density interpolation
    '''
    def __init__(self, xMat, fx):
        logger.debug('initializing new instance of iwdInterpolation')
        self.center = xMat.mean(0)
        angles = convexHull._angle_to_points(xMat, self.center)
        sortIndices = angles.argsort()
        self.angles = angles[sortIndices]
        self.xMat = xMat[sortIndices, :]

        #self.x = [x[0] for x in zip(self.xMat.transpose())]
        self.fx = fx[sortIndices]
        self.nPoints = len(xMat)

        self.hullMat = convexHull.convex_hull(self.xMat.T, False).T
        self.hullAngles = convexHull._angle_to_points(self.hullMat, self.center)
        # np.apply_along_axis(convexHull.angle_to_point, 0, self.hullMat, self.center)
        #self.hull = [x[0] for x in zip(self.hullMat.transpose())]
        self.hullPoints = len(self.hullMat)
        self.plot()
#        p.show()

        #logger.debug('self.x       %s' %self.x)
        #logger.debug('self.fx      %s' %self.fx)
        #logger.debug('self.xMat    %s' %self.xMat)
        #logger.debug('center       %s' %self.center)
        #logger.debug('hull         %s' %self.hull)
        #logger.debug('hullPoints   %s' %self.hullPoints)
        #logger.debug('hullAngles   %s' %self.hullAngles)
#        self.plot()
        
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
                #determine two closest points of the hull
                angle_x0 = convexHull._angle_to_point(x0Ele, self.center)        
                logger.debug('angle_x0: %s' % angle_x0)
                pos = [i for i,x in enumerate(angle_x0 > self.hullAngles) if x == False]
                if len(pos) == 0:
                    pos = 0
                else:
                    pos = pos[0]
    
                if pos >= 0:
                    hullPointA = self.hullMat[pos-1]
                else:
                    hullPointA = self.hullMat[self.nPoints-1]
                logger.debug('hullPointA: %s' % hullPointA)
                hullPointB = self.hullMat[pos]
                logger.debug('hullPointB: %s' % hullPointB)
    
                projected_x0 = convexHull.project_point_to_line_segment(hullPointA, hullPointB, x0Ele)[0]
                logger.debug('projected_x0: %s' % projected_x0)
                
                p.plot(x0Ele[0], x0Ele[1], 'ko')
                p.plot(projected_x0[0], projected_x0[1], 'ro')
                
                q1 = projected_x0 - 1e-14 * (x0Ele - projected_x0) # be safely inside
                logger.debug('q1: %s' % q1)
                dx = 1e-3
                q2 = q1 - dx * (x0Ele - q1)
                logger.debug('q2: %s' % q2)
                
                f_q1 = self.__call__(q1)
                f_q2 = self.__call__(q2)
                
                df = np.linalg.norm(q1 - q2) * (f_q1 - f_q2)
                fx0 = f_q1 + np.linalg.norm(x0Ele - q1) * df
                gxList.append(fx0[0])
        return np.array(gxList)     
    

    def d(self, x1, x2):
        distanceVectors = x1 - x2
        distanceNorms = np.array([ np.linalg.norm(distanceVector) for distanceVector in distanceVectors ])
        d = 1.0 / distanceNorms**2
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
        angle_x0 = convexHull._angle_to_point(x0, self.center)        
        logger.debug('angle_x0: %s' % angle_x0)
#        print angle_x0 > self.angles
        pos = [i for i,x in enumerate(angle_x0 > self.hullAngles) if x == False]
        logger.debug('pos: %s' % pos)
        if len(pos) == 0:
            pos = 0
        else:
            pos = pos[0]

        if pos >= 0:
            A_prev_next = convexHull.area_of_triangle(self.center, self.hullMat[pos-1], self.hullMat[pos])
            A_prev_x0 = convexHull.area_of_triangle(self.center, self.hullMat[pos-1], x0)
        else:
            A_prev_next = convexHull.area_of_triangle(self.center, self.hullMat[self.hullPoints-1], self.x[pos])
            A_prev_x0 = convexHull.area_of_triangle(self.center, self.hullMat[self.hullPoints-1], x0)
        A_next_x0 = convexHull.area_of_triangle(self.center, self.hullMat[pos], x0)

        if A_prev_next > A_prev_x0 + A_next_x0:
            isInside = True
        else:
            isInside = False
        logger.debug('exiting is inside')
        return isInside
        


if __name__ == '__main__':   
    np.random.seed(123)
    nPoints = 100
    
    def fun(x):
        x = np.asarray(x)
        if x.ndim == 1:
            x = np.array([x])
        return np.array([ np.sum(np.sin(xEle)) for xEle in x ])
        

    xMat = np.random.random_sample((nPoints, 2))
    fx = fun(xMat)

    print fx 

    iwd = iwdInterpolation(xMat, fx)

    print 'evaluation with point outside:'
    testVec = np.array([1,0.6])
    print iwd.check_is_inside(testVec)
    print iwd(testVec)
 
    print 'exact value' 
    print fun(testVec)

    print 'evaluation with point inside:'
    testVec = np.array([0.5, 0.9])
    print iwd.check_is_inside(testVec)
    print iwd(testVec)

    print 'exact value' 
    print fun(testVec)

    print 'done'

