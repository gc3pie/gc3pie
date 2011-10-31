#!/usr/bin/env python

import numpy as n, pylab as p, time
import os

from pymods.support.support import wrapLogger

logger = wrapLogger(loggerName = 'convexHullLogger', streamVerb = 'INFO', logFile = os.path.join(os.getcwd(), 'convexHull.log'))


def _angle_to_points(points, centre):
    '''calculate angle in 2-D between points and x axis'''
    resOut = []
    for point in points:
        delta = point - centre
        res = n.arctan(delta[1] / delta[0])
        if delta[0] < 0:
            res += n.pi
        resOut.append(res)
    return n.array(resOut)

def _angle_to_point(point, centre):
    '''calculate angle in 2-D between points and x axis'''
    delta = point - centre
    res = n.arctan(delta[1] / delta[0])
    if delta[0] < 0:
        res += n.pi
    return res


def _draw_triangle(p1, p2, p3, **kwargs):
    tmp = n.vstack((p1,p2,p3))
    x,y = [x[0] for x in zip(tmp.transpose())]
    p.fill(x,y, **kwargs)
    #time.sleep(0.2)


def area_of_triangle(p1, p2, p3):
    '''calculate area of any triangle given co-ordinates of the corners'''
    return n.linalg.norm(n.cross((p2 - p1), (p3 - p1)))/2.


def convex_hull(points, graphic=True, smidgen=0.0075):
    '''Calculate subset of points that make a convex hull around points

    Recursively eliminates points that lie inside two neighbouring points until only convex hull is remaining.
    
    :Parameters:
        points : ndarray (2 x m) 
            array of points for which to find hull
        graphic : bool
            use pylab to show progress?
        smidgen : float
            offset for graphic number labels - useful values depend on your data range

    :Returns:
        hull_points : ndarray (2 x n)
            convex hull surrounding points
    '''
    logger.debug('start convex hull')

    if graphic:
        p.clf()
        p.plot(points[0], points[1], 'ro')
    n_pts = points.shape[1]
    assert(n_pts > 1)
    centre = points.mean(1)
    if graphic: p.plot((centre[0],),(centre[1],),'bo')
    angles = n.apply_along_axis(_angle_to_point, 0, points, centre)

    logger.debug('angles: %s' %angles)

    pts_ord = points[:,angles.argsort()]
    logger.debug('pts_ord: %s' %pts_ord)

    if graphic:
        for i in xrange(n_pts):
            p.text(pts_ord[0,i] + smidgen, pts_ord[1,i] + smidgen, '%d' % i)
    pts = [x[0] for x in zip(pts_ord.transpose())]
    logger.debug('pts: %s' %pts)
    prev_pts = len(pts) + 1
    k = 0
    while prev_pts > n_pts:
        logger.debug(' ')
        logger.debug('new round of points')
        logger.debug('current # of points %d' %n_pts )
        prev_pts = n_pts
        n_pts = len(pts)
        if graphic: p.gca().patches = []
        i = -2
        while i < (n_pts - 2):
            logger.debug('i: %d' % i)
            Aij = area_of_triangle(centre, pts[i],     pts[(i + 1) % n_pts])
            logger.debug('Aij: %s' % Aij)
            Ajk = area_of_triangle(centre, pts[(i + 1) % n_pts], pts[(i + 2) % n_pts])
            logger.debug('Aik: %s' % Ajk)
            Aik = area_of_triangle(centre, pts[i],     pts[(i + 2) % n_pts])
            logger.debug('Aik: %s' % Aik)
            if graphic:
                _draw_triangle(centre, pts[i], pts[(i + 1) % n_pts], facecolor='blue', alpha = 0.2)
                _draw_triangle(centre, pts[(i + 1) % n_pts], pts[(i + 2) % n_pts], facecolor='green', alpha = 0.2)
                _draw_triangle(centre, pts[i], pts[(i + 2) % n_pts], facecolor='red', alpha = 0.2)
            if Aij + Ajk < Aik:
                if graphic: 
                    p.plot((pts[i + 1][0],),(pts[i + 1][1],),'go')
                del pts[i+1]
            i += 1
            n_pts = len(pts)
        k += 1
    return n.asarray(pts).transpose()


def project_point_to_line_segment(A, B, points):
    '''
    Parameters:
      A: Vector of beginning line points
      B: Vector of ending line points
      points: The points for which the projection is to be done. 
    Returns:
      qOut: the closest points (projections) on the line segment
    '''
    if A.ndim == 1:
        A = n.array([A])
        B = n.array([B])
        points = n.array([points])

    qOut = n.empty( (0, 2) )
    for a, b, point in zip(A,B, points):
        # vector from A to B
        AB = (b-a)
        # squared distance from A to B
        AB_squared = n.dot(AB,AB)

        if AB_squared == 0:
            # A and B are the same point
            q = a
        else:
            # vector from A to p
            Ap = (point-a)
            # from http://stackoverflow.com/questions/849211/
            # Consider the line extending the segment, parameterized as A + t (B - A)
            # We find projection of point p onto the line.
            # It falls where t = [(p-A) . (B-A)] / |B-A|^2
            t = n.dot(Ap,AB)/AB_squared
            if t < 0.0:
            # "Before" A on the line, just return A
                q = a
            elif t > 1.0:
                # "After" B on the line, just return B
                q = b
            else:
                # projection lines "inbetween" A and B on the line
                q = a + t * AB
            #q = q[:, n.newaxis]
            q = n.array([q])
        qOut = n.append(qOut, q, 0)
    return qOut


if __name__ == "__main__":
    print 'entered main'
    #points = n.random.random_sample((2,40))
    #print points
    #hull_pts = convex_hull(points)

#    p.show()
    # p.clf()
    # p.plot(points[0], points[1], 'ro')


#    pylab.savefig('simple_plot3') # 
#    p.show() 

    # print hull_pts

    A = n.array([[1, 2], [2, 4]])
    B = n.array([[3, 4], [1, 2]])
    p = n.array([[3.5, 10], [3,4]])
    q = project_point_to_line_segment(A, B, p)
    print q
    
    _angle_to_point(n.array([[1, 4], [1, 6]]), n.array([2, 4]))
    
    print 'done'


