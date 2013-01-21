#! /usr/bin/env python
#
"""
Implementation of Fliege 2012
"""
# Copyright (C) 2013 University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__version__ = '$Revision$'
__author__ = 'Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
__docformat__ = 'reStructuredText'


import os
import sys
import random  # used for non-repetitive random number

import numpy as np
import numpy.linalg

# floating point constants (machine-dependent)
fpinfo = np.finfo(float)
EPSILON = fpinfo.eps
TINY = fpinfo.tiny
LARGE = fpinfo.max / 2.

# Set numpy print options
np.set_printoptions(linewidth=300, precision=5, suppress=True)

def main_algo(A, b, sample_fn=np.random.random):
    '''
    Return a list of (approximate) solutions to the linear system `Ax = b`.

    :param A: a NumPy `m` times `n` 2D-array, representing a matrix with `m` rows and `n` columns
    :param b: a Numpy 1D-array real-valued vector of `n` elements
    :param sample_fn: a function returning a random 1D-vector; the default implementation chooses all coordinates uniformly from the interval [0,1].
    '''
    # Input:
    # * A is a with m row vectors
    m = A.shape[0]
    # * each row vector has n columns
    n = A.shape[1]
    # b is a column vector with n rows
    assert m == b.shape[0]
    # consistency checks
    assert n**2/n>4, "n is not large enough. Increase eq. system to fulfill n**2>4n"
    # DEBUG
    print "Given data:"
    print "  m = ", m
    print "  n = ", n
    print "  A = ", str.join("\n       ", str(A).split('\n'))
    print "  b = ", b

    # Step 2. Generate random sample of normals (fulfilling Assumption 1.)
    v = [ sample_fn(n) for _ in range(n+1) ] # n+1 random vectors of n elements each
    print "Initial choices of v's:"
    for l, v_l in enumerate(v):
        print "  v_%d = %s" % (l, v_l)

    # save all (i,j) pairs for later -- this is invariant in the Step 3 loop
    all_ij_pairs = [ (i,j) for i in range(n+1) for j in range(n+1) if i<j ]
    #print 'potential ij_pairs = ', all_ij_pairs
    assert len(all_ij_pairs) == ((n+1)*n / 2)

    # Step 3.
    for k in range(m): # loop over eqs
        print "Step 3, iteration %d starting ..." % k
        # Step 3(a): choose n+1 random pairs (i_l, j_l) with i_l < j_l
        # pick randomly from all_ij_pairs set to get n+1 random pairs
        ij_pairs = random.sample(all_ij_pairs, n+1)
        assert len(ij_pairs) == n+1
        print '  ij_pairs = ', ij_pairs
        # Step 3(b): recombine the v's; use x's as temporary storage
        x = [ rec(v[i], v[j], A[k,:], b[k])
              for l, (i, j) in enumerate(ij_pairs)
        ]
        # Step 3(d): rename x's -> v's
        for l in range(n+1):
            v[l] = x[l]

    # final result
    return v


def rec(u, v, a, beta, q=0):
    '''
    Recombination function, as defined in Fliege (2012).

    If the optional parameter `q` is > 0, then ensure that the
    denominator in the `t` factor is larger than `q`, by substituting
    `v` for a random convex combination of `u` and `v`.

    '''
    t0 = beta - np.dot(a, v)
    t1 = np.dot(a, (u - v))
    #assert np.abs(t1) > EPSILON
    while q > 0 and abs(t1) < q:
        # see remark 8 on page 7
        c = 1. + t0 * random.random()
        v = u + c*(v - u)
        t1 = np.dot(a, (u - v))
    t = t0 / t1
    return  (t * u + (1. - t) * v)


def _check_distance(A, b, vs):
    print "Final values of v's:"
    for l, v_l in enumerate(vs):
      print "  v_%d = %s" % (l, v_l)
    print "Distances of solutions computed by Fliege's algorithm:"
    for l, v_l in enumerate(vs):
        dist = np.linalg.norm(np.dot(A,v_l) - b)
        print ("  |Av_%s - b| = %g" % (l, dist))

    print "Numpy's `linalg.solve` solution:"
    v_prime = np.linalg.solve(A,b)
    print "  v' = %s" % v_prime
    print "Distance of Numpy's `linalg.solve` solution:"
    dist_prime = np.linalg.norm(np.dot(A,v_prime) - b)
    print ("  |Av' - b| = %g" % dist_prime)


def test_with_random_matrix(dim=5):
    A = np.random.randint(low=1, high=9,size=(dim, dim))
    b = np.random.randint(low=1, high=9,size=(dim,))

    rank = np.linalg.matrix_rank(A)
    assert rank == dim, 'Matrix needs to have full rank. '

    res = main_algo(A, b)

    _check_distance(A, b, res)


def test_with_identity_matrix(dim=5):
    A = np.eye(dim)
    b = np.random.randint(low=1, high=9,size=(dim,))

    res = main_algo(A, b)

    _check_distance(A, b, res)


if __name__ == '__main__':

    # Fix random numbers for debugging
    #np.random.seed(100)

    test_with_identity_matrix()
    #test_with_random_matrix()
