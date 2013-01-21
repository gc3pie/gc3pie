#! /usr/bin/env python
#
"""
Implementation of Fliege 2012
"""
# Copyright (C) 2011, 2012, 2013 University of Zurich. All rights reserved.
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

# Set numpy print options
np.set_printoptions(linewidth = 300, precision = 5, suppress = True)

# m equations, n unknowns

def main_algo(A, b):
    '''
    A: m*n
    b: m*1
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
    print 'm = ', m
    print 'n = ', n
    print 'A = \n', A
    print 'b = \n', b

    # Step 2. Generate random sample of normals (fulfilling Assumption 1.)
#    sample_fun = np.random.randn # standard normal
    sample_fun = np.random.random # uniform (0,1)
    v = [ sample_fun(n) for _ in range(n+1) ] # n+1 random vectors of n elements each
    print 'initial v ='
    for v_ele in v:
      print v_ele

    # save all (i,j) pairs for later -- this is invariant in the Step 3 loop
    all_ij_pairs = [ (i,j) for i in range(n+1) for j in range(n+1) if i<j ]
    print 'potential ij_pairs = ', all_ij_pairs
    assert len(all_ij_pairs) == ((n+1)*n / 2)

    # Step 3.
    for k in range(m): # loop over eqs
        print "Step 3, iteration %d starting ..." % k
        # Step 3(a): choose n+1 random pairs (i_l, j_l) with i_l < j_l
        # pick randomly from all_ij_pairs set to get n+1 random pairs
        ij_pairs = random.sample(all_ij_pairs, n+1)
        assert len(ij_pairs) == n+1
        print '  ij_pairs = ', ij_pairs
        # Step 3(b):
        x = [ rec(v[i], v[j], A[k,:], b[k])
              for l, (i, j) in enumerate(ij_pairs)
          ]
        # Step 3(d):
        for l in range(n+1):
            v[l] = x[l]

    # final result
    return v


def rec(u,v,a,beta):
    '''
    Recombination function:
    u,v,a Rn. beta real
    '''
    # print 'u = ', u
    # print 'v = ', v
    # print 'a = ', a
    # print 'beta = ', beta
    assert ( np.dot(a, (u-v)) ) != 0
    t = (beta - np.dot(a, v)) / np.dot(a, (u - v)) # t is scalar
    return  (t * u + (1. - t) * v)

def _check_distance(A, b, xs):
    print "Distances of solutions compute by Fliege's algorithm:"
    print "Final v:"
    for x in xs:
      print x
    for i, x in enumerate(xs):
        dist = np.linalg.norm(np.dot(A,x) - b)
        print ("  |Av_%s - b| = %g" % (i, dist))

    print "Distance of Numpy's `linalg.solve` solution:"
    x_prime = np.linalg.solve(A,b)
    print "x = \n%s" % x_prime
    dist_prime = np.linalg.norm(np.dot(A,x_prime) - b)
    print ("  |Ax' - b| = %g" % dist_prime)


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

    #test_with_identity_matrix()
    test_with_random_matrix()
