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
__author__ = 'Benjamin Jonen <benjamin.jonen@bf.uzh.ch>, Riccardo Murri <riccardo.murri@gmail.com>'
__docformat__ = 'reStructuredText'


# set `numtype` to the a function that takes a (small) integer number
# and returns it converted to a chosen exact/arbitrary-precision
# numeric type.  The constructors of the Python standard
# `fractions.Fraction` and `fractions.Decimal` types are both OK.
import fractions
numtype = fractions.Fraction

import math
import os
import sys
import random  # used for non-repetitive random number

import numpy as np
import numpy.linalg

# Set numpy print options
np.set_printoptions(linewidth=300, precision=5, suppress=True)

## linear algebra utilities

def dot_product(x, y):
    """Return the dot product of two vectors `x` and `y`."""
    assert len(x) == len(y)
    return sum((x[i]*y[i]) for i in range(len(x)))

def matrix_vector_product(M, x):
    """
    Return the product of matrix `M` and vector `x`.
    """
    assert len(M) == 0 or len(M[0]) == len(x)
    return [ dot_product(m, x) for m in M ]

def norm(x):
    """
    Return the Euclidean norm of vector `x`.
    """
    return math.sqrt(sum(x[i]*x[i] for i in range(len(x))))

def identity_matrix(N):
    """
    Return the N by N identity matrix.
    """
    return [
        [ numtype(1 if (i==j) else 0) for j in range(N) ]
        for i in range(N)
    ]

def make_random_vector(dim, distribution=(lambda: numtype(random.random()))):
    """
    Return a random vector of size `dim`.

    Entries are sampled from the given distribution function.
    """
    return [ numtype(distribution()) for _ in range(dim) ]


## main algorithm

def main_algo(A, b, sample_fn=make_random_vector):
    '''
    Return a list of (approximate) solutions to the linear system `Ax = b`.

    :param A: a NumPy `m` times `n` 2D-array, representing a matrix with `m` rows and `n` columns
    :param b: a Numpy 1D-array real-valued vector of `n` elements
    :param sample_fn: a function returning a random 1D-vector; the default implementation chooses all coordinates uniformly from the interval [0,1].
    '''
    # Input:
    # * A is a with m row vectors
    m = len(A)
    # * each row vector has n columns
    n = len(A[0])
    # b is a column vector with n rows
    assert m == len(b)
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
        x = [ rec(v[i], v[j], A[k], b[k])
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
    assert len(u) == len(v)
    assert len(v) == len(a)
    t0 = beta - dot_product(a, v)
    t1 = dot_product(a, [(u[i] - v[i]) for i in range(len(u))])
    t = t0 / t1
    return  [ (t*u[i] + (1-t)*v[i]) for i in range(len(u)) ]


def _check_distance(A, b, vs):
    print "Final values of v's:"
    for l, v_l in enumerate(vs):
      print "  v_%d = %s" % (l, v_l)
    print "Distances of solutions computed by Fliege's algorithm:"
    for l, v_l in enumerate(vs):
        dist = norm([
            matrix_vector_product(A,v_l)[i] - b[i]
            for i in range(len(b))
        ])
        print ("  |Av_%d - b| = %g" % (l, dist))

    print "Numpy's `linalg.solve` solution:"
    A_ = np.array([ [ float(A[i][j])
                      for j in range(len(A[0])) ]
                    for i in range(len(A)) ],
                  ndmin=2)
    b_ = np.array([ float(b[k]) for k in range(len(b)) ])
    v_prime = np.linalg.solve(A_, b_)
    print "  v' = %s" % v_prime
    print "Distance of Numpy's `linalg.solve` solution:"
    dist_prime = np.linalg.norm(np.dot(A_,v_prime) - b_)
    print ("  |Av' - b| = %g" % dist_prime)


def test_with_random_matrix(dim=5):
    A = np.random.randint(low=1, high=9,size=(dim, dim))
    b = np.random.randint(low=1, high=9,size=(dim,))

    rank = np.linalg.matrix_rank(A)
    assert rank == dim, 'Matrix needs to have full rank. '

    x = main_algo(A, b)

    _check_distance(A, b, x)


def test_with_identity_matrix(dim=5):
    A = identity_matrix(dim)
    b = [ numtype(random.randint(1,9)) for _ in range(dim) ]

    x = main_algo(A, b)

    _check_distance(A, b, x)


if __name__ == '__main__':

    # Fix random numbers for debugging
    #np.random.seed(100)

    test_with_identity_matrix()
    #test_with_random_matrix()
