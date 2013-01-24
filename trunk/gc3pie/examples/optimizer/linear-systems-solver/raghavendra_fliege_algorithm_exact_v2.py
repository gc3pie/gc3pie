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

def scalar_vectror_product(alpha, x):
    return [ alpha*x_l for x_l in x ]

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

def prettyprint_vector(x):
    return '[ ' + str.join(" ", (str(x_l) for x_l in x)) + ' ]'


## auxiliary functions

def randomized(seq):
    """
    Return the elements of `seq` one by one in a random order.
    """
    already = set()
    l = len(seq)
    while True:
        i = random.randint(0, l-1)
        if i in already:
            continue
        yield seq[i]
        already.add(i)
        if len(already) == l:
            break


## main algorithm

def main_algo(A, b, sample_fn=make_random_vector, N=None):
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
    # by default, start with a population of `n+1` vectors
    if N is None:
        N = n+1
    # DEBUG
    print "Given data:"
    print "  m = ", m
    print "  n = ", n
    print "  A = ", '[' + str.join("\n        ", (prettyprint_vector(a) for a in A)) + ']'
    print "  b = ", prettyprint_vector(b)

    # Step 2. Generate random sample of normals (fulfilling Assumption 1.)
    v = [ sample_fn(n) for _ in range(N) ] # N random vectors of n elements each
    print "Initial choices of v's:"
    for l, v_l in enumerate(v):
        print "  v_%d = %s" % (l, prettyprint_vector(v_l))

    # save all (i,j) pairs for later -- this is invariant in the Step 3 loop
    all_ij_pairs = [ (i,j) for i in range(N) for j in range(N) if i<j ]
    #print 'potential ij_pairs = ', all_ij_pairs
    assert len(all_ij_pairs) == (N*(N-1) / 2)

    # Step 3.
    for k in range(m): # loop over eqs
        print "Step 3, iteration %d starting ..." % k
        # Step 3(a): randomize order of (i,j) pairs
        ij_pairs = randomized(all_ij_pairs)
        # Step 3(b): recombine the v's; use x's as temporary storage
        x = [ None ] * len(v)
        for l in range(len(v)):
            try:
                while True:
                    i, j = ij_pairs.next()
                    # retry until we get a pair that is not linearly dependent
                    try:
                        x[l] = rec(v[i], v[j], A[k], b[k], k)
                        break
                    except ZeroDivisionError:
                        # retry with another i,j pair
                        print (">>> REPAIR; drawing another (i,j) pair from the ballot")
                        continue
            except StopIteration:
                print (">> WARNING: not enough diversity in vector population:"
                       " dropping vectors v[%d] to v[%d]"
                       % (l, N))
                # no more i,j pairs -- drop x[l], ..., x[N]
                N = l
                del v[N:]
                del x[N:]
                break
        # Step 3(d): rename x's -> v's
        for l in range(N):
            v[l] = x[l]
        # check progress
        #_check_distance(A, b, v, k)

    # final result
    return v


def perturb(x, k):
    """Randomly perturb vector `x`, but keep the first `k` coordinates fixed."""
    return [ x_l for x_l in x[:k] ] + [ (x_l + numtype(random.random())) for x_l in x[k:] ]


def rec(u, v, a, beta, k):
    '''
    Recombination function.  This is a modified version of the one
    defined in Fliege's arXiv:1209.3995v1.

    If the two vectors `u` and `v` are linearly dependent, or their
    difference `u-v` lies in the kernel of `a`, then perturb them
    randomly but keep the first `k` coordinates fixed.

    '''
    assert len(u) == len(v)
    assert len(v) == len(a)
    while u == v:
        print ("<<<< WARNING in rec(): u==v, perturbing v.")
        v = perturb(v, k)
        print ("Now v = %s" % prettyprint_vector(v))
    attempts = 0
    while True:
        t0 = beta - dot_product(a, v)
        t1 = dot_product(a, [(u[i] - v[i]) for i in range(len(u))])
        if t1 != 0:
            break
        # else...
        print ("<<<< STOP in rec()")
        print ("  u = %s" % prettyprint_vector(u))
        print ("  v = %s" % prettyprint_vector(v))
        print ("  a = %s" % prettyprint_vector(a))
        if attempts > 3:
            raise ZeroDivisionError("<<<< Maximum number of retries exceeded!")
        else:
            print ("<<<< RETRY with a perturbed v")
            v = perturb(v, k)
            print ("Now v = %s" % prettyprint_vector(v))
            attempts += 1
            continue
    t = t0 / t1
    return  [ (t*u[i] + (1-t)*v[i]) for i in range(len(u)) ]


def _check_distance(A, b, vs, k=None):
    if k is None:
        k = len(b)
    if k == len(b):
        print "Final values of v's:"
    else:
        print "Current values of v's:"
    for l, v_l in enumerate(vs):
      print "  v_%d = %s" % (l, prettyprint_vector(v_l))
    print "Distances of solutions computed by Fliege's algorithm:"
    for l, v_l in enumerate(vs):
        dist = norm([
            matrix_vector_product(A,v_l)[i] - b[i]
            for i in range(k)
        ])
        print ("  |Av_%d - b| = %g" % (l, dist))

    if k == len(b):
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

    test_with_identity_matrix(10)
    #test_with_random_matrix()
