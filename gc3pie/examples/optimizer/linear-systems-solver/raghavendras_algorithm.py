#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
"""
Implementation of Raghavendra's linear systems solver.

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
__author__ = 'Riccardo Murri <riccardo.murri@gmail.com>'
__docformat__ = 'reStructuredText'


import math
import os
import sys
import random  # used for non-repetitive random number


## simple-minded implementation of modular arithmetic in Python

def egcd(a, b):
    """
    Extended GCD.

    Given integers `a` and `b`, return a triple `(g, u, v)` such that
    `g` is the GCD of `a` and `b`, and the Bézout identity
    `u*a + v*b = g` holds.

    See http://code.activestate.com/recipes/474129-extended-great-common-divisor-function/#c4
    """
    u, u1 = 1, 0
    v, v1 = 0, 1
    g, g1 = a, b
    while g1:
        q = g // g1
        u, u1 = u1, u - q * u1
        v, v1 = v1, v - q * v1
        g, g1 = g1, g - q * g1
    return u, v, g


class Modular(object):
    """A simple implementation of modular arithmetic in Python.

    Initialize a `Modular` instance with integer value and modulus::

      >>> x = Modular(3, 13)
      >>> y = Modular(9, 13)

    The modulus is available as attribute `.modulus` on each `Modular`
    instance::

      >>> x.modulus
      13

    `Modular` instances support the four arithmetic operations and the
    equality comparison::

      >>> x + y == Modular(-1, 13)
      True
      >>> x - y
      Modular(7, 13)

    """

    __slots__ = ('value', 'modulus')

    def __init__(self, value, modulus):
        self.modulus = modulus
        self.value = (value % modulus)

    def __int__(self):
        return self.value

    def __long__(self):
        return self.value

    def __str__(self):
        return ("%d(mod %d)" % (self.value, self.modulus))

    def __repr__(self):
        return ("Modular(%d, %d)" % (self.value, self.modulus))

    def inverse(self):
        g, u, v = egcd(self.value, self.modulus)
        assert g == 1
        # now `u*value + v*modulus = 1`, hence `u` is the inverse
        return Modular(u, self.modulus)

    # comparison operators; only == and != make sense for integers mod p

    def __eq__(self, other):
        assert self.modulus == other.modulus
        return self.value == other.value

    def __ne__(self, other):
        assert self.modulus == other.modulus
        return self.value == other.value

    # arithmetic operations

    def __add__(self, other):
        assert self.modulus == other.modulus
        return Modular(self.value + other.value, self.modulus)

    def __sub__(self, other):
        assert self.modulus == other.modulus
        return Modular(self.value - other.value, self.modulus)

    def __mul__(self, other):
        assert self.modulus == other.modulus
        return Modular(self.value * other.value, self.modulus)

    def __div__(self, other):
        assert self.modulus == other.modulus
        return Modular(self.value * other.inverse().value, self.modulus)

    # arithmetic operations, in-place modifiers

    def __iadd__(self, other):
        assert self.modulus == other.modulus
        self.value += other.value
        self.value %= self.modulus

    def __isub__(self, other):
        assert self.modulus == other.modulus
        self.value -= other.value
        self.value %= self.modulus

    def __imul__(self, other):
        assert self.modulus == other.modulus
        self.value *= other.value
        self.value %= self.modulus

    def __idiv__(self, other):
        assert self.modulus == other.modulus
        self.value *= other.inverse().value
        self.value %= self.modulus

    # arithmetic operations, reversed order variant
    #
    # Note: these are called when __op__(x,y) has failed, so we know
    # that `other` is *not* a Modular instance

    def __radd__(self, other):
        return Modular(other + self.value, self.modulus)

    def __rsub__(self, other):
        return Modular(other - self.value, self.modulus)

    def __rmul__(self, other):
        return Modular(other * self.value, self.modulus)

    def __rdiv__(self, other):
        other_ = Modular(other, self.modulus)
        return (other_ / self)


## linear algebra utilities

def vector_sum(x, y):
    """Return the sum of two vectors `x` and `y`."""
    assert len(x) == len(y)
    return [ x[i]+y[i] for i in range(len(x)) ]

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

# be careful! already with q=3 we have N=7169 ...
q = 13

numtype = (lambda x: Modular(x, q))


def make_random_vector(dim, distribution=(lambda: random.randint(0, q-1))):
    """
    Return a random vector of size `dim`.

    Entries are sampled from the given distribution function.
    """
    return [ numtype(distribution()) for _ in range(dim) ]


def solve(A, b, N, sample_fn=make_random_vector):
    '''
    Return a list of (approximate) solutions to the linear system `Ax = b`.

    :param A: a NumPy `m` times `n` 2D-array, representing a matrix with `m` rows and `n` columns
    :param b: a Numpy 1D-array real-valued vector of `n` elements
    :param sample_fn: a function returning a random 1D-vector; the default implementation chooses all coordinates uniformly from the interval [0,1].
    '''
    # Input:
    # * A is a matrix with m row vectors
    m = len(A)
    # * each row vector has n columns
    n = len(A[0])
    # b is a column vector with n rows
    assert m == len(b)
    # DEBUG
    print "Given data:"
    print "  m = ", m
    print "  n = ", n
    print "  A = ", '[' + str.join("\n        ", (prettyprint_vector(a) for a in A)) + ']'
    print "  b = ", prettyprint_vector(b)
    print "  N = ", N

    # Step 1. Sample N random vectors S_0 = {z_1, ... , z_N} where each z_i is i.i.d uniform.
    z = [ sample_fn(n) for _ in range(N) ]
    #print "Initial choices of z's:"
    #for l, z_l in enumerate(z):
    #    print "  z_%d = %s" % (l, prettyprint_vector(z_l))

    # Step 2.
    for i in range(m): # for each constraint `A_i x = b_i` do:
        print "Step 2, imposing %d-th constraint ..." % i
        # Step 2(a): selection
        t = [ z_l for z_l in z if (dot_product(A[i], z_l) == b[i]) ]
        # Step 2(b): recombination
        if len(t) == 0: # if T=\emptyset, then SYSTEM INFEASIBLE
            print ("*** SYSTEM INFEASIBLE: No vector satisfies constraint A_%d" % i)
            raise RuntimeError("SYSTEM INFEASIBLE: No vector satisfies constraint A_%d" % i)
        else:
            z = recombine(t, N)

    # final result
    return z


def recombine(t, N):
    '''
    Recombination function.
    '''
    s = [ ]
    for i in range(N):
        y = random.sample(t, q+1)
        # compute the sum of y's
        sum_y = [ 0 for _ in range(len(y[0])) ]
        for y_i in y:
            sum_y = vector_sum(sum_y, y_i)
        s.append(sum_y)
    return s


def _check_solution(A, b, z):
    for z_l in z:
        assert z_l == z[0]
    print "OK: All final vectors are equal!"
    for i in range(len(b)):
        assert b[i] == dot_product(A[i], z[0])
    print "OK: final vector(s) are a solution of the system Ax=b!"


def test_with_random_matrix(dim=5, N=None):
    A = [ make_random_vector(dim) for _ in range(dim) ]
    b = make_random_vector(dim)

    n = len(b)
    if N is None:
        N = 1 + int(145*n*q*q*math.log(q))

    x = solve(A, b, N)

    _check_solution(A, b, x)


def test_with_identity_matrix(dim, N=None):
    A = identity_matrix(dim)
    b = make_random_vector(dim)

    n = len(b)
    if N is None:
        N = 1 + int(145*n*q*q*math.log(q))

    x = solve(A, b, N)

    _check_solution(A, b, x)


if __name__ == '__main__':

    # Fix random numbers for debugging
    #np.random.seed(100)

    test_with_identity_matrix(5)
    test_with_random_matrix(5)
