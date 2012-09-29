#!/usr/bin/env python

import numpy as np
import scipy as scp
import scipy.stats
import os, sys
path2Pymods = os.path.join(os.path.dirname(__file__), '../../')
if not sys.path.count(path2Pymods):
  sys.path.append(path2Pymods)
from pymods.support.support import wrapLogger

# Note: When a matrix and an array are multiplied, the operator * refers to np.dot not np.multiply. 
def tauchen(F = np.matrix([[0.1, 0], [0, 0.05]]), F0 = np.array([[0.9], [0.95]]), Sigma = np.matrix([[0.01, 0.0120], [0.0120, 0.09]]), 
            Ns = 3, bandwidth = 1, verb = 'DEBUG'):
  '''
   function [P, y, x, MVndx, Moments0, p0] = tauchen(F, F0, Sigma, Ns, bandwidth)
   convert VAR(1) for y into Markov-Chain using Tauchen's method
   cool: Y can have arbitrary dimension!
   Y_t = F0 + F Y_{t-1} + e_t and E e_t e_t' = Sigma
   P is transition matrix
   y and x are midpoints of the Markov grid. x being the orthogonalized processes (using a choleski of Sigma)
   Ns is number of states *per variable* ! (Nstar = Ns^Ny)
   bandwidth is multiple of standard deviations which will be covered per variable

   Note: this function imposes same bandwidth and number of gridpoints per element of Y (makes algorithm more straightforward)
  '''

  # Get logger
  logger = wrapLogger(loggerName = 'mcInterface', streamVerb = verb, logFile = '')
  
  # Make sure that input matrices are not arrays to allow for * notation
  F     = np.mat(F)
  Sigma = np.mat(Sigma)

  # construct the range over which each of the compents may vary
  # Y_t = F0 + F Y_{t-1} + Q \varepsilon_t
  Ny       = np.size(F, 0)
  Nstar    = Ns**Ny
  Q        = np.linalg.cholesky(Sigma)   # cholesky in Matlab is the transpose of numpy. -> Drop one transpose
  iQ       = np.linalg.inv(Q)
  Iy       = np.eye(Ny)
  EY       = np.linalg.inv(Iy - F) * F0
  VarY     = disclyap(F, Q * np.transpose(Q));

  # X_t = Q^{-1} Y_t = F0x + Fx X_{t-1} + \varepsilon_t 
  Fx       = iQ * F * Q
  F0x      = iQ * F0
  EX       = iQ * EY
  VarX     = iQ * VarY * iQ.T # = dlyap(F, Iy);
  StdX     = np.sqrt(VarX.diagonal()).T


  # construct univariate grids for x (always midpoints!)
  griddy  = np.tile(np.nan, (Ns, Ny))
  Ub      = EX + bandwidth * StdX
  Lb      = EX - bandwidth * StdX
  steps   = (Ub - Lb) / (Ns - 1)
  for x in range(Ny):
    griddy[:, x] = np.linspace(Lb[x,0], Ub[x,0], Ns)
    #np.arange(Lb[x, 0], Ub[x, 0], steps[x, 0])


  # Index for Multivariate Grid
  MVndx                = gridMVndx(Ns, Ny);
  #indexArr = np.array(list(getIndex([Ns, Ny], restr = None, direction = 'columnwise')))
  #indexArr = np.reshape(indexArr, (Ny, Ns, Ny))
  #midPattern = np.array(list(getIndex([Ns, Ns], restr = None, direction = 'columnwise')))

  # note: midpoints also used for conditional means! (see below)
  griddyRaveled = np.ravel(griddy, order = 'F')
  x = griddyRaveled[MVndx]
  y = x * Q.T

  endpoints = griddy[:-1,:] + np.tile(steps.T / 2, ( Ns - 1, 1) )
  
  
  # conditional distributions
  # note usage of griddy, not XuvgridMid!
  condmean = x * Fx.T + np.tile(F0x.T, (Nstar,1) )
  condstd  = np.ones((1, Ny))
  P = np.tile(np.nan, (Nstar, Nstar))
  for s in range(Nstar):
    E = np.tile(condmean[s,:], (Ns-1, 1))
    V = np.tile(condstd, (Ns-1, 1))
    cdfValues = scipy.stats.norm.cdf(endpoints, E, V)
    probby = np.diff(np.vstack((np.zeros((1, Ny)), cdfValues, np.ones((1,Ny)))), axis = 0)
    probbyRaveled = np.ravel(probby, order = 'F')
    P[s,:] = np.prod(probbyRaveled[MVndx], axis = 1)
    
    
  # construct unconditional distribution -- diagonalize VarX !!

  if Ny > 1:
    logger.debug('p0 not correctly implemented! need MVgrid')
   
  colly         = np.linalg.inv(np.linalg.cholesky(VarX).T)
  uncondmean    = (colly * EX).T
  uncondstd     = np.sqrt(np.diag(colly * VarX * colly.T)).T
  E             = np.tile(uncondmean, (Ns-1, 1))
  V             = np.tile(uncondstd, (Ns-1, 1))
  cdfValues     = scipy.stats.norm.cdf(endpoints * colly.T, E, V)
  ProbUV        = np.diff(np.vstack((np.zeros((1, Ny)), cdfValues, np.ones((1,Ny)))), axis = 0)
  probUVRaveled = np.ravel(ProbUV, order = 'F')
  p0            = np.prod(probUVRaveled[MVndx], axis = 1)
  
  Moments0 = {}
  Moments0['EY']   = EY
  Moments0['EX']   = EX
  Moments0['VarY'] = VarY
  Moments0['VarX'] = VarX
  
  returnDict             = {}
  returnDict['P']        = P
  returnDict['y']        = y
  returnDict['x']        = x
  returnDict['MVndx']    = MVndx
  returnDict['Moments0'] = Moments0
  returnDict['p0']       = p0

  
  logger.debug('done tauchen')
  return returnDict



def gridMVndx(Ns, Ny):
  # function MVndx = gridMVndx(Ns, Ny)
  # to be used for constructing Xgrid  = griddy(MVndx);

  Nstar = Ns**Ny
#  MVndx = np.tile(np.nan, (Nstar, Ny))
  MVndx = np.zeros((Nstar, Ny), dtype = int)
  UVndx = np.arange(Ns, dtype = int).T
  MVndx[:, 0] = np.tile(UVndx, Ns**(Ny-1))

  for j in range(1, Ny):
    n1          = Ns ** j
    n2          = Ns ** (Ny - ( j + 1 ))
    MVndx[:, j]  = np.tile(np.kron(UVndx, np.ones(n1, dtype = int)), n2) + j * Ns
  return MVndx



def disclyap(a1, b1, vecflag = False):
# disclyap -- extension of Hansen/Sargent's DOUBLEJ.M
#
#  function V = disclyap(a1,b1,vecflag)
#  Computes infinite sum V given by
#
#         V = SUM (a1^j)*b1*(a1^j)'
#
#  where a1 and b1 are each (n X n) matrices with eigenvalues whose moduli are
#  bounded by unity, and b1 is an (n X n) matrix.
#  The sum goes from j = 0 to j = infinity.  V is computed by using
#  the following "doubling algorithm".  We iterate to convergence on
#  V(j) on the following recursions for j = 1, 2, ..., starting from
#  V(0) = b1:
#
#       a1(j) = a1(j-1)*a1(j-1)
#       V(j) = V(j-1) + a1(j-1)*V(j-1)*a1(j-1)'
#
#  The limiting value is returned in V.
#
# -----------------------------------------------------------------------------
# EMT added following comments and the vecflag argument (default=false)
# -----------------------------------------------------------------------------
#
# 1) L&S p. 76, write
# X = doublej(A, B * B') solves X = A * X * A' + B * B' where a1 = A and B*B' = b1
#
# 2) Note on algorithm
# * let $V_k \equiv \sum_{j=0}^K A^j C'C A^j'$
# * then $V_{2k} = V_k + A^k V_k A^k'$
# * let $k=2^j$ and rewrite $V_k \equiv V_j$ and $A_j\equiv A^{2^j}$ so that $A_{j+1} = A_j^2$
# * then $V_{j+1} = V_j + A_j^2 V_j A_j^2'$
#
# the doubling uses fewer iterations (namely only $j$ instead of $2^j$ and is thus much faster
#
# 3) Solves "discrete Lyapunov" equation (but can also handle constants in the SS)
#
# 4) new parameter: if vecflag == true: apply vec formula of Hamilton instead of doubling
#    will assume invertibility, i.e. no constant in SS

  if vecflag:
    Np          = np.size(a1, 0)
    Inp2        = np.eye(Np**2)
    vecSIGMA    = np.dot(np.dot(np.inv((Inp2 - np.kron(a1, a1))), Inp2), b2[:])
    V           = np.reshape(vecSIGMA, (Np, Np))
  else:
    alpha0   =  a1
    gamma0   =  b1
    delta  =  5
    ijk   =  1
    while delta > np.finfo(float).eps:
      alpha1   =  alpha0 * alpha0;
      gamma1   =  gamma0 + alpha0 * gamma0 * alpha0.transpose()
      #    delta    =  max(max(abs(gamma1-gamma0)));
      delta    = np.max(np.abs(gamma1[:] - gamma0[:]))
      gamma0   = gamma1
      alpha0   = alpha1
      ijk      = ijk + 1
      if ijk > 50:
        print('Error: ijk = %d, delta = %f check aopt and c for proper configuration' % (ijk, delta))
    V = gamma1
  return V



if __name__ == '__main__':    
  returnDict = tauchen()
  T = returnDict['P']
  S = returnDict['y']
  print T
  print S