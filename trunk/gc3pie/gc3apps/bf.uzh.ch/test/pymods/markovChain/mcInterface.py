#!/usr/bin/env python

import numpy as np
import sys, shutil, os
from johannes1987 import tauchen as Tauchen1987Joh
from knotekTerry import fn_var_to_markov
from MkovM import MkovM
from momentMatching import optShock, optTrans
import platform
if platform.architecture()[0] == '32bit':
  import markov
elif platform.architecture()[0] == '64bit':
  import markov64 as markov
import logbook
path2Pymods = os.path.join(os.path.dirname(__file__), '../../')
if not sys.path.count(path2Pymods):
  sys.path.append(path2Pymods)
from pymods.support.wrapLogbook import wrapLogger
                                                                                                                                       
def mcInterface(method, uncMean, persistence, varCov, nval, verb = 'DEBUG'):
  '''
    Inputs: 
      1) method
      2) uncMean as column vector
      3) persistence as matrix
      4) varCov as matrix
      5) nval as column vector
  '''
  # Get a logger instance
  logger = wrapLogger(loggerName = 'mcInterface', streamVerb = verb, logFile = '')
  
  # Make sure that input matrices are not arrays to allow for * notation
  # Also enforce column vectors
  nVars       = len(nval)
  uncMean     = np.mat(uncMean).reshape(len(uncMean), 1)
  persistence = np.mat(persistence)
  varCov      = np.mat(varCov)
  nval        = np.mat(nval).reshape(len(nval), 1)
  
  if nval[0, 0] == 1:
    ShockMatrix = uncMean.T
    TransMatrix = 1
    return

  
  # Map unconditional mean into an intercept of a VAR process
  I = np.mat(np.eye(len(nval)))

  intercept = ( I - persistence ) * uncMean
  # Derivation: varEps
  # see wave Variance Covariance Vector Operations for rules
  # Y_t = alpha + theta Y_{t-1} + eps_{t+1}
  # V(Y_t) = V(theta Y_{t-1} + V(eps_{t+1})
  # V(Y) = theta V(Y) theta' + V(eps)
  # V(eps) = (I-theta I theta') V(Y)
  varEps    = ( I - persistence * I * persistence.T ) * varCov
  
  logger.debug('uncMean %s' % uncMean)
  logger.debug('persistence %s' % persistence)
  logger.debug('varCov %s' % varCov)
  
  if method.lower() == 'Tauchen1987Joh'.lower():
    Ns = nval[0, 0]
    bandwidth = 1
    retDict = Tauchen1987Joh(persistence, intercept, varCov, Ns, bandwidth, verb)
    ShockMatrix = retDict['y']
    TransMatrix = retDict['P']
    
  elif method.lower() ==  'KnotekTerry'.lower():
    '''
      needs 3 shock states to match standard deviation and/or persistence. 
    '''
    A0 = np.eye(nVars)
    A1 = intercept
    A2 = persistence
    sigma = varEps
    N = nval
    random_draws = 10000
    method = 1
    
    [P,states,zbar] = fn_var_to_markov(A0,A1,A2,sigma,N,random_draws,method)
    
    ShockMatrix = states.T
    TransMatrix = P
    
  elif method.lower() == 'Tauchen1987fortran'.lower():
    '''
      doesn't match persistence
    '''
    pathToFile = os.path.dirname( __file__ )
    if not os.path.isfile(os.path.join(os.getcwd(), 'GHQUAD.DAT')):
      shutil.copy(os.path.join(pathToFile, 'GHQUAD.DAT'), os.getcwd())
    markovInputFile = open('PARMS', 'w')
    print >> markovInputFile, len(nval)
    print >> markovInputFile, 1
    for ele in nval.flat:
      print >> markovInputFile, ele
    for ele in uncMean.flat:
      print >> markovInputFile, ele
    for ele in persistence.flat:
      print >> markovInputFile, ele
    for ele in varCov.flat:
      print >> markovInputFile, ele

    markovInputFile.close()
    states = nval.prod()
    vars = len(nval)
    TransMatrix, ShockMatrix = markov.markov(states, vars)
    os.remove('GHQUAD.DAT')
    os.remove('PARMS')
    os.remove('dog')
    
  elif method.lower() == 'momentMatching'.lower():
    dims = {}
    # all variables have the same # of states. 
    dims['states'] = np.prod(nval)
    dims['vars']   = len(nval)
    shockTarget = {}
    shockTarget['E'] = np.ravel(uncMean)
    shockTarget['Std'] = np.asarray(np.sqrt(np.diag(varCov)))
    shockTarget['Cor'] = np.asarray([varCov[0, 1] / np.sqrt(varCov[0,0]) * np.sqrt(varCov[1,1])])
    S = optShock(dims, shockTarget)
    # sort S
    ind = np.lexsort((S.T[1], S.T[0]))
    ShockMatrix = S[ind].copy()
        
    transTarget = {}
    transTarget['Theta'] = persistence
    

    TransMatrix = optTrans(dims, transTarget, S)
    
  else:
    print('method not implemented. exiting... ')
    os._exit(1)

  logger.debug('ShockMatrix %s' % ShockMatrix)
  logger.debug('TransMatrix %s' % TransMatrix)
    

  return ShockMatrix, TransMatrix


if __name__ == '__main__':  
  E = np.matrix([[0.], [0.]])

  sigmaY = 0.16
  sigmaZ = 0.09
#  corrYZ = 0.4
  corrYZ = 0.

  Theta = np.mat(np.zeros((2,2)))
  Theta[0, 0] =   0.1
  Theta[0, 1] =   0.
  Theta[1, 0] =   0.
  Theta[1, 1] =   0.05
  
  covYZ  = corrYZ * sigmaY * sigmaZ
  V = np.mat(np.zeros((2,2)))
  V[0, 0] = sigmaY**2
  V[0, 1] = covYZ
  V[1, 0] = covYZ
  V[1, 1] = sigmaZ**2
  
  nval = np.matrix([[3], [3]])
  
#  ShockMatrix, TransMatrix = mcInterface('Tauchen1987fortran', E, Theta, V, nval)
  ShockMatrix, TransMatrix = mcInterface('Tauchen1987Joh', E, Theta, V, nval)
  
#  ShockMatrix, TransMatrix = mcInterface('momentMatching', E, Theta, V, nval)
  KnotekTerry = MkovM(ShockMatrix, TransMatrix)
  print KnotekTerry
  KnotekTerry.simulation()




