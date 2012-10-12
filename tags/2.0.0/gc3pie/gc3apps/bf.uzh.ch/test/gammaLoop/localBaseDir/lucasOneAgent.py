#!/usr/bin/env python

# Standard call: 
# Inputs: parameters file, debug level, determistic flag
# ../../inFiles/parameters.in debug false

import os, re, sys
# Establish path to pymods
try: 
  import pymods
except ImportError: # pythonpath not set. Attempt relative import. 
  curPath = os.getcwd()
  curDir = os.path.split(curPath)[1]
  if curDir == 'lucasOneAgentEZ':
    path2Pymods = os.path.join(curPath, '../../')
    path2Src    = os.path.join(curPath, '../../src')
  elif curDir == 'bin':
    path2Pymods = os.path.join(curPath, '../code')
    path2Src    = os.path.join(curPath, '../code/src')
  else:
    print 'aborting... dont recognize current path and path to pymods is not in pythonpath'
    sys.exit()
  if not sys.path.count(path2Pymods):
    sys.path.append(path2Pymods)
    sys.path.append(path2Src)

from pymods.markovChain.mcInterface import MkovM
from pymods.support.support import wrapLogger
from pymods.support.support import getParameter
from pymods.support.support import myArrayPrint

import numpy as np
import pandas
import scipy.optimize
from genMarkov import genMarkov

from numpy.core.umath_tests import inner1d

pprintFun = myArrayPrint(width = 12, prec = 7)
np.set_string_function(pprintFun, repr = False)

logger = wrapLogger(loggerName = 'lucasMainLog', streamVerb = 'DEBUG', logFile = None)

def lucasOneAgent(shockMatrix, transMatrix, beta, g, Etg, PD, PB, markovFilePath, deterministic = False):
  '''
    markovFilePath: path to parameters.in file
    determistic:    boolean indicating whether to compute special determistic or stochastic case. 
    
    One agent economy. Therefore we have: 
      C_t = Y_t
      C_{t+1} = Y_{t+1}
    In the normalied world: 
      c_t = 1
      c_{t+1} = 1
  '''
  
  logFile     = os.path.join(os.getcwd(), 'output/logs/lucasOneAgent.log')
  lucasLogger = wrapLogger(loggerName = 'lucasOneAgentLog', streamVerb = verb, logFile = logFile)

  gamma       = float(getParameter(markovFilePath, 'gamma', 'bar-separated'))
  psi         = float(getParameter(markovFilePath, 'psi', 'bar-separated'))
  cBar        = float(getParameter(markovFilePath, 'cBar', 'bar-separated'))
  mkov        = MkovM(shockMatrix, transMatrix)
  
  lucasLogger.debug('\n')
  lucasLogger.info('log file written to %s\n' % logFile)
  #np.set_printoptions(precision = 7, linewidth = 300)
  
  if deterministic:
    shockMatrix = np.array([[g]])
    transMatrix = np.array([[1]])
   
  states   = len(shockMatrix)
  c        = np.ones(states)
  rho      = 1. / psi
  gammaSeq = [ gamma ] * len(PD)

  if psi < 0: # CRRA case
    lucasLogger.debug('Using CRRA Euler Equations')
    # infinitely lived stock (lucas tree)
    qS_init = np.ones(states)
    def compute_qS(qS):
      euler_qS = eulerCRRA(g, beta, gamma, transMatrix, PD + qS) - qS
      return euler_qS  
    qS = scipy.optimize.newton_krylov(compute_qS, qS_init)
      
    # Price remaining assets
    for state in range(len(shockMatrix)):
      qD = eulerCRRA(g, beta, gamma, transMatrix, payoff = PD)
      qB = eulerCRRA(g, beta, gamma, transMatrix, payoff = PB)
  elif psi >= 0: # Epstein Zin case
    lucasLogger.debug('Using Epstein Zin Euler Equations')
    # guess initial value function
    V_init = g
    lucasLogger.debug('Initial V:      %s ' % V_init)
    V = V_init
    lucasLogger.info('starting backward recursion: ')
    dif = 1.
    counter = 0
    while dif > 1.e-10:
      newV = EpsteinZin(c - cBar, g, V, beta, gamma, psi, transMatrix)
      dif = np.sum(np.abs(newV - V))
      V = newV.copy()
      lucasLogger.debug('iteration: %d dif: %g V: %s' % ( counter, dif, V))
      counter += 1
    lucasLogger.info('converged after %d iterations: ' % counter)
    lucasLogger.info('converged value function: %s' % V)

    # infinitely lived stock 
    qS_init = np.ones(states)
    def compute_qS(qS):
      euler_qS = eulerEpsteinZin(g, V, beta, gamma, psi, transMatrix, payoff = PD + qS) - qS
      return euler_qS  
    qS = scipy.optimize.newton_krylov(compute_qS, qS_init)
    
    # Price remaining assets
    for state in range(len(shockMatrix)):
      qD = eulerEpsteinZin(g, V, beta, gamma, psi, transMatrix, payoff = PD)
      qB = eulerEpsteinZin(g, V, beta, gamma, psi, transMatrix, payoff = PB)
    
  # Generate output:   
  
  # stock
  lucasLogger.info('')
  lucasLogger.info('qS:                    %s' % qS)
  EtPS = inner1d(transMatrix, ( PD + qS ) * g)
  lucasLogger.info('EtPS:                  %s ' % EtPS)
  EtRS = EtPS / qS
  lucasLogger.info('EtRS:                  %s' % EtRS)
  if not deterministic:
    ERS = np.dot(mkov.getlmbda(), EtRS)
    lucasLogger.info('ERS:                   %s' % ERS)
  
  # bond
  lucasLogger.info('')
  lucasLogger.info('qB:                    %s' % qB)
#  EtPB = np.dot(transMatrix, PB * g)
  EtPB = inner1d(transMatrix, PB * g)
  lucasLogger.info('EtPB:                  %s' % EtPB)
  EtRB = EtPB / qB
  lucasLogger.info('EtRB:                  %s' % EtRB)
  if not deterministic:
    ERB = np.dot(mkov.getlmbda(), EtRB)
    lucasLogger.info('ERB:                   %s' % ERB)
    
  
  # dividend asset
  lucasLogger.info('')
  lucasLogger.info('qD:                    %s' % qD)
  EtPD = np.dot(transMatrix, PD * g)
  lucasLogger.info('EtPD:                  %s ' % EtPD)
  EtRD = EtPS / qD
  lucasLogger.info('RtD:                   %s' % EtRD)
  if not deterministic:
    ERD = np.dot(mkov.getlmbda(), EtRD)
    lucasLogger.info('ERD:                   %s' % ERD)
  
  # risk premium
  c_rp = EtRS - EtRB
  lucasLogger.info('\ncond risk prem:        %s' % c_rp)
  if not deterministic:
    rp = np.dot(mkov.getlmbda(), c_rp)
    lucasLogger.info('unc risk premium:      %s' % rp)
    lucasLogger.debug('\ndone with riskpremium computation')
  
  # Write prices to file
  savePath = os.path.join(os.getcwd(), 'output')
  lucasLogger.debug('writing qS/qB to path %slucasOneagent_qS|lucasOneagent_qS' % savePath)
  np.savetxt(os.path.join(savePath, 'lucasOneAgent_qS.in'), qS, fmt = '%15.10f')
  np.savetxt(os.path.join(savePath, 'lucasOneAgent_qB.in'), qB, fmt = '%15.10f')
  if psi > 0:
    np.savetxt(os.path.join(savePath, 'lucasOneAgent_V.in'), V, fmt = '%15.10f')
  

def CRRA(c, gamma = 2):
  return 1. / ( 1. - gamma) * c** (1. - gamma)

def dCRRA(c, gamma = 2):
  return c**(-gamma)

def eulerCRRA(g, beta, gamma, transMatrix, payoff):
  if payoff.ndim > 1:
    euler = beta * inner1d(transMatrix, g**(1.-gamma) * ( payoff ) )
  else:
    euler = beta * np.dot(transMatrix, g**(1.-gamma) * ( payoff ) )
  return euler

def EpsteinZin(c, g, V, beta, gamma, psi, transMatrix):
  rho = 1. / psi
  newV = ( c**(1 - rho) + beta * np.dot(transMatrix, (g * V)**(1. - gamma))**((1. - rho) / (1. - gamma)) )**( 1. / (1. - rho) )
  return newV

def eulerEpsteinZin(g, V, beta, gamma, psi, transMatrix, payoff):
  rho = 1. / psi
  if payoff.ndim > 1:
    euler = ( beta * ( np.dot(transMatrix, (g * V)**(1.-gamma)) )**((gamma - rho)/(1.-gamma)) * inner1d(transMatrix, g**(1.-gamma) * V**(rho-gamma) * payoff) )
  else:
    euler = ( beta * ( np.dot(transMatrix, (g * V)**(1.-gamma)) )**((gamma - rho)/(1.-gamma)) * np.dot(transMatrix, g**(1.-gamma) * V**(rho-gamma) * payoff) )
  return euler

if __name__ == '__main__':  
  logger.info('start lucas one agent')
  
  if len(sys.argv) >= 3:
    verb = sys.argv[2]
  else: 
    verb = 'DEBUG'
  if len(sys.argv) >= 4:
    deterministic = sys.argv[3] == 'deterministic'
  else:
    deterministic = False
    
  import warnings
  
  logger.info('computing lucasOneAgent')
  with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    shockMatrix, transMatrix, beta, g, Etg, PD, PB, incGrIndAg0 = genMarkov(sys.argv[1])
    lucasOneAgent(shockMatrix, transMatrix, beta, g, Etg, PD, PB, sys.argv[1], deterministic = deterministic)
  logger.info('\ndone with lucasOneAgent.py')
