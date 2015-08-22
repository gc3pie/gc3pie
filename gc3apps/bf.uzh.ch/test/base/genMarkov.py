#!/usr/bin/env python

import os, re, sys
fileName = sys.argv[0]

# Establish path to pymods
try: 
  import pymods
except ImportError: # pythonpath not set. Attempt relative import. 
  curPath = os.getcwd()
  curDir = os.path.split(curPath)[1]
  if curDir == 'src':
    path2Pymods = os.path.join(curPath, '../')
    path2Src    = os.path.join(curPath, '.')
  elif curDir == 'bin':
    path2Pymods = os.path.join(curPath, '../code')
    path2Src    = os.path.join(curPath, '../code/src')
  else:
    print 'aborting... dont recognize current path and path to pymods is not in pythonpath'
    sys.exit()
  if not sys.path.count(path2Pymods):
    sys.path.append(path2Pymods)
    sys.path.append(path2Src)

from pymods.markovChain.mcInterface import mcInterface
from pymods.markovChain.mcInterface import MkovM
from pymods.support.support import wrapLogger
from pymods.support.support import getParameter
import numpy as np
import pandas
import scikits.statsmodels.tsa.api
import scikits.statsmodels.tsa
import scipy.linalg
import scipy.optimize
from pymods.support.support import myArrayPrint

pprintFun = myArrayPrint(width = 12, prec = 7)
#np.set_string_function(pprintFun, repr = False)

logger = wrapLogger(loggerName = 'genMarkovLog', streamVerb = 'DEBUG', logFile = os.path.join(os.getcwd(), 'output/logs/genMarkov.log'))

def genMarkov(markovFilePath, verb = 'INFO', nSimulation = int(5.e+4)):

  logger.setStreamVerb(verb = verb)
  logger.info('')
  #os.system('cat ' + markovFilePath)
  
  logger.debug('markovFilePath is %s' % markovFilePath)
  
  # Read paramter file
  beta = float(getParameter(markovFilePath, 'beta', 'bar-separated'))
  muG = float(getParameter(markovFilePath, 'muG', 'bar-separated'))
  sigmaG = float(getParameter(markovFilePath, 'sigmaG', 'bar-separated'))
  p = float(getParameter(markovFilePath, 'p', 'bar-separated'))
  dy = float(getParameter(markovFilePath, 'dy', 'bar-separated'))
  nAgent = int(getParameter(markovFilePath, 'nAgent', 'bar-separated'))
  theta = float(getParameter(markovFilePath, 'theta', 'bar-separated'))
  periodsPerYear = int(getParameter(markovFilePath, 'periodsPerYear', 'bar-separated'))
  
  # Output read parameters
  logger.debug('')
  logger.debug('read the following yearly parameters: ')
  logger.debug('beta           = %s' % beta)
  logger.debug('muG           = %s' % muG)
  logger.debug('sigmaG        = %s' % sigmaG)
  logger.debug('p              = %s' % p)
  logger.debug('dy             = %s' % dy)
  logger.debug('nAgent         = %s' % nAgent)
  logger.debug('theta          = %s' % theta)
  logger.debug('periodsPerYear = %s' % periodsPerYear)

  # perform consitency check on paramters: 
  assert periodsPerYear >= 1
  assert beta >= 0.8 and beta <= 1.0
  assert sigmaG >= 0.
  assert muG <= 1.0
  
  # computing scaled parameters
  muG_sc = (1. + muG) ** (1./ periodsPerYear)
  sigmaG_sc = sigmaG / np.sqrt(periodsPerYear)
  beta_sc  = beta ** (1./ periodsPerYear)
  
  # scaled parameters: 
  logger.info('')
  logger.info('computed the following scaled 1/%s parameters: ' % periodsPerYear)
  logger.info('beta_sc        = %s' % beta_sc)
  logger.info('muG_sc        = %s' % (muG_sc - 1.))
  logger.info('sigmaG_sc     = %s' % sigmaG_sc)
  logger.info('')
  
  # Build trans matrix
  TransMatrix = np.array([[p, 1.-p], [1.-p, p]])
  
  ShockMatrix = np.array([[ muG_sc - sigmaG_sc], [muG_sc + sigmaG_sc]])
  mkov = MkovM(ShockMatrix, TransMatrix)
  logger.debug(ShockMatrix)
  logger.debug(TransMatrix)
  #mkov.simulation()
  logger.debug(mkov)
  
  if nAgent > 1:
    # Add unemployment shocks
    #TransMatrixUnem = np.triu(np.ones( (nAgent, nAgent)), 1 ) * ( ( 1. - p) / ( nAgent - 1.) ) + np.tril(np.ones( (nAgent, nAgent)), -1 ) * ( ( 1. - p) / ( nAgent - 1.) ) + np.eye(nAgent) * p
    ShockMatrixUnem = np.triu(np.ones( (nAgent, nAgent)), 1 ) *  dy / ( nAgent - 1 ) + np.tril(np.ones( (nAgent, nAgent)), -1 ) * dy / (nAgent - 1 ) + np.eye(nAgent) * ( -dy )
  
    # Print unemployment shock matrix
    logger.info('ShockMatrixUnem \n %s' % ShockMatrixUnem)
    
    # Combine aggregate uncertainty with idiosyncratic unemployment uncertainty
    fullShockMatrix = np.hstack( ( np.repeat(ShockMatrix, nAgent, axis = 0), np.tile(ShockMatrixUnem, (2, 1))) )
    #fullTransMatrix = np.kron(TransMatrix, TransMatrixUnem)
    
    # ugly ugly ugly
    fullShockMatrix = fullShockMatrix[:nAgent + 1]
    fullShockMatrix[nAgent][ShockMatrix.shape[1]:] = np.zeros( (1, nAgent) )
    
    # more ugly ugly ugly
    pers = TransMatrix[0,0]
    fullTransMatrix = np.vstack( (np.hstack( ( np.eye(nAgent) * pers, np.ones( (nAgent, 1) ) * (1.-pers) ) ), np.append(((1.-pers)/ nAgent) *np.ones(nAgent), pers) ) )
  else: 
    fullShockMatrix = ShockMatrix
    fullTransMatrix = TransMatrix
  
  np.set_printoptions(precision = 7, linewidth = 300)
    
  logger.info('fullShock\n %s' % fullShockMatrix )  
  logger.info('fullTrans\n %s' % fullTransMatrix ) 

###################  
  # Compute PD
  
  # nD_t+1 = nYbar_t+1 - n(1-theta)E_tYbar_t+1
  # divide through with Ybar_t+1
  # nd_t+1 = n  - n(1-theta) E_t[g_t+1]/ g_t+1
  g       = fullShockMatrix[:, 0]
  Etg     = np.dot(fullTransMatrix, g)
  PD      = 1. - ( 1. - theta ) * np.reshape(Etg, (len(Etg), 1)) / g
  PB      = np.reshape(Etg, (len(Etg), 1)) / g
##################


  try: 
    np.savetxt(os.path.join(os.getcwd(), 'output', 'shockMatrix.in'), fullShockMatrix, fmt = '%15.10f')
    np.savetxt(os.path.join(os.getcwd(), 'output', 'transMatrix.in'), fullTransMatrix, fmt = '%15.10f')
    np.savetxt(os.path.join(os.getcwd(), 'output', 'p_a.in'), PD, fmt = '%15.10f')
    np.savetxt(os.path.join(os.getcwd(), 'output', 'p_b.in'), PB, fmt = '%15.10f')
  except IOError:
    logger.critical('cannot write to output/.')
    
  logger.info('')
  #Estimate moments for full markov chain. 
  fullMChain = MkovM(fullShockMatrix, fullTransMatrix)
  varSim, Theta = fullMChain.simulation(nSimulation)
  # rename columns
  varSim = varSim.rename(columns = {'0': 'agIncGrowth'})
  for agent in range(1, nAgent):
    varSim = varSim.rename(columns = {str(agent): 'dy_agent' + str(agent)})
  qPers = Theta[1, 1]
  yearlyStockSeries = pandas.DataFrame(varSim[::4])
  model = scikits.statsmodels.tsa.api.VAR(yearlyStockSeries)
  results = model.fit(1)
  Theta = results.params[1:,:]
  aPers = Theta[1, 1]
  logger.info('quarterly pers %s, annual pers %s, diagnol pers %s' % (qPers, aPers, TransMatrix[0, 0]) )
  
   
  np.set_string_function(None)
  
  varSim['simInc'] = float(nAgent)
  varSim['incGrIndAg0'] = float(1.)
  varSim['incShareAg0'] = ( 1. + varSim['dy_agent1'] ) / nAgent

  for row in varSim.rows():
    if row > 0:
      varSim['simInc'][row] = varSim['simInc'][row-1] * varSim['agIncGrowth'][row]
      varSim['incGrIndAg0'][row] = ( varSim['incShareAg0'][row] / varSim['incShareAg0'][row - 1] ) * varSim['agIncGrowth'][row]

  varSim['simIndIncAg0'] = varSim['incShareAg0'] * varSim['simInc']
  
  x = varSim.ix[:,['0','incShareAg0', 'incGrIndAg0']]
  logger.info(x[:100])
##  logger.info('VAR for growth, incshareAg0, incGrIndAg0')
##  model = scikits.statsmodels.tsa.api.VAR(x)
##  result = model.fit(1)
##  logger.info(result.summary())
  
  logger.info('incShareAg0')
  incShareAg0 = x['incShareAg0']
  logger.info(incShareAg0.describe())
  logger.info('skewness %s' % scipy.stats.kurtosis(incShareAg0))
  logger.info('kurtosis %s' % scipy.stats.skew(incShareAg0))
  
  logger.info('\nincGrIndAg0')
  incGrIndAg0 = x['incGrIndAg0']
  logger.info(incGrIndAg0.describe())
  logger.info('skewness %s' % scipy.stats.skew(incGrIndAg0))
  logger.info('kurtosis %s' % (scipy.stats.kurtosis(incGrIndAg0)))
  #scikits.statsmodels.tsa.ar_model.AR(np.asarray(varSim['simIndIncAg0']))
  
  #varSim = varSim[:100]
  #N = len(varSim)
  #rng = pandas.DateRange('1/1/1900', periods = N, timeRule = 'Q@JAN')    
  #ts = pandas.Series(varSim['1'], index = rng)

  return fullShockMatrix, fullTransMatrix, beta_sc, g, Etg, PD, PB, incGrIndAg0
    
    
    
if __name__ == '__main__':  
  logger.info('start setting up markov chain')
  
  if len(sys.argv) == 3:
    verb = sys.argv[2]
  else: 
    verb = 'DEBUG'
  genMarkov(sys.argv[1], verb)
  
  logger.info('done with genMarkov.py')
