#!/usr/bin/env python

import numpy as np
import os, sys
path2Pymods = os.path.join(os.path.dirname(__file__), '../../')
if not sys.path.count(path2Pymods):
  sys.path.append(path2Pymods)
from pymods.support.wrapLogbook import wrapLogger

verb = 'INFO'
logger = wrapLogger(loggerName = 'MkovM.log', streamVerb = verb, logFile = None)

class MkovM:
  def __init__(self, S, T):
    self.S = np.asarray(S)
    self.T = np.asarray(T)
    self.states = np.size(S, 0)
    self.vars   = np.size(S, 1)
    self.lmbda  = self.getlmbda()
    self.E      = self.getE()
    self.Et     = self.getEt()
    self.V      = self.getV()
    self.E2     = self.getE2()
    self.E2_lag = self.getE2_lag()
    self.getCov = self.getV
    
  def __getitem__(self, key):
    if key == 'E':
      return self.E
    if key == 'Et':
      Et = self.getEt()
      return Et
    if key == 'Cor':
      return getLowerTr(self.getCor())
    if key == 'Std':
      return self.getStd()
    if key == 'Theta':
      return self.getTheta()
    
  def getDeviation(self, targetMoment, moment, norm = 2):
    mkovMoment = self.__getitem__(moment)
    if np.isscalar(targetMoment):
      targetMoment = np.array([targetMoment])
    active = np.invert(np.isinf(targetMoment))
    deviation = mkovMoment[active] - targetMoment[active]
    normDeviation = np.linalg.norm(deviation, ord = norm)
    return normDeviation

  def getlmbda(self):
    # Compute uncoditional state probabilities 
    P = np.mat(self.T)**1000
    return np.asarray(P[1,:])
    
  def getE(self):
    # Unconditional expectations
    E = np.empty(self.vars)
    for ixVar in range(self.vars):
      E[ixVar] = np.dot(self.lmbda, self.S[:, ixVar])
    return E
  
  def getEt(self):
    # Compute conditional means
    Et = np.zeros( (self.states, self.vars) )
    for ixState in range(self.states):
      for ixVar in range(self.vars):
        Et[ixState, ixVar] = np.dot(self.T[ixState, :],  self.S[:, ixVar])
    return Et
        
  def getV(self): 
    V = np.zeros( ( self.vars, self.vars) )
    for ixVar1 in range(self.vars):
      for ixVar2 in range(self.vars):
        V[ixVar1, ixVar2] = np.sum( self.lmbda * ( self.S[:, ixVar1] - self.E[ixVar1] ) * ( self.S[:, ixVar2] - self.E[ixVar2] ) )
    return V
  
  def getE2(self): 
    E2 = np.zeros( ( self.vars, self.vars) )
    for ixVar1 in range(self.vars):
      for ixVar2 in range(self.vars):
        E2[ixVar1, ixVar2] = np.sum( self.lmbda * self.S[:, ixVar1] * self.S[:, ixVar2] )       
    return E2
        
  def getE2_lag(self): 
    E2_lag = np.zeros( ( self.vars, self.vars) )
    for ixVar1 in range(self.vars):
      for ixVar2 in range(self.vars):
        E2_lag[ixVar1, ixVar2] = np.sum( self.lmbda * self.S[:, ixVar1] * self.Et[:, ixVar2] )       
    return E2_lag
  
  def getStd(self):
    return np.diag(self.V)**(1./2.)
    
  def getCor(self):
    corMat = np.zeros( (self.vars, self.vars) )
    Std = self.getStd();
    for ixVar1 in range(self.vars):
      for ixVar2 in range(self.vars):
        corMat[ixVar1, ixVar2] = self.V[ixVar1, ixVar2] / ( Std[ixVar1] * Std[ixVar2] )    
        # check if one of the std is 0, then corr has to be zero
        if ( Std[ixVar1] < 1.e-10 ) or ( Std[ixVar2] < 1.e-10 ):
          corMat[ixVar1, ixVar2] = 0
    return corMat
          
  def getSkew(self):
    # Unconditional skewness.
    # Returns vars*1 vector of skewness
    skew = np.zeros( (self.vars) );
    Std = self.getStd()
    for ixVar in range(self.vars):
      mu3 = np.sum(self.lmbda * ( self.S[:, ixVar] - self.E[ixVar] ) **3)
      skew[ixVar] = mu3 / self.V[ixVar, ixVar] ** (3. / 2.)
      # check if std is 0, then corr has to be zero
      if Std[ixVar] < 1e-10: 
        skew[ixVar] = 0
    return skew
  
  def getKurt(self):
    '''
      Unconditional kurtosis
    '''
    kurt = np.zeros(self.vars)
    for ixVar in range(self.vars):
      mu4 = np.sum( self.lmbda * ( self.S[:, ixVar] - self.E[ixVar] ) ** 4)
      kurt[ixVar] = mu4 / self.V[ixVar, ixVar] ** 2
    return kurt
    
  def getTheta(self):
    Theta = np.zeros( (self.vars, self.vars) )
    for ixVar in range(self.vars):
      beta = self.olsCoefficients(np.hstack( (self.E[ixVar], self.E[:].T) ), self.E2, self.E2_lag[:, ixVar])
      # beta = [alpha, beta_1, ..., beta_vars]
      Theta[ixVar, :] = beta[1:self.vars + 1]
    return Theta
        
  def olsCoefficients(self, E, EXSquared, EYX):
    '''
      computes the coefficients of a regression of the form
      y = beta0 + beta1 x1 + beta2 x2 + beta3 x3 + ... + epsilon
          for reference page 71. Johnston - Dinardo
      
      Inputs:
      E: vector of the moments of y and x
          E = [ E y , E x1, E x2, ... ]
      
      # ESquared: matrix of the squared sums with the following form:
                    E x1^2    E x1 x2     E x1 x3
                    E x2 x1   E x2^2      E x2 x3
      
      EYX: comoments of y and x:
        EYX = [E y x1, E y x2, ...]
    '''
  
    n = len(E)
    E = E[:]
    
    A = np.zeros( (n, n) )
    B = np.zeros( (n) )
    
    # constructing left hand side for the equation beta has to fulfill
    A[0, 0] = 1
    A[0, 1:] = E[1:].T
    A[1:, 0] = E[1:]
    A[1:, 1:] = EXSquared
    
    # constructing right hand side
    B[0]  = E[0]
    B[1:] = EYX[:]
      
    beta = np.linalg.solve(A,B)
    return beta
  
  def getEt(self):
    # Compute conditional means
    Et = np.zeros( (self.states, self.vars) )
    for ixState in range(self.states):
      for ixVar in range(self.vars):
        Et[ixState, ixVar] = np.dot(self.T[ixState, :], self.S[:, ixVar])
    return Et
  
  def getVEt(self):
    VEt = np.zeros( (self.vars, self.vars) )
    Et = self.getEt()    
    for ixVar1 in range(self.vars):
      for ixVar2 in range(self.vars):
        VEt[ixVar1, ixVar2] = np.sum( self.lmbda * ( Et[:, ixVar1] - self.E[ixVar1] ) * ( Et[:, ixVar2] - self.E[ixVar2] ) )
        if VEt[ixVar1, ixVar2] < -1.e-4: # Take some slack here. 
          print 'what?'
    return VEt

  
  def getVt(self):
    Vt = []
    condV = np.zeros( (self.vars, self.vars) )
    for ixState in range(self.states):
      for ixVar1 in range(self.vars):
        for ixVar2 in range(self.vars):
          condV[ixVar1, ixVar2] = np.dot(self.T[ixState, :], ( self.S[:, ixVar1] - self.Et[ixState, ixVar1] ) * ( self.S[:, ixVar2] - self.Et[ixState, ixVar2] ) )
          if condV[ixVar1, ixVar2] < -1.e-4:
            print 'what?'
      Vt.append(condV)
    return Vt
  
  def getStdt(self):
    Stdt = []
    Vt = self.getVt()
    for ixState in range(self.states):
      Stdt.append(np.diag(Vt[ixState]) ** (1. / 2.))
    return Stdt
 
  def getCovt(self):
    return self.getVt()
  
  def getCort(self):
    '''
      Returns a cell with a conditional correlation matrix for each state. 
    '''
    cort    = np.zeros( (self.vars, self.vars) )
    corCell = np.array([])
    Stdt    = self.getStdt()
    Vt      = self.getVt()
    for ixState in range(self.states):
      for ixVar1 in range(self.vars):
        for ixVar2 in range(self.vars):
          cort[ixVar1, ixVar2] = Vt[ixState][ixVar1, ixVar2] / ( Stdt[ixState][ixVar1] * Stdt[ixState][ixVar2] )
      corCell = np.append(corCell, cort)
    return cort
      
      
  def getStdEt(self):
    StdEt = np.zeros( (self.vars, self.vars) )
    VEt   = self.getVEt();
    for ixVar1 in range(self.vars):
      for ixVar2 in range(self.vars):
        StdEt[ixVar1, ixVar2] = VEt[ixVar1, ixVar2] ** (1. / 2. ) 
    return StdEt

  def getCovSEtS(self): 
    '''
      % CorSEtS is the covariance of the shock matrix (S) with E_t(S). 
      % CorSEtS = cor(ixVar1, E_t(ixVar2))
      % CorSEtS = [ x E_t(x)  x E_t(y) ; 
      %             y E_t(x)  y E_t(y) ]
    '''
    CovSEtS = np.zeros( (self.vars, self.vars) )
    Et = self.getEt()
    for ixVar1 in range(self.vars):
      for ixVar2 in range(self.vars):
        CovSEtS[ixVar1, ixVar2] = np.sum( self.lmbda * ( self.S[:, ixVar1] - self.E[ixVar1] ) * ( Et[:, ixVar2] - self.E[ixVar2] ) )
    return CovSEtS
  
  def getCorSEtS(self):
    '''
      CorSEtS is the correlation of the shock matrix (S) with E_t(S). 
      CorSEtS = cor(ixVar1, E_t(ixVar2))
      CorSEtS = [ x E_t(x)  x E_t(y) ; 
                  y E_t(x)  y E_t(y) ]
    '''
    CorSEtS = np.zeros( (self.vars, self.vars) )
    CovSEtS = self.getCovSEtS()
    StdEt   = self.getStdEt();
    Std     = self.getStd();
    for ixVar1 in range(self.vars):
      for ixVar2 in range(self.vars):
        CorSEtS[ixVar1, ixVar2] = CovSEtS[ixVar1, ixVar2] / ( Std[ixVar1] * StdEt[ixVar2, ixVar2] )
    return CorSEtS
  
  def __str__(self):
    np.set_printoptions(precision = 4, suppress = True)
    outStr = ''
    outStr += 'States = %d, Vars = %d\n' % (self.states, self.vars)
    outStr += 'Shock matrix (S)\n'
    outStr += self.S.__str__() + '\n'
    outStr += 'Transition matrix (T)\n'
    outStr += self.T.__str__() + '\n'
    outStr += 'Unconditional probs(lambda)\n'
    outStr += self.lmbda.__str__() + '\n'
    outStr += 'Unconditional means(E) \n'
    outStr += self.E.__str__() + '\n'
    #outStr += 'Unconditional means sqr (E2) \n'
    #outStr += self.E2.__str__() + '\n'
    #outStr += 'Unconditional means sqr lagged (E2_lag) \n'
    #outStr += self.E2_lag.__str__() + '\n'
    #outStr += 'Unconditional variance \n'
    #outStr += self.V.__str__() + '\n'
    outStr += 'Unconditional std \n'
    outStr += self.getStd().__str__() + '\n'
    #outStr += 'Unconditional cov \n'
    #outStr += self.getCov().__str__() + '\n'
    outStr += 'Unconditional correlation \n'
    outStr += self.getCor().__str__() + '\n'
    outStr += 'Unconditional skewness \n'
    outStr += self.getSkew().__str__() + '\n'
    outStr += 'Unconditional kurtosis \n'
    outStr += self.getKurt().__str__() + '\n'
    outStr += 'Persistence \n'
    outStr += self.getTheta().__str__() + '\n'
    outStr += 'Conditional means(Et) \n'
    outStr += self.Et.__str__() + '\n'
    outStr += 'Conditional std(Std_t) \n'
    outStr += self.getStdt().__str__() + '\n'
    #outStr += 'Conditional covariance \n'
    #outStr += self.getCovt().__str__() + '\n'
    outStr += 'Conditional correlation \n'
    outStr += self.getCort().__str__() + '\n'
    outStr += 'Stadnard deviation E_t: std(E_t)\n'
    outStr += self.getStdEt().__str__() + '\n'
    outStr += 'Correlation S E_t(S)\n'
    outStr += ' [ corr(x,E_t(x)),   corr(x, E_t(y)) ] \n'
    outStr += ' [ corr(y,E_t(x)),   corr(y, E_t(y)) ] \n'
    outStr += self.getCorSEtS().__str__() + '\n'
    
    
    return outStr
  
  
  
  def simulation(self, N = 50000):
    import pandas
    import scikits.statsmodels.tsa.api
    np.set_string_function(None)
     
    nSims = 1
    
    logger.debug('\n\n--------- SIMULATION---------\n\n')
    
    logger.debug('T = %d\n' % N)
    
    cumTransMatrix = np.cumsum(self.T, 1)
    
    varSim = np.zeros( (N, self.vars ) )
    shocks = np.zeros( (N), dtype = int )
    
    shocks[0] = 1
    
    varSim[0, :] = self.S[shocks[0], :]
    
    lastShock = 1
    for t in range(1, N):
      shockR = np.random.random_sample()
      itemindex = np.where(shockR < cumTransMatrix[lastShock, :])
      shock = itemindex[0][0]
      shocks[t] = shock
      for ixVar in range(self.vars):
        varSim[t, ixVar] = self.S[shock, ixVar]
      lastShock = shock
      
    index = np.arange(N)
    varSim = pandas.DataFrame(data = varSim, index = index, columns = map(str, range(self.vars)))
    logger.debug(varSim)
    
    logger.debug('\nUnconditional means(E)')
    logger.debug(varSim.mean())
    logger.debug('\nUnconditional std')
    logger.debug(varSim.std())
    logger.debug('\nUnconditional skewness')
    logger.debug(varSim.skew())
    try: 
      logger.debug('\nUnconditional correlation')
      logger.debug(varSim.corr())
    except: 
      pass
    
    model = scikits.statsmodels.tsa.api.VAR(varSim)
#    model = scikits.statsmodels.tsa.vector_ar.var_model.VAR(varSim)
    results = model.fit(1)
    Theta = results.params[1:,:]
    logger.debug('\n Persistence')
    logger.debug(Theta)
    
    logger.debug('done with simulation')
    
    return varSim, Theta
    
def getLowerTr(arrayIn):
  lowerTr = np.tril(arrayIn, -1)
  return lowerTr[lowerTr.nonzero()]
  


if __name__ == '__main__': 
  
  S = np.array([
      [0.9639,    0.6973],
      [0.9937,    0.8961],
      [0.9968,    1.1344],
      [0.9974,    1.0177],
      [1.0080,    0.9492],
      [1.0092,    1.0290],
      [1.0228,    0.9954]
      ])
  
  nStates = np.size(S, 0)
  T = 1. / nStates * np.ones((nStates, nStates))
  
  x = MkovM(S, T)
  print x
  x.simulation()
  print x['Std']
  print('done')

