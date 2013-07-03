#!/usr/bin/env python

'''
  Moment matching. 
  Matlab working debug revision forwardPremium 218. markovChain 15. 
'''

import numpy as np
import scipy, scipy.linalg, scipy.optimize
import MkovM
try: 
  import cvxopt
except ImportError:
  print 'Warning: Could not import cvxopt. Momentmatching might not work. '
try: 
  import openopt
except ImportError: 
  print 'Warning: Could not import openopt. Momentmatching might not work. '

np.set_printoptions(precision = 7, linewidth = 300)
#np.seterr(invalid='raise')

def optShock(dims, target):
  
  # Dimensions
  states = dims['states']
  vars   = dims['vars']
    
  # Set trans matrix
  #   lambda = rand(states, 1);
  #   lambda = lambda / sum(lambda);
  lmbda = ( 1. / states ) * np.ones( (states) )
  T = np.tile(lmbda.T, (states, 1))
  
  
  # Set starting point
  S = np.ones( (states, vars) ) * target['E']
  np.random.seed(100)
  S = S + np.random.normal(0., 0.1, (states, vars) )
  
  # Initialize persistent T and target
  objective = objectiveShockMatrix(T, target)
  
  # reshape column major
  S0 = np.reshape(S, (states*vars, 1), 'F')
  objective(S0)
  
  if dims['vars'] >= 3:
    print '3 vars error'
  
  # * for unpacking. Match unconditional mean. nVars constraints
  Aeq = scipy.linalg.block_diag(*[lmbda]*vars)
  beq = target['E']
  
  
  if dims['states'] == 6:
    # enforce pattern for first variable. states/2 constraints. 
    Aeq = np.vstack( (Aeq, np.hstack( (np.kron(np.eye(states / 2), np.array([-1, 1])), np.zeros( ( states / 2, states) ) ))))
    beq = np.concatenate( (beq, np.zeros( (states / 2) ) ) )
  elif dims['states'] == 9:
    Aeq = np.vstack( (Aeq, np.hstack( (np.hstack( (np.eye(6), np.zeros( (6, 3) ) ) ) + np.hstack( ( np.zeros( (6, 3) ), -np.eye(6) ) ), np.zeros( ( 6, 9 ) ) ) ) ) )
    beq = np.concatenate( (beq, np.zeros( (6) ) ) )
    
    Aeq = np.vstack( ( Aeq, np.array([[0., 1., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
                                      [0., 0., 0., 0., 1., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
                                      [0., 0., 0., 0., 0., 0., 0., 1., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.]]) ) )
    beq = np.concatenate( (beq, np.zeros( (3) ) ) )
  else: 
    print '# of states not implemented'
  def linearCosntr(x):
    return -scipy.linalg.norm(np.dot(Aeq, x).flat - beq)

  LB = np.zeros( (states * vars) )
  UB = np.ones( (states * vars) ) * 10.
  
  if ( (objective(S0) < 1e-8) and ( np.max(np.abs(np.dot(Aeq, S0) - beq)) < 1e-8) and ( np.sum(S0 < LB) == 0 ) ):
    Z = S0
    fval = objective(S0)
  else:
    Svec = scipy.optimize.fmin_cobyla(objective, S0, linearCosntr, iprint = '0')
    #p = openopt.NLP(objective, S0, df=None,  c=None,  dc=None, h=None,  dh=None,  A=None,  b=None,  Aeq=Aeq,  beq=beq, 
        #lb=LB, ub=UB, gtol=1e-7, xtol = 1e-9, ftol = 1e-15, contol=1e-7, iprint = 50, maxIter = 10000, maxFunEvals = 1e7, name = 'NLP_1')
    #solver = 'ralg'
    ##  solver = 'algencan'
      ##solver = 'ipopt'
      ##solver = 'scipy_slsqp'
    ##  solver = 'lincher'
    ##  solver = 'scipy_cobyla' # step size too large
    ##  solver = 'mma'
    ##  solver = 'gsubg'
    #r = p.solve(solver, plot=0) # string argument is solver name
    #Svec = p.xf
##    cvxopt.solvers.cp(F[, G, h[, dims[, A, b[, kktsolver]]]])
    
    
  fOpt = objective(Svec)    
  if fOpt > 1e-5:
    print 'computeShockMatrix: Warning, fval is %g \n' % fOpt

  Sopt = np.reshape(Svec, (states, vars), 'F' )
  testM = MkovM.MkovM(Sopt, T)
  print testM
  
  # changes correlation... 
  #Sopt = np.sort(Sopt, axis = 0)
  
  return Sopt


def optTrans(dims, target, S):

  # Dimensions
  states = dims['states']
  vars   = dims['vars']
  
  # Set starting trans matrix
  lmbda = np.ones( (states) ) / states
  T = np.tile(lmbda, (states, 1) )
  np.random.seed(100)
  T =   T + np.random.normal(0., 0.01, (states, states) )
  
  # Initialize persistent S and target
  objective = objectiveTransMatrix(S, target)
  
  # Rewrite T as row vector. 
  T0 = np.ravel(T, 'F')
  
  # Set linear equality constraints. 
  Aeqs = {}
  beqs = {}
  
  Aeqs['RowSumOne'] = np.kron(np.eye(states), np.ones( (1,states) ))
  beqs['RowSumOne'] = np.ones(states)
  
  Aeqs['ColSumOne'] = np.tile(np.eye(states), (1, states) )
  beqs['ColSumOne'] = np.ones(states)
  
  # Assemble constraint: Ax = b
  Aeq = np.empty( (0, states * states) )
  beq = np.array( [] )
  
  for AeqEle, beqEle in zip(Aeqs, beqs):
    Aeq = np.vstack( (Aeq, Aeqs[AeqEle]) )
    beq = np.concatenate( (beq, beqs[beqEle]) )
  
  slack = 0.7;
  LB = np.ones( (states * states) ) * ( 1. / states ) * slack
  UB = np.ones( (states * states) ) * ( 1. / states ) * ( 1. / slack )
  
  def linearCosntr(x):
    return -scipy.linalg.norm(np.dot(Aeq, x).flat - beq)
  
  def lowerBoundCosntr(x):
    constrVec = np.array([x - LB * np.ones( np.size(x) ) ])
    return scipy.linalg.norm(constrVec, ord = 1)

  def upperBoundCosntr(x):
    constrVec = np.array([UB * np.ones( np.size(x) ) - x ])
    return scipy.linalg.norm(constrVec, ord = 1)
  
  
  #Tvec = scipy.optimize.fmin_cobyla(objective, T0, [lowerBoundCosntr, upperBoundCosntr, linearCosntr], iprint = '2', rhobeg = 0.001, rhoend = 0.0001, maxfun = 100000)
#  Tvec = scipy.optimize.fmin_slsqp(objective, T0, eqcons=[linearCosntr], ieqcons=[], bounds=[ (slack / states , 1. / (slack * states)) for ix in range(len(T0)) ], fprime=None, fprime_eqcons=None, fprime_ieqcons=None, args=(), iter=100, acc=1e-06, iprint=10, full_output=0, epsilon=1.4901161193847656e-08)
#  fOpt = objective(Tvec)
  p = openopt.NLP(objective, T0, df=None,  c=None,  dc=None, h=None,  dh=None,  A=None,  b=None,  Aeq=Aeq,  beq=beq, 
	        lb=LB, ub=UB, gtol=1e-7, xtol = 1e-7, ftol = 1e-7, contol=1e-7, iprint = 50, maxIter = 500, maxFunEvals = 1e7, name = 'NLP_1')
  solver = 'ralg'
#  solver = 'algencan'
  #solver = 'ipopt'
  #solver = 'scipy_slsqp'
#  solver = 'lincher'
#  solver = 'scipy_cobyla' # step size too large
#  solver = 'mma'
#  solver = 'gsubg'
  r = p.solve(solver, plot=0) # string argument is solver name
  Tvec = p.xf
  fOpt = objective(Tvec)
  if fOpt > 1e-5:
    print 'computeTransMatrix: Warning, fval is %g \n' % fOpt
  
  Topt = np.reshape(Tvec, (states, states), 'F' )
  
  curMchain = MkovM.MkovM(S, Topt)
  print curMchain
  
  # Report output
  for moment in target:
    print 'after optimization mchain moment vs target moment for moment %s are' % (moment)
    print curMchain[moment]
    print target[moment]
  
  
  return Topt 
  
  
class objectiveTransMatrix(object):
  def __init__(self, S, target):
    self.S = S
    self.target = target
    self.states = np.size(S, axis = 0)
    self.vars   = np.size(S, axis = 1)
  
  def __call__(self, Tvec):
    np.set_printoptions(precision = 7, linewidth = 300)
    # Initialize variables
    out = np.zeros( (1,1) )
    
    T = np.reshape(Tvec, (self.states, self.states), 'F' )
    
    #print T
    
    curMkov = MkovM.MkovM(self.S, T)
    
    out = 0
    for moment in self.target:
      if moment == 'Et':
        weight = 20000
        weight = 10
      elif moment == 'StdEt':
        weight = 20000
      else:
        weight = 100
      sqrdDeviation = curMkov.getDeviation(self.target[moment], moment)
      out += weight * sqrdDeviation

    if np.isnan(out):
      print 'objectiveShockMatrix: return NaN'
    
    if ~np.isreal(out):
      print 'objectiveShockMatrix: returning imaginary number'
      
    return out
  
  
  
class objectiveShockMatrix(object):
  def __init__(self, T, target):
    self.T = T
    self.target = target
    self.states = np.size(T, axis = 0)
  
    
  def __call__(self, Svec):
  # Initialize variables
    vars = len(Svec) / self.states
    S = np.reshape(Svec, (self.states, vars), 'F' )
    
    curMkov = MkovM.MkovM(S, self.T)
  
    out = 0
    for moment in self.target:
      if moment == 'Std':
        weight = 100
      else:
        weight = 1
      sqrdDeviation = curMkov.getDeviation(self.target[moment], moment)
      out += weight * sqrdDeviation
    
    if np.isnan(out):
      print 'objectiveShockMatrix: return NaN'
    
    if ~np.isreal(out):
      print 'objectiveShockMatrix: returning imaginary number'

    return out

 



if __name__ == '__main__': 
  shockTarget = {}
  shockTarget['E'] = np.array([1.0, 1.0])
  shockTarget['Std'] = np.array([0.1, 0.15])
  shockTarget['Cor'] = 0.1
  
  dims = {}
  dims['states'] = 6
  dims['vars']   = 2
  
  perturb = np.zeros( (6, 2) )
  
  S = optShock(dims, shockTarget)
  
  # sort S
  ind = np.lexsort((S.T[1], S.T[0]))
  S = S[ind].copy()

  print S
  
  transTarget = {}
#transTarget['Et']    = np.array([[np.inf ] *6, [0.99, 0.99, 0.99, 1.01, 1.01, 1.01]  ]).T
  transTarget['Theta'] = np.array([[np.inf, np.inf], [np.inf, 0.4]])
  
  T = optTrans(dims, transTarget, S)

  
  
  