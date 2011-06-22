#!/usr/bin/env python

"""
Computes targets for global optimization for the forwardPremium project. 
"""
# Copyright (C) 2011 University of Zurich. All rights reserved.
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
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'

from __future__ import print_function

import os, re, sys
path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
  sys.path.append(path2Pymods)
import numpy as np
import numpy.ma as ma
import copy
from pymods.classes.tableDict import tableDict
import logbook

def oneCtryPair(tableIn, varsIn, valsIn, targetVar, logLevel = 'INFO', logFile = None):
  '''
    Given tableIn find the value of targetVar for each paraCombo (represented by a line). 
  '''  
  mySH = logbook.StreamHandler(stream = sys.stdout, level = logLevel.upper(), format_string = '{record.message}', bubble = True)
  mySH.format_string = '{record.message}'
 # mySH.push_application()
  myFH = logbook.FileHandler(filename = __name__ + '.log', level = 'DEBUG', bubble = True)
  myFH.format_string = '{record.message}'
 # myFH.push_application()   
  
  logger = logbook.Logger(__name__)
  
  logger.handlers.append(mySH)
  logger.handlers.append(myFH)
  
  logger.debug('entering oneCtryPair')
  
  if not tableIn:
    resultVec = np.empty(len(valsIn))
    resultVec.fill(1000)
    return resultVec 
  
  table = tableIn
  logger.debug(table)
  if not targetVar in table.cols:    
    logger.debug('The target variable does not exist in the overview table')
    sys.exit()
  # Convert masked array back to regular array
#  resultVec = np.array(table[targetVar])
  resultVec = np.array([])
  # Now check for possible failures  
  # Unpack valsIn from list to arguments (i.e. f([a,b,c]) -> f(a,b,c))
  # http://docs.python.org/tutorial/controlflow.html#tut-unpacking-arguments
#  paraCombos = zip(*valsIn)
  paraCombos = valsIn
  # go through all different para combos
  for ixPara, paraCombo in enumerate(paraCombos):
    testTable = copy.deepcopy(table)
    # check that the para combo exists    
    # check for each var
    for ixVar, var in enumerate(varsIn):
      testTable.subset( np.abs(testTable[var] - paraCombo[ixVar]) < 1e-10 )
    if len(testTable) >= 1:
      resultVec = np.insert(resultVec, ixPara, testTable['normDev'][0])
      logger.debug('paracombo %s found!' % paraCombo)
      logger.debug(testTable[var])
      logger.debug(paraCombo[ixVar])
    elif not testTable:
    # some arbitrary, large value
      logger.debug('paracombo %s not found!' % paraCombo)
      resultVec = np.insert(resultVec, ixPara, 1000)
    else:
      logger.critical('testTable is not length 1 or gone. This makes no sense... check')
      sys.exit()
  if not len(resultVec) == len(valsIn): 
    logger.critical('inconsistent computation...check')
    sys.exit()
  
        
  xvals = [ val[0] for val in valsIn ]
  logger.debug(xvals)
  logger.debug(resultVec)
  
  logger.debug('done oneCtryPair')

##  myFH.pop_application()
##  mySH.pop_application()
  logger.handlers = []

  return resultVec

#def oneHabitPerCtryPair(tableIn, varsIn, valsIn, targetVar, logLevel = 'INFO', logFile = None):
# One habit per ctry pair. For n ctrys there are (n^2 -n) / 2 ctry pairs. 

# oneHabitPerCtry. 
  

def freeParas(tableIn):
  '''
    Look for the lowest deviation for each country pair, varying habit of both countries independently from 
    other country pairs. 
  '''
  tablesPaperPath = '/mnt/shareOffice/ForwardPremium/Results/tablesPaper/'
  outputFileName = 'freeParas.tex'
  outputFile = open(os.path.join(tablesPaperPath, outputFileName), 'w')
  
  
  print(tableIn)
  table = tableDict.fromTextFile(tableIn, 15, 5)
  print(table)
  CtryPairs = np.unique(table['ctryPair'])
  print(CtryPairs)
    
  for ixCtryPair, CtryPair in enumerate(CtryPairs):
    print(CtryPair)
    tableSub = table.getSubset( table['ctryPair'] == CtryPair )
    tableSub.sort(['normDev'])   
    tableSub = tableSub[0]
    print(tableSub)
    if ixCtryPair == 0: 
      fullTable = tableSub
    else:
      fullTable = fullTable.getAppended(tableSub)

  print(fullTable)

  print('Optimization results')
  optTable = copy.deepcopy(fullTable)
  if 'sigmaA' in optTable.cols:
    optTable.keep(['ctry1', 'ctry2', 'normDev', 'EA', 'sigmaA', 'famaFrenchBeta', 'empBeta' ])
    optTable.order(['ctry1', 'ctry2', 'normDev', 'EA', 'sigmaA', 'famaFrenchBeta', 'empBeta' ])
  elif 'EA' in optTable.cols:
    optTable.keep(['ctry1', 'ctry2', 'normDev', 'EA', 'famaFrenchBeta', 'empBeta' ])
    optTable.order(['ctry1', 'ctry2', 'normDev', 'EA', 'famaFrenchBeta', 'empBeta' ])
  else:
    print('Couldnt find sigmaA or EA... something wrong!')
  optTable.setWidth(20)
  optTable.setPrec(3)
  print(optTable)
  
  
  print('Now produce table for paper and send latex file to writeup folder: ')
  fullTable.prec = 3
  if 'sigmaA' in fullTable.cols:
    fullTable.keep(['ctry1', 'ctry2', 'famaFrenchBeta', 'empBeta', 'se', 'EA', 'sigmaA'])
    fullTable.order(['ctry1', 'ctry2', 'EA', 'sigmaA', 'empBeta', 'se', 'famaFrenchBeta'])
  elif 'EA' in fullTable.cols:
    fullTable.keep(['ctry1', 'ctry2', 'famaFrenchBeta', 'empBeta', 'se', 'EA'])
    fullTable.order(['ctry1', 'ctry2', 'EA', 'empBeta', 'se', 'famaFrenchBeta'])
  else:
    print('Couldnt find sigmaA or EA... something wrong!')

  fullTable.rename('famaFrenchBeta'   , 'Model $\\beta$')
  fullTable.rename('empBeta'          , 'Emp. $\\beta$')
  
  fullTable.subset( fullTable['ctry1'] != 'CA' )
  
  print(fullTable)
  
  alignment = 'llrrrrr'
  fullTable.print2latex(fileIn = outputFile, alignment = alignment)
  
def sameParas(tableIn):
  '''
    Try to find one pair of E and sigma that applies to all countries and all possible 
    country combinations. So far this is done heuristically by looking at subsets. 
  '''
  tablesPaperPath = '/mnt/shareOffice/ForwardPremium/Results/tablesPaper/'
  outputFileName = 'sameParas.tex'
  outputFile = open(os.path.join(tablesPaperPath, outputFileName), 'w')
  
  print('Reading table: ', tableIn)
  table = tableDict.fromTextFile(tableIn, 15, 5)
  print(table)
  ctryPairs = np.unique(table['ctryPair'])
  print('Unique ctryPairs are: \n', ctryPairs)
  if 'sigmaA' in table.cols:
    sigmas = np.unique( table['sigmaA'] )
    print('Sigmas are: \n', sigmas)
  Es = np.unique( table['EA'] )
  print('Es are: \n', Es)
  

  
#  table.subset( table['ctry1'] != 'CA' )
  
  # Pick out rows for which criteria table['sigmaA'] == sigma is fullfilled. 
##  ixBool = [ False ] * table.nRows
##  ixBool = ma.array(ixBool)
##  sigmas = [ 0.005, 0.006, 0.007 ]
##  for sigma in sigmas:
##    ixBool = ixBool | ( table['sigmaA'] == sigma )
##  table.subset( ixBool )  
  

  
##  ixBool = [ False ] * table.nRows
##  ixBool = ma.array(ixBool)
##  Es = [ 0.94, 0.95, 0.96 ]
##  for E in Es:
##    ixBool = ixBool | ( table['EA'] == E )
##  table.subset( ixBool )    

  

  optMaxNormDev = 100
  if 'sigmaA' in table.cols:
    for ixSigma, sigma in enumerate(sigmas):
      tableSub = table.getSubset( table['sigmaA'] == sigma )
      for ixE, E in enumerate(Es):
        tableSubSub = tableSub.getSubset( tableSub['EA'] == E )
        if tableSubSub.nRows < len(ctryPairs): 
          print('Incomplete run for E = {0:.3f} sigma = {1:.3f}. Skipping... '.format(E, sigma))
          continue
        maxNormDev = max(tableSubSub['normDev'])
        if maxNormDev < optMaxNormDev: 
          optMaxNormDev = maxNormDev
          optE          = E
          optSigma      = sigma
    print('Table applying best (E,sigma) pair\n')
    tableSub = table.getSubset( (table['EA'] == optE) & (table['sigmaA'] == optSigma) )
    print(tableSub)
    print('best (E,sigma) pair: ')
    print('optimal E:'      , optE)
    print('optimal sigma:'  , optSigma)
    print('normDev:'        , optMaxNormDev)
  elif 'EA' in table.cols:
    for ixE, E in enumerate(Es):
      tableSubSub = table.getSubset( table['EA'] == E )
      if tableSubSub.nRows < len(ctryPairs): 
        print('Incomplete run for E = {0:.3f}. Skipping... '.format(E))
        continue
      maxNormDev = max(tableSubSub['normDev'])
      if maxNormDev < optMaxNormDev: 
        optMaxNormDev = maxNormDev
        optE          = E
    print('Table applying best (E) pair\n')
    tableSub = table.getSubset( (table['EA'] == optE) )
    print(tableSub)
    print('best (E) pair: ')
    print('optimal E:'      , optE)
    print('normDev:'        , optMaxNormDev)
  else:
    print('Couldnt find sigmaA or EA... something wrong!')
          
          
   
      
##  finalTable = table.getSubset( (table['EA'] == 0.95) & (table['sigmaA'] == 0.006) )
##  finalTable.keep(['Ctry1', 'Ctry2', 'FamaFrenchBeta', 'beta', 's.e.', 'EA', 'sigmaA'])
##  finalTable.order(['Ctry1', 'Ctry2', 'EA', 'sigmaA', 'beta', 's.e.', 'FamaFrenchBeta'])
##  finalTable.drop(['EA', 'sigmaA'])
##  finalTable.rename('FamaFrenchBeta', 'Model $\\beta$')
##  finalTable.rename('beta'          , 'Emp. $\\beta$')
##  print(finalTable)
  
##  finalTable.prec = 3
##  alignment = 'llrrr'
##  finalTable.print2latex(fileIn = outputFile, alignment = alignment)
    
    
  
if __name__ == '__main__':
  print('start') 
  if len(sys.argv) == 1:
    print('plz specify table to analyze as first argument... ')
    sys.exit()
 # freeParas(sys.argv[1])
 # sameParas(sys.argv[1])

  print('done')
  
  
  
  