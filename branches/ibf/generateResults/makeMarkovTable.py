#!/usr/bin/env python3

"""
Create a markov overview table for the forwardPremium project. 
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
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>, Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'

import os, re, sys
import numpy as np
from pyMethods.support.support import importMatlabFile, extractList, flattenMatStruct, table_list2Dict, table_mergeDicts, table_listAppend, printDict, getParameter
from pyMethods.classes.tableDict import tableDict

def makeMarkovTable(runDir = None):

  if runDir == None:
    fpResultsFolder = '/mnt/shareOffice/ForwardPremium/Results/runs/'
    completedLoops = os.listdir(fpResultsFolder)
    completedLoops.sort()
    for ixLoop, loopFolder in enumerate(completedLoops):
      print(str(ixLoop) + ' --> ' + loopFolder)
    ans = input("Plz enter index for loop to analyze? Or quit (q)? ")
    if ans == 'q': sys.exit()
    ans = int(ans)

    if not ans in list(range(0, len(completedLoops))): 
      print('entry makes no sense, aboritng...')
      sys.exit()
#    os.chdir(completedLoops[ans])
    resultDir = os.path.join(fpResultsFolder, completedLoops[ans])
    completedLoops = os.listdir(resultDir)
    completedLoops.sort()
    for ixLoop, loopFolder in enumerate(completedLoops):
      print(str(ixLoop) + ' --> ' + loopFolder)
    ans = input("Plz enter index for loop to analyze? Or quit (q)? ")
    if ans == 'q': sys.exit()
    ans = int(ans)

    if not ans in list(range(0, len(completedLoops))): 
      print('entry makes no sense, aboritng...')
      sys.exit()
    runDir = os.path.join(resultDir, completedLoops[ans])
  
  
  print(runDir)
  tablePath = '/mnt/shareOffice/ForwardPremium/Data/Code/output/tableEmpMoments/'
  regexIn = '(\s*)([a-zA-Z0-9]+)(\s+)([a-zA-Z0-9\s,;\[\]\-]+)(\s*)'
  parasA = { 'Ctry': None, 'base': None}
  parasB = dict(parasA) # make deep copy
  paras = {'A': parasA , 'B': parasB  }
  for ctryAlph in ['A', 'B']:    
    paraFileName = 'markov' + ctryAlph + '.in'
    fileIn = os.path.join(runDir, paraFileName)
    for para in paras[ctryAlph]:       
      paras[ctryAlph][para] = getParameter(fileIn, para, regexIn)
  print(paras)
      
  # get moments
  mkovEmpTables = []
  for ctryAlph in ['A', 'B']:
    tables = []
    for anaPart in ['emp', 'mkov']:
    # Check base
      if paras['A']['base'] == 'basket':
        fileBase = anaPart + 'Base' + 'basket' + '_' + '~' + paras['A']['Ctry'] + '~' + paras['B']['Ctry']
      else:
        fileBase = anaPart + 'Base' + 'basket'
      fileName = fileBase + 'Ctry' + paras[ctryAlph]['Ctry'] +'.mat'
      table = matStruct2Dict(os.path.join(tablePath, fileName), 'table')
  #    print(table)
      tables.append(table)
    mkovEmp = tables[0].merged(tables[1], 'Parameter')
    mkovEmp.drop('_merge')
    mkovEmpTables.append(mkovEmp)
    print(mkovEmp)
  
  # Make full table from each country's table. Append table 1 to table 0. 
  mkovEmpTable = mkovEmpTables[0].getAppended(mkovEmpTables[1])
  print(mkovEmpTable)
  
  # Generate country pair
  CtryPair = paras['A']['Ctry'] + paras['B']['Ctry']
  
  # Save resulting table
  fileName = CtryPair + 'markovTable'
  tableDir = '/mnt/shareOffice/ForwardPremium/Results/tablesPaper/'
  tableDirFile = os.path.join(tableDir, fileName)
  #mkovEmpFile = open(os.path.join(tableDir, fileName + 'tableDict'), 'w')
  #print(mkovEmpTable, file = mkovEmpFile)
  
  
  
  # For specific US , JAPAN table
  mkovEmpTable['sortKey'] = [ 6, 0, 3, 1, 4, 2, 5, 7 + 6, 7 + 0, 7 + 3, 7 + 1, 7 + 4, 7 + 2, 7 + 5 ]
  mkovEmpTable.sort(['sortKey'])
  mkovEmpTable.drop(['sortKey'])
  print('after sorting')
  print(mkovEmpTable)
  mkovEmpTable['Parameter'] = [ 'Mean', 'Std.', 'Pers.', 'Mean', 'Std.',  'Pers.' , 'Corr.', 
                                 'Mean', 'Std.', 'Pers.', 'Mean', 'Std.',  'Pers.' , 'Corr.',   ]
  mkovEmpTable[' '] =     [ '$\\mathbb{E}[\\Delta s]$' , '$\\sigma[\\Delta s]$' , '$\\theta_{s}$'  , '$\\mathbb{E}[\\Delta y]$' , '$\\sigma[\\Delta y]$' , '$\\theta_{y}$',   '$\\rho_{\\Delta s, \\Delta y}$', 
                             '$\\mathbb{E}[\\Delta s]$' , '$\\sigma[\\Delta s]$' , '$\\theta_{s}$'  , '$\\mathbb{E}[\\Delta y]$' , '$\\sigma[\\Delta y]$' , '$\\theta_{y}$',   '$\\rho_{\\Delta s, \\Delta y}$']
  mkovEmpTable['Ctry'] = [ paras['A']['Ctry'] ] * 7 + [ paras['B']['Ctry'] ] * 7
  mkovEmpTable['markovVar'] = [ 'FX' ] * 3  +  [ 'GDP' ] * 3 + [' '] +[ 'FX' ] * 3  +[ 'GDP' ] * 3 + [' ']
  mkovEmpTable.order(['Ctry', 'markovVar', 'Parameter', ' '])
  mkovEmpTable.width = 30
  mkovEmpTable.prec  = 3
  alignment = 'llllrrr'  
  print(mkovEmpTable)
  mkovEmpTable.print2latex(fileIn = tableDirFile, alignment = alignment)
  
  
  print('hello')
  




def matStruct2Dict(fileIn, tableName):
  '''
   1) read matlab file
   2) Extract list with recursive function. 
   3) 
  '''
  # Make markov table, joining discretization with empirical

  matFile = importMatlabFile(fileIn, struct_as_record = False)
  matStruct = flattenMatStruct(matFile[tableName])
#  tableDictOut = tableDict()
  tableDictOut = tableDict.fromRowList(matStruct.keys, matStruct.table)
  #table_list = matStruct.table
  #table_list.insert(0, matStruct.keys)
  #tableDictOut = table_list2Dict(table_list)
  return tableDictOut




  
  
  
if __name__ == '__main__':
  print('start') 
 # makeMarkovTable('/mnt/shareOffice/ForwardPremium/Results/2011-02-23-18-17-51LoopOver[b1Bbar,b2Abar,EA,EB]/2011-02-23-18-17-51_b1Bbar=0.000_b2Abar=0.000_EA=0.000_EB=0.000/')
  makeMarkovTable()  
  print('\ndone') 