#!/usr/bin/env python

"""
Summarize all results from forwardPremium runs into one summary table. 
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

from __future__ import print_function

import os, re, sys
#sys.exit()

import numpy as np
import time
path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
  sys.path.append(path2Pymods)
from pymods.support.support import is_float, is_int
from pymods.support.support import getUniqueList, getParameter, importMatlabFile, extractList
from pymods.classes.tableDict import tableDict
import logbook

def createOverviewTable(resultDir, outFile, slUIPFile, exportFileName, sortTable = True, logLevel = 'INFO', logFile = None):
  '''
    Function takes a resultDir as input and then reads the c++ output file simulation.out. 
    All the simulation results are written in a tableDict. Then the according empirical estimation 
    results are merged into the table. 
    Finally, the whole table is written to outFile located in resultDir. 
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

    
  logger.debug('entering createOverviewTable')
    
  #print('resultDir: ', resultDir)
#  resultDirs = os.listdir(resultDir)
#  (dirpath, dirnames, filenames) = os.walk(resultDir)
  
  pathToInput  = 'input/'
  pathToOutput = 'output/'
  pathToOutput2 = 'output/output'
#  pathToOutput = 'output/output/'

  # Extract the relevant result folders
  resultFolders = [] 
  for (folder, dirnames, filenames) in os.walk(resultDir):
    # Walk through all subdirectories and look into those with para.* format
    (head, folderTail) = os.path.split(os.path.normpath(folder))
    if not re.search('para.*', folderTail):
      continue
    fileName = os.path.join(folder, pathToOutput, outFile)
    fileName2 = os.path.join(folder, pathToOutput2, outFile)
    if os.path.isfile(fileName) and os.path.getsize(fileName):
#      print(fileName, ' found')
      logger.debug('%s found', fileName)
      resultFolders.append((folder, pathToOutput, outFile))
    elif os.path.isfile(fileName2) and os.path.getsize(fileName2):
      logger.debug('%s found', fileName2)
      resultFolders.append((folder, pathToOutput2, outFile))
    else:
#      print(fileName, ' does not exist')
      logger.debug(fileName + ' does not exist')
  if not resultFolders:
    logger.debug('nothing to do, all folders empty')
    return None
  
  logger.debug('resultFolders: ')
  logger.debug(resultFolders)

  # initialize arrays
  headers = ['run']
  resultEntry = []

  # Set up result matrix 
  logger.debug('\n')
  logger.debug('Set up result matrix')
  for ixFolder, (folderPath, outputPath, outFilePath) in enumerate(resultFolders):
    fileName = os.path.join(folderPath, outputPath, outFilePath)
    logger.debug('Reading folder: ' + folderPath + '( ' + fileName + ' )')
    (head, folderPath) = os.path.split(os.path.normpath(folderPath))
    resultEntry.append([])
    descr = re.match('para[^_]+_(.*)', folderPath).groups(1)
    resultEntry[ixFolder].append(descr[0])
    # open specific simu.out file
    try: 
      outFileHandle = open(fileName)
    except: 
      continue
    lines = outFileHandle.readlines()
    for ixLine, line in enumerate(lines):
      line = line.rstrip()
      if line: 
        lineEles = line.rstrip().split(':')
        (head, element) = [ ele.strip() for ele in lineEles]
        logger.debug('head=' +  head + ' element=' + element)
        if ixFolder == 0:
          newHead = reformatString(head)
          logger.debug('head = ' + newHead)
          headers.append(newHead)
        if is_int(element):
          resultEntry[ixFolder].append(int(element))
        elif is_float(element):
          resultEntry[ixFolder].append(float(element))
        else:
          resultEntry[ixFolder].append(element)
    logger.debug('headers = ')
    logger.debug(headers)
    logger.debug('')
#    time.sleep(0.5)
      
  # Create a tableDict from the read data. 
  overviewTableDict = tableDict.fromRowList(headers, resultEntry)
  logger.debug(overviewTableDict)
  
  # split run field
  parameters = overviewTableDict['run'][0].split('_')
  keys = []
  for para in parameters:
    para = para.split('=')
    key = para[0]
    if key == 'beta':
      # handle the insane case where we loop over the time discount factor which has the same name
      # as the slope coefficient from the UIP regression
      key = 'beta_disc'
    keys.append(key)

  # Keys from the run field
  keys = getUniqueList(keys)
  logger.debug('keys = %s', keys)
  
  # Set up a runs dictionary from the keys obtained from the run field. 
  runs = {}
  runs = runs.fromkeys(keys)

  for key in runs:
    runs[key] = []
    
  # Set up the keys for the final table. 
  # Combine keys from run column with the other column labels. 
  # Delete run field from relevant keys
  #keys = keys + headers.tolist()
  #keys.remove('run')
  runs['keys'] = keys

  # generate runs columns
  for ixRun, run in enumerate(overviewTableDict['run']):
    parameters = run.split('_')
    for para in parameters:
      para = para.split('=')
      key = para[0]
      if key == 'beta':
        # handle the insane case where we loop over the time discount factor which has the same name
        # as the slope coefficient from the UIP regression
        key = 'beta_disc'
      val = para[1]
      if run.count(key) > 1:
        try:
          runs[key][ixRun].append(val)
        except IndexError:
          runs[key].append([])
          runs[key][ixRun].append(val)
      else:
        runs[key].append(val)
  
  splitRunField = tableDict.fromDict(runs)
  logger.debug(splitRunField)

  # Add newly obtained run fields to full dictionary. 
  overviewTableDict.hzcatTableDict(splitRunField)
  logger.debug(overviewTableDict.cols)
  logger.debug(overviewTableDict)
  
  # Split beta from c++ simulation into actual value and standard error
  overviewTableDict.insertEmptyCol('se_simBeta', float)
  overviewTableDict.insertEmptyCol('simBeta', float)
  overviewTableDict.insertEmptyCol('se_simAlpha', float)
  overviewTableDict.insertEmptyCol('simAlpha', float)
  for ixRow in range(overviewTableDict.nRows):
    betaParts = overviewTableDict['beta'][ixRow].split()
    alphaParts = overviewTableDict['alpha'][ixRow].split()
    overviewTableDict['se_simBeta'][ixRow] = float(betaParts[1][1:-1])
    overviewTableDict['se_simAlpha'][ixRow] = float(alphaParts[1][1:-1])
    overviewTableDict['simBeta'][ixRow]    = betaParts[0]
    overviewTableDict['simAlpha'][ixRow]    = alphaParts[0]
  overviewTableDict.drop('beta')
  overviewTableDict.drop('alpha')
  
  
  # Delete run variable
  #overviewTableDict.drop('run')
  
  # Get country pair for each run
  CtryPairs = []
  for resultFolder in resultFolders:
    # make ctry pair field
    Ctry1 = getParameter(os.path.join(resultDir, resultFolder[0], pathToInput, 'markovA.in'), 'Ctry', '(\s*)([a-zA-Z0-9]+)(\s+)([a-zA-Z0-9\s,;\[\]\-]+)(\s*)')
    Ctry2 = getParameter(os.path.join(resultDir, resultFolder[0], pathToInput, 'markovB.in'), 'Ctry', '(\s*)([a-zA-Z0-9]+)(\s+)([a-zA-Z0-9\s,;\[\]\-]+)(\s*)')
    # Create a country pair column
    CtryPairs.append(Ctry1 + Ctry2)
    
  overviewTableDict['ctryPair'] = CtryPairs
  
  logger.debug(overviewTableDict.cols)
  logger.debug(overviewTableDict)

  # Get the full results table for the UIP regression
  slUIP = readSlUIP(slUIPFile)
  logger.debug('print out the read table from our UIP regression b4 adjusting columns: ')
  logger.debug(slUIP) 
  for ixCol, col in enumerate(slUIP.cols):
    slUIP.rename(col, reformatString(col))
  slUIP.rename('beta', 'empBeta')
  slUIP.rename('cI_[', 'empCI_[')
  slUIP.rename('cI_]', 'empCI_]')
  slUIP.rename('tstat', 'empTstat')
  slUIP.rename('start', 'empStart')
  slUIP.rename('end', 'empEnd')
  
  logger.debug('logger.debug out the read table from our UIP regression after adjusting columns: ')
  logger.debug(slUIP) 
  
  if 'Ctry' in overviewTableDict.cols: overviewTableDict.drop('Ctry')
  
  overviewTableDict.merge(slUIP, column = 'ctryPair') 
  
  logger.debug('Columns of merged table: ')
  logger.debug(overviewTableDict.cols)
  overviewTableDict.drop([ 'avg_consumption_tradables_a', 'std_consumption_tradables_a', 'avg_consumption_tradables_b', 
                          'std_consumption_tradables_b', 'avg_q1', 'std_q1', 'avg_q2', 'std_q2', 'avg_hA', 'std_hA', 'avg_hB', 'std_hB',
                          'simBeta', 'se_simBeta', 'simAlpha', 'se_simAlpha', 'empCI_[', 'empCI_]', 'empTstat',  'empStart', 'empEnd'])
  overviewTableDict.order([ 'ctry1', 'ctry2'])

  logger.debug('\n overviewTable after merging and deleting some vars')
  logger.debug(overviewTableDict)
  
  if sortTable:
    overviewTableDict.sort(['ctryPair', 'run'])
  overviewTableDict.drop('run')

  # Only retain the successful merges. If nothing left quit. 
  overviewTableDict.subset(overviewTableDict.data['_merge'] == 3)
  if overviewTableDict.nRows == 0:
    logger.debug('could find the Ctry pair in empirical data. merge != 3 for all observations. ')
    sys.exit()
  overviewTableDict.drop(['_merge'])
  logger.debug(overviewTableDict)
    
  # Establish optimization criteria
  overviewTableDict['dev']     = np.abs( overviewTableDict['famaFrenchBeta'] - overviewTableDict['empBeta'] )
  overviewTableDict['normDev'] = overviewTableDict['dev'] / overviewTableDict['se']
  
  logger.debug('Try to order in this way: Ctry1, Ctry2, EA, EB, sigmaA, sigmaB, famaFrenchBeta, empBeta')
  logger.debug(overviewTableDict.cols)
  logger.debug(overviewTableDict)
  if 'sigmaA' in overviewTableDict.cols:
    overviewTableDict.order([ 'ctry1', 'ctry2', 'normDev', 'EA', 'EB', 'sigmaA', 'sigmaB', 'famaFrenchBeta', 'empBeta', 'se' ])
  elif 'EA' in overviewTableDict.cols:
    overviewTableDict.order([ 'ctry1', 'ctry2', 'normDev', 'EA', 'EB', 'famaFrenchBeta', 'empBeta', 'se' ])
  else:
    logger.debug('Couldnt apply the ordering')

  overviewTableDict.setWidth(15)
  overviewTableDict.setPrec(3)   
  logger.info(overviewTableDict)  

  if exportFileName:
    exportFilePath = os.path.join(resultDir, exportFileName)
    logger.debug('Writing table to ' + exportFilePath)
    overviewSimu = open(exportFilePath, 'w')  

    print(overviewTableDict, file = overviewSimu)    
    # flush the output in case this script is called within a larger project
    # by default output gets flushed when buffer full or program exits. 
    overviewSimu.flush()
    logger.debug('Done writing table')
  
  
    
  logger.debug('done createOverviewTable')  
    
##  myFH.pop_application()
##  mySH.pop_application()
  
  logger.handlers = []
  return overviewTableDict
  
  
  


  # ----------------
  
  
def reformatString(stringIn):
  '''
    Makes sure that no spaces are in column names and formats 
    strings according to our convenction: 
    Words separated by underscores with lower case start for next word. 
    Example: Avg. Consumption Tradables A -> avg_consumption_tradables_a
  '''
  strParts = stringIn.split()
  for ixPart, part in enumerate(strParts):
    strParts[ixPart] = part[0].lower() + part[1:]
    strParts[ixPart] = strParts[ixPart].replace('.', '')
    strParts[ixPart] = strParts[ixPart].replace('-', '_')
    if ixPart == 0:
      newStr = strParts[ixPart]
    else:
      newStr = newStr + '_' + strParts[ixPart]
  return newStr
  
def readSlUIP(slUIPFile):
  '''
    Read matlab file storing the uip output. 
    Generate country pair variable. 
  '''
  matLabIn = importMatlabFile(slUIPFile)
  slUIP = matLabIn['slUIP']
  slUIP_extracted = extractList(slUIP)
  slUIP_tabledict = tableDict.fromRowList(slUIP_extracted[0], slUIP_extracted[1:])
  
  slUIP_tabledict['CtryPair'] = list(map(''.join, zip(slUIP_tabledict['Ctry1'], slUIP_tabledict['Ctry2'])))
  
  return slUIP_tabledict


if __name__ == '__main__':
  print('start') 
  print(sys.argv)
  print(len(sys.argv))
  if len(sys.argv) == 1:
    logger.debug('plz specify resultsfolder as first argument... ')
    sys.exit()
  createOverviewTable(sys.argv[1], 'simulation.out', '../gpremium.src/slUIP.mat', 'overviewSimu', 'debug', 'someLog')
  print('\ndone') 




 
