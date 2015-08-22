#!/usr/bin/env python

'''
  Create an overview table after running gIdRiskUML.py
  Syntax: 
  ------
    syntax: path2createTable/createTable.py . [sortcols] [ordercols]
  Inputs: 
  -------
    1) path
    2) sortcols   -  comma separated list, no spaces
    3) ordercols  -  comma separated list, no spaces
  outputs: 
  -------
    1) overviewSim file

  Example:  
    1) ~/workspace/idrisk/model/code/gPremiumScripts/gIdRiskUML.py -x /home/jonen/workspace/idrisk/model/bin/idRiskOut -b ~/workspace/idrisk/model/base para.loop  -C 5 -N -A /home/jonen/apppot0+ben.diskUpd.img
    2) 
'''


import os, re, sys
import numpy as np
path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
  sys.path.append(path2Pymods)
from pymods.support.support import is_float, is_int
from pymods.support.support import getUniqueList
from pymods.classes.tableDict import tableDict
from pymods.support.support import wrapLogger



def createOverviewTable(resultDir, outFile = 'simulation.out', exportFileName = 'overviewSimu', sortCols = [], orderCols = [], verb = 'DEBUG'):
  '''
    create simple overview tables for idRisk project
  '''
  #set up logger
  logger = wrapLogger(loggerName = 'createTableLog', streamVerb = verb, logFile = None)

  # Extract the relevant result folders
  resultFolders = [] 
  for (folder, dirnames, filenames) in os.walk(resultDir):
    # Walk through all subdirectories and look into those with para.* format
   # (head, folderTail) = os.path.split(os.path.normpath(folder))
    # if it's one of these output.1 folders, ignore
    if re.search('output\.', folder):
      continue
    if not re.search('para.*', folder):
      continue
    if not outFile in filenames:
      continue
    fileName = os.path.join(folder, outFile)
    if not os.path.getsize(fileName):
      continue
    resultFolder, pathToOutputFile = re.match('(.*para[^_]*_[^/]*)/(.*)', fileName).groups()
    logger.debug('%s found' % fileName)
    # Check if simulation.out ended properly. 
   # fileName = os.path.join(pathToFile[0], pathToFile[1], pathToFile[2])
    outFileHandle = open(fileName)
    lines = outFileHandle.read()
    if lines.rfind('simulation ended'):
      resultFolders.append((resultFolder, pathToOutputFile))
    else:
      logger.warning('simulation.out did not end properly. Check: %s' % (fileName))
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
  for ixFolder, (resultFolder, pathToOutputFile) in enumerate(resultFolders):
    fileName = os.path.join(resultFolder, pathToOutputFile)
    logger.debug('Reading folder: ' + resultFolder)
   # (head, folderPath) = os.path.split(os.path.normpath(folderPath))
    resultEntry.append([])
    descr = re.match('.*para[^_]*_([^/]*).*', fileName).groups(1)
    logger.debug('descr is '  + descr[0])
    resultEntry[ixFolder].append(descr[0])
    # open specific simu.out file
    try: 
      outFileHandle = open(fileName)
    except: 
      continue
    lines = outFileHandle.readlines()
    for ixLine, line in enumerate(lines):
      line = line.rstrip()
      if line.find('simulation ended') >= 0: continue
      if line: 
        lineEles = line.rstrip().split(':')
        if lineEles[0] == 'Iteration': continue
        if re.search('iBar_Shock[2-9]Agent0', lineEles[0]): continue
        if len(lineEles) != 2: continue
        if len(lineEles[1].split()) > 1: # dont read vectors
          continue
        if len(lineEles[1].split('.')) > 2: 
          continue
        (head, element) = [ ele.strip() for ele in lineEles]
        logger.debug('head=' +  head + ' element=' + element)
        if ixFolder == 0:
          newHead = reformatString(head)
          logger.debug('head = ' + newHead)
          headers.append(newHead)
        if not element:
          logger.debug('element is none')
        elif is_int(element):
          resultEntry[ixFolder].append(int(element))
        elif is_float(element):
          resultEntry[ixFolder].append(float(element))
        else:
          resultEntry[ixFolder].append(element)
    logger.debug('headers = ')
    logger.debug(headers)
    logger.debug('')
    
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
  logger.debug('keys = ' + str(keys))
  
  # Set up a runs dictionary from the keys obtained from the run field. 
  runs = {}
  runs = runs.fromkeys(keys)

  for key in keys:
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
##      if run.count('_' + key) > 1:
##        try:
##          runs[key][ixRun].append(val)
##        except IndexError:
##          runs[key].append([])
##          runs[key][ixRun].append(val)
##      else:
      runs[key].append(val)
  
  splitRunField = tableDict.fromDict(runs)
  logger.debug('\nsplit Run Field: ')
  logger.debug(splitRunField)
  
  
  # Add newly obtained run fields to full dictionary. 
  overviewTableDict.hzcatTableDict(splitRunField, append = False)
  #splitRunField.hzcatTableDict(overviewTableDict)
  logger.debug(overviewTableDict.cols)
  logger.debug(overviewTableDict)
  
  # Drop run field
  overviewTableDict.drop('run')

  # create some additional columns for analysis
  overviewTableDict['eR_a_sc'] = overviewTableDict['eR_a']**4
  overviewTableDict['eR_b_sc'] = overviewTableDict['eR_b']**4
  overviewTableDict['rP_ana_sc'] = overviewTableDict['eR_a_sc'] - overviewTableDict['eR_b_sc']
  overviewTableDict['std_R_a_sc'] = overviewTableDict['std_R_a(quar)'] * 2
  overviewTableDict['std_R_b_sc'] = overviewTableDict['std_R_b(quar)'] * 2

  # place new columns in orderCols
  for ele in [ 'eR_a_sc', 'eR_b_sc', 'rP_ana_sc', 'std_R_a_sc', 'std_R_b_sc' ]:
    if ele in orderCols: continue
    orderCols.append(ele)


  # cols = list(overviewTableDict.cols)
  # print cols
  # cols.insert(cols.index('eR_a')         + 1, 'eR_a_sc')
  # cols.insert(cols.index('eR_b')         + 1, 'eR_b_sc')
  # cols.insert(cols.index('rP_ana')       + 1, 'rP_ana_sc')
  # cols.insert(cols.index('std_R_a(quar)' + 1), 'std_R_a_sc')
  # cols.insert(cols.index('std_R_b(quar)' + 1), 'std_R_b_sc')
  # overviewTableDict.cols = np.array(cols)
  
  if 'warning_____t_iteration_converged_no_more_than' in overviewTableDict.cols:
    overviewTableDict.rename('warning_____t_iteration_converged_no_more_than', 't_maxConv')    
    
  
##  if 'gamma' in overviewTableDict.cols:
##    overviewTableDict.sort(['gamma'])
    
  if sortCols:
    overviewTableDict.sort(sortCols)
  if orderCols:
    overviewTableDict.order(orderCols)
  
  overviewTableDict.setWidth(26)
  overviewTableDict.setPrec(10)   
  logger.debug(overviewTableDict)

  logger.debug('possible sort/order columns are')
  logger.debug(overviewTableDict.cols)
  logger.debug('syntax: createTable.py . [sortcols] [ordercols]')
  
  if exportFileName:
    exportFilePath = os.path.join(resultDir, exportFileName)
    logger.debug('Writing table to ' + exportFilePath)
    overviewSimu = open(exportFilePath, 'w')  

    print >> overviewSimu, overviewTableDict
    # flush the output in case this script is called within a larger project
    # by default output gets flushed when buffer full or program exits. 
    overviewSimu.flush()
    logger.debug('Done writing table')
    return overviewTableDict
    
##  import matplotlib
##  matplotlib.use('Agg')
##  import matplotlib.pyplot as plt
##  import matplotlib.mlab as mlab
##  # make plot
##  fig = plt.figure()
##  ax = fig.add_subplot(111)
##  x = overviewTableDict['gamma']
##  y = overviewTableDict['equity_premium']
##  ax.plot(x, y)
##  filename = '/mnt/shareOffice/idRisk/results/loopOverGamma/gammaPlot.svg'
##  fig.savefig(filename)
###  os.chmod(filename, 0o660)
##  os.system('chmod 660 ' + filename)
  
    
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

    
    
if __name__ == '__main__':
  if len(sys.argv) == 1:
    logger.debug('plz specify resultsfolder as first argument... ')
    sys.exit()
  if len(sys.argv) == 2:
    sortCols = []
    orderCols = []
  if len(sys.argv) >= 3:
    sortCols = sys.argv[2].split(',')
    orderCols = []
  if len(sys.argv) >= 4:
    orderCols = sys.argv[3].split(',')
  createOverviewTable(resultDir = sys.argv[1], outFile = 'simulation.out', exportFileName = 'overviewSimu', sortCols = sortCols, orderCols = orderCols )
  
  
  
