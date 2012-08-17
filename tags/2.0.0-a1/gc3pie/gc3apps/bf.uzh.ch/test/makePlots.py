#!/usr/bin/env python

import os, re, sys, os.path
import copy
import numpy as np
path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
  sys.path.append(path2Pymods)
from pymods.classes.tableDict import tableDict
from pymods.support.support import wrapLogger

# check matplotlib plot properties (kwargs) here: 
# http://matplotlib.sourceforge.net/api/pyplot_api.html#matplotlib.pyplot.plot
import matplotlib.cm
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab

from createTable import createOverviewTable

verb = 'DEBUG'
logger = wrapLogger(loggerName = 'createTableLog', streamVerb = verb, logFile = None)

# list of characters for line style
# http://www.thetechrepo.com/main-articles/469
  
  
  
def momentPlots(baseName, path, xVar, overlay, conditions = {}, xVarRange = None, figureFile = None, type = 'presentation'):
  
  if os.path.isdir(path):
#  if not tableFile:
    tableFile = os.path.join(path, 'overviewSimu')
  elif os.path.isfile(path):
    tableFile = path
  else: 
    logger.critical('path is not a directory or a table file.')
  
  logger.info('\nCreating plot for path %s with conditions %s' % (path, conditions))
   
  # make sure xVar is not conditioned upon -> we want to use it for xaxis.  
  if xVar in conditions.keys():
    conditions.pop(xVar)
  
  # create a file name for emerging plots
  moments = ['EP', 'e_rs', 'e_rb', 'sigma_rs', 'sigma_rb']
  if not figureFile:
    paraMoments = moments + conditions.keys()
  #  paraMomentVals   = map(str, [EP, e_rs, e_rb, sigma_rs, sigma_rb] + conditions.values())
    paraMomentVals   = map(str, overlay.values() + conditions.values())
    paraString = '_'.join(map(''.join, zip(paraMoments, paraMomentVals)))
    fileName = baseName + 'xVar' + xVar + '__' + paraString
    fileName = fileName.replace('.', '') # kill all dots for latex
    figureFile = os.path.join('/mnt/resultsLarge/idRisk/plots', fileName + '.eps')
  
  # create table if not existent already
  if not os.path.isfile(tableFile) or not os.path.getsize(tableFile):
    tablePath = os.path.dirname(tableFile)
    createOverviewTable(tablePath)
    
  # read and adjust table
  overviewTab = tableDict.fromTextFile(tableFile, None, 20, 5)
  logger.debug('Read the following table: ')

  # Rename variables
  logger.debug(overviewTab)
  overviewTab.rename('eR_a', 'e_rs')
  overviewTab.rename('std_R_a(quar)', 'sigma_rs')
  overviewTab.rename('eR_b', 'e_rb')
  overviewTab.rename('std_R_b(quar)', 'sigma_rb')
  overviewTab.rename('rP_ana', 'EP')
  
  # Scale to yearly values
  overviewTab['e_rs'] = overviewTab['e_rs'] ** 4
  overviewTab['e_rb'] = overviewTab['e_rb'] ** 4
  overviewTab['EP'] = overviewTab['e_rs'] - overviewTab['e_rb']
  overviewTab['sigma_rs'] = overviewTab['sigma_rs'] * 2
  overviewTab['sigma_rb'] = overviewTab['sigma_rb'] * 2
  
  # make consistency check
  if not xVar in overviewTab.cols:
    print 'xVar %s not found in table' % xVar
    sys.exit()
  
  overviewTab.order([xVar] + conditions.keys() + moments)
  overviewTab.sort(conditions.keys() + [xVar])
    
  logger.debug('table after order and sort')
  logger.debug(overviewTab)

  # Copy table and impose conditions
  overviewTabSub = copy.deepcopy(overviewTab)

  logger.debug('keep only relevant variables')
  varsToKeep = ['EP', 'e_rs', 'e_rb', 'sigma_rs', 'sigma_rb']
  if not varsToKeep.count(xVar): varsToKeep.insert(0, xVar)
  overviewTabSub.keep(varsToKeep)
  
  # Subset y series
  for cond in conditions:
    overviewTabSub = overviewTabSub.getSubset( (overviewTabSub[cond] == conditions[cond] ))
  # Subset x range
  if xVarRange:
    overviewTabSub.subset( (overviewTabSub[xVar] >= xVarRange[0]) & (overviewTabSub[xVar] <= xVarRange[1]) )
  # make sure the table is not empty
  if len(overviewTabSub) == 0:
    logger.critical('table empty. Returning')
    return 1

  logger.info('table after imposing conditions:')
  logger.info(overviewTabSub)
  x = overviewTabSub[xVar]
  
  # Scale variables to percentage points
  overviewTabSub['EP'] = overviewTabSub['EP'] * 100
  overviewTabSub['e_rs'] = (overviewTabSub['e_rs'] -1 ) * 100
  overviewTabSub['e_rb'] = (overviewTabSub['e_rb'] -1 ) * 100
  overviewTabSub['sigma_rs'] = overviewTabSub['sigma_rs']  * 100
  overviewTabSub['sigma_rb'] = overviewTabSub['sigma_rb']  * 100
  
  logger.info('Final plot data')
  logger.info(overviewTabSub)

  # make plot
  fig = plt.figure()
  ax = fig.add_subplot(111)
  
  # set formatting for plots
  if type == 'presentation':
    colors = {'EP': 'k', 'e_rs': 'b', 'e_rb': 'g', 'sigma_rs': 'r', 'sigma_rb': 'c'}
    linewidths = { 'EP': 3, 'e_rs': 3, 'e_rb': 3, 'sigma_rs': 3, 'sigma_rb': 3 }
    linestyles = { 'EP': '-', 'e_rs': '-', 'e_rb': '-', 'sigma_rs': '-', 'sigma_rb': '-' }
    
  elif type == 'paper':
    colors = {'EP': 'k', 'e_rs': 'b', 'e_rb': 'g', 'sigma_rs': 'r', 'sigma_rb': 'c'}
    linewidths = { 'EP': 3, 'e_rs': 3, 'e_rb': 3, 'sigma_rs': 3, 'sigma_rb': 3 }
    linestyles = { 'EP': '-', 'e_rs': '-', 'e_rb': '-', 'sigma_rs': '-', 'sigma_rb': '-' }
  else: 
    logger.critical('unknown plot type to create. ')
    
  labels = { 'EP': '$E(R_m-R_f)$', 'e_rs': '$E(R_m)$', 'e_rb': '$E(R_f)$', 'sigma_rs': '$\sigma(R_m)$', 'sigma_rb': '$\sigma(R_f)$' }
  
  yVars = list(varsToKeep)
  yVars.remove(xVar)
  for yvar in yVars:
    ax.plot(x, overviewTabSub[yvar], linewidth = linewidths[yvar], linestyle = linestyles[yvar], color = colors[yvar], antialiased = True, label = labels[yvar])
  
##  if overlay['EP']:
##    ax.plot(x, overviewTabSub['EP'] * 100           , linewidths = 3, linestyle = '-', color = 'k', antialiased = True, label = '$E(R_m-R_f)$')
##  if overlay['e_rs']:  
##    ax.plot(x, (overviewTabSub['e_rs'] -1 ) * 100   , linewidth = 3, linestyle = '-', color = 'b', antialiased = True, label = '$E(R_m)$')
##  if overlay['e_rb']:
##    ax.plot(x, (overviewTabSub['e_rb'] -1 ) * 100   , linewidth = 3, linestyle = '-', color = 'g', antialiased = True, label = '$E(R_f)$')
##  if overlay['sigma_rs']:  
##    ax.plot(x, overviewTabSub['sigma_rs'] * 100     , linewidth = 3, linestyle = '-', color = 'r', antialiased = True, label = '$\sigma(R_m)$')
##  if overlay['sigma_rb']:
##    ax.plot(x, overviewTabSub['sigma_rb'] * 100     , linewidth = 3, linestyle = '-', marker = '', color = 'c', antialiased = True, label = '$\sigma(R_f)$')
##  #ax.plot(x, overviewTabSub['iBar_Shock0Agent0'] * 10     , linewidth = 3, linestyle = '-', marker = '', color = 'c', antialiased = True, label = 'iBar_Shock0Agent0')    
    
  if type == 'presentation':
    ax.legend(loc = 'lower left')
    if xVar == 'dy':
      ax.set_xlabel('$\Delta y$', fontsize = 18)
    else:
      ax.set_xlabel(xVar, fontsize = 18)
    ax.set_ylabel(r'%', fontsize = 18)
  elif type == 'paper':
    ax.legend(loc = 'lower left')
    if xVar == 'dy':
      ax.set_xlabel('$\Delta y$', fontsize = 10)
    else:
      ax.set_xlabel(xVar, fontsize = 10)
    ax.set_ylabel(r'%', fontsize = 10)
  else: 
    logger.critical('unknown plot type to create. ')
  
  fig.savefig(figureFile)
  os.system('chmod 660 ' + figureFile)
  
  logger.info('figure saved to %s' % figureFile)
  

if __name__ == '__main__':
  logger.info('start main')
#  dyPlot()
#  momentsDyPlot()

#  ezMomentsDyPlot(xVar = 'psi', EP = True, e_rs = True, e_rb = True, sigma_rs = True, sigma_rb = True, nAgent = 3, gamma = 5, psi = 0.5,  dy = 0.)


  overlay1 = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
  overlay2 = {'EP': True, 'e_rs': False, 'e_rb': False, 'sigma_rs': True, 'sigma_rb': True}
  overlay3 = {'EP': True, 'e_rs': False, 'e_rb': False, 'sigma_rs': False, 'sigma_rb': False}
  overlays = [ overlay1, overlay2, overlay3 ]

  #logger.info('')
  #logger.info('Case 1')
  #baseName = 'pDy_CRRAMoments'
  #path = '/mnt/resultsLarge/idRisk/pDy'
  #conditions = {'nAgent': 6, 'p': 0.75, 'dy': 0.5}
  #momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay1, conditions = conditions)
  
  #logger.info('')
  #logger.info('Case 2')
  #baseName = 'nAgent8Gamma2_CRRAMoments'
  #path = '/mnt/resultsLarge/idRisk/nAgent8Gamma2'
  #conditions = {}
  #overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
  #momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay2, conditions = conditions)

  #logger.info('')
  #logger.info('Case 3')
  #baseName = 'ezNAgent6GammaPsi_ezMoments'
  #path = '/mnt/resultsLarge/idRisk/ezNAgent6GammaPsi'
  #conditions = {'gamma': 2, 'psi': 1.5}
  #overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
  #momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay2, conditions = conditions)

  #logger.info('')
  #logger.info('Case 4')
  #baseName = 'ezNAgent6GammaPsi_ezMoments'
  #path = '/mnt/resultsLarge/idRisk/ezNAgent6GammaPsi'
  #conditions = {'gamma': 5, 'psi': 0.5}
  #overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
  #momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay2, conditions = conditions)

  #logger.info('')
  #logger.info('Case 5')
  #baseName = 'ezNAgent6GammaPsi_ezMoments'
  #path = '/mnt/resultsLarge/idRisk/ezNAgent6GammaPsi'
  #conditions = {'gamma': 5, 'psi': 1.5}
  #overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
  #momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay2, conditions = conditions)
 
  #logger.info('') 
  #logger.info('Case 6')
  #baseName = 'ezNAgent6GammaPsi_ezMoments'
  #path = '/mnt/resultsLarge/idRisk/ezNAgent6GammaPsi'
  #conditions = {'gamma': 10, 'psi': 0.2}
  #overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
  #momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay2, conditions = conditions)
  
  #logger.info('')
  #logger.info('Case 7')
  #baseName = 'ezNAgent6GammaPsi_ezMoments'
  #path = '/mnt/resultsLarge/idRisk/ezNAgent6GammaPsi'
  #conditions = {'gamma': 10, 'psi': 0.5}
  #overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
  #momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay2, conditions = conditions)
  
  #logger.info('')
  #logger.info('Case 8')
  #baseName = 'ezNAgent6GammaPsi_ezMoments'
  #path = '/mnt/resultsLarge/idRisk/ezNAgent6GammaPsi'
  #conditions = {'gamma': 10, 'psi': 1.5}
  #overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
  #momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay2, conditions = conditions)
  
  #logger.info('')
  #logger.info('Case 9')
  #baseName = 'eznAgent8Gamma5_ezMoments'
  #path = '/mnt/resultsLarge/idRisk/nAgent8Gamma5'
  #conditions = {'gamma': 5, 'psi': 1.5}
  #overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
  #momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay2, conditions = conditions)
  
  
  #logger.info('')
  #logger.info('Case 10')
  #baseName = 'eznAgent8Gamma2_ezMoments'
  #path = '/mnt/resultsLarge/idRisk/nAgent8Gamma2'
  #conditions = {'gamma': 2}
  #overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
  #for overlay in overlays:
    #momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay, conditions = conditions)
    
  #logger.info('')
  #logger.info('Case 10')
  #baseName = 'eznAgent8Gamma2_ezMoments'
  #path = '/mnt/resultsLarge/idRisk/betaDy'
  #conditions = {'gamma': 2, 'psi': 0.5, 'dy': 0.6}
  #overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
  #for overlay in [overlay1]:
    #momentPlots(baseName = baseName, path = path, xVar = 'beta_disc', overlay = overlay, conditions = conditions)
    
    
  logger.info('')
  logger.info('Case 10')
  baseName = 'CRRA_base'
  path = '/mnt/ocikbgtw/home/jonen/workspace/idrisk/model/results/dyLoop/optimalRuns'
  conditions = {}
  overlay = {'EP': True, 'e_rs': True, 'e_rb': True, 'sigma_rs': True, 'sigma_rb': True}
  for overlay in [overlay]:
    momentPlots(baseName = baseName, path = path, xVar = 'dy', overlay = overlay, conditions = conditions, xVarRange = [0.,0.6], type = 'paper')
    
    
#/home/resultsLarge/idRisk/betaDy
  
  
  logger.info('done main')