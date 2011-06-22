#!/usr/bin/env python3

"""
Create plots for forwardPremium project. 
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


#from pyMethods.plotting import gplot
from pyMethods.classes.tableDict import tableDict
from pyMethods.support.support import transposed
import numpy as np
import tempfile
import subprocess
import time
import io
import os, sys
import shutil

def betaPlots(tableFile, var1, var2, var2Vals, CtryPairs = None):
  # Modify the table as needed: 
  overviewTab = tableDict.fromTextFile(tableFile, 20, 5)
  if not CtryPairs:
    CtryPairs = np.unique(overviewTab['CtryPair'])
  for CtryPair in CtryPairs:
    betaPlot(overviewTab, CtryPair, var1, var2, var2Vals)

def betaPlot(overviewTab, CtryPair, var1, var2, var2Vals):
  # -----------------------
   
  ## Modify the table as needed: 
  print(overviewTab)
  overviewTab = overviewTab.getSubset( overviewTab['CtryPair'] == CtryPair ) 
  print(overviewTab) 
  
  
  # Create folder to store script and graphs. 
  tmpPlots = os.path.join(os.getcwd(), 'tmpPlots')
  if not os.path.exists(tmpPlots):
    os.mkdir(tmpPlots)
  gplotScriptFileName = os.path.join(tmpPlots, 'gplotScript')
  
  # Open script file and input basic setup. 
  gplotScript = open(gplotScriptFileName, 'w')
  print('''\
#!/usr/bin/env gnuplot
#set xtics font "Arial,5"
#set ytics font "Arial,5"
#set key font "8,8"
set style data lp
set ylabel "UIP Slope"
set border 0" 
set yzeroaxis lt -1 lw 0.5
set xzeroaxis lt -1 lw 0.5
set xtics axis
set xtics scale 0
set ytics scale 0
      \n''', file = gplotScript, end = '')
#  print('set title "' + CtryPair + '"', file = gplotScript)
  if var1 == 'EA':
    print('set xrange [0:50]', file = gplotScript)
    print('set xlabel "Implied risk aversion of average habit"', file = gplotScript)
  elif var1 == 'sigmaA':
    print('set xlabel "Std. habit"', file = gplotScript)

  # Create temporary files with data and write lines to plot the data into the script file 
  Y = []
  lspec = []
  plotCmd = 'plot'
  lt = [1, 2, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28]
  colors = ['midnight-blue', 'black', 'dark-orange', 'midnight-blue', 'black', 'dark-orange', 
            'midnight-blue', 'black', 'dark-orange', 'dark-orange', 'midnight-blue', 'black', 'dark-orange'
            'midnight-blue', 'black', 'dark-orange', 'dark-orange', 'midnight-blue', 'black', 'dark-orange'
            'midnight-blue', 'black', 'dark-orange', 'dark-orange', 'midnight-blue', 'black', 'dark-orange'
            'midnight-blue', 'black', 'dark-orange', 'dark-orange', 'midnight-blue', 'black', 'dark-orange'
            ]
  ctr = 0
  tmpFolder = '/tmp/'
  for ixVar2, var2Val in enumerate(list(np.unique(overviewTab[var2]))):
    print('var =', var2Val)
    if var2Vals and not var2Val in var2Vals: continue
    
    lspec.append('title "' + str(var2Val) + '"')
    
    # Generate subtable and write x, y pairs to temporary data file
    subTable = overviewTab.getSubset( (overviewTab[var2] == var2Val) )
    print(subTable)
    if var1 == 'EA':
      x = list(2 / (1 - subTable['EA']))
    elif var1 == 'sigmaA':
      x = subTable['sigmaA']
      
    y = list(map(float, subTable['FamaFrenchBeta'].tolist()))
    tmpFileName = tempfile.NamedTemporaryFile(mode='w', suffix='', prefix='tmp', dir=tmpFolder, delete=False).name
    tmpFile = open(tmpFileName, 'w')
    for xEle, yEle in zip(x,y):
      print(str(xEle) + '\t' + str(yEle), file = tmpFile)
    tmpFile.close()
    
    # Write plotting command in script file
    if var1 == 'EA':
      gplotString = '{0:6s} "{1}" using 1:2:(1.0) smooth acsplines lw 5 lt {2} lc rgb \'{3}\' title "{4:.3f}"\n'.format(plotCmd, tmpFileName, lt[ctr], colors[ctr], var2Val)
    elif var1 == 'sigmaA':
      gplotString = '{0:6s} "{1}" using 1:2:(1.0) smooth csplines lw 5 lt {2} lc rgb \'{3}\' title "{4:.3f}"\n'.format(plotCmd, tmpFileName, lt[ctr], colors[ctr], var2Val)
    else:
      print('not supported')
      sys.exit()
      
    print(gplotString, file = gplotScript, end = '')
    
    # Increment counter and set plotCmd to replot
    ctr += 1
    plotCmd = 'replot'

  # Pause to analyze resulting graph
  print('pause 2', file = gplotScript)

  # Define plot file
  outputFileBase = var1 +'Plot' + CtryPair
  
  # Delete old plot file
  try: 
    os.remove(outputFileBase + '.tex')
    os.remove(outputFileBase + '.eps')
  except: 
    pass
  
  outputFileDir = os.path.join(tmpPlots, outputFileBase)
  
  # Reset terminal to final plot type and replot the whole thing. 
  #print('set terminal postscript eps size 3.5,2.62 enhanced color font "Helvetica,12" linewidth 2', file = gplotScript)
  print('set terminal epslatex size 15 cm,10 cm color colortext linewidth 2', file = gplotScript)
  print('set output "{0}"'.format(outputFileDir + '.tex'), file = gplotScript)
  print('replot\n', file = gplotScript)

  outputFileDirsa = outputFileDir + 'sa'
  print('set terminal epslatex size 6.6, 5 standalone color linewidth 2 font 11', file = gplotScript)
  print('set output "{0}"'.format(outputFileDirsa + '.tex'), file = gplotScript)
  print('replot', file = gplotScript)
  
  #outputFileDirPng = outputFileDir + '.png'
  #print('set terminal pngcairo truecolor size 350,262 enhanced font \'Verdana,5\'', file = gplotScript)
  #print('set output "{0}"'.format(outputFileDirPng), file = gplotScript)
  #print('replot', file = gplotScript)
  
  #outputFileDirSvg = outputFileDir + '.svg'
  #print('set terminal svg size 350,262 fname \'Verdana\' font "arial,1"', file = gplotScript)
  #print('set output "{0}"'.format(outputFileDirSvg), file = gplotScript)
  #print('replot', file = gplotScript)
  
  outputFileDirEps = outputFileDir + 'EPS.eps'
  print('set terminal postscript eps', file = gplotScript)
  print('set output "{0}"'.format(outputFileDirEps), file = gplotScript)
  print('replot', file = gplotScript)

  # Close gplotScript file  
  gplotScript.close()
  
  # Make sure gplotscript is executable
  os.chmod(gplotScriptFileName, 0o770)
       
  # Execute the file to generate plot results
  os.system(gplotScriptFileName)
  
  # Folder to store resulting figures in:
  figureFolder = '/mnt/shareOffice/ForwardPremium/Results/figures/'
  
  #os.system('latex -output-directory ' + tmpPlots + ' ' +   outputFileDirsa + '.tex')
  #print('dvips -o ' + outputFileDirsa + '.ps ' + outputFileDirsa + '.dvi')
  #os.system('dvips -o ' + outputFileDirsa + '.ps ' + outputFileDirsa + '.dvi') 
  #shutil.copy(outputFileDirsa + '.ps', figureFolder)
  
  shutil.copy(os.path.join(tmpPlots, outputFileBase + '.tex'), figureFolder)
  shutil.copy(os.path.join(tmpPlots, outputFileBase + '.eps'), figureFolder)
  
  #shutil.copy(os.path.join(tmpPlots, outputFileDirSvg), figureFolder)
  #shutil.copy(os.path.join(tmpPlots, outputFileDirPng), figureFolder)
  shutil.copy(os.path.join(tmpPlots, outputFileDirEps), figureFolder)
  
  #for tempFile in os.listdir(tmpPlots):
    #if tempFile == os.path.basename(gplotScriptFileName): 
      #continue
    #os.remove(os.path.join(tmpPlots, tempFile))
  
  print('done')

  
if __name__ == '__main__':
  print('start') 
 # betaPlots('/mnt/shareOffice/ForwardPremium/Results/tablesAggregate/[Ctry,Ctry,sigmaA,sigmaB,EA,EB]_2011-03-01-17-38__2011-03-03-20-27', 'EA', 'sigmaA', [0.000, 0.003, 0.006])
  betaPlots('/mnt/shareOffice/ForwardPremium/Results/tablesAggregate/[Ctry,Ctry,sigmaA,sigmaB,EA,EB]_2011-03-01-17-38__2011-03-03-20-27', 'sigmaA', 'EA', [0.000, 0.900, 0.910], ['JPUS'])
  print('\ndone') 
