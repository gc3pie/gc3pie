#!/usr/bin/env python3

"""
Combine different overviewTables created by createOverviewTable to make one big table. 
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


import os, re
from pyMethods.classes.tableDict import tableDict

def combineOverviews(runDir = None, ansList = None):

  overviewSimuFile = 'overviewSimu'
  completedLoops = []
  if runDir == None:
    fpResultsFolder = '/mnt/shareOffice/ForwardPremium/Results/runs/'
    tablesAggregateFolder = '/mnt/shareOffice/ForwardPremium/Results/tablesAggregate'
    completedLoops = os.listdir(fpResultsFolder)
    completedLoops.sort()
    if ansList == None:
      for ixLoop, loopFolder in enumerate(completedLoops):
        print(str(ixLoop) + ' --> ' + loopFolder)
      ans = input("Plz enter comma separated list for loops to combine ovTables for? Or quit (q)? ")
      if ans == 'q': sys.exit()
      ansList = [ int(ele) for ele in ans.split(',') ]
      for ele in ansList:
        if not ele in list(range(0, len(completedLoops))): 
          print('entry makes no sense, aboritng...')
          sys.exit()
  selectedLoops = [ completedLoops[ele] for ele in ansList ]
  tableList = [ os.path.join(loop, overviewSimuFile) for loop in selectedLoops ]
  print(tableList)
  tableDicts = [ tableDict.fromTextFile(os.path.join(fpResultsFolder,  table), 20, 5) for table in tableList ]
  fullTable = tableDicts[0]
  for ixTable, table in enumerate(tableDicts):
    if ixTable == 0: continue
    fullTable = fullTable.getAppended(table)
  fullTable.sort(['CtryPair'])
  print(fullTable)

  outName = ''
  loopOver = re.match('.*(\[.*\]).*',selectedLoops[0]).groups()
  outName += loopOver[0]
  outName += '_'
  for ixLoop, loop in enumerate(selectedLoops):
    date = re.match('([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2})',loop).groups()
    if ixLoop > 0: outName += '__'
    outName += date[0]

    
  outFile = open(os.path.join(tablesAggregateFolder, outName), 'w')
  print(fullTable, file = outFile)

  print('Combined table written to: ', outName)  
  
if __name__ == '__main__':
  print('start') 
  combineOverviews(runDir = None, ansList = [6,8])
  print('done')