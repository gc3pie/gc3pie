#! /usr/bin/env python
#
"""
Driver script for running the `housing` application on SMSCG.
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
# GNU General Public License for more details.sjp
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__version__ = '$Revision$'
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>, Benjamin Jonen < benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """
"""
__docformat__ = 'reStructuredText'

import shutil


# Calls: 
# -x /home/benjamin/workspace/fpProj/model/bin/forwardPremiumOut -b ../base/ para.loop  -C 1 -N -X i686
# 5-x /home/benjamin/workspace/idrisk/bin/idRiskOut -b ../base/ para.loop  -C 1 -N -X i686
# -x /home/benjamin/workspace/idrisk/model/bin/idRiskOut -b ../base/ para.loop  -C 1 -N
# Need to set path to linux kernel and apppot, e.g.: export PATH=$PATH:~/workspace/apppot:~/workspace/

# Remove all files in curPath if -N option specified. 
if __name__ == '__main__':    
    import sys
    if '-N' in sys.argv:
        import os, shutil
        path2Pymods = os.path.join(os.path.dirname(__file__), '../')
        if not sys.path.count(path2Pymods):
            sys.path.append(path2Pymods)
        from pymods.support.support import rmFilesAndFolders
        curPath = os.getcwd()
        filesAndFolder = os.listdir(curPath)
        if 'ghousing.csv' in filesAndFolder:
            if 'para.loop' in os.listdir(os.getcwd()):
                shutil.copyfile(os.path.join(curPath, 'para.loop'), os.path.join('/tmp', 'para.loop'))
                rmFilesAndFolders(curPath)
                shutil.copyfile(os.path.join('/tmp', 'para.loop'), os.path.join(curPath, 'para.loop'))
            else: 
                rmFilesAndFolders(curPath)


# ugly workaround for Issue 95,
# see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import ghousing

# std module imports
import numpy as np
import os
import re
import sys
import time

from supportGc3 import lower, flatten, str2tuple, getIndex, extractVal, str2vals
from supportGc3 import format_newVal, update_parameter_in_file, safe_eval, str2mat, mat2str, getParameter
from housing import housingApplication, housingApppotApplication
from paraLoop import paraLoop

path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
    sys.path.append(path2Pymods)

from pymods.support.support import wrapLogger
from pymods.classes.tableDict import tableDict

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, existing_file
import gc3libs.utils
import gc3libs.application.apppot

import gc3libs.debug

# import personal libraries
path2SrcPy = os.path.join(os.path.dirname(__file__), '../srcPy')
if not sys.path.count(path2SrcPy):
    sys.path.append(path2SrcPy)
from plotSimulation import plotSimulation


## custom application class

logger = wrapLogger(loggerName = 'ghousing.log', streamVerb = 'INFO', logFile = os.path.join(os.getcwd(), 'ghousing.log'))

class ghousing(SessionBasedScript, paraLoop):
    """
Read `.loop` files and execute the `housingOut` program accordingly.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.2',
            # only '.loop' files are considered as valid input
            input_filename_pattern = '*.loop',
            stats_only_for = Application,
        )
        paraLoop.__init__(self, 'INFO')

    def setup_options(self):
        self.add_param("-b", "--initial", metavar="DIR",
                       dest="initial",
                       help="Include directory contents in any job's input."
                       " Use this to specify the initial guess files.")
        self.add_param("-n", "--dry-run",
                       dest = 'dryrun', action="store_true", default = False,
                       help="Take the loop for a test run")
        self.add_param("-x", "--executable", metavar="PATH",
                       dest="executable", default=os.path.join(
                           os.getcwd(), "forwardPremiumOut"),
                       help="Path to the `idRisk` executable binary"
                       "(Default: %(default)s)")
        self.add_param("-X", "--architecture", metavar="ARCH",
                       dest="architecture", default=Run.Arch.X86_64,
                       help="Processor architecture required by the executable"
                       " (one of: 'i686' or 'x86_64', without quotes)")
        self.add_param("-A", "--apppot", metavar="PATH",
                       dest="apppot",
                       type=existing_file, default=None,
                       help="Use an AppPot image to run idRisk."
                       " PATH can point either to a complete AppPot system image"
                       " file, or to a `.changes` file generated with the"
                       " `apppot-snap` utility.")


    def parse_args(self):
        """
        Check validity and consistency of command-line options.
        """
        if not os.path.exists(self.params.executable):
            raise gc3libs.exceptions.InvalidUsage(
                "Path '%s' to the 'housingOut' executable does not exist;"
                " use the '-x' option to specify a valid one."
                % self.params.executable)
        if os.path.isdir(self.params.executable):
            self.params.executable = os.path.join(self.params.executable,
                                                  'housing')
        gc3libs.utils.test_file(self.params.executable, os.R_OK|os.X_OK,
                                gc3libs.exceptions.InvalidUsage)



    def new_tasks(self, extra):
        # setup AppPot parameters
        use_apppot = False
        apppot_img = None
        apppot_changes = None
        if self.params.apppot:
            use_apppot = True
            if self.params.apppot.endswith('.changes.tar.gz'):
                apppot_changes = self.params.apppot
            else:
                apppot_img = self.params.apppot

        inputs = self._search_for_input_files(self.params.args)
        
        # Copy base dir
        localBaseDir = os.path.join(os.getcwd(), 'localBaseDir')
        gc3libs.utils.copytree(self.params.initial, localBaseDir)

        for para_loop in inputs:
            path_to_base_dir = os.path.dirname(para_loop)
#            self.log.debug("Processing loop file '%s' ...", para_loop)
            for jobname, substs in self.process_para_file(para_loop):
##                self.log.debug("Job '%s' defined by substitutions: %s.",
##                               jobname, substs)
                executable = os.path.basename(self.params.executable)
                inputs = { self.params.executable:executable }
                # make a "stage" directory where input files are collected
                path_to_stage_dir = self.make_directory_path(self.params.output, jobname, path_to_base_dir)
                input_dir = path_to_stage_dir #os.path.join(path_to_stage_dir, 'input')
                gc3libs.utils.mkdir(input_dir)
                prefix_len = len(input_dir) + 1
                # 2. apply substitutions to parameter files
                for (path, changes) in substs.iteritems():
                    for (var, val, index, regex) in changes:
                        update_parameter_in_file(os.path.join(localBaseDir, path),
                                                 var, index, val, regex)
                fillInputDir(localBaseDir, input_dir)
                # 3. build input file list
                for dirpath,dirnames,filenames in os.walk(input_dir):
                    for filename in filenames:
                        # cut the leading part, which is == to path_to_stage_dir
                        relpath = dirpath[prefix_len:]
                        # ignore output directory contents in resubmission
                        if relpath. startswith('output'):
                            continue
                        remote_path = os.path.join(relpath, filename)
                        inputs[os.path.join(dirpath, filename)] = remote_path
                # all contents of the `output` directory are to be fetched
                outputs = { 'output/':'' }
                kwargs = extra.copy()
                kwargs['stdout'] = os.path.join('housingStdOut.log')
                kwargs['join'] = True
                kwargs['output_dir'] = os.path.join(path_to_stage_dir, 'output')
                kwargs['requested_architecture'] = self.params.architecture
                print 'kwargs = %s' % kwargs
                print 'inputs = %s' % inputs
                print 'outputs = %s' % outputs

                kwargs.setdefault('tags', [ ])	

                # adaptions for uml
                if use_apppot:
                    if apppot_img is not None:
                        kwargs['apppot_img'] = apppot_img
                    if apppot_changes is not None:
                        kwargs['apppot_changes'] = apppot_changes
                    cls = housingApppotApplication
                    pathToExecutable = '/home/user/job/' + executable
                else:
                    cls = housingApplication
                    pathToExecutable = executable

                # hand over job to create
                yield (jobname, cls, [pathToExecutable, [], inputs, outputs], kwargs)                
                
def fillInputDir(baseDir, input_dir):
    '''
      Copy folder /input and all files in the base dir to input_dir. 
      This is slightly more involved than before because we need to 
      exclude the markov directory which contains markov information
      for all country pairs. 
    '''
    gc3libs.utils.copytree(baseDir , input_dir)
    
def combinedThresholdPlot():
    import copy
    folders = [folder for folder in os.listdir(os.getcwd()) if os.path.isdir(folder) and not folder == 'localBaseDir' and not folder == 'ghousing.jobs']
    tableList = [ (folder, os.path.join(os.getcwd(), folder, 'output', 'ownershipThreshold_1.out')) for folder in folders ]
    tableDicts = dict([ (folder, tableDict.fromTextFile(table, width = np.max([len(folder) for folder in folders]) + 5, prec = 10)) for folder, table in tableList if os.path.isfile(table)])
    tableKeys = tableDicts.keys()
    tableKeys.sort()
    if tableDicts:
        for ixTable, tableKey in enumerate(tableKeys):
#            print tableKey
#            print tableDicts[tableKey]
            table = copy.deepcopy(tableDicts[tableKey])
            table.keep(['age', 'yst1'])
            table.rename('yst1', tableKey)
            if ixTable == 0: 
                fullTable = copy.deepcopy(table)
            else: 
                fullTable.merge(table, 'age')
                if '_merge' in fullTable.cols:
                    fullTable.drop('_merge')
                logger.info(fullTable)
        logger.info(fullTable)
        f = open(os.path.join(os.getcwd(), 'ownerThresholds'), 'w')  
        print >> f, fullTable
        f.flush() 
        plotSimulation(path = os.path.join(os.getcwd(), 'ownerThresholds'), xVar = 'age', yVars = list(fullTable.cols), figureFile = os.path.join(os.getcwd(), 'ownerThresholds.eps'), verb = 'CRITICAL' )

def combinedOwnerSimuPlot():
    import copy
    logger.debug('starting combinedOwnerSimuPlot')
    folders = [folder for folder in os.listdir(os.getcwd()) if os.path.isdir(folder) and not folder == 'localBaseDir' and not folder == 'ghousing.jobs']
    logger.debug('folders are %s ' % folders)
    tableList = [ (folder, os.path.join(os.getcwd(), folder, 'output', 'aggregate.out')) for folder in folders ]
    tableDicts = dict([ (folder, tableDict.fromTextFile(table, width = np.max([len(folder) for folder in folders]) + 5, prec = 10)) for folder, table in tableList if os.path.isfile(table)])
    tableKeys = tableDicts.keys()
    tableKeys.sort()
    if tableDicts:
        for ixTable, tableKey in enumerate(tableKeys):
            table = copy.deepcopy(tableDicts[tableKey])
            table.keep(['age', 'owner'])
            table.rename('owner', tableKey)
            if ixTable == 0: 
                fullTable = copy.deepcopy(table)
            else: 
                fullTable.merge(table, 'age')
                fullTable.drop('_merge')
            logger.info(fullTable)

        empOwnershipFile = os.path.join(os.getcwd(), 'localBaseDir', 'input', 'PSIDOwnershipProfilealleduc.out')
        empOwnershipTable = tableDict.fromTextFile(empOwnershipFile, width = 20, prec = 10)
        empOwnershipTable.rename('PrOwnership', 'empOwnership') 
        fullTable.merge(empOwnershipTable, 'age')
        fullTable.drop('_merge')
        if fullTable:
            logger.info(fullTable)
            f = open(os.path.join(os.getcwd(), 'ownerSimu'), 'w')  
            print >> f, fullTable
            f.flush()
        else: 
            logger.info('no owner simus')
        logger.debug('done combinedOwnerSimuPlot')
        
        plotSimulation(path = os.path.join(os.getcwd(), 'ownerSimu'), xVar = 'age', yVars = list(fullTable.cols), yVarRange = (0., 1.), figureFile = os.path.join(os.getcwd(), 'ownerSimu.eps'), verb = 'CRITICAL' )

def combineRunningTimes():
    folders = [folder for folder in os.listdir(os.getcwd()) if os.path.isdir(folder) and not folder == 'localBaseDir' and not folder == 'ghousing.jobs']
    runTimeFileList = [ (folder, os.path.join(os.getcwd(), folder, 'output', 'runningTime.out')) for folder in folders ]
    print runTimeFileList
    runTimes = {} # in minutes
    for folder, fle in runTimeFileList:
        if not os.path.isfile(fle): continue
        runningTimeFile = open(fle)
        lines = runningTimeFile.readlines()
        for line in lines:
            if line.find('Full running'):
                match = re.match('(.*=)([0-9\.\s]*)(.*)', line.rstrip()).groups()
                if not match: 
                    runTimeSec = 0.
                else:
                    runTimeSec = float(match [1].strip()) # in seconds
        runTimes[folder] = runTimeSec / 60.
    logger.info('running times are \n %s' % runTimes)
    f = open(os.path.join(os.getcwd(), 'runTimes.out'), 'w')
    folderKeys = runTimes.keys()
    folderKeys.sort()
    for key in folderKeys:
        print >> f, '%s = %f12.1' % (key, runTimes[key])
    f.flush()
    f.close()


## run scriptfg


if __name__ == '__main__':
    logger.info('Starting: \n%s' % ' '.join(sys.argv))
    ghousing().run()
    # create overview plots across parameter combinations
    try:
        combinedThresholdPlot()
    except:
        logger.critical('problem creating combinedThresholdPlot. Investigate...')
    try: 
        combinedOwnerSimuPlot()
    except:
        logger.critical('problem creating combinedOwnerSimuPlot. Investigate...')
    try: 
        combineRunningTimes()
    except:
        logger.critical('problem creating combineRunningTimes. Investigate...')
    logger.info('main done')

