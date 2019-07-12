#! /usr/bin/env python
#
"""
Driver script for running the `forwardPremium` application on SMSCG.
"""
# Copyright (C) 2011  University of Zurich. All rights reserved.
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
__author__ = 'Benjamin Jonen <benjamin.jonen@bf.uzh.ch'
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'

from __future__ import absolute_import, print_function
import os, sys
import numpy as np

# import personal libraries
path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
    sys.path.append(path2Pymods)
path2Src = os.path.join(os.path.dirname(__file__), '../src')
if not sys.path.count(path2Src):
    sys.path.append(path2Src)

# Remove all files in curPath if -N option specified. 
if __name__ == '__main__':    
    import sys
    if '-N' in sys.argv:
        import os, shutil
        path2Pymods = os.path.join(os.path.dirname(__file__), '../')
        if not sys.path.count(path2Pymods):
            sys.path.append(path2Pymods)
        curPath = os.getcwd()
        filesAndFolder = os.listdir(curPath)
        if 'taskColAppPot.csv' in filesAndFolder: # if another paraSearch was run in here before, clean up. 
            if 'para.loop' in os.listdir(os.getcwd()):
                shutil.copyfile(os.path.join(curPath, 'para.loop'), os.path.join('/tmp', 'para.loop'))
                shutil.rmtree(curPath)
                shutil.copyfile(os.path.join('/tmp', 'para.loop'), os.path.join(curPath, 'para.loop'))
                os.remove(os.path.join('/tmp', 'para.loop'))
            else:
                shutil.rmtree(curPath)

if __name__ == "__main__":
    import taskColAppPot


# superclasses
from idRisk import idRiskApplication, idRiskApppotApplication
from paraLoop import paraLoop


# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, existing_file
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

import gc3libs.application.apppot

import gc3libs.debug

# logger
from pymods.support.support import fillInputDir
from pymods.support.support import wrapLogger
import pymods.support.support as support
from pymods.classes.tableDict import tableDict

# Set up logger
logger = wrapLogger(loggerName = 'idRiskParaSearchLogger', streamVerb = 'DEBUG', logFile = os.path.join(os.getcwd(), 'idRiskParaSearch.log'))

# call -x /home/benjamin/workspace/idrisk/model/bin/idRiskOut -b /home/benjamin/workspace/idrisk/model/base para.loop -C 10 -N -A '/home/benjamin/apppot0+ben.diskUpd.img'

class solveParaCombination(SequentialTaskCollection):

    def __init__(self, substs, solverParas, **sessionParas):

        logger.debug('entering solveParaCombination.__init__ for job %s' % sessionParas['jobname'])
        self.iter    = 0

        self.jobname = 'solverParacombination' + sessionParas['jobname']
        self.substs = substs

        self.sessionParas     = sessionParas
        self.pathToExecutable = sessionParas['pathToExecutable']
        self.architecture     = sessionParas['architecture']
        self.localBaseDir     = sessionParas['localBaseDir']
        
        self.paraFolder = os.path.join(os.getcwd(), sessionParas['jobname'])
        
        self.wBarLower_task = idRiskParaSearchDriver(self.paraFolder, self.substs, solverParas, **sessionParas)

        SequentialTaskCollection.__init__(self, self.jobname, [self.wBarLower_task])

        logger.debug('done gParaSearchDriver.__init__ for job %s' % sessionParas['jobname'])

    def __str__(self):
        return self.jobname

    def next(self, *args): 
        self.iter += 1
        if self.wBarLower_task.execution.returncode == 13:
            logger.critical('wBarLower failed. terminating para combo')
            self.execution.returncode = 13
            return Run.State.TERMINATED
        elif self.wBarLower_task.execution.returncode == 0:
            #wBarTable = tableDict.fromTextFile(os.path.join(self.paraFolder, 'optimwBarLower', 'overviewSimu'), width = 20, prec = 10)
            #optimalRunTable = wBarTable.getSubset( np.abs(wBarTable['wBarLower'] - self.wBarLower_task.costlyOptimizer.best_x) < 1.e-7 )
            #optimalRunFile = open(os.path.join(self.paraFolder, 'optimalRun'), 'w')
            #print >> optimalRunFile, optimalRunTable
            self.execution.returncode = 0
            return Run.State.TERMINATED
        else:
            print 'unknown return code'
            os._exit

class idRiskParaSearchDriver(SequentialTaskCollection):

    def __init__(self, paraFolder, substs, solverParas, **sessionParas):

        logger.debug('entering solveParaCombination.__init__ for job %s' % sessionParas['jobname'])
        self.iter    = 0

        self.jobname = 'idRiskParaSearchDriver' + sessionParas['jobname']
        self.substs = substs

        self.sessionParas     = sessionParas
        self.pathToExecutable = sessionParas['pathToExecutable']
        self.architecture     = sessionParas['architecture']
        self.localBaseDir     = sessionParas['localBaseDir']
        
        self.paraFolder = os.path.join(os.getcwd(), sessionParas['jobname'])
        # setup AppPot parameters
        use_apppot = False
        apppot_img = None  
        apppot_changes = None
        apppot_file = sessionParas['AppPotFile']
        if apppot_file:
            use_apppot = True
            if apppot_file.endswith('.changes.tar.gz'):
                apppot_changes = apppot_file
            else:
                apppot_img = apppot_file
        pathToExecutable = sessionParas['pathToExecutable']
        localBaseDir     = sessionParas['localBaseDir']
        architecture     = sessionParas['architecture']
        jobname          = sessionParas['jobname']
        executable = os.path.basename(self.pathToExecutable)
        inputs = { self.pathToExecutable:executable }
        # make a "stage" directory where input files are collected
        path_to_stage_dir = os.path.join(self.paraFolder, jobname)
        gc3libs.utils.mkdir(path_to_stage_dir)
        prefix_len = len(path_to_stage_dir) + 1
        # 2. apply substitutions to parameter files
        for (path, changes) in substs.iteritems():
            for (var, val, index, regex) in changes:
                support.update_parameter_in_file(os.path.join(localBaseDir, path),
                                         var, index, val, regex)
        support.fillInputDir(localBaseDir, path_to_stage_dir)
        # 3. build input file list
        for dirpath,dirnames,filenames in os.walk(path_to_stage_dir):
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
        kwargs = {}
        kwargs['stdout'] = 'idRisk.log'
        kwargs['join'] = True
        kwargs['output_dir'] = os.path.join(path_to_stage_dir, 'output')
        kwargs['requested_architecture'] = self.architecture
        
        # adaptions for uml
        if use_apppot:
            if apppot_img is not None:
                kwargs['apppot_img'] = apppot_img
            if apppot_changes is not None:
                kwargs['apppot_changes'] = apppot_changes
            cls = idRiskApppotApplication
        else:
            cls = idRiskApplication 
        kwargs.setdefault('tags', [ ])

        # hand over job to create
#        self.curApplication = cls('/home/user/job/' + executable, [], inputs, outputs, **kwargs)
        self.curApplication = cls('./' + executable, [], inputs, outputs, **kwargs)

        SequentialTaskCollection.__init__(self, self.jobname, [ self.curApplication ])

        logger.debug('done gParaSearchDriver.__init__ for job %s' % sessionParas['jobname'])

    def __str__(self):
        return self.jobname

    def next(self, *args): 
        self.iter += 1
        if self.curApplication.execution.returncode == 13:
            logger.critical('wBarLower failed. terminating para combo')
            self.execution.returncode = 13
            return Run.State.TERMINATED
        elif self.curApplication.execution.returncode == 0:            
            self.execution.returncode = 0
            return Run.State.TERMINATED
        else:
            print 'unknown return code'
            os._exit


class taskColAppPotScript(SessionBasedScript, paraLoop):
    """
      Read `.loop` files and execute the `forwardPremium` program accordingly.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.2',
            # only '.loop' files are considered as valid input
            input_filename_pattern = '*.loop',
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
                       help="Path to the `forwardPremium` executable binary"
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
        self.add_param("-mP", "--makePlots", metavar="ARCH", type = bool, 
                       dest="makePlots", default = True,
                       help="Generate population plots each iteration.  ")
        self.add_param("-xVars", "--xVars", metavar="ARCH",
                       dest="xVars", default = 'EA',
                       help="x variables over which to optimize")
        self.add_param("-xVarsDom", "--xVarsDom", metavar="ARCH",
                       dest="xVarsDom", default = '0.5 0.9',
                       help="Domain to sample x values from. Space separated list. ")
        self.add_param("-targetVars", "--target_fx", metavar="ARCH",
                       dest="targetVars", default = '0.1',
                       help="Domain to sample x values from. Space separated list. ")
        self.add_param("-target_fx", "--target_fx", metavar="ARCH",
                       dest="target_fx", default = '0.1',
                       help="Domain to sample x values from. Space separated list. ")
        self.add_param("-sv", "--solverVerb", metavar="ARCH",
                       dest="solverVerb", default = 'DEBUG',
                       help="Separate verbosity level for the global optimizer ")
        self.add_param("-yC", "--yConvCrit", metavar="ARCH", type = float, 
                       dest="convCrit", default = '1.e-2',
                       help="Convergence criteria for y variables. ")

    def parse_args(self):
        """
        Check validity and consistency of command-line options.
        """
        if not os.path.exists(self.params.executable):
            raise gc3libs.exceptions.InvalidUsage(
                "Path '%s' to the 'forwardPremium' executable does not exist;"
                " use the '-x' option to specify a valid one."
                % self.params.executable)
        if os.path.isdir(self.params.executable):
            self.params.executable = os.path.join(self.params.executable,
                                                  'forwardPremium')
        gc3libs.utils.check_file_access(self.params.executable, os.R_OK|os.X_OK,
                                gc3libs.exceptions.InvalidUsage)



    def new_tasks(self, extra):     

        paraLoopFile = self._search_for_input_files(self.params.args).pop()  # search_... returns set.

        # Copy base dir
        localBaseDir = os.path.join(os.getcwd(), 'localBaseDir')
        shutil.copytree(self.params.initial, localBaseDir)

        for jobname, substs in self.process_para_file(paraLoopFile):
            # yield job
            sessionParas = {}
            sessionParas['AppPotFile'] = self.params.apppot
            sessionParas['pathToExecutable'] = self.params.executable
            sessionParas['architecture'] = self.params.architecture
            sessionParas['localBaseDir'] = localBaseDir
            sessionParas['jobname'] = jobname
            # Compute domain
            xVarsDom = self.params.xVarsDom.split()
            lowerBds = np.array([xVarsDom[i] for i in range(len(xVarsDom)) if i % 2 == 0], dtype = 'float64')
            upperBds = np.array([xVarsDom[i] for i in range(len(xVarsDom)) if i % 2 == 1], dtype = 'float64')
            domain = zip(lowerBds, upperBds)
            solverParas = {}
            solverParas['xVars'] = self.params.xVars.split()
            solverParas['xInitialParaCombo'] = np.array([lowerBds, upperBds])
            solverParas['targetVars'] = self.params.xVars.split()
            solverParas['target_fx'] = map(float, self.params.target_fx.split())
            solverParas['plotting'] = False
            solverParas['convCrit'] = self.params.convCrit
            yield (jobname, solveParaCombination, [ substs, solverParas ], sessionParas)
            
            
if __name__ == '__main__':
    logger.info('Starting: \n%s' % ' '.join(sys.argv))
    taskColAppPotScript().run()
    logger.info('main done')
logger.info('done')