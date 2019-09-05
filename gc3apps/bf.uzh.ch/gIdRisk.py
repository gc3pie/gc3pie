#! /usr/bin/env python
#
"""
Driver script for running the `idRisk` application on SMSCG.
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
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>, Benjamin Jonen < benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """
  2011-05-16:
    * New "-X" command-line option for setting the binary architecture.
  2011-05-06:
    * Workaround for Issue 95: now we have complete interoperability
      with GC3Utils.
"""
__docformat__ = 'reStructuredText'

from __future__ import absolute_import, print_function
import shutil
import sys
import os

# Calls: -x /home/benjamin/workspace/fpProj/model/bin/forwardPremiumOut -b ../base/ para.loop  -C 1 -N -X i686
# 5-x /home/benjamin/workspace/idrisk/bin/idRiskOut -b ../base/ para.loop  -C 1 -N -X i686
# -x /home/benjamin/workspace/idrisk/model/bin/idRiskOut -b ../base/ para.loop  -C 1 -N

# Remove all files in curPath if -N option specified.
if __name__ == '__main__':

    if '-N' in sys.argv:
        path2Pymods = os.path.join(os.path.dirname(__file__), '../')
        if not sys.path.count(path2Pymods):
            sys.path.append(path2Pymods)
        curPath = os.getcwd()
        filesAndFolder = os.listdir(curPath)
        if 'gIdRisk.csv' in filesAndFolder or 'idRiskParaSearch.csv' in filesAndFolder: # if another paraSearch was run in here before, clean up.
            if 'para.loop' in os.listdir(os.getcwd()):
                shutil.copyfile(os.path.join(curPath, 'para.loop'), os.path.join('/tmp', 'para.loop'))
                shutil.rmtree(curPath)
                shutil.copyfile(os.path.join('/tmp', 'para.loop'), os.path.join(curPath, 'para.loop'))
            else:
                shutil.rmtree(curPath)


# ugly workaround for Issue 95,
# see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gIdRisk

# std module imports
import numpy as np
import os
import re
import sys
import time

from supportGc3 import lower, flatten, str2tuple, getIndex, extractVal, str2vals
from supportGc3 import format_newVal, update_parameter_in_file, safe_eval, str2mat, mat2str, getParameter
#from forwardPremium import GPremiumApplication
from idRisk import idRiskApplication
from paraLoop import paraLoop

path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
    sys.path.append(path2Pymods)

from pymods.support.support import fillInputDir
from pymods.support.support import wrapLogger


# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript
#from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

import gc3libs.debug


## custom application class

logger = wrapLogger(loggerName = 'gIdRisk.log', streamVerb = 'INFO', logFile = os.path.join(os.getcwd(), 'gParaSearch.log'))

class gIdRiskScript(SessionBasedScript, paraLoop):
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
        inputs = self._search_for_input_files(self.params.args)

        # Copy base dir
        localBaseDir = os.path.join(os.getcwd(), 'localBaseDir')
#        gc3libs.utils.copytree(self.params.initial, '/mnt/shareOffice/ForwardPremium/Results/sensitivity/wGridSize/dfs')
        shutil.copytree(self.params.initial, localBaseDir)

        for path in inputs:
            para_loop = path
            path_to_base_dir = os.path.dirname(para_loop)
#            self.log.debug("Processing loop file '%s' ...", para_loop)
            for jobname, substs in self.process_para_file(para_loop):
##                self.log.debug("Job '%s' defined by substitutions: %s.",
##                               jobname, substs)
                executable = os.path.basename(self.params.executable)
                inputs = { self.params.executable:executable }
                # make a "stage" directory where input files are collected
                path_to_stage_dir = self.make_directory_path(
                    self.params.output, jobname)
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
                        if relpath.startswith('output'):
                            continue
                        remote_path = os.path.join(relpath, filename)
                        inputs[os.path.join(dirpath, filename)] = remote_path
                # all contents of the `output` directory are to be fetched
                outputs = { 'output/':'' }
                kwargs = extra.copy()
                kwargs['stdout'] = 'forwardPremiumOut.log'
                kwargs['join'] = True
                kwargs['output_dir'] = os.path.join(path_to_stage_dir, 'output')
                kwargs['requested_architecture'] = self.params.architecture
                print 'kwargs = %s' % kwargs
                print 'inputs = %s' % inputs
                print 'outputs = %s' % outputs
                # hand over job to create
                yield (jobname, gIdRisk.idRiskApplication,
                       ['./' + executable, [], inputs, outputs], kwargs)



## run script

if __name__ == '__main__':
    logger.info('Starting: \n%s' % ' '.join(sys.argv))
    gIdRiskScript().run()
    logger.info('main done')
