#! /usr/bin/env python
#
"""
Housing-specific methods and overloads.
"""
# Copyright (C) 2011, 2012  University of Zurich. All rights reserved.
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

__author__ = 'Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'
# summary of user-visible changes
__changelog__ = """

"""
__docformat__ = 'reStructuredText'

from __future__ import absolute_import, print_function
import gc3libs.debug
import gc3libs.application.apppot
import re, os
import numpy as np
from supportGc3 import lower, flatten, str2tuple, getIndex, extractVal, str2vals
from supportGc3 import format_newVal, update_parameter_in_file, safe_eval, str2mat, mat2str, getParameter
from paraLoop import paraLoop

from gc3libs import Application, Run
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
import shutil

import logbook, sys
from supportGc3 import wrapLogger
# import personal libraries
path2SrcPy = os.path.join(os.path.dirname(__file__), '../src')
if not sys.path.count(path2SrcPy):
    sys.path.append(path2SrcPy)
from plotSimulation import plotSimulation
from plotAggregate import makeAggregatePlot
from pymods.classes.tableDict import tableDict
from plotOwnership import plotOwnerProfiles

path2Pymods = os.path.join(os.path.dirname(__file__), '../')
if not sys.path.count(path2Pymods):
    sys.path.append(path2Pymods)
from pymods.support.support import getParameter

logger = wrapLogger(loggerName = __name__ + 'logger', streamVerb = 'DEBUG', logFile = __name__ + '.log')

class housingApplication(Application):

    application_name = 'housing'

    _invalid_chars = re.compile(r'[^_a-zA-Z0-9]+', re.X)

    def __init__(self, executable, arguments, inputs, outputs, output_dir, **extra_args):
#        extra_args.setdefault('requested_walltime', 2*hours)
        Application.__init__(self, executable, arguments, inputs, outputs, output_dir, **extra_args)

    def fetch_output_error(self, ex):

        if self.execution.state == Run.State.TERMINATING:
        # do notify task/main application that we're done
        # ignore error, let's continue
            self.execution.state = Run.State.TERMINATED
            logger.debug('fetch_output_error occured... continuing')
            if self.persistent_id:
                logger.debug('jobid: %s exception: %s' % (self.persistent_id, str(ex)))
            else:
                logger.debug('info: %s exception: %s' % (self.info, str(ex)))
            return None
        else:
        # non-terminal state, pass on error
            return ex

    # def submit_error(self, ex):
    #     logger.debug('submit_error occured... continuing')

    #     try:
    #         if self.lrms_jobid:
    #             logger.debug('jobid: %s info: %s exception: %s' % (self.lrms_jobid, self.info, str(ex)))
    #         else:
    #             logger.debug('info: %s exception: %s' % (self.info, str(ex)))
    #     except AttributeError:
    #         logger.debug('no `lrms_jobid` hence submission didnt happen')
    #     return None


    def terminated(self):
        """
        Analyze the retrieved output, with a threefold purpose:

        - work around a bug in ARC where the output is stored in a
          subdirectory of the output directory.

        - make plots for post-analysis
        """
        output_dir = self.output_dir
        # if files are stored in `output/output/`, move them one level up
        if os.path.isdir(os.path.join(output_dir, 'output')):
            wrong_output_dir = os.path.join(output_dir, 'output')
            for entry in os.listdir(wrong_output_dir):
                dest_entry = os.path.join(output_dir, entry)
                if os.path.isdir(dest_entry):
                    # backup with numerical suffix
                    gc3libs.utils.backup(dest_entry)
                os.rename(os.path.join(wrong_output_dir, entry), dest_entry)
            try:
                os.rmdir(wrong_output_dir)
            except:
                logger.warning('could not delete wront output dir = %s' % wrong_output_dir)


        ## set the exitcode based on postprocessing the main output file
        #aggregateOut = os.path.join(output_dir, 'aggregate.out')
        #genParametersFile = os.path.join(os.getcwd(), 'localBaseDir', 'input', 'genParameters.in')
        #ctry =       getParameter(genParametersFile, 'ctry', 'space-separated')
        #if ctry == 'us':
            #curPanel = 'PSID'
        #elif ctry == 'de':
            #curPanel = 'SOEP'
        #elif ctry == 'uk':
            #curPanel = 'BHPS'
        #else:
            #logger.critical('unknown profile %s' % profile)
            #os.exit(1)
        #empOwnershipFile = os.path.join(os.path.split(output_dir)[0], 'input', curPanel + 'OwnershipProfilealleduc.out')
        #ownershipTableFile = os.path.join(output_dir, 'ownershipTable.out')
        #if os.path.exists(aggregateOut):
            #self.execution.exitcode = 0
            ## make plot of predicted vs empirical ownership profile
            #aggregateOutTable = tableDict.fromTextFile(aggregateOut, width = 20, prec = 10)
            #aggregateOutTable.keep(['age', 'owner'])
            #aggregateOutTable.rename('owner', 'thOwnership')
            #empOwnershipTable = tableDict.fromTextFile(empOwnershipFile, width = 20, prec = 10)
            #empOwnershipTable.rename('PrOwnership', 'empOwnership')
            #ownershipTable = aggregateOutTable.merged(empOwnershipTable, 'age')
            #ownershipTable.drop('_merge')
            #yVars = ['thOwnership', 'empOwnership']
            ## add the individual simulations
            #for profile in [ '1', '2', '3' ]:
                #profileOwnershipFile = os.path.join(output_dir, 'simulation_' + profile + '.out')
                #if not os.path.exists(profileOwnershipFile): continue
                #profileOwnershipTable = tableDict.fromTextFile(profileOwnershipFile, width = 20, prec = 10)
                #profileOwnershipTable.keep(['age', 'owner'])
                #profileOwnershipTable.rename('owner', 'thOwnership_' + profile)
                #ownershipTable.merge(profileOwnershipTable, 'age')
                #ownershipTable.drop('_merge')
                #yVars.append('thOwnership_' + profile)
            #f = open(ownershipTableFile, 'w')
            #print >> f, ownershipTable
            #f.close()
            #try:
                #plotSimulation(table = ownershipTableFile, xVar = 'age', yVars = yVars, yVarRange = (0., 1.), figureFile = os.path.join(self.output_dir, 'ownership.png'), verb = 'CRITICAL')
            #except:
                #logger.debug('couldnt make ownershipTableFile')

            #try:
                #self.execution.exitcode = plotOwnerProfiles(path2input = os.path.join(os.getcwd(), 'localBaseDir', 'input'),
                                                        #path2output = output_dir, simuFileName = 'aggregate.out')
            #except:
                #logger.debug('couldnt make ownerprofile plot. plotOwnerProfiles crashed, couldnt even set exitcode. ')
            # make plot of life-cycle simulation (all variables)
            try:
                makeAggregatePlot(self.output_dir)
#                plotSimulation(table = os.path.join(output_dir, 'aggregate.out'), xVar = 'age', yVars = ['wealth', 'theta1', 'theta2', 'theta3', 'theta4', 'theta5', 'cons', 'income'], figureFile = os.path.join(self.output_dir, 'aggregate.png'), verb = 'CRITICAL' )
            except:
                logger.debug('coulndt make aggregate.out plot')
            #if os.path.exists('ownershipThreshold_1.out'):
                #plotSimulation(path = os.path.join(output_dir, 'ownershipThreshold_1.out'), xVar = 'age', yVars = [ 'Yst1', 'Yst4' ], figureFile = os.path.join(self.output_dir, 'ownershipThreshold_1.eps'), verb = 'CRITICAL' )
                #plotSimulation(path = os.path.join(output_dir, 'ownershipThreshold_1.out'), xVar = 'age', yVars = [ 'yst1', 'yst4' ], figureFile = os.path.join(self.output_dir, 'normownershipThreshold_1.eps'), verb = 'CRITICAL' )
        else:
            # no `simulation.out` found, signal error
            self.execution.exitcode = 2

class housingApppotApplication(housingApplication, gc3libs.application.apppot.AppPotApplication):
    _invalid_chars = re.compile(r'[^_a-zA-Z0-9]+', re.X)
    def __init__(self, executable, arguments, inputs, outputs, output_dir, **extra_args):
#        extra_args.setdefault('requested_walltime', 2*hours) # unnecessary.. gc3pie automatically sets default to 8
        gc3libs.application.apppot.AppPotApplication.__init__(self, executable, arguments, inputs, outputs, output_dir, **extra_args)
