#! /usr/bin/env python

"""
ForwardPremium-specific methods and overloads.
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

# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA


from __future__ import absolute_import, print_function

import glob
import os
import re
import shutil

import gc3libs.debug
from gc3libs import Application, Run
from gc3libs.quantity import hours
from paraLoop import paraLoop
from supportGc3 import wrapLogger


__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>, Benjamin Jonen <benjamin.jonen@bf.uzh.ch>'

# summary of user-visible changes
__changelog__ = """

"""

__docformat__ = 'reStructuredText'

logger = wrapLogger(loggerName=__name__ + 'logger', streamVerb='DEBUG', logFile=__name__ + '.log')


class GPremiumApplication(Application):

    application_name = 'forwardpremium'

    _invalid_chars = re.compile(r'[^_a-zA-Z0-9]+', re.X)

    def __init__(self, executable, arguments, inputs, outputs, output_dir, **extra_args):
        Application.__init__(self, executable, arguments, inputs, outputs, output_dir, requested_walltime=1 * hours)

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

        - set the exit code based on whether there is a
          `simulation.out` file containing a value for the
          `FamaFrenchBeta` parameter;

        - parse the output file `simulation.out` (if any) and set
          attributes on this object based on the values stored there:
          e.g., if the `simulation.out` file contains the line
          ``FamaFrenchBeta: 1.234567``, then set `self.FamaFrenchBeta
          = 1.234567`.  Attribute names are gotten from the labels in
          the output file by translating any invalid character
          sequence to a single `_`; e.g. ``Avg. hB`` becomes `Avg_hB`.

        - work around a bug in ARC where the output is stored in a
          subdirectory of the output directory.
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
        # set the exitcode based on postprocessing the main output file
        simulation_out = os.path.join(output_dir, 'simulation.out')
        # -- clean the output dir from all files but simulation.out --
        # forwardPremiumOut creates .pol files which are huge. These are deleted if forwardPremiumOut runs through
        # if it doesn't however this is the dirty way of cleaning out the unnecessary output.
        # This issue should be fixed by using log4cpp for output and not couts.
        for dirFile in os.listdir(output_dir):
            if dirFile != simulation_out:
                os.remove(dirFile)
        # ------------------------------------

        self.execution.exitcode = 0 if os.path.exists(simulation_out) else 2
#            ofile = open(simulation_out, 'r')
#            # parse each line of the `simulation.out` file,
#            # and try to set an attribute with its value;
#            # ignore errors - this parser is not very sophisticated!
#            for line in ofile:
#                if ':' in line:
#                    try:
#                        var, val = line.strip().split(':', 1)
#                        value = float(val)
#                        attr = self._invalid_chars.sub('_', var)
#                        setattr(self, attr, value)
#                    except:
#                        pass
#            ofile.close()
#            if hasattr(self, 'FamaFrenchBeta'):
#                self.execution.exitcode = 0
#                self.info = ("FamaFrenchBeta: %.6f" % self.FamaFrenchBeta)
#            elif self.execution.exitcode == 0:
#                # no FamaFrenchBeta, no fun!
#                self.execution.exitcode = 1


class paraLoop_fp(paraLoop):
    '''
      Adds functionality for the forwardPremium application to the general paraLoop class by adding
      1) getCtryParas
      2) fillInputDir.
      Both functions are used to prepare the input folder sent to the grid.
    '''
    def __init__(self, verbosity):
        paraLoop.__init__(self, verbosity)

    def getCtryParas(self, baseDir, Ctry1, Ctry2):
        '''
          Obtain the right markov input files (markovMatrices.in, markovMoments.in
          and markov.out) and overwrite the existing ones in the baseDir/input folder.
        '''
        import glob
        # Find Ctry pair
        inputFolder = os.path.join(baseDir, 'input')
        markov_dir = os.path.join(baseDir, 'markov')
        CtryPresetPath = os.path.join(markov_dir, 'presets', Ctry1 + '-' + Ctry2)
        filesToCopy = glob.glob(CtryPresetPath + '/*.in')
        filesToCopy.append(os.path.join(CtryPresetPath, 'markov.out'))
        for fileToCopy in filesToCopy:
            shutil.copy(fileToCopy, inputFolder)
#        if not os.path.isdir(outputFolder):
#            os.mkdir(outputFolder)
#        shutil.copy(os.path.join(CtryPresetPath, 'markov.out'), inputFolder)

    def fillInputDir(self, baseDir, input_dir):
        '''
          Copy folder /input and all files in the base dir to input_dir.
          This is slightly more involved than before because we need to
          exclude the markov directory which contains markov information
          for all country pairs.
        '''
        inputFolder = os.path.join(baseDir, 'input')
        shutil.copytree(inputFolder, os.path.join(input_dir, 'input'))
        filesToCopy = glob.glob(baseDir + '/*')
        for fileToCopy in filesToCopy:
            if os.path.isdir(fileToCopy):
                continue
            shutil.copy(fileToCopy, input_dir)
