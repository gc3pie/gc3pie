#! /usr/bin/env python
#
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
import shutil

import logbook, sys
from supportGc3 import wrapLogger

logger = wrapLogger(loggerName = __name__ + 'logger', streamVerb = 'DEBUG', logFile = __name__ + '.log')

class idRiskApplication(Application):

    application_name = 'idrisk'

    _invalid_chars = re.compile(r'[^_a-zA-Z0-9]+', re.X)

    def __init__(self, executable, arguments, inputs, outputs, output_dir, **extra_args):
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
        if os.path.exists(simulation_out):
            self.execution.exitcode = 0
        else:
            # no `simulation.out` found, signal error
            self.execution.exitcode = 2



class idRiskApppotApplication(idRiskApplication, gc3libs.application.apppot.AppPotApplication):
    _invalid_chars = re.compile(r'[^_a-zA-Z0-9]+', re.X)

    def __init__(self, executable, arguments, inputs, outputs, output_dir, **extra_args):
        print 'extra_args = %s' % extra_args
        gc3libs.application.apppot.AppPotApplication.__init__(self, executable, arguments, inputs, outputs, output_dir, **extra_args)
