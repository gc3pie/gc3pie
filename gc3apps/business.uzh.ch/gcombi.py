#! /usr/bin/env python
#
#   gcombi.py -- Front-end script for evaluating Matlab functions
#   function over a large number of parameters.
#
#   Copyright (C) 2015, 2016  University of Zurich. All rights reserved.
#
#   This program is free software: you can redistribute it and/or
#   modify it under the terms of the GNU General Public License as
#   published by the Free Software Foundation, either version 3 of
#   the License, or (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
"""
Front-end script for submitting multiple Matlab jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gcombi.py --help`` for program usage instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2016-04-20:
  * Initial version
"""
__author__ = 'Tyanko Aleksiev <tyanko.aleksiev@uzh.ch>'
__docformat__ = 'reStructuredText'


if __name__ == "__main__":
    import gcombi
    gcombi.GCombiScript().run()

import os
import sys
import time
import tempfile

import shutil

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

## custom application class
class GCombiApplication(Application):
    """
    Custom class to for the execution of three matlab models used by Gaia Lombardi at from
    the Department of Economics: project fehr.econ.uzh.
    Each model takes as input two parameters: 'phenotype' and 'chromosome' and this is the work-flow:

    model1 phenotype chromosome -> outout1
    model2 phenotype output1 -> output2
    model3 phenotype output2 -> output3.

    The current implementation of the three models requires a submission like this:

    model1 phenotype chromosome
    model2 phenotype chromosome
    model3 phenotype chromosome

    The project is one shot and has to execute 96 phenotypes each against 22 chromosoms for a total of
    ~21000 cpu hours. More than 4GB are needed for single execution so at least a 2cpu-8ram-hpc flavor
    has to be used.
    """
    application_name = 'gcombi'

    def __init__(self, input_phenotype, input_chromosom, **extra_args):

        inputs = dict()
        outputs = dict()

        output_dir = "./results"
        outputs[output_dir] = output_dir

        # execution wrapper needs to be added anyway
        gcombi_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gcombi.sh")
        inputs[gcombi_wrapper_sh] = os.path.basename(gcombi_wrapper_sh)
        inputs[input_phenotype] = os.path.basename(input_phenotype)
        inputs[input_chromosom] = os.path.basename(input_chromosom)

        command = "./%s ./%s ./%s" % (os.path.basename(gcombi_wrapper_sh), os.path.basename(input_phenotype), os.path.basename(input_chromosom))

        Application.__init__(
            self,
            arguments = command,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gcombi.log',
            join=True,
            executables = "./%s" % os.path.basename(gcombi_wrapper_sh),
            **extra_args)


class GCombiScript(SessionBasedScript):
    """
    The ``gcombi`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gcombi`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gcombi``
    aggregates them into a single larger output file located in
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            application = GCombiApplication,
            stats_only_for = GCombiApplication,
            )

    def setup_args(self):
        self.add_param('phenotypes_dir', type=str, help="Specify the directory containing the phenotypes")
        self.add_param('chromosomes_dir', type=str, help="Specify the directory containing the cromosomes for that phenotype")

    def parse_args(self):
        """
        Check presence of input folder (should contains matlab scripts).
        path to command_file should also be valid.
        """

    def new_tasks(self, extra):
        """
        """
        tasks = []
        allchromosoms = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22']

        for phenotype in os.listdir(self.params.phenotypes_dir):
            jobname = phenotype.split(".")[0]
            phenotype_input_file = self.params.phenotypes_dir + os.path.basename(phenotype)

            for chromosom in os.listdir(self.params.chromosomes_dir):
                chromosom_input_file = self.params.chromosomes_dir + os.path.basename(chromosom)
                jobname = "gcombi-%s-%s" % (os.path.basename(phenotype).split(".")[0], os.path.basename(chromosom).split(".")[0])

                extra_args = extra.copy()

                extra_args['jobname'] = jobname

                extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', jobname)
                extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', jobname)

                self.log.debug("Creating Application for parameters : %s %s" %
                               (phenotype, chromosom))

                tasks.append(GCombiApplication(
                    phenotype_input_file,
                    chromosom_input_file,
                    **extra_args))

        return tasks
