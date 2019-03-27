#! /usr/bin/env python
#
#   gprecovery.py -- Front-end script for running ParRecoveryFun Matlab
#   function with a given combination of reference models.
#
#   Copyright (C) 2014, 2015  University of Zurich. All rights reserved.
#
#   This program is free software: you can redistribute it and/or
#   modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
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

It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gprecovery.py --help`` for program usage
instructions.

Input parameters consists of:

...

Options:
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """
  2014-03-14:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gprecovery
    gprecovery.GprecoveryScript().run()

import os
import sys
import time
import tempfile

import shutil
import random

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

MODELS_SPECS = [1,2,3,4]
## custom application class
class GprecoveryApplication(Application):
    """
    Custom class to wrap the execution of the Matlab script.
    """
    application_name = 'matlab-mcr'

    def __init__(self, model_index, seed, **extra_args):

        self.output_dir = extra_args['output_dir']

        self.result_dir = extra_args['result_dir']

        inputs = dict()
        outputs = dict()
        executables = []

        if 'run_binary' in extra_args:
            inputs[os.path.abspath(extra_args['run_binary'])] = './par_recovery'
            arguments = "./par_recovery "
            executables.append('./par_recovery')
        else:
            arguments = "par_recovery "

        # self.output_filename = "ParRecovery_Genmodel%s_.mat" % str(model_index)
        # outputs[self.output_filename] = os.path.join(self.result_dir, "ParRecovery_Genmodel%s_%s.mat" % (str(model_index),extra_args['repetition']))
        self.output_filename = "ParRecovery_Genmodel%s_%s.mat" % (str(model_index),extra_args['repetition'])
        outputs["ParRecovery_Genmodel%s_.mat" % str(model_index)] = self.output_filename

        arguments += "%s %s" % (str(model_index), str(seed))

        Application.__init__(
            self,
            arguments = arguments,
            inputs = inputs,
            outputs = outputs,
            stdout = 'gprecovery.log',
            join=True,
            executables = executables,
            **extra_args)


    def terminated(self):
        """
        Move output file in 'result_dir'
        """
        if os.path.isfile(os.path.join(self.output_dir,self.output_filename)):
            shutil.move(os.path.join(self.output_dir,self.output_filename),
                             os.path.join(self.result_dir,self.output_filename))
        else:
            gc3libs.log.error("Expected output file %s not found."
                              % os.path.join(self.output_dir,self.output_filename))


class GprecoveryScript(SessionBasedScript):
    """
    Fro each network file (with '.dat' extension) found in the 'input folder',
    GprecoveryScript generates as many Tasks as 'benchmarks' defined.

    The ``gprecovery`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.

    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gprecovery`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GprecoveryApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GprecoveryApplication,
            )

    def setup_args(self):

        self.add_param('models', type=str,
                       help="list of models to process. Valid ranges are: %s."
                       " Syntax allowed: "
                       " [index|index_start:index_end|index_1,...,index_N]."
                       " Es. 1,3 | 1:4 | 3" % MODELS_SPECS)

    def setup_options(self):
        self.add_param("-b", "--binary", metavar="[STRING]",
                       dest="run_binary", default=None,
                       help="Location of the Matlab compiled binary "
                       "version of the ParRecoveryFun. Default: None.")

        self.add_param("-E", "--random_range", type=int, metavar="[int]",
                       dest="random_range", default=1000,
                       help="Upper limit for the random seed used in the "
                       "fmin function. Default: 1000.")

        self.add_param("-R", "--repeat", type=int, metavar="[int]",
                       dest="repeat", default=1,
                       help="Repeat all simulation [repeat] times. "
                       " Default: 1 (no repeat).")

        self.add_param("-S", "--store_results", type=str, metavar="[STRING]",
                       dest="store_results", default=None,
                       help="Location where all results will be aggregated. "
                       "Default: (session folder).")

    def parse_args(self):
        """
        Check validity of input parameters and selected benchmark.
        """

        self.models = []

        if self.params.run_binary:
            if not os.path.isfile(self.params.run_binary):
                raise gc3libs.exceptions.InvalidUsage("ParRecoveryFun binary "
                                                      " file %s not found"
                                                      % self.params.run_binary)
        try:
            if self.params.models.count(':') == 1:
                start, end = self.params.models.split(':')
                if (int(start) <= int(end)) and (int(start) in MODELS_SPECS) and (int(end) in MODELS_SPECS):
                    self.models = range(int(start), int(end)+1)
                else:
                    raise gc3libs.exceptions.InvalidUsage(
                        "Model not in valid range. "
                        "Range: %s" % str(MODELS_SPECS))
            elif self.params.models.count(',') >= 1:
                self.models = [ int(s) for s in self.params.models.split(',')
                                if int(s) in MODELS_SPECS ]
            else:
                if int(self.params.models) in MODELS_SPECS:
                    self.models = [ int(self.params.models) ]
                else:
                    gc3libs.log.error("Model %s not in valid range. "
                                      "Range: %s" % (self.params.models,
                                                     str(MODELS_SPECS)))

        except ValueError:
            raise gc3libs.exceptions.InvalidUsage(
                "Invalid argument '%s', use on of the following formats: "
                " INT:INT | INT,INT,INT,..,INT | INT " % (models,))


    def new_tasks(self, extra):
        """
        For each of the network data and for each of the selected benchmarks,
        create a GprecoveryApplication.

        First loop the input files, then loop the selected benchmarks
        """
        tasks = []

        for model in self.models:

            for repeat in range(1,(self.params.repeat + 1)):

                # XXX: need to find a more compact name
                # jobname = "gprecovery-%s-%s" % (model,data_index)
                jobname = "gprecovery-%d-%d" % (model,repeat)

                seed = random.randint(1,self.params.random_range)

                extra_args = extra.copy()

                if self.params.run_binary:
                    extra_args['run_binary'] = self.params.run_binary

                extra_args['jobname'] = jobname
                extra_args['repetition'] = repeat

                if self.params.store_results:
                    if not os.path.isdir(self.params.store_results):
                        os.makedirs(self.params.store_results)
                    extra_args['result_dir'] = self.params.store_results
                else:
                    extra_args['result_dir'] = self.params.output
                    extra_args['result_dir'] = extra_args['result_dir'].replace('NAME', self.params.session)

                extra_args['output_dir'] = self.params.output
                extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', '%s.%s' % (str(model),str(repeat)))
                extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', '%s.%s' % (str(model),str(repeat)))
                extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', '%s.%s' % (str(model),str(repeat)))
                extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', '%s.%s' % (str(model),str(repeat)))


                tasks.append(GprecoveryApplication(
                    model,
                    seed,
                    **extra_args))

        return tasks
