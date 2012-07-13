#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011 GC3, University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
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
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
# summary of user-visible changes
__changelog__ = """
  2011-05-06:
    * Workaround for Issue 95: now we have complete interoperability
      with GC3Utils.
"""
__docformat__ = 'reStructuredText'


# ugly workaround for Issue 95,
# see: http://code.google.com/p/gc3pie/issues/detail?id=95
#if __name__ == "__main__":
#    import gdemo

#import gdemo
#import ConfigParser
#import csv
#import math
import os
import os.path
#import shutil
import sys
#from texttable import Texttable
#import types

## interface to Gc3libs

import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, _Script
from gc3libs.dag import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

class GdemoApplication(Application):

    def __init__(self, value_a, value_b, iteration, **kw):

        gc3libs.log.info("Calling GdemoApplication.__init__(%d,%d,%d) ... " % (value_a,value_b,iteration))

        self.iteration = iteration
        gc3libs.Application.__init__(self,
                                     executable = "/usr/bin/expr",
                                     arguments = [str(value_a), "+", str(value_b)],
                                     inputs = [],
                                     outputs = [],
                                     output_dir = os.path.join(os.getcwd(),"Gdemo_result",str(iteration)),
                                     stdout = "stdout.txt",
                                     stderr = "stderr.txt",
                                     # set computational requirements. XXX this is mandatory, thus probably should become part of the Application's signature
                                     requested_memory = 1,
                                     requested_cores = 1,
                                     requested_walltime = 1,
                                     *kw
                                     )

class Gdemo(SessionBasedScript):
    """
    gdemo
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.3',
            # input_filename_pattern = '*.ini',
            )


    def _setup(self):
        _Script.setup(self)

        self.add_param("-v", "--verbose", action="count", dest="verbose", default=0,
                       help="Be more detailed in reporting program activity."
                       " Repeat to increase verbosity.")

        self.add_param("-J", "--max-running", type=int, dest="max_running", default=50,
                       metavar="NUM",
                       help="Allow no more than NUM concurrent jobs (default: %(default)s)"
                       " to be in SUBMITTED or RUNNING state."
                       )
        self.add_param("-C", "--continuous", type=int, dest="wait", default=0,
                       metavar="INTERVAL",
                       help="Keep running, monitoring jobs and possibly submitting new ones or"
                       " fetching results every INTERVAL seconds. Exit when all jobs are finished."
                       )
        self.add_param("-w", "--wall-clock-time", dest="wctime", default=str(8), # 8 hrs
                       metavar="DURATION",
                       help="Each job will run for at most DURATION time"
                       " (default: %(default)s hours), after which it"
                       " will be killed and considered failed. DURATION can be a whole"
                       " number, expressing duration in hours, or a string of the form HH:MM,"
                       " specifying that a job can last at most HH hours and MM minutes."
                       )
        return

    def parse_args(self):
        self.init_value = 1
        self.add_value = 1

    def new_tasks(self, extra):

        kw = extra.copy()
        name = "GC3Pie_demo"

        gc3libs.log.info("Calling Gdemo.next_tastk() ... ")

        yield (name, DemoIteration, [
                self.init_value,
                self.add_value
                ], kw)



## support classes

# For each input file, a new computation is started, totally
# `self.params.iterations` passes, each pass corresponding to
# an application of the `self.params.executable` function.
#
# This is the crucial point: 

class DemoIteration(SequentialTaskCollection):
    """
    Perform a (predefined) number of iterations of a certain function
    over a set of values.  The function to be iterated is implemented
    in the form of an executable program, that takes a single input
    file IN_VALUES_FILE (a list of values) and creates a single output
    file OUT_VALUES_FILE.

    The several-passes computation is implemented as a sequential task
    collection (totalling `total_iterations` steps); each step of
    which is a parallel collection of tasks, each of which is a
    single-core task executing the given program.
    """
    
    def __init__(self, init_value, add_value, grid=None, **kw):
        """
        Create a new tasks that runs `executable` over a set of values
        (initially given by `initial_values_file`, then the output of
        a run is fed as input to the next one), riterating the process
        `total_iterations` times.

        If `slice_size` is a positive integer, then chop the input into
        chunks of -at most- the given size and compute them as separate
        independent jobs.

        Extra keyword arguments are saved and passed down to construct
        the `ValueFunctionIterationApplication`.
        """

        self.init = init_value
        self.increment = add_value
        self.limit = 10
        self.jobname = "Gdemo_Iternation"

        gc3libs.log.info("Calling DemoIteration.__init__() ... ")

        # create initial task and register it
        initial_task = GdemoApplication(self.init, self.increment, 0)
        SequentialTaskCollection.__init__(self, self.jobname, [initial_task], grid)


    def __str__(self):
        return self.jobname


    def next(self, iteration):
        """
        If there are more iterations to go, enqueue the corresponding jobs.

        See: `SequentialTaskCollection.next`:meth: for a description
        of the contract that this method must implement.
        """

        gc3libs.log.info("Calling GdemoIteration.next(%d) ... " % int(iteration))

        last_application = self.tasks[iteration]

        f = open(os.path.join(last_application.output_dir,last_application.stdout))
        computed_value = int(f.read())
        f.close()

        if computed_value == self.limit:
            self.returncode = 0
            return Run.State.TERMINATED
        else:
            self.add(GdemoApplication(computed_value, self.increment, iteration+1))
            return Run.State.RUNNING

    def terminated(self):
        """
        Collect the output of all iterations into a single '.csv'
        file.
        """
        gc3libs.log.debug("  ...done.")


# run script
if __name__ == '__main__':
    import gdemo_session
    gdemo_session.Gdemo().run()
