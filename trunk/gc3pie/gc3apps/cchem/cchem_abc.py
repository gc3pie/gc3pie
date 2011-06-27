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

[ Sequential -> [ Parallel -> Sequencial ] ] -> Parallel

provided 10 parameters and 400 mzXML input files

for i in range(0,9):
	Par(400)
	   +> Seq(4) -> Par(2) -> (3)
			   +-> Seq(4)
			   +-> Seq(5)

"""
__docformat__ = 'reStructuredText'


# ugly workaround for Issue 95,
# see: http://code.google.com/p/gc3pie/issues/detail?id=95
#if __name__ == "__main__":
#    import gdemo

#import abc
import os
import os.path
import sys

import cchem_abc
## interface to Gc3libs

import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, _Script
#import gc3libs.utils

EXECUTABLE="/home/sergio/dev/comp.chem/xrls/abc.sh"
ABC_EXECUTABLE="/home/sergio/dev/comp.chem/xrls/abc.x"

class ABCApplication(Application):
    def __init__(self, executable, abc_executable, input_file, output_folder, **kw):

        gc3libs.Application.__init__(self,
                                     executable = os.path.basename(executable),
                                     arguments = [os.path.basename(input_file)],
                                     executables = [abc_executable],
                                     # inputs = {abc_executable:os.path.basename(abc_executable), input_file:os.path.basename(input_file)},
                                     inputs = [(abc_executable, os.path.basename(abc_executable)), (input_file, os.path.basename(input_file)), (executable, os.path.basename(executable))],
                                     outputs = [],
                                     # output_dir = os.path.join(output_folder,os.path.basename(input_file)),
                                     join = True,
                                     stdout = os.path.basename(input_file)+".out",
                                     # set computational requirements. XXX this is mandatory, thus probably should become part of the Application's signature
                                     #requested_memory = 1,
                                     #requested_cores = 1,
                                     #requested_walltime = 1,
                                     **kw
                                     )

    def terminated(self):
        pass

class ABCWorkflow(SessionBasedScript):
    """
    gdemo
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            application = ABCApplication,
            input_filename_pattern = '*.d',
            )


    # def _setup(self):
    #     _Script.setup(self)

    #     self.add_param("-v", "--verbose", action="count", dest="verbose", default=0,
    #                    help="Be more detailed in reporting program activity."
    #                    " Repeat to increase verbosity.")

    #     self.add_param("-J", "--max-running", type=int, dest="max_running", default=50,
    #                    metavar="NUM",
    #                    help="Allow no more than NUM concurrent jobs (default: %(default)s)"
    #                    " to be in SUBMITTED or RUNNING state."
    #                    )
    #     self.add_param("-C", "--continuous", type=int, dest="wait", default=0,
    #                    metavar="INTERVAL",
    #                    help="Keep running, monitoring jobs and possibly submitting new ones or"
    #                    " fetching results every INTERVAL seconds. Exit when all jobs are finished."
    #                    )
    #     self.add_param("-w", "--wall-clock-time", dest="wctime", default=str(8), # 8 hrs
    #                    metavar="DURATION",
    #                    help="Each job will run for at most DURATION time"
    #                    " (default: %(default)s hours), after which it"
    #                    " will be killed and considered failed. DURATION can be a whole"
    #                    " number, expressing duration in hours, or a string of the form HH:MM,"
    #                    " specifying that a job can last at most HH hours and MM minutes."
    #                    )
    #     return

    def parse_args(self):
        self.input_folder = self.params.args[0]
        self.output_folder = self.params.output
    #     self.parameters = [1,2,3,4,5,6]

    #     if not os.path.isdir(self.input_folder):
    #         raise RuntimeError("Not Valid Input folder %s" % self.input_folder)

    #     self.output_folder = self.params.output
        gc3libs.log.info("input folder [%s] output folder [%s]" % (self.input_folder,self.output_folder))

    def new_tasks(self, extra):

         kw = extra.copy()
         name = "GC3Pie_demo"   

         for input_file in os.listdir(self.input_folder):
             name = "ABC_"+str(os.path.basename(input_file))

             gc3libs.log.info("Calling ABCWorkflow.next_tastk() for param [%s] ... " % os.path.abspath(os.path.join(self.input_folder,input_file)))

             yield (name, cchem_abc.ABCApplication, [
                     cchem_abc.EXECUTABLE,
                     cchem_abc.ABC_EXECUTABLE,
                     os.path.abspath(os.path.join(self.input_folder,input_file)),
                     self.output_folder,
                     ], kw)

# run script
if __name__ == '__main__':
    ABCWorkflow().run()
