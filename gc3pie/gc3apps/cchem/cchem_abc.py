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
  2011-06-27:
    * Defined ABCApplication and basic SessionBasedScript
"""
__docformat__ = 'reStructuredText'


# ugly workaround for Issue 95,
# see: http://code.google.com/p/gc3pie/issues/detail?id=95
#if __name__ == "__main__":
#    import cchem_abc

import os
import os.path
import sys

import cchem_abc

## interface to Gc3libs
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, _Script

EXECUTABLE="/home/sergio/dev/comp.chem/xrls/abc.sh"
ABC_EXECUTABLE="/home/sergio/dev/comp.chem/xrls/abc.x"

APPPOT_IMAGE=""
APPPOT_RUN=""

class ABCApplication(Application):
    def __init__(self, executable, abc_executable, input_file, output_folder, **kw):

        gc3libs.Application.__init__(self,
                                     executable = os.path.basename(executable),
                                     arguments = [os.path.basename(input_file)],
                                     executables = [abc_executable],
                                     inputs = [(abc_executable, os.path.basename(abc_executable)), (input_file, os.path.basename(input_file)), (executable, os.path.basename(executable))],
                                     outputs = [],
                                     # output_dir = os.path.join(output_folder,os.path.basename(input_file)),
                                     join = True,
                                     stdout = os.path.basename(input_file)+".out",
                                     **kw
                                     )

    def terminated(self):
        pass



# & (executable = "$APPPOT_STARTUP" )
# (arguments = "--apppot" "abc.cow,abc.img")
# (jobname = "ABC_uml")
# (executables = "apppot-run")
# (gmlog=".arc")
# (join="yes")
# (stdout=".out")
# (inputFiles=("abc.img" "gsiftp://idgc3grid01.uzh.ch/local_repo/ABC/abc.img") ("nevpt2_cc-pV5Z.g3c" "./nevpt2_cc-pV5Z.g3c") ("apppot-run" "./gfit3c_abc.sh") ("dimensions" "./dimensions"))
# (outputfiles=("abc.x" "abc.nevpt2_cc-pV5Z") ("nevpt2_cc-pV5Z_log.tgz" ""))
# (runtimeenvironment = "TEST/APPPOT-0")
# (wallTime="10")
# (memory="2000")

class Gfit3C_ABC_uml_Application(Application):
    def __init__(self, abc_uml_image_file, abc_apppotrun_file, output_folder, g3c_input_file=None, dimension_file=None, surface_file=None, **kw):

        inputs = [("abc.img", abc_uml_image_file), ("apppot-run", abc_apppotrun_file)]

        if surface_file:
            inputs.append((surface_file,os.path.basename(surface_file)))
            abc_prefix = os.path.basename(surface_file)
        elif g3c_input_file and dimension_file:
            inputs.append((g3c_input_file, os.path.basename(g3c_input_file)))
            inputs.append((dimension_file,"dimensions"))
            abc_prefix = os.path.basename(g3c_input_file)
        else:
            raise gc3libs.exceptions.InvalidArgument("Missing critical argument surface file [%s], g3c_file [%s], dimension_file [%s]" % (surface_file, g3c_input_file, dimension_file))

        kw['tags'] = "TEST/APPPOT-0"

        gc3libs.Application.__init__(self,
                                     executable = "$APPPOT_STARTUP",
                                     arguments = ["--apppot", "abc.cow,abc.img"],
                                     executables = ["apppot-run"],
                                     # inputs = [(abc_apppotrun_file, "apppot-run"), (abc_uml_image_file, "abc.img"), (input_file, os.path.basename(input_file))],
                                     inputs = inputs,
                                     outputs = [ ("acb.x", "abc."+abc_prefix), os.path.basename(input_file)+"_log.tgz"],
                                     join = True,
                                     stdout = os.path.basename(input_file)+".log",
                                     **kw
                                     )

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

        gc3libs.log.info("input folder [%s] output folder [%s]" % (self.input_folder,self.output_folder))

    def new_tasks(self, extra):

         kw = extra.copy()
         name = "GC3Pie_demo"   

         inputs = SessionBasedScript._search_for_input_files(self, self.params.args)

         for path in inputs:
             name = "ABC_"+str(os.path.basename(path))

             path = os.path.abspath(path)

             gc3libs.log.info("Calling ABCWorkflow.next_tastk() for param [%s] ... " % path)

             yield (name, cchem_abc.ABCApplication, [
                     cchem_abc.EXECUTABLE,
                     cchem_abc.ABC_EXECUTABLE,
                     path,
                     self.params.output,
                     ], kw)

# run script
if __name__ == '__main__':
    ABCWorkflow().run()
