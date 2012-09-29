#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2011, 2012 GC3, University of Zurich. All rights reserved.
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
import tarfile

## interface to Gc3libs

import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, _Script
from gc3libs.workflow import SequentialTaskCollection, ParallelTaskCollection
import gc3libs.utils

class XtandemPostApplication(Application):
    def __init__(self, param_value, output_folder, iteration, **extra_args):

        gc3libs.log.info("\t\t\t\t\t\tCalling XtandemPostApplication.__init__(%d,%d) ... " % (param_value,iteration))

        arguments = ["/bin/echo", "POST-Parameter: ",str(param_value),  " POST-Iteration: ", str(iteration)]

        tarfile_name = os.path.join(output_folder,str(param_value),str(iteration))+"POST.tgz"

        tar = tarfile.open(tarfile_name, "w")

        for root, subFolders, files in os.walk(os.path.join(output_folder,str(param_value))):
            for file in files:
                # arguments.append(" ["+os.path.relpath(os.path.join(root,file),output_folder)+"] ")
                tar.add(os.path.join(root,file))
        tar.close()

        self.iteration = iteration
        gc3libs.Application.__init__(self,
                                     arguments = arguments,
                                     inputs = {tarfile_name:os.path.basename(tarfile_name)},
                                     outputs = [],
                                     output_dir = os.path.join(output_folder,str(param_value),str(iteration),"POST"),
                                     join = True,
                                     stdout = "stdout.log",
                                     *extra_args
                                     )

    def terminated(self):
        gc3libs.log.info("\t\t\t\t\t\tCalling XtandemPostApplication.terminated ... " )
        self.execution.returncode = 0

class XtandemApplicationB(Application):
    def __init__(self, param_value, input_file, output_folder, iteration, **extra_args):

        gc3libs.log.info("\t\t\t\t\tCalling XtandemApplicationB.__init__(%d,%s,%d) ... " % (param_value,input_file,iteration))

        self.iteration = iteration
        gc3libs.Application.__init__(self,
                                     arguments = ["/bin/echo", "Parameter: ",str(param_value), " FileName: ", input_file, " Iteration: ", str(iteration)],
                                     inputs = [],
                                     outputs = [],
                                     output_dir = os.path.join(output_folder,str(param_value),str(iteration),os.path.basename(input_file),"B"),
                                     stdout = "stdout.txt",
                                     stderr = "stderr.txt",
                                     **extra_args
                                     )
    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\t\t\t\tCalling XtandemApplicationB.terminated()")


class XtandemApplicationA(Application):
    def __init__(self, param_value, input_file, output_folder, iteration, **extra_args):

        gc3libs.log.info("\t\t\t\t\tCalling XtandemApplicationA.__init__(%d,%s,%d) ... " % (param_value,input_file,iteration))

        self.iteration = iteration
        gc3libs.Application.__init__(self,
                                     arguments = ["/bin/echo", "Parameter: ",str(param_value), " FileName: ", input_file, " Iteration: ", str(iteration)],
                                     inputs = [],
                                     outputs = [],
                                     output_dir = os.path.join(output_folder,str(param_value),str(iteration),os.path.basename(input_file),"A"),
                                     stdout = "stdout.txt",
                                     stderr = "stderr.txt",
                                     **extra_args
                                     )
    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\t\t\t\tCalling XtandemApplicationA.terminated()")


class GdemoWorkflow(SessionBasedScript):
    """
    gdemo
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = '0.2',
            input_filename_pattern = '*.ini',
            )


    def parse_args(self):
        self.input_folder = str(self.params.args[0])
        # self.parameters = [1,2,3,4,5,6]
        self.parameters = [1]

        if not os.path.isdir(self.input_folder):
            raise RuntimeError("Not Valid Input folder %s" % self.input_folder)

        self.output_folder = self.params.output

    def new_tasks(self, extra):

        extra_args = extra.copy()
        name = "GC3Pie_demo"

        for param in self.parameters:
            name = "Gdemo_param_"+str(param)

            gc3libs.log.info("Calling Gdemo_workflow.next_tastk() for param [%d] ... " % param)

            yield (name, gdemo_workflow.MainSequentialIteration, [
                    param,
                    self.input_folder,
                    self.output_folder,
                    ], extra_args)

## support classes

# For each input file, a new computation is started, totally
# `self.params.iterations` passes, each pass corresponding to
# an application of the `self.params.executable` function.
#
# This is the crucial point:

class MainParallelIteration(ParallelTaskCollection):

    def __init__(self, param_value,
                 input_file_folder,
                 output_folder):

        self.jobname = "Gdemo_MainParal_"+str(param_value)

        gc3libs.log.info("\t\tCalling MainParallelIteration.__init(%d,%s)" % (param_value,input_file_folder))

        self.tasks = []
        for input_file in os.listdir(input_file_folder):
            self.tasks.append(
                InnerParallelIteration(
                    param_value,
                    os.path.abspath(input_file),
                    output_folder
                    )
                )
        ParallelTaskCollection.__init__(self, self.tasks)


    def __str__(self):
        return self.jobname

    def terminated(self):
        self.execution.returncode = 0
        gc3libs.log.info("\t\tMainParallelIteration.terminated")


class InnerParallelIteration(ParallelTaskCollection):
    def __init__(self, param_value,
                 input_file,
                 output_folder,
                 extra={}):


        gc3libs.log.info("\t\t\tCalling InnerParallelIteration.init(%d,%s)" % (param_value,input_file))

        tasks = []

        self.jobname = "Gdemo_paral_"+str(param_value)
        extra_args = extra.copy()
        # XXX: do I need this ?
        extra_args['parent'] = self.jobname
        tasks.append(
            InnerSequentialIterationA(
                param_value,
                input_file,
                output_folder,
                iteration=0,
                **extra_args
                )
            )
        tasks.append(
            InnerSequentialIterationB(
                param_value,
                input_file,
                output_folder,
                iteration=0,
                **extra_args
                )
            )

        # actually init jobs
        ParallelTaskCollection.__init__(self, tasks)

    def __str__(self):
        return self.jobname

    def terminated(self):
        gc3libs.log.info("\t\t\tInnerParallelIteration.terminated")
        self.execution.returncode = 0

class InnerSequentialIterationA(SequentialTaskCollection):
    def __init__(self, param_value, input_file_name, output_folder, iteration, **extra_args):

        gc3libs.log.info("\t\t\t\tCalling InnerSequentialIterationA.__init__ for param [%d] and file [%s]" % (param_value, input_file_name))

        self.param_value = param_value
        self.input_file = input_file_name
        self.output_folder = output_folder
        self.jobname = "Gdemo_InnerSequenceA_"+str(self.param_value)

        initial_task = XtandemApplicationA(param_value, input_file_name, output_folder, iteration)
        SequentialTaskCollection.__init__(self, [initial_task])

    def __str__(self):
        return self.jobname

    def next(self, iteration):
        if iteration < 4:
            gc3libs.log.info("\t\t\t\tCalling InnerSequentialIterationA.next(%d) ... " % int(iteration))
            self.add(XtandemApplicationA(self.param_value, self.input_file, self.output_folder, iteration+1))
            return Run.State.RUNNING
        else:
            self.execution.returncode = 0
            return Run.State.TERMINATED

    def terminated(self):
        gc3libs.log.info("\t\t\t\tInnerSequentialIterationA.terminated [%d]" % self.execution.returncode)


class InnerSequentialIterationB(SequentialTaskCollection):
    def __init__(self, param_value, input_file_name, output_folder, iteration, **extra_args):

        gc3libs.log.info("\t\t\t\tCalling InnerSequentialIterationB.__init__ for param [%d] and file [%s]" % (param_value, input_file_name))

        self.param_value = param_value
        self.input_file = input_file_name
        self.output_folder = output_folder
        self.jobname = "Gdemo_InnerSequenceB_"+str(self.param_value)

        initial_task = XtandemApplicationB(param_value, input_file_name, output_folder, iteration)
        SequentialTaskCollection.__init__(self, [initial_task])

    def __str__(self):
        return self.jobname

    def next(self, iteration):
        if iteration < 4:
            gc3libs.log.info("\t\t\t\tCalling InnerSequentialIterationB.next(%d) ... " % int(iteration))
            self.add(XtandemApplicationB(self.param_value, self.input_file, self.output_folder, iteration+1))
            return Run.State.RUNNING
        else:
            self.execution.returncode = 0
            return Run.State.TERMINATED

    def terminated(self):
        gc3libs.log.info("\t\t\t\tInnerSequentialIterationB.terminated [%d]" % self.execution.returncode)


class MainSequentialIteration(SequentialTaskCollection):
    def __init__(self, param_value, inputfile_folder, output_folder, **extra_args):
        self.param_value = param_value
        self.inputfile_folder = inputfile_folder
        self.output_folder = output_folder
        self.jobname = "Gdemo_MainSequence_"+str(self.param_value)


        gc3libs.log.info("\t Calling MainSequentialIteration.__init(%d,%s,%s)" % (param_value,inputfile_folder,output_folder))

        self.initial_task = MainParallelIteration(param_value,inputfile_folder,output_folder)

        SequentialTaskCollection.__init__(self, [self.initial_task])

    def next(self, iteration):

        if iteration < 3:
            gc3libs.log.info("\t Calling MainSequentialIteration.next(%d) ... " % int(iteration))
            self.add(XtandemPostApplication(self.param_value, self.output_folder, iteration+1))
            return Run.State.RUNNING
        else:
            self.execution.returncode = 0
            return Run.State.TERMINATED

    def terminated(self):
        gc3libs.log.info("\t MainSequentialIteration.terminated [%s]" % self.execution.returncode)



# run script
if __name__ == '__main__':
    import gdemo_workflow
    GdemoWorkflow().run()
