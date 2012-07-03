#! /usr/bin/env python
#
#   imsb_example.py -- prototype of simple Xtandem workflow
#
#   Copyright (C) 2011, 2012 GC3, University of Zurich
#
#   This program is free software: you can redistribute it and/or modify
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
 prototype of simple Xtandem workflow
"""

__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: http://code.google.com/p/gc3pie/issues/detail?id=95
if __name__ == "__main__":
    import imsb_example
    ImsbExample().run()

# gc3 library imports
import gc3libs
from gc3libs import Application, Run, Task, RetryableTask
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.dag import SequentialTaskCollection, ParallelTaskCollection, ChunkedParameterSweep

class ApplicakeApplication(Application):
    def __init__(self, applic, input_file, output_file, output_dir, args=None):

        arguments = [ applic, input_file, output_file ]
        arguments += args

        inputs[input_file] = os.path.basename(input_file)

        self.output_file = output_file
        
        Application.__init__(
            self,
            # executable = executable_name,
            executable = "applicake_wrap.py",
            # GEOtop requires only one argument: the simulation directory
            # In our case, since all input files are staged to the
            # execution directory, the only argument is fixed to ``.``
            arguments = arguments,
            inputs = inputs,
            outputs = [ self.output_file ],
            # outputs = outputs,
            output_dir = output_dir,
            stdout = 'applicake.log',
            join=True,
            tags = [ 'ENV/APPLICAKE-1.0' ],
            **kw)

    def termianted(self):
        # If output file not present
        # simply retry task
        if not os.path.isfile(self.output_file):
            # XXX: choose either to terminate and fail the whole sequence, or to retry the task
            self.execution.exitcode == 99
        else:
            self.execution.exitcode == 0

class ApplicakeTask(RetriableTask):
    def __init__(self, applic, input_file, output_file, output_dir, **kw):
        RetryableTask.__init__(
            self,
            # task name
            "IMSB_"+str(applic), # jobname
            # actual computational job
            ApplicakeApplication(applic, input_file, output_file, output_dir, **kw)
            # keyword arguments
            **kw)

    def retry(self):
        if self.task.execution.exitcode == 99:
            return True
        else:
            return False

class ImsbSequence(SequentialTaskCollection):

    def __init__(self, input_files_path, output_folder, grid=None, **kw):
        """
        considering that the sequence is pre-defined
        we can wimply create the whole sequence as part of the initialization process
        """
        self.jobname = "IMSB_Sequence"

        # Define the sequence
        task_list = []

        # Task 1: copy2dropbox 
        protoxml2openbis_file = os.path.join(input_files_path,"protxml2openbis")
        copy2dropbox_file = os.path.join(input_files_path,"copy2dropbox.ini")
        
        task_list.append(ApplicakeTask("Copy2IdentDropbox", protxml2openbis_file, copy2dropbox_file, output_folder, **kw))


        # Task 2: protxml2openbis
        protxml2modifications_file = os.path.join(input_files_path,"protxml2modifications")

        task_list.append(ApplicakeTask("ProtXml2Openbis", protxml2modifications_file, copy2dropbox_file, output_folder, **kw))

        # Task 3: protxml2modifications
        protxml2spectralcount_file = os.path.join(input_files_path, "protxml2spectralcount.ini")

        task_list.append(ApplicakeTask("ProtXml2Modifications", protxml2spectralcount_file, protxml2modifications_file))

        # XXX: and continue till all the sequence is defined....
        SequentialTaskCollection.__init__(self, self.jobname, task_list, grid)

    
    def __str__(self):
        return self.jobname

    def next(self, done):
        if self.tasks[done].execution.returncode != 0:
            # Consider terminate the whole sequence
            self.execution.returncode = self.tasks[done].execution.returncode
            return Run.State.TERMINATED


        
        
class ImsbExample(SessionBasedScript):
    """
    prototype of simple Xtandem workflow
    """

    def new_tasks(self, extra):
        for path in self._validate_input_folders(self.params.args):
            # construct GEOtop job
            yield (
                # job name
                gc3libs.utils.basename_sans(path),
                # task constructor
                ImsbSequence,
                [ # parameters passed to the constructor, see `ImsbSequence.__init__`
                    path,                   # path to the directory containing input files
                ],
                # extra keyword arguments passed to the constructor,
                # see `GeotopTask.__init__`
                extra.copy()
                )

