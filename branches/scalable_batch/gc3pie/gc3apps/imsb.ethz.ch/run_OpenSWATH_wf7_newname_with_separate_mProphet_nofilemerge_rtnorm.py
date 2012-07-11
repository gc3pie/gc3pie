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

        # Task 1:  Run ExtractChromatogram for the RT Peptides
        task_list.append(ExtractChromatogramParallel(input_files_path, output_folder, **kw))

        # Task 2: merge files and run RT Normalizer
        task_list.append(FileMergerApplication(output_folder, **kw))

        # Task 3: Run ExtractChromatogram
        # Compress is postprocess of the whole Task
        task_list.append(ExtractChromatogramParallel(input_files_path, output_folder, **kw))

        # Task 4: Step 2 MRMAnalyzer
        # Compress is postprocess of the whole Task
        task_list.append(MRMAnalyzerParallel(input_files_path,output_folder,**kw))

        # Task 5: Step 3 FeatureXMLToTSV on files featureXML
        # Compress is postprocess of the whole Task
        task_list.append(FeatureXMLToTSVParallel(input_files_path,output_folder,**kw))

        # Task 6: mprophet from *_.short_format.csv
        # Compress and additional log information are postprocess of the whole Task
        task_list.append(mQuest_mProphetParallel(input_files_path, output_folder, **kw))

        # Task 7: fdr_cutoff.py and count_pep_prot.py
        # for fdr in 0.20 0.15 0.10 0.05  0.01 0.002 0.001;
        # should they run locally ?
        task_list.append(CheckResultsParallel(output_folder, **kw))

        # Init
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

