#! /usr/bin/env python
#
#   gstructure.py -- Front-end script for submitting multiple `gstructure` jobs.
#
#   Copyright (C) 2013, 2014  University of Zurich. All rights reserved.
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
Front-end script for submitting multiple `Structure` jobs.
It uses the generic `gc3libs.cmdline.SessionBasedScript` framework.

See the output of ``gstructure --help`` for program usage instructions.
"""

from __future__ import absolute_import, print_function

# summary of user-visible changes
__changelog__ = """

  2014-02-27:
  * Initial release, forked off the ``gpyrad`` sources.
"""
__author__ = 'Tyanko Aleksiev <tyanko.aleksiev@chem.uzh.ch>'
__docformat__ = 'reStructuredText'


# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gstructure
    gstructure.GStructureScript().run()

from pkg_resources import Requirement, resource_filename
import os
import csv
import posix

# gc3 library imports
import gc3libs.utils
import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

## custom application class

class GStructureApplication(Application):
    """
    """

    application_name = 'structure'

    def __init__(self, input_file, **extra_args):
        """
        The wrapper script is being used for start the simulation.
        """
        files_to_send = []

        gstructure_wrapper_sh = resource_filename(Requirement.parse("gc3pie"),
                                              "gc3libs/etc/gstructure_wrapper.sh")

        basename_input_file = os.path.basename(input_file)
        files_to_send.append((gstructure_wrapper_sh,os.path.basename(gstructure_wrapper_sh)))
        files_to_send.append((input_file,basename_input_file))


        cmd = "./gstructure_wrapper.sh -d "

        if 'mainparam_file' in extra_args:
            cmd += " -p %s " % extra_args['mainparam_file']
            files_to_send.append(extra_args['mainparam_file'],'mainparams.txt')

        if 'extraparam_file' in extra_args:
            cmd += " -x %s " % extra_args['extraparam_file']
            files_to_send.append(extra_args['extraparam_file'],'extraparams.txt')

        if 'output_file' in extra_args:
            cmd += " -u %s " % extra_args['output_file']
        else:
            output_file = basename_input_file.split(".")[0] + ".out"
            cmd += " -u %s " % output_file

        if extra_args.has_key('k_range'):
            cmd += " -g %s " % extra_args['k_range']
        else:
            cmd += " -g %s " % extra_args['k_range'].default

        if extra_args.has_key('replica'):
            cmd += " -e %s " % extra_args['replica']
        else:
            cmd += " -e %s " % extra_args['replica'].default

        cmd += " %s " % extra_args['loc']

        cmd += " %s " % extra_args['ind']

        cmd += " %s " % basename_input_file

        extra_args['requested_memory'] = 7*GB

        self.output_dir = basename_input_file + "_output"
        extra_args['output_dir'] = self.output_dir

        Application.__init__(
            self,
            # arguments should mimic the command line interfaca of the command to be
            # executed on the remote end
            arguments = cmd,
            inputs = files_to_send,
            outputs = gc3libs.ANY_OUTPUT,
            stdout = 'gstructure.log',
            join=True,
            **extra_args)

class GStructureTask(RetryableTask, gc3libs.utils.Struct):
    """
    Run ``gstructure`` on a given simulation directory until completion.
    """
    def __init__(self, input_file, **extra_args):
        RetryableTask.__init__(
            self,
            # actual computational job
            GStructureApplication(input_file, **extra_args),
            # keyword arguments
            **extra_args)

## main script class

class GStructureScript(SessionBasedScript):
    """
Scan the specified INPUT directories recursively for simulation
directories and submit a job for each one found; job progress is
monitored and, when a job is done, its output files are retrieved back
into the simulation directory itself.

The ``gstructure`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``gstructure`` will delay submission of
newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__, # module version == script version
            application = GStructureTask,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for = GStructureTask,
            )


    def setup_options(self):

        self.add_param("-p", "--mainparams", metavar="MAIN_CONFIG_FILE", default="mainparams.txt",
                       dest="mainparams_file", help="Uses a different main parameters file.")

        self.add_param("-x", "--extraparams", metavar="EXTRA_CONFIG_FILE", default="extraparams.txt",
                       dest="extraparams_file", help="Uses a different extra parameters file.")

        self.add_param("-u", "--output", metavar="OUTPUT_FILE_NAME",
                       dest="output_file", help="Output file name where results will be saved. "
                       "Default: INPUT_FILE.out" )

        self.add_param("-g", "--K-range", metavar="K_RANGE", default="1:20",
                       dest="k_range", help="Structure K range. "
                       "Default: %(default)s" )

        self.add_param("-e", "--replica", metavar="REPLICA_NUM", default=3,
                       dest="replica", help="Structure replicates. "
                       "Default: %(default)s" )

        self.add_param("-T", "--control-file", metavar="CONTROL_FILE",
                       dest="control_file", help="Control csv file for managing multiple input files with differetn loc and ind")

        self.add_param("--loc", type=int, metavar="LOC",
                       dest="loc", help="Number of loci in the structure input file")

        self.add_param("--ind", type=int, metavar="IND",
                       dest="ind", help="Number of individuals in the structure input file")

        self.add_param("--input_source", type=str,metavar="INPUT_SOURCE",
                       dest="input_source", help="Structure input file/Structure input directory")

    def parse_args(self):

        if self.params.input_source:
            self.params.input_source = os.path.abspath(self.params.input_source)

    def new_tasks(self, extra):

        tasks = []
        input_files = []
        inputs = []
        extentions = ( '.tsv', '.txt', '.struc', '.tab' )
        extra_args = extra.copy()

        if self.params.mainparams_file:
            extra_args['mainparams_file'] = self.params.mainparams_file

        if self.params.extraparams_file:
            extra_args['extraparams_file'] = self.params.extraparams_file

        if self.params.k_range:
            extra_args['k_range'] = self.params.k_range

        if self.params.replica:
            extra_args['replica'] = self.params.replica

        # Check if control_file variable is defined. If yes,
        # proceed differently for crating the tasks.
        if self.params.control_file:
            gc3libs.log.warning("As you issued the script with the control, file options loc and ind paramters will be ignored")

            if self.params.control_file.endswith('.csv'):
                try:
                    inputfile = open(self.params.control_file, 'r')
                except (OSError, IOError), ex:
                    self.log.warning("Cannot open input file '%s': %s: %s",
                                     path, ex.__class__.__name__, str(ex))
                for row in csv.reader(inputfile):
                    extra_args['input_source'] = row[0]
                    extra_args['loc'] = row[1]
                    extra_args['ind'] = row[2]

                    input_file = row[0]

                    tasks.append(GStructureTask(
                        input_file,
                        **extra_args
                        ))
        else:
             if os.path.isdir(self.params.input_source):

                for i in self._list_local_folder(self.params.input_source):
                    for ext in extentions:
                        if i.endswith(ext):
                            input_files.append(i)

             elif os.path.isfile(self.params.input_source):

                for ext in extentions:
                        if self.params.input_source.endswith(ext):
                            input_files.append(self.params.input_source)

             for input_file in input_files:

                    jobname = "%s" % input_file.split(".")[0]

                    extra_args['jobname'] = jobname

                    # FIXME: ignore SessionBasedScript feature of customizing
                    # output folder

                    extra_args['loc'] = self.params.loc
                    extra_args['ind'] = self.params.ind
                    extra_args['input_source'] = self.params.input_source
                    #extra_args['output_dir'] = self.params.input_source

                    self.log.info("Creating Task for input file: %s" % input_file)

                    tasks.append(GStructureTask(
                        input_file,
                        **extra_args
                        ))

        return tasks


    def _list_local_folder(self, input_folder):
        """
        return a list of all files in the input folder
        """

        return [ os.path.join(input_folder,infile) for infile in os.listdir(input_folder) ]
