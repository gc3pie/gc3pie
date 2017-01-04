#! /usr/bin/env python
#
#   bidsapps.py -- Front-end script for running the docking program rDock
#   over a list of ligand files.
#
#   Copyright (C) 2014, 2015 S3IT, University of Zurich
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

See the output of ``bidsapps.py --help`` for program usage
instructions.

Input parameters consists of:

...

Options:
"""

# fixme
# specify exec instance id & flavour via command, not conf file? no write to conf file

__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2016-12-12:
  * Initial version
"""
__author__ = 'Franz Liem <franziskus.liem@uzh.ch>'
__docformat__ = 'reStructuredText'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import bidsapps

    bidsapps.BidsAppsScript().run()

import os
import sys
import time
import tempfile
import re
import stat

import shutil
from bids.grabbids import BIDSLayout

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file, positive_int
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

DEFAULT_CORES = 2
DEFAULT_MEMORY = Memory(4000, MB)

DEFAULT_REMOTE_INPUT_FOLDER = "./"
DEFAULT_REMOTE_OUTPUT_FOLDER = "./output"


## custom application class
class BidsAppsApplication(Application):
    """
    """
    application_name = 'bidsapps'

    def __init__(self,
                 analysis_level,
                 subject_id, bids_input_folder,
                 bids_output_folder,
                 docker_image,
                 runscript_args,
                 **extra_args):
        # self.application_name = "freesurfer"
        # conf file freesurfer_image
        self.output_dir = []  # extra_args['output_dir']

        inputs = dict()
        outputs = dict()
        self.output_dir = extra_args['output_dir']

        # fixme
        # wrapper = resource_filename(Requirement.parse("gc3pie"), "gc3libs/etc/echo_and_run_cmd.py")
        wrapper = "/home/ubuntu/gtrac_long_repo/gc3pie/gc3libs/etc/echo_and_run_cmd.py"
        inputs[wrapper] = os.path.basename(wrapper)

        docker_cmd_input_mapping = "{bids_input_folder}:/data/in:ro".format(bids_input_folder=bids_input_folder)

        docker_cmd_output_mapping = "{bids_output_folder}:/data/out".format(bids_output_folder=bids_output_folder)

        docker_mappings = "-v %s -v %s " % (docker_cmd_input_mapping, docker_cmd_output_mapping)
        docker_cmd = "docker run {docker_mappings} {docker_image}".format(docker_mappings=docker_mappings,
                                                                          docker_image=docker_image)

        if analysis_level == "participant":
            # runscript = runscript, runscript_args = runscript_args)
            wf_cmd = "/data/in  /data/out {analysis_level} " \
                     "--participant_label {subject_id} {runscript_args} ".format(analysis_level=analysis_level,
                                                                                 subject_id=subject_id,
                                                                                 runscript_args=runscript_args)

            cmd = " {docker_cmd} {wf_cmd}".format(docker_cmd=docker_cmd, wf_cmd=wf_cmd)

            Application.__init__(self,
                                 arguments="python ./%s %s" % (inputs[wrapper], cmd),
                                 inputs=inputs,
                                 outputs=[DEFAULT_REMOTE_OUTPUT_FOLDER],
                                 stdout='bidsapps.log',
                                 join=True,
                                 **extra_args)


            #


class BidsAppsScript(SessionBasedScript):
    """
    
    The ``bidsapps`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gnift`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    This class is called when bidsapps command is executed.
    Loops through subjects in bids input folder and starts instance
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version=__version__,  # module version == script version
            application=BidsAppsApplication,
            stats_only_for=BidsAppsApplication,
        )

    def setup_args(self):
        self.add_param("docker_image", type=str, help="xxx")

        self.add_param("bids_input_folder", type=str, help="Root location of input data. Note: expects folder in "
                                                           "BIDS format.")

        self.add_param("bids_output_folder", type=str, help="xxx")

        self.add_param("analysis_level", type=str, choices=['participant', 'group'],
                       help="analysis_level: participant: 1st level\n"
                            "group: second level")

    def setup_options(self):
        self.add_param("-pl", "--participant_label",
                       help='The label of the participant that should be analyzed. The label '
                            'corresponds to sub-<participant_label> from the BIDS spec '
                            '(so it does not include "sub-"). If this parameter is not '
                            'provided all subjects should be analyzed. Multiple '
                            'participants can be specified with a space separated list.',
                       nargs="+")
        self.add_param("-pf", "--participant_file",
                       help='A text file with the labels of the participant that should be analyzed. No header; one '
                            'subject per line.'
                            'Specs according to --participant_label. If --participant_label and --participant_file '
                            'are specified, both are used. Note: If -pl or -pf is not specified, analyze all subjects')
        self.add_param("-pel", "--participant_exclusion_label",
                       help='The label of the participant that should NOT be analyzed. Multiple '
                            'participants can be specified with a space separated list.',
                       nargs="+")
        self.add_param("-pef", "--participant_exclusion_file",
                       help='A tsv file with the labels of the participant that should not be analyzed, '
                            'despite being listed in --participant_file.')

        self.add_param("-ra", "--runscript_args", type=str, dest="runscript_args", default=None,
                       help='BIDSAPPS: add application-specific arguments '
                            'passed to the runscripts in qotation marks: '
                            'e.g. \"--license_key xx\" ')

        # Overwrite script input options to get more specific help
        self.add_param("-o", "--output", type=str, dest="output", default=None,
                       help="BIDSAPPS: local folder where logfiles are copied to")

        self.add_param("-c", "--cpu-cores", dest="ncores",
                       type=positive_int, default=1,  # 1 core
                       metavar="NUM",
                       help="Set the number of CPU cores required for each job"
                            " (default: %(default)s). NUM must be a whole number.\n"
                            " NOTE: Parameter is NOT piped into bidsapp n_cpus. Specifiy --n_cpus as -ra")

        self.add_param("-m", "--memory-per-core", dest="memory_per_core",
                       type=Memory, default=2 * GB,  # 2 GB
                       metavar="GIGABYTES",
                       help="Set the amount of memory required per execution core;"
                            " default: %(default)s. Specify this as an integral number"
                            " followed by a unit, e.g., '512MB' or '4GB'."
                            " NOTE: Parameter is NOT piped into bidsapp mem_mb. Specifiy --mem_mb as -ra")

    def new_tasks(self, extra):
        """
        For each subject, create an instance of GniftApplication
        """
        tasks = []
        subject_list = []

        # build subject list form either input arguments (participant_label, participant_file) or
        # input data in bids_input_folder,
        # then remove subjects form list according to participant_exclusion_file (if any)
        def read_subject_list(list_file):
            "reads text file with subject id per line and returns as list"
            with open(list_file) as fi:
                l = fi.read().strip().split("\n")
            return [s.strip() for s in l]

        if self.params.participant_label:
            clean_list = [s.strip() for s in self.params.participant_label]
            subject_list += clean_list

        if self.params.participant_file:
            subject_list += read_subject_list(self.params.participant_file)

        if not subject_list:
            subject_list = self.get_input_subjects(self.params.bids_input_folder)

        # force unique
        subject_list = list(set(subject_list))

        subject_exclusion_list = []
        if self.params.participant_exclusion_label:
            clean_list = [s.strip() for s in self.params.participant_exclusion_label]
            subject_exclusion_list += clean_list

        if self.params.participant_exclusion_file:
            subject_exclusion_list += read_subject_list(self.params.participant_exclusion_file)

        for exsub in subject_exclusion_list:
            if exsub in subject_list:
                subject_list.remove(exsub)
            else:
                gc3libs.log.warning("Subject on exclusion list, but not in inclusion list: %s" % exsub)

        # create output folder and check permission (others need write permission)
        # Riccardo: on the NFS filesystem, `root` is remapped transparently to user
        # `nobody` (this is called "root squashing"), which cannot write on the
        # `/data/nfs` directory owned by user `ubuntu`.
        if not os.path.exists(self.params.bids_output_folder):
            os.makedirs(self.params.bids_output_folder)
            # add write perm for others
            os.chmod(self.params.bids_output_folder, os.stat(self.params.bids_output_folder).st_mode | stat.S_IWOTH)

        # check if output folder has others write permission
        if not os.stat(self.params.bids_output_folder).st_mode & stat.S_IWOTH:
            raise OSError("BIDS output folder %s \nothers need write permission. "
                          "Stopping." % self.params.bids_output_folder)

        if self.params.analysis_level == "participant":
            for subject_id in subject_list:
                extra_args = extra.copy()
                extra_args['jobname'] = subject_id
                extra_args['output_dir'] = self.params.output
                extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', '%s' % extra_args['jobname'])

                mem_mb = self.params.memory_per_core.amount(unit=MB)
                tasks.append(BidsAppsApplication(
                    self.params.analysis_level,
                    subject_id,
                    self.params.bids_input_folder,
                    self.params.bids_output_folder,
                    self.params.docker_image,
                    self.params.runscript_args,
                    **extra_args))

        return tasks

    def get_input_subjects(self, bids_input_folder):
        """
        """
        layout = BIDSLayout(bids_input_folder)
        return layout.get_subjects()
