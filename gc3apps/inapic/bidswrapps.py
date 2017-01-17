#! /usr/bin/env python
#
#   bidswrapps.py -- Front-end script for running the docking program rDock
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

See the output of ``bidswrapps.py --help`` for program usage
instructions.

Input parameters consists of:

...

Options:
"""

# fixme
# TODO move to own repo (echo_and_run_cmd into same folder; -> script_dir)
# specify exec instance id & flavour via command, not conf file? no write to conf file
# TODO riccardo how to log
# TODO clean way to add volumes to docker
# TODO allow participant_label on group level


__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2016-12-12:
  * Initial version
"""
__author__ = 'Franz Liem <franziskus.liem@uzh.ch>'
__docformat__ = 'reStructuredText'


if __name__ == "__main__":
    import bidswrapps

    bidswrapps.BidsWrappsScript().run()

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
class BidsWrappsApplication(Application):
    """
    """
    application_name = 'bidswrapps'

    def __init__(self,
                 analysis_level,
                 subject_id,
                 bids_input_folder,
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
        # script_dir = os.path.abspath(os.path.dirname(sys.argv[0]))
        wrapper = "/home/ubuntu/gtrac_long_repo/gc3pie/gc3libs/etc/echo_and_run_cmd.py"
        inputs[wrapper] = os.path.basename(wrapper)

        # fixme add ro again, after dcm2niix release
        # docker_cmd_input_mapping = "{bids_input_folder}:/data/in:ro".format(bids_input_folder=bids_input_folder)
        docker_cmd_input_mapping = "{bids_input_folder}:/data/in".format(bids_input_folder=bids_input_folder)

        docker_cmd_output_mapping = "{bids_output_folder}:/data/out".format(bids_output_folder=bids_output_folder)

        docker_mappings = "-v %s -v %s " % (docker_cmd_input_mapping, docker_cmd_output_mapping)
        docker_cmd = "docker run {docker_mappings} {docker_image}".format(docker_mappings=docker_mappings,
                                                                          docker_image=docker_image)


        # runscript = runscript, runscript_args = runscript_args)
        wf_cmd = "/data/in  /data/out {analysis_level} ".format(analysis_level=analysis_level)
        if subject_id:
            wf_cmd += "--participant_label {subject_id} ".format(subject_id=subject_id)
        if runscript_args:
            wf_cmd += "{runscript_args} ".format(runscript_args=runscript_args)

        cmd = " {docker_cmd} {wf_cmd}".format(docker_cmd=docker_cmd, wf_cmd=wf_cmd)


        Application.__init__(self,
                             arguments="python ./%s %s" % (inputs[wrapper], cmd),
                             inputs=inputs,
                             outputs=[DEFAULT_REMOTE_OUTPUT_FOLDER],
                             stdout='bidswrapps.log',
                             join=True,
                             **extra_args)


        #


class BidsWrappsScript(SessionBasedScript):
    """
    
    The ``bidswrapps`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gnift`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    This class is called when bidswrapps command is executed.
    Loops through subjects in bids input folder and starts instance
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version=__version__,  # module version == script version
            application=BidsWrappsApplication,
            stats_only_for=BidsWrappsApplication,
        )

    def setup_args(self):
        self.add_param("docker_image", type=str, help="Name of docker image to run. \n\n"
                                                      "If image has no entry point give container name and entry "
                                                      "point under '' e.g. 'container:v1 python script.py'")

        self.add_param("bids_input_folder", type=str, help="Root location of input data. Note: expects folder in "
                                                           "BIDS format.")

        self.add_param("bids_output_folder", type=str, help="xxx")

        self.add_param("analysis_level", type=str,
                       help="analysis_level: participant: 1st level\n"
                            "group: second level. Bids-Apps specs allow for multiple substeps (e.g., group1, group2")

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
                       help='BIDS Apps: add application-specific arguments '
                            'passed to the runscripts in qotation marks: '
                            'e.g. \"--license_key xx\" ')

        # Overwrite script input options to get more specific help
        self.add_param("-o", "--output", type=str, dest="output", default=None,
                       help="BIDS Apps: local folder where logfiles are copied to")

        self.add_param("-c", "--cpu-cores", dest="ncores",
                       type=positive_int, default=1,  # 1 core
                       metavar="NUM",
                       help="Set the number of CPU cores required for each job"
                            " (default: %(default)s). NUM must be a whole number.\n"
                            " NOTE: Parameter is NOT piped into BIDS Apps' n_cpus. Specifiy --n_cpus as -ra")

        self.add_param("-m", "--memory-per-core", dest="memory_per_core",
                       type=Memory, default=2 * GB,  # 2 GB
                       metavar="GIGABYTES",
                       help="Set the amount of memory required per execution core;"
                            " default: %(default)s. Specify this as an integral number"
                            " followed by a unit, e.g., '512MB' or '4GB'."
                            " NOTE: Parameter is NOT piped into BIDS Apps' mem_mb. Specifiy --mem_mb as -ra")

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

        if self.params.analysis_level.startswith("participant"):
            for subject_id in subject_list:
                extra_args = extra.copy()
                extra_args['jobname'] = subject_id
                extra_args['output_dir'] = self.params.output
                extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', '%s' % extra_args['jobname'])

                # mem_mb = self.params.memory_per_core.amount(unit=MB)
                tasks.append(BidsWrappsApplication(
                    self.params.analysis_level,
                    subject_id,
                    self.params.bids_input_folder,
                    self.params.bids_output_folder,
                    self.params.docker_image,
                    self.params.runscript_args,
                    **extra_args))

        elif self.params.analysis_level.startswith("group"):
            extra_args = extra.copy()
            extra_args['jobname'] = self.params.analysis_level
            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', '%s' % extra_args['jobname'])
            tasks.append(BidsWrappsApplication(
                self.params.analysis_level,
                None,  # subject_id
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
