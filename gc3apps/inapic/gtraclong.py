#! /usr/bin/env python
#
#   gtraclong.py -- Front-end script for running longitudinal Tracula
#   over a list of subject files.
#
#   Copyright (C) 2016, 2017 S3IT, University of Zurich
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

See the output of ``gtracling.py --help`` for program usage
instructions.

Assumes data is organized according to BIDS standard (http://bids.neuroimaging.io) e.g., for subject abcd


"""

__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2016-07-05:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch> and Franz Liem <liem@cbs.mpg.de>'
__docformat__ = 'reStructuredText'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gtraclong

    gtraclong.GtraclongScript().run()

import os
import sys
import time
import tempfile
import re
from glob import glob
from collections import OrderedDict
import shutil

from pkg_resources import Requirement, resource_filename

import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, MiB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask

DEFAULT_CORES = 1
DEFAULT_MEMORY = Memory(3000, MB)

DEFAULT_REMOTE_INPUT_FOLDER = "./"
DEFAULT_REMOTE_DWI_FOLDER = "./dwi"
DEFAULT_REMOTE_FS_FOLDER = "./FS/"

DEFAULT_REMOTE_OUTPUT_FOLDER = "./output"
DMRIC_PATTERN = "dmrirc"
DEFAULT_TRAC_COMMAND = "trac-all -prep -c {dmrirc} -debug"

dmrirc_str = "setenv SUBJECT $PWD \n" + \
             "setenv SUBJECTS_DIR $SUBJECT/FS \n" + \
             "set dtroot = $SUBJECT/output \n" + \
             "set subjlist = (lhab_{slim_sub_id}.cross.{slim_ses_id}tp) \n" + \
             "set baselist = (lhab_{slim_sub_id}.base) \n" + \
             "set dcmroot = $SUBJECT/dwi \n" + \
             "set dcmlist = (*_dwi.nii.gz) \n" + \
             "set becfile = $SUBJECT/*_dwi.bval \n" + \
             "set bvalfile = $SUBJECT/*_dwi.bval \n"


## custom application class
class GtraclongApplication(Application):
    """
    """
    application_name = 'gtraclong'

    def __init__(self, info_dict, **extra_args):
        self.output_dir = extra_args['output_dir']

        inputs = dict()
        outputs = dict()

        # List of folders to copy to remote
        inputs[info_dict["dwi_folder"]] = DEFAULT_REMOTE_DWI_FOLDER
        inputs[info_dict["fs_base_folder"]] = os.path.join(DEFAULT_REMOTE_FS_FOLDER, os.path.basename(info_dict["fs_base_folder"]))
        inputs[info_dict["fs_long_folder"]] = os.path.join(DEFAULT_REMOTE_FS_FOLDER, os.path.basename(info_dict["fs_long_folder"]))
        inputs[info_dict["dmrirc_file"]] = DEFAULT_REMOTE_INPUT_FOLDER

        # fixme
        # wrapper = resource_filename(Requirement.parse("gc3pie"),
        #                             "gc3libs/etc/gtraclong_wrapper.py")
        wrapper = "/home/ubuntu/gtrac_long_repo/gc3pie/gc3libs/etc/gtraclong_wrapper.py"
        inputs[wrapper] = os.path.basename(wrapper)

        arguments = "./%s %s" % (inputs[wrapper],
                                 info_dict["dmrirc_file"])

        if extra_args['requested_memory'] < DEFAULT_MEMORY:
            gc3libs.log.warning("GtracApplication for subject %s running with memory allocation " \
                                "'%d GB' lower than suggested one: '%d GB'," % (info_dict["sub_id"],
                                                                                extra_args['requested_memory'].amount(
                                                                                    unit=GB),
                                                                                DEFAULT_MEMORY.amount(unit=GB)))

        Application.__init__(
            self,
            arguments=arguments,
            inputs=inputs,
            outputs=[DEFAULT_REMOTE_OUTPUT_FOLDER],
            stdout='gtraclong.log',
            join=True,
            **extra_args)


class GtraclongScript(SessionBasedScript):
    """
    
    The ``gtrac`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gtrac`` will delay submission of
    newly-created jobs so that this limit is never exceeded.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version=__version__,  # module version == script version
            application=GtraclongApplication,
            # only display stats for the top-level policy objects
            # (which correspond to the processed files) omit counting
            # actual applications because their number varies over
            # time as checkpointing and re-submission takes place.
            stats_only_for=GtraclongApplication,
        )

    def setup_args(self):

        self.add_param('input_data', type=str,
                       help="Root location of input data. "
                            "Note: expects BIDS folder structure.")

    def new_tasks(self, extra):
        """
        For each input folder, create an instance of GtracApplication
        """
        tasks = []

        for info_dict in self.get_input_subject_info(self.params.input_data):
            # extract root folder name to be used as jobname
            print("XXX")
            print(info_dict)
            extra_args = extra.copy()
            jobname = {"{sub_id}_{ses_id}".format(sub_id=info_dict["sub_id"], ses_id=info_dict["ses_id"])}
            extra_args['jobname'] = jobname

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', 'run_%s' % jobname)

            tasks.append(GtraclongApplication(info_dict, **extra_args))

        return tasks

    def get_input_subject_folder(self, input_folder):
        """
        Check and validate input subfolders
        """
        #
        # for r,d,f in os.walk(input_folder):
        #     for infile in f:
        #         if infile.startswith(DMRIC_PATTERN):
        #             yield (os.path.abspath(r),os.path.basename(r),infile)
        #
        pass

    def get_input_subject_info(self, input_folder):
        """
        returns info_dict
        info_dict includes: sub_id, ses_id, dwi_folder, fs_base_folder, fs_long_folder, dmrirc_file
        """

        # fixme
        # sub_info = OrderedDict()


        FS_folder = os.path.join(input_folder, "FS")
        nifti_folder = os.path.join(input_folder, "nifti")
        dmrirc_folder = os.path.join(input_folder, "dmrirc")

        sub_folders = glob(os.path.join(nifti_folder, "sub*"))

        for sub_folder in sub_folders:
            slim_sub_id = os.path.basename(sub_folder)[-4:]
            ses_folders = glob(os.path.join(sub_folder, "ses*"))

            for ses_folder in ses_folders:
                slim_ses_id = os.path.basename(ses_folder)[-1:]
                dwi_folder = os.path.join(ses_folder, "dwi")
                fs_base_folder = os.path.join(FS_folder, "lhab_{slim_sub_id}.base".format(slim_sub_id=slim_sub_id))
                fs_long_folder = os.path.join(FS_folder,
                                              "lhab_{slim_sub_id}.cross.{slim_ses_id}tp.long.lhab_{slim_sub_id}.base".format(
                                                  slim_sub_id=slim_sub_id, slim_ses_id=slim_ses_id))
                # fixme
                # sub_info[(slim_sub_id, slim_ses_id)] = {"dwi_folder": dwi_folder, "fs_base_folder": fs_base_folder,
                #                                         "fs_long_folder": fs_long_folder}
                if os.path.exists(dwi_folder) & os.path.exists(fs_base_folder) & os.path.exists(fs_long_folder):
                    # create dmrirc file
                    dmrirc_sub_ses_folder = os.path.join(dmrirc_folder, slim_sub_id, slim_ses_id)
                    dmrirc_sub_ses_file = os.path.join(dmrirc_sub_ses_folder, "dmrirc")
                    if os.path.exists(dmrirc_sub_ses_folder):
                        shutil.rmtree(dmrirc_sub_ses_folder)
                    os.makedirs(dmrirc_sub_ses_folder)

                    with open(dmrirc_sub_ses_file, "w") as fi:
                        fi.write(dmrirc_str.format(slim_sub_id=slim_sub_id, slim_ses_id=slim_ses_id))

                    yield {"sub_id": slim_sub_id,
                           "ses_id": slim_ses_id,
                           "dwi_folder": dwi_folder,
                           "fs_base_folder": fs_base_folder,
                           "fs_long_folder": fs_long_folder,
                           "dmrirc_file": dmrirc_sub_ses_file}
