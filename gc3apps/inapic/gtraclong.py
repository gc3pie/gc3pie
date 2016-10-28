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

See the output of ``gtraclong.py --help`` for program usage instructions.
"""

__version__ = 'development version (SVN $Revision$)'
# summary of user-visible changes
__changelog__ = """
  2016-10-28:
  * Now uses longitudinal stream (based on gtrac.py)
  * Input data is expected in BIDS standard

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
from glob import glob
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
DEFAULT_MEMORY = Memory(7, GB)
DEFAULT_WALLTIME = Duration(100, hours)

DEFAULT_REMOTE_INPUT_FOLDER = "./"
DEFAULT_REMOTE_DMRIRC_FILE = "./dmrirc"
DEFAULT_REMOTE_DWI_FOLDER = "./dwi"
DEFAULT_REMOTE_FS_FOLDER = "./freesurfer/"

DEFAULT_REMOTE_OUTPUT_FOLDER = "./output"
DMRIC_PATTERN = "dmrirc"
DEFAULT_TRAC_COMMAND = "trac-all -prep -c {dmrirc} -debug"

fs_cross_str = "{bids_sub}_{bids_ses}"
fs_base_str = "{bids_sub}.base"
fs_long_str = "{bids_sub}_{bids_ses}.long." + fs_base_str
dwi_file_str = "{bids_ses}/dwi/{bids_sub}_{bids_ses}_run-1_dwi.nii.gz"
bvec_str = "$SUBJECT/dwi/{bids_ses}/dwi/{bids_sub}_{bids_ses}_run-1_dwi.bvec"
bval_str = "$SUBJECT/dwi/{bids_ses}/dwi/{bids_sub}_{bids_ses}_run-1_dwi.bval"


## custom application class
class GtraclongApplication(Application):
    application_name = 'gtraclong'
    def __init__(self, bids_sub, dwi_folder, fs_folder_list, dmrirc_sub_file, **extra_args):
        inputs = dict()
        outputs = dict()

        self.output_dir = extra_args['output_dir']

        # List of folders to copy to remote
        inputs[dwi_folder] = DEFAULT_REMOTE_DWI_FOLDER
        for fs in fs_folder_list:
            inputs[fs] = os.path.join(DEFAULT_REMOTE_FS_FOLDER, os.path.basename(fs))
        inputs[dmrirc_sub_file] = DEFAULT_REMOTE_DMRIRC_FILE

        # fixme
        # wrapper = resource_filename(Requirement.parse("gc3pie"),
        #                            "gc3libs/etc/gtraclong_wrapper.py")
        wrapper = "/home/ubuntu/gtrac_long_repo/gc3pie/gc3libs/etc/gtraclong_wrapper.py"
        inputs[wrapper] = os.path.basename(wrapper)

        arguments = "./%s %s" % (
            inputs[wrapper], os.path.join(DEFAULT_REMOTE_INPUT_FOLDER, os.path.basename(dmrirc_sub_file)))

        # check if requested memory and walltime is lower than recommended by default
        self._check_requests(bids_sub, extra_args)

        Application.__init__(
            self,
            arguments=arguments,
            inputs=inputs,
            outputs=[DEFAULT_REMOTE_OUTPUT_FOLDER],
            stdout='gtraclong.log',
            join=True,
            **extra_args)

    def _check_requests(self, bids_sub, extra_args):
        if extra_args['requested_memory'] < DEFAULT_MEMORY:
            gc3libs.log.warning("GtraclongApplication for subject %s running with memory allocation "
                                "'%d GB' lower than suggested one: '%d GB'," % (bids_sub,
                                                                                extra_args['requested_memory'].amount(
                                                                                    unit=GB),
                                                                                DEFAULT_MEMORY.amount(unit=GB)))
        if extra_args['requested_walltime'] < DEFAULT_WALLTIME:
            gc3libs.log.warning("GtraclongApplication for subject %s running with walltime "
                                "'%d hours' lower than suggested one: '%d hours'," % (bids_sub,
                                                                                      extra_args[
                                                                                          'requested_walltime'].amount(
                                                                                          unit=hours),
                                                                                      DEFAULT_WALLTIME.amount(
                                                                                          unit=hours)))


class GtraclongScript(SessionBasedScript):
    """
    The ``gtraclong`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gtraclong`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Assumes data is organized according to BIDS standard (http://bids.neuroimaging.io) e.g., for subject abcd
    BIDS/
        freesurfer
            lhab_abcd.base
            lhab_abcd.cross.1tp
            lhab_abcd.cross.1tp.long.lhab_abcd.base
            lhab_abcd.cross.2tp
            lhab_abcd.cross.2tp.long.lhab_abcd.base
        sourcedata
            sub-lhababcd
                ses-tp1
                    dwi
                        sub-lhababcd_ses-tp1_run-1_dwi.bval
                        sub-lhababcd_ses-tp1_run-1_dwi.bvec
                        sub-lhababcd_ses-tp1_run-1_dwi.nii.gz
                ses-tp2
                    dwi
                        sub-lhababcd_ses-tp2_run-1_dwi.bval
                        sub-lhababcd_ses-tp2_run-1_dwi.bvec
                        sub-lhababcd_ses-tp2_run-1_dwi.nii.gz
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
        self.add_param('input_data', type=str,  help="Root location of input data. "
                                                     "Note: expects BIDS folder structure.")

    def new_tasks(self, extra):
        """
        For each timepoint, create an instance of GtracApplication
        """
        tasks = []

        for (bids_sub, dwi_folder, fs_folder_list, dmrirc_sub_ses_file) in self.get_input_subject_info(
                self.params.input_data):
            extra_args = extra.copy()
            jobname = bids_sub
            extra_args['jobname'] = jobname

            extra_args['output_dir'] = self.params.output
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('SESSION', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('DATE', 'run_%s' % jobname)
            extra_args['output_dir'] = extra_args['output_dir'].replace('TIME', 'run_%s' % jobname)

            tasks.append(GtraclongApplication(bids_sub, dwi_folder, fs_folder_list, dmrirc_sub_ses_file, **extra_args))
        return tasks

    def get_input_subject_info(self, input_folder):
        """
        collectes subject info and creates dmrirc file
        returns bids_sub, dwi_folder, fs_folder_list, dmrirc_sub_file
        """
        FS_folder = os.path.join(input_folder, "freesurfer")
        nifti_folder = os.path.join(input_folder, "sourcedata")
        dmrirc_folder = os.path.join(input_folder, "dmrirc")

        sub_folders = sorted(glob(os.path.join(nifti_folder, "sub*")))

        for sub_folder in sub_folders:
            bids_sub = "sub-lhab" + os.path.basename(sub_folder)[-4:]
            bids_ses_list, dwi_folder, fs_folder_list = self._build_folder_list(FS_folder, bids_sub, sub_folder)

            # check if all folders are there
            data_missing, data_missing_files = self._check_for_missing_data(dwi_folder, fs_folder_list)

            if not data_missing:
                dmrirc_sub_file = self._create_dmrirc_file(bids_sub, bids_ses_list, bval_str, bvec_str, dmrirc_folder,
                                                           dwi_file_str, fs_base_str, fs_cross_str)
                yield (bids_sub, dwi_folder, fs_folder_list, dmrirc_sub_file)
            else:
                gc3libs.log.warning("Data is missing: %s" % data_missing_files)

    def _check_for_missing_data(self, dwi_folder, fs_folder_list):
        data_missing = False
        data_missing_files = ""
        if not os.path.exists(dwi_folder):
            data_missing = True
            data_missing_files += dwi_folder
        for fs in fs_folder_list:
            if not os.path.exists(fs):
                data_missing = True
                data_missing_files += fs
        return data_missing, data_missing_files

    def _build_folder_list(self, FS_folder, bids_sub, sub_folder):
        # returns subject's dwi folder and list of freesurfer folders
        bids_ses_list = []
        ses_folders = sorted(glob(os.path.join(sub_folder, "ses*")))
        dwi_folder = os.path.join(sub_folder)

        fs_folder_list = []
        for ses_folder in ses_folders:
            bids_ses = "ses-tp" + os.path.basename(ses_folder)[-1:]
            bids_ses_list.append(bids_ses)

            fs_folder_list.append(os.path.join(FS_folder, fs_cross_str.format(bids_sub=bids_sub, bids_ses=bids_ses)))
            fs_folder_list.append(os.path.join(FS_folder, fs_base_str.format(bids_sub=bids_sub)))
            fs_folder_list.append(os.path.join(FS_folder, fs_long_str.format(bids_sub=bids_sub, bids_ses=bids_ses)))
        return bids_ses_list, dwi_folder, fs_folder_list

    def _create_dmrirc_file(self, bids_sub, bids_ses_list, bval_str, bvec_str, dmrirc_folder, dwi_file_str,
                            fs_base_str, fs_cross_str):
        """
        creates one drmirc file per subject
        """
        dmrirc_sub_folder = os.path.join(dmrirc_folder, bids_sub)
        dmrirc_sub_file = os.path.join(dmrirc_sub_folder, "dmrirc")
        if os.path.exists(dmrirc_sub_folder):
            shutil.rmtree(dmrirc_sub_folder)
        os.makedirs(dmrirc_sub_folder)

        fs_cross_list, fs_base_list, dwi_file_list, bvec_list = [], [], [], []
        for bids_ses in bids_ses_list:
            fs_cross_list.append(fs_cross_str.format(bids_sub=bids_sub, bids_ses=bids_ses))
            fs_base_list.append(fs_base_str.format(bids_sub=bids_sub))
            dwi_file_list.append(dwi_file_str.format(bids_sub=bids_sub, bids_ses=bids_ses))
            bvec_list.append(bvec_str.format(bids_sub=bids_sub, bids_ses=bids_ses))

        fs_cross_list = " ".join(fs_cross_list)
        fs_base_list = " ".join(fs_base_list)
        dwi_file_list = " ".join(dwi_file_list)
        bvec_list = " ".join(bvec_list)
        bval_str_exec = bval_str.format(bids_sub=bids_sub, bids_ses=bids_ses_list[0])

        dmrirc_str = "setenv SUBJECT $PWD \n" + \
                     "setenv SUBJECTS_DIR $SUBJECT/freesurfer \n" + \
                     "set dtroot = $SUBJECT/output \n" + \
                     "set subjlist = (" + fs_cross_list + ") \n" + \
                     "set baselist = (" + fs_base_list + ") \n" + \
                     "set dcmroot = $SUBJECT/dwi \n" + \
                     "set dcmlist = (" + dwi_file_list + ") \n" + \
                     "set bveclist = (" + bvec_list + " )\n" + \
                     "set bvalfile = " + bval_str_exec + " \n"

        with open(dmrirc_sub_file, "w") as fi:
            fi.write(dmrirc_str.format(bids_sub=bids_sub, bids_ses=bids_ses))
        return dmrirc_sub_file
