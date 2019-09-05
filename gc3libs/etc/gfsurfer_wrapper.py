#!/usr/bin/env python

"""
export SUBJECTS_DIR=/our/out/data/path

 ************
 DATA INPUT
 ************
recon-all -i in_data_path/s01/TP1/T1w_b.nii.gz -subjid s01.cross.TP1

 ************
 CROSS SECTIONAL PROCESSING s01
 ************
recon-all -s s01.cross.TP1 -all

# Optional

 ************
 LONG PROCESSING s01
 ************
recon-all -long s01.cross.TP1 s01.base -all -qcache
"""

from __future__ import absolute_import, print_function, unicode_literals
import sys
import os
import subprocess
import argparse

NII_EXTENSION = "nii"
FSAVERAGE = "fsaverage"
FS_SUBJECT_FSAVERAGE = os.path.join(os.environ.get("FREESURFER_HOME", ''),"subjects",FSAVERAGE)
FS_SEQ_LONG = "long"
FS_SEQ_CROSS = "cross"
FS_SEQ_DEFAULT = FS_SEQ_CROSS
FS_SEQ=[FS_SEQ_LONG,FS_SEQ_CROSS]

# def Usage():
#     print ("Usage: gnift_wrapper.py [OPTIONS] <subject name> <NIFIT input file> <subjects dir>")

def RunFreesurfer():
    """
    By default the input files are in the same local directory as the wrapper executed.
    """

    parser = argparse.ArgumentParser(description='Run Freesurfer.')
    parser.add_argument('subject', metavar='SUBJECT',
                        help='Subject name')
    parser.add_argument('nifti', metavar='NIFTI',
                        help='NIFTI input file')
    parser.add_argument('output', metavar='OUTPUT',
                        help='Subject dir')
    parser.add_argument('--seq', dest='sequence', action="append",
                        default=[FS_SEQ_CROSS],
                        help='Freesurfer sequence. Valid values %s' % FS_SEQ)

    args = parser.parse_args()

    # Create output folder and add simlink to FSAVERAGE
    os.mkdir(args.output)
    os.symlink(FS_SUBJECT_FSAVERAGE,os.path.join(args.output,FSAVERAGE))

    # Verisfy input arguments
    try:
        assert os.path.isfile(args.nifti), \
            "Input NIFTI file %s not found" % args.nifti

        if args.sequence:
            args.sequence = FS_SEQ_DEFAULT
    except AssertionError as ex:
        raise OSError(ex.message)

    input_nii = dict()
    cross_files = []
    os.environ["SUBJECTS_DIR"] = args.output
    quality_checker = os.environ["QA_TOOLS"]

    if FS_SEQ_CROSS in args.sequence:

        # DATA INPUT and  CROSS SECTIONAL PROCESSING
        print("Start DATA INPUT on %s" % args.subject)
        "Example: recon-all -i file.nii.gz -subjid s01.cross.TP1"
        command="recon-all -i %s -subjid %s.crossTP1" % (args.nifti,args.nifti.split(".")[0])
        runme(command)

        print("Start CROSS SECTIONAL PROCESSING")
        "Example: recon-all -s s01.cross.TP1 -all"
        command="recon-all -s %s.crossTP1 -all" % args.nifti.split(".")[0]
        runme(command)

    if FS_SEQ_LONG in args.sequence:
        # LONG PROCESSING s01
        print("Start LONG PROCESSING on %s" % args.subject)
        # Example: recon-all -long s01.cross.TP1 s01.base -all -qcache
        command = "recon-all -long %scross -all -qcache" % args.subject
        runme(command)

def runme(command):
    """
    Comodity function to run commands using `subprocess` module
    Input: command to run
    Output: none
    Raise Exception in case command fails
    """
    proc = subprocess.Popen(
        [command],
        shell=True,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE)

    print("Running command %s" % command)
    (stdout, stderr) = proc.communicate()

    if proc.returncode != 0:
        print("Execution failed with exit code: %d" % proc.returncode)
        print("Output message: %s" % stdout)
        print("Error message: %s" % stderr)
        raise Exeption(stderr)

if __name__ == '__main__':
    sys.exit(RunFreesurfer())
