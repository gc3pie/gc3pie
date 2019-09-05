#!/usr/bin/env python

"""
export SUBJECTS_DIR=/our/out/data/path

************
SUBJECT: s01
************
 ************
 DATA INPUT s01
 ************
recon-all -i in_data_path/s01/TP1/T1w_a.nii.gz -i in_data_path/s01/TP1/T1w_b.nii.gz -subjid s01.cross.TP1
recon-all -i in_data_path/s01/TP2/T1w_a.nii.gz -i in_data_path/s01/TP2/T1w_b.nii.gz -subjid s01.cross.TP2
recon-all -i in_data_path/s01/TP3/T1w_a.nii.gz -i in_data_path/s01/TP3/T1w_b.nii.gz -subjid s01.cross.TP3

 ************
 CROSS SECTIONAL PROCESSING s01
 ************
recon-all -s s01.cross.TP1 -all
recon-all -s s01.cross.TP2 -all
recon-all -s s01.cross.TP3 -all

 ************
 BASE PROCESSING s01
 ************
recon-all -base s01.base -tp s01.cross.TP1 -tp s01.cross.TP2 -tp s01.cross.TP3 -all

 ************
 LONG PROCESSING s01
 ************
recon-all -long s01.cross.TP1 s01.base -all -qcache
recon-all -long s01.cross.TP2 s01.base -all -qcache
recon-all -long s01.cross.TP3 s01.base -all -qcache

XXX: Allow differnt type of NII extensions. e.g. nii.tgz
XXX: How to formally verify correctness of output from step 'cross'.
     For the itme being do nothing on this.
XXX: Remove simlinks from output folder once completed.
"""

from __future__ import absolute_import, print_function, unicode_literals
import sys
import os
import subprocess

NII_EXTENSION = "nii"
FSAVERAGE = "fsaverage"
FS_SUBJECT_FSAVERAGE = os.path.join(os.environ.get("FREESURFER_HOME", ''),"subjects",FSAVERAGE)

def Usage():
    print ("Usage: gnift_wrapper.py <subject name> <NIFIT input folder> <subjects dir>")

def RunFreesurfer(subject, input, output):
    """
    Walk through `input` and search for every subfolder with at least 1 `nii` file.
    Record reference to `nii` files per subfolder.
    Run Data Input, Cross Sectional, Base and Long for all of them.
    Use only 1 core, run the step sequentially.
    """

    # Create output folder and add simlink to FSAVERAGE
    os.mkdir(output)
    os.symlink(FS_SUBJECT_FSAVERAGE,os.path.join(output,FSAVERAGE))

    input_nii = dict()
    cross_files = []
    os.environ["SUBJECTS_DIR"] = output

    for r,d,f in os.walk(input):
        for filename in f:
            if filename.endswith(NII_EXTENSION):
                timepoint = os.path.basename(r)
                if not timepoint in list(input_nii.keys()):
                    # Initialise
                    input_nii[timepoint] = list()
                input_nii[timepoint].append(os.path.join(r,filename))


    # DATA INPUT and  CROSS SECTIONAL PROCESSING
    print("Start DATA INPUT on %d Timepoints" % len(list(input_nii.keys())))
    for timepoint in sorted(input_nii.keys()):
        inputs = '-i '+' -i '.join(x for x in input_nii[timepoint])
        data_input_outputfolder =  "%s.cross.%s" % (subject, timepoint)

        "Example: recon-all -i in_data_path/s01/TP1/T1w_a.nii.gz -i in_data_path/s01/TP1/T1w_b.nii.gz -subjid s01.cross.TP1"
        command="recon-all -deface %s -subjid %s" % (inputs, data_input_outputfolder)
        runme(command)

        # Verify output
        try:
            assert os.path.isdir(os.path.join(output,data_input_outputfolder)), \
            "Output folder '%s' not found" % os.path.join(output,data_input_outputfolder)
        except AssertionError as ex:
            print(ex.message)
            return 1

        cross_files.append(data_input_outputfolder)

        print("Start CROSS SECTIONAL PROCESSING")
        "Example: recon-all -s s01.cross.TP1 -all"
        command="recon-all -deface -s %s -all" % data_input_outputfolder
        runme(command)


    # BASE PROCESSING
    print("Start BASE PROCESSING with %d timepoints" % len(cross_files))
    inputs = '-tp '+' -tp '.join(x for x in sorted(cross_files))
    basefile = "%s.base" % subject
    command = "recon-all -deface -base %s %s -all" % (basefile,inputs)
    runme(command)
    # XXX: what to verify here ?


    # LONG PROCESSING s01
    print("Start LONG PROCESSING with %d timepoints" % len(cross_files))
    for cross in sorted(cross_files):
        # Example: recon-all -long s01.cross.TP1 s01.base -all -qcache
        command = "recon-all -deface -long %s %s -all -qcache" % (cross,basefile)
        runme(command)
        # XXX: what to verify here ?

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
    if (len(sys.argv) != 4):
        sys.exit(Usage())
    sys.exit(RunFreesurfer(sys.argv[1], sys.argv[2], sys.argv[3]))
