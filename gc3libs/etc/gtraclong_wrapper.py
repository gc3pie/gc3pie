#!/usr/bin/env python

"""
export SUBJECTS_DIR=/our/out/data/path

************
SUBJECT: s01
************
trac-all -prep -c {dmrirc} -debug
trac-all -bedp -c {dmrirc}
trac-all -path -c {dmrirc}
"""

from __future__ import absolute_import, print_function, unicode_literals
import sys
import os
import subprocess

FSAVERAGE = "fsaverage"
FS_SUBJECT_FSAVERAGE = os.path.join(os.environ.get("FREESURFER_HOME", ''),"subjects",FSAVERAGE)

TRAC_CMD_STEP1="trac-all -prep -c {dmrirc} -debug"
TRAC_CMD_STEP2="trac-all -bedp -c {dmrirc}"
TRAC_CMD_STEP3="trac-all -path -c {dmrirc}"

TRAC_PIPELINE = [ TRAC_CMD_STEP1,
                  TRAC_CMD_STEP2,
                  TRAC_CMD_STEP3 ]

def Usage():
    print ("Usage: gtrac_wrapper.py <dmrirc file>")

def RunTrac(dmrirc_input):
    """
    For a given subject dir, execute the 3 trac steps sequentially.
    """

    # Verify input arguments
    assert os.path.isfile(dmrirc_input)

    ret = 0

    for step in TRAC_PIPELINE:
        cmd = step.format(dmrirc=dmrirc_input)
        print("Running '%s' " % (cmd,))
        (ret,stdout,stderr) = runme(cmd)
        if ret != 0:
            print("[failed]")
            print("Execution failed with exit code: %d" % ret)
            print("Output message: %s" % stdout)
            print("Error message: %s" % stderr)
            break
        else:
            print("[ok]")
    return ret

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

    (stdout, stderr) = proc.communicate()
    return (proc.returncode, stdout, stderr)

if __name__ == '__main__':
    if (len(sys.argv) != 2):
        sys.exit(Usage())
    sys.exit(RunTrac(sys.argv[1]))
