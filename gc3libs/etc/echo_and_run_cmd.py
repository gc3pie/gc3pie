import sys
import os
import subprocess


def Usage():
    print("Usage: echo_and_run_cmd.py <cmd>")
    print("Echoes and runs command")


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


def echo_and_run_cmd(cmd):
    print("[RUNNING]:\n'%s' " % cmd)
    (ret, stdout, stderr) = runme(cmd)

    # format byte return
    try:
        stdout = stdout.decode("utf-8")
    except:
        pass
    try:
        stderr = stderr.decode("utf-8")
    except:
        pass

    if ret != 0:
        print("[failed]: %s" % cmd)
        print("Execution failed with exit code: %d" % ret)
        print("Output message: %s" % stdout)
        print("Error message: %s" % stderr)
    else:
        print("[ok]: %s \n" % cmd)
        print(stdout)
    return ret


if __name__ == '__main__':
    if (len(sys.argv) < 2):
        sys.exit(Usage())
    cmd = ' '.join(sys.argv[1:])
    #fixme
    os.system("tree /data.nfs")
    os.system("ls -la /data.nfs/*/*/*/*")
    sys.exit(echo_and_run_cmd(cmd))
