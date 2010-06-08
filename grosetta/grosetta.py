#! /usr/bin/env python
#
"""
Front-end script for submitting ROSETTA jobs to SMSCG.
"""
__docformat__ = 'reStructuredText'
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'


import sys
import os
import os.path
from optparse import OptionParser
import logging
import csv
import time


## defaults

rcdir = os.path.expanduser('~/.gc3')

class Struct(dict):
    """
    A `dict`-like object, whose keys can be accessed with the usual
    '[...]' lookup syntax, or with the '.' get attribute syntax.
    """
    def __init__(self, **kw):
        self.update(kw)
    def __setattr__(self, key, val):
        self[key] = val
    def __getattr__(self, key):
        return self[key]

class Job(Struct):
    """
    Instances of this class live a double life as (1) dictionary-like
    records storing information about a particular computational job
    instance; (2) interfaces to Grid middleware for managing the
    life-cycle of a computational job.
    """

    def __init__(self, **kw):
        Struct.__init__(self, **kw)
        self.xrsl = os.path.join(rcdir, 'rosetta.xrsl')
        self.mw = gc3utils.Gcli(*gc3utils.utils.import_config(config_file_path))
        self.app = gc3utils.Application.Application(
            application_tag = "rosetta",
            input_file_name = input_file_name,
            job_local_dir = options.job_local_dir,
            requested_memory = options.memory_per_core,
            requested_cores = options.ncores,
            requested_walltime = options.walltime,
            application_arguments = options.application_arguments
            )
        
    def submit(self):
        j = self.mw.gsub(self.app)
        gc3utils.utils.persist_job(j)
        self.jobid = j.unique_token

    def get_status(self):
        j = self.mw.gstat(gc3utils.utils.get_job(self.jobid))
        gc3utils.utils.persist_job(j)
        self.status = j.status

    def get_output(self):
        self.mw.gget(gc3utils.utils.get_job(self.jobid))
        gc3utils.utils.persist_job(j)


## parse command-line


cmdline = OptionParser("grosetta [options] [INPUT ...]",
                       description="""
Submit a ROSETTA docking job on each of the INPUT files, and collect
back output files when the jobs are done.  Each of the INPUT
parameters can be either a single '.pdb' file, or a directory, which
is recursively scanned for '.pdb' files.

The `grosetta` command keeps a record of the subitted jobs in a
session file; at each invocation of the command, jobs corresponding to
input files specified on the command line are recorded in the session,
the status of all recorded jobs is updated, and finally a table of all
known jobs is printed.
""")
cmdline.add_option("-w", "--wall-clock-time", dest="wctime", default=str(6*60*60), # 6 hrs
                   metavar="DURATION",
                   help="Each SMSCG job will run for at most DURATION time, after which it"
                   " will be killed and considered failed. DURATION can be a whole"
                   " number, expressing duration in seconds, or a string of the form HH:MM,"
                   " specifying that a job can last at most HH hours and MM minutes."
                   )
cmdline.add_option("-o", "--output", dest="output", default='PATH/NAME.INSTANCE',
                   metavar='DIRECTORY',
                   help="Output files from all jobs will be collected in the specified"
                   " DIRECTORY path; by default, output files are placed in the same"
                   " directory where the corresponding input file resides.  If the"
                   " destination directory does not exist, it is created."
                   " Some special strings will be substituted into DIRECTORY,"
                   " to specify an output location that varies with the submitted job:"
                   " NAME is replaced by the input file name (w/out the '.pdb' extension);"
                   " PATH is replaced by the directory where the input file resides;"
                   " INSTANCE is replaced by the sequential no. of the ROSETTA job;"
                   " DATE is replaced by the submission date in ISO format (YYYY-MM-DD);"
                   " TIME is replaced by the submission time formatted as HH:MM."
                   )
cmdline.add_option("-J", "--max-running", type="int", dest="max_running", default=50,
                   metavar="NUM",
                   help="Allow no more than NUM concurrent jobs in the grid."
                   )
cmdline.add_option("-P", "--passes-per-file", type="int", dest="passes_per_file", 
                   default=10000,
                   metavar="NUM",
                   help="Execute NUM passes of ROSETTA docking over each input file."
                   )
cmdline.add_option("-p", "--passes-per-job", type="int", dest="passes_per_job", 
                   default=15,
                   metavar="NUM",
                   help="Execute NUM passes of ROSETTA docking in a single job."
                   " This parameter should be tuned so that the running time"
                   " of a job does not exceed the maximum wall-clock time."
                   )
cmdline.add_option("-s", "--session", dest="session", 
                   default=os.path.join(rcdir, 'grosetta.csv'),
                   metavar="FILE",
                   help="Use FILE to store the status of running jobs.  Any input files"
                   " specified on the command line will be added to the existing"
                   " session.  Any jobs already in the session will be monitored and"
                   " their output will be fetched if the jobs are done."
                   )
cmdline.add_option("-c", "--continuous", type="int", dest="wait", default=0,
                   metavar="INTERVAL",
                   help="Keep running, monitoring jobs and possibly submitting new ones or"
                   " fetching results every INTERVAL seconds."
                   )
(options, args) = cmdline.parse_args()

# consistency check
if options.max_running < 1:
    cmdline.error("Argument to option -J/--max-running must be a positive integer.")
if options.passes_per_file < 1:
    cmdline.error("Argument to option -P/--passes-per-file must be a positive integer.")
if options.passes_per_job < 1:
    cmdline.error("Argument to option -p/--passes-per-job must be a positive integer.")
if options.wait < 0: 
    cmdline.error("Argument to option -c/--continuous must be a positive integer.")

n = options.wctime.count(":")
if 0 == n: # wctime expressed in seconds
    duration = int(options.wctime)
    if duration < 1:
        cmdline.error("Argument to option -w/--wall-clock-time must be a positive integer.")
    options.wctime = duration
elif 1 == n: # wctime expressed as 'HH:MM'
    hrs, mins = str.split(":", options.wctime)
    options.wctime = hrs*60*60 + mins*60
elif 2 == n: # wctime expressed as 'HH:MM:SS'
    hrs, mins, secs = str.split(":", options.wctime)
    options.wctime = hrs*60*60 + mins*60 + secs
else:
    cmdline.error("Argument to option -w/--wall-clock-time must have the form 'HH:MM' or be a duration expressed in seconds.")


## build input file list

inputs = []
for path in args:
    if os.path.isdir(path):
        # recursively scan for .pdb files
        for dirpath, dirnames, filenames in os.walk(inputs):
            for filename in filenames:
                if filename.endswith('.pdb'):
                    # like `inputs.append(dirpath + filename)` but make path absolute
                    inputs.append(os.path.realpath(os.path.join(dirpath, filename)))
    elif os.path.exists(path):
        inputs.append(path)
    elif not path.endswith(".pdb") and os.path.exists(path + ".pdb"):
        inputs.append(path + '.pdb')
    else:
        logging.error("Cannot access input path '%s' - ignoring it.", path)


## compute job list

jobs = dict() # use (NAME,INSTANCE) as primary key
for input in inputs:
    for nr in range(0, options.passes_per_file, options.passes_per_job):
        jobs[input, instance] = Struct(
            input = input,
            instance = ("%d--%d" % (nr, nr + options.passes_per_job)),
            status = 'CREATED',
            jobid = None,
            )


## create/retrieve session

try:
    session_file_name = os.path.realpath(options.session)
    if os.path.exists(session_file_name):
        session = file(session_file_name, "r+b")
    else:
        session = file(session_file_name, "w+b")
except IOError, x:
    logging.critical("Cannot open session file '%s' in read/write mode: %s. Aborting."
                     % (options.session, str(x)))
    sys.exit(1)

for row in csv.DictReader(session,  # FIXME: field list must match `job` attributes!
                          ['input', 'instance', 'status', 'jobid']):
    if jobs.has_key((row.input, row.instance)):
        # update status etc.
        jobs[row.input, row.instance].update(row)
    else:
        jobs[row.input, row.instance] = row


## iterate through job list, updating status and acting accordingly

def main(jobs):
    # print table header
    print ("%-15s  %-15s  %-15s  %-s" % ("Input file name", "Instance count", "Status", ""))
    print (80 * "=")
    # build table
    in_flight_count = 0
    for job in jobs.iteritems():
        if job.status == 'CREATED':
            # try to submit; go to 'SUBMITTED' if successful, 'FAILED' if not
            pass
        if job.status == 'SUBMITTED' or job.status == 'RUNNING':
            # update status 
            pass
        if job.status == 'EXECUTED':
            # get output; go to 'DONE' if successful, 'FAILED' if not
            pass
        if job.status == 'DONE':
            # nothing more to do - remove from job list
            del jobs[job.input, job.instance]
        if job.status == 'FAILED':
            # what should we do?
            # just keep the job around for a while and then remove it?
            pass
        print ("%(input)-15s  %(instance)-15s  %(status)-15s" % job)
    if len(jobs) == 0:
        print ("There are no jobs in session file '%s'." % options.session)

main(jobs)
if options.wait > 0:
    try:
        while True:
            time.sleep(options.wait)
            main(jobs)
    except KeyboardInterrupt: # gracefully intercept Ctrl+C
        pass

sys.exit(0)
