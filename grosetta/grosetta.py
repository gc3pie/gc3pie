#! /usr/bin/env python
#
#   grosetta.py -- Front-end script for submitting ROSETTA jobs to SMSCG.
#
#   Copyright (C) 2010, 2011 GC3, University of Zurich
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
Front-end script for submitting ROSETTA jobs to SMSCG.

Exitcode tracks job status; use the "-b" option to get a 0/1 exit code.
The exitcode is a bitfield; the 4 least-significant bits have the following
meaning:
   ===    ============================================================
   Bit    Meaning
   ===    ============================================================
   0      Set if a fatal error occurred: `grosetta` could not complete
   1      Set if there are jobs in `FAILED` state
   2      Set if there are jobs in `RUNNING` or `SUBMITTED` state
   3      Set if there are jobs in `NEW` state
   ===    ============================================================
This boils down to the following rules:
   * exitcode == 0: all jobs are `DONE`, no further `grosetta` action
   * exitcode == 1: an error interrupted `grosetta` execution
   * exitcode == 2: all jobs finished, but some are in `FAILED` state
   * exitcode > 3: run `grosetta` again to progress jobs

See the output of ``grosetta --help`` for program usage instructions.
"""
# summary of user-visible changes
__changelog__ = """
  2010-12-20:
    * Initial release, forking off the old `grosetta`/`gdocking` sources.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'

import csv
import grp
import logging
import os
import os.path
from optparse import OptionParser
import pwd
import sys
import tarfile
import time


## interface to Gc3libs

import gc3libs
import gc3libs.application.rosetta
import gc3libs.Default
import gc3libs.core
import gc3libs.utils



## parse command-line

PROG = os.path.basename(sys.argv[0])

cmdline = OptionParser("%s [options] FLAGSFILE INPUT ... [: OUTPUT ...]" % PROG,
                       description="""
Run MiniRosetta on the specified INPUT files and fetch OUTPUT files
back from the execution machine; if OUTPUT is omitted, all '*.pdb',
'*.sc' and '*.fasc' files are retrieved.  Several instances can be run in
parallel, depending on the '-P' and '-p' options.

The `grosetta` command keeps a record of jobs (submitted, executed and
pending) in a session file (set name with the '-s' option); at each
invocation of the command, the status of all recorded jobs is updated,
output from finished jobs is collected, and a summary table of all
known jobs is printed.  New jobs are added to the session if the number 
of wanted decoys (option '-P') is raised.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; `grosetta` will delay submission
of newly-created jobs so that this limit is never exceeded.

Note: the list of INPUT and OUTPUT files must be separated by ':'
(on the shell command line, put a space before and after).
""")
cmdline.add_option("-b", action="store_true", dest="boolean_exitcode", default=False,
                   help="Set exitcode to 0 or 1, depending on whether this program"
                   " executed correctly or not, regardless of the submitted jobs status."
                   )
cmdline.add_option("-C", "--continuous", type="int", dest="wait", default=0,
                   metavar="INTERVAL",
                   help="Keep running, monitoring jobs and possibly submitting new ones or"
                   " fetching results every INTERVAL seconds. Exit when all jobs are finished."
                   )
cmdline.add_option("-c", "--cpu-cores", dest="ncores", type="int", default=1, # 1 core
                   metavar="NUM",
                   help="Require the specified number of CPU cores (default: %default)"
                   " for each Rosetta 'docking_protocol' job. NUM must be a whole number."
                   )
cmdline.add_option("-J", "--max-running", type="int", dest="max_running", default=50,
                   metavar="NUM",
                   help="Allow no more than NUM concurrent jobs (default: %default)"
                   " to be in SUBMITTED or RUNNING state."
                   )
cmdline.add_option("-l", "--list", action="store_true", dest="list", default=False,
                   help="List all submitted jobs and their statuses."
                   )
cmdline.add_option("-m", "--memory-per-core", dest="memory_per_core", type="int", default=2, # 2 GB
                   metavar="GIGABYTES",
                   help="Require that at least GIGABYTES (a whole number)"
                        " are available to each execution core. (Default: %default)")
cmdline.add_option("-N", "--new-session", dest="new_session", action="store_true", default=False,
                   help="Discard any information saved in the session file (see '--session' option)"
                   " and start a new session afresh.  Any information about previous jobs is lost.")
cmdline.add_option("-o", "--output", dest="output", default=os.getcwd(),
                   metavar='DIRECTORY',
                   help="Output files from all jobs will be collected in the specified"
                   " DIRECTORY path; by default, output files are placed in the same"
                   " directory where the corresponding input file resides.  If the"
                   " destination directory does not exist, it is created."
                   " Some special strings will be substituted into DIRECTORY,"
                   " to specify an output location that varies with the submitted job:"
                   " PATH is replaced by the directory where the session file resides;"
                   " INSTANCE is replaced by the sequential no. of the ROSETTA job;"
                   " DATE is replaced by the submission date in ISO format (YYYY-MM-DD);"
                   " TIME is replaced by the submission time formatted as HH:MM."
                   )
cmdline.add_option("-P", "--total-decoys", type="int", dest="total_decoys", 
                   default=1,
                   metavar="NUM",
                   help="Compute NUM decoys per input file (default: %default)."
                   )
cmdline.add_option("-p", "--decoys-per-job", type="int", dest="decoys_per_job", 
                   default=1,
                   metavar="NUM",
                   help="Compute NUM decoys in a single job (default: %default)."
                   " This parameter should be tuned so that the running time"
                   " of a single job does not exceed the maximum wall-clock time."
                   )
cmdline.add_option("-s", "--session", dest="session", 
                   default=os.path.join(os.getcwd(), 'grosetta.csv'),
                   metavar="FILE",
                   help="Use FILE to store the index of running jobs (default: '%default')."
                   " Any input files specified on the command line will be added to the existing"
                   " session.  Any jobs already in the session will be monitored and"
                   " their output will be fetched if the jobs are done."
                   )
cmdline.add_option("-v", "--verbose", type="int", dest="verbose", default=0,
                   metavar="LEVEL",
                   help="Increase program verbosity"
                   " (default is 0; any higher number may spoil screen output).",
                   )
cmdline.add_option("-w", "--wall-clock-time", dest="wctime", default=str(8), # 8 hrs
                   metavar="DURATION",
                   help="Each Rosetta job will run for at most DURATION time"
                   " (default: %default hours), after which it"
                   " will be killed and considered failed. DURATION can be a whole"
                   " number, expressing duration in hours, or a string of the form HH:MM,"
                   " specifying that a job can last at most HH hours and MM minutes."
                   )
cmdline.add_option("-x", "--protocol", dest="protocol", default="minirosetta.static",
                   metavar="PROTOCOL",
                   help="Run the specified Rosetta protocol/application; default: %default")
(options, args) = cmdline.parse_args()

# set up logging
loglevel = max(1, logging.ERROR - 10 * options.verbose)
gc3libs.configure_logger(loglevel)
logger = logging.getLogger()
logger.setLevel(loglevel)


# consistency check
if options.max_running < 1:
    cmdline.error("Argument to option -J/--max-running must be a positive integer.")
if options.total_decoys < 1:
    cmdline.error("Argument to option -P/--decoys-per-file must be a positive integer.")
if options.decoys_per_job < 1:
    cmdline.error("Argument to option -p/--decoys-per-job must be a positive integer.")
if options.wait < 0: 
    cmdline.error("Argument to option -C/--continuous must be a positive integer.")

n = options.wctime.count(":")
if 0 == n: # wctime expressed in hours
    duration = int(options.wctime)*60*60
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
options.walltime = int(options.wctime / 3600)

# XXX: ARClib errors out if the download directory already exists, so
# we need to make sure that each job downloads results in a new one.
# The easiest way to do so is to append 'INSTANCE' to the `output_dir`
# (if it's not already there).
if not 'INSTANCE' in options.output:
    options.output = os.path.join(options.output, 'INSTANCE')


# parse arguments
try:
    flags_file = args[0]; del args[0]
    if ':' in args:
        separator = args.index(':')
        inputs = args[:separator]
        outputs = args[(separator+1):]
    else:
        inputs = args
        outputs = []
except:
    sys.stderr.write("Incorrect usage; please run '%s --help' to read instructions." % sys.argv[0])

if not os.path.isabs(flags_file):
    flags_file = os.path.join(os.getcwd(), flags_file)
if not os.path.exists(flags_file):
    cmdline.error("Flags file '%s' does not exist." % flags_file)
    sys.exit(1)
logger.info("Using flags file '%s'", flags_file)

        

## create/retrieve session

def load(session, store):
    """
    Load all jobs from a previously-saved session file.
    The `session` argument can be any file-like object suitable
    for passing to Python's stdlib `csv.DictReader`.
    """
    result = [ ]
    for row in csv.DictReader(session,  # FIXME: field list must match `job` attributes!
                              ['instance', 'persistent_id', 'state', 'info', 'computed']):
        if row['instance'].strip() == '':
            # invalid row, skip
            continue 
        # resurrect saved state
        task = store.load(row['persistent_id'])
        # update state etc.
        task.update(row)
        # append to this list
        result.append(task)
    return result

def save(tasks, session, store):
    """
    Save tasks into a given session file.  The `session`
    argument can be any file-like object suitable for passing to
    Python's standard library `csv.DictWriter`.
    """
    for task in tasks:
        store.save(task)
        csv.DictWriter(session, 
                       ['instance', 'persistent_id', 'state', 'info', 'computed'], 
                       extrasaction='ignore').writerow(task)

# create a `Persistence` instance to save/load jobs

class _JobIdFactory(gc3libs.persistence.Id):
    """
    Override :py:class:`Id` behavior and generate IDs starting with a
    lowercase ``job`` prefix.
    """
    def __new__(cls, obj, prefix=None, seqno=None):
        return gc3libs.persistence.Id.__new__(cls, obj, 'job', seqno)

store = gc3libs.persistence.FilesystemStore(idfactory=_JobIdFactory)

# load the session file, or create a new empty one if not existing
try:
    session_file_name = os.path.realpath(options.session)
    if os.path.exists(session_file_name) and not options.new_session:
        session = file(session_file_name, "r+b")
    else:
        session = file(session_file_name, "w+b")
except IOError, x:
    logger.critical("Cannot open session file '%s' in read/write mode: %s. Aborting."
                     % (options.session, str(x)))
    sys.exit(1)
tasks = load(session, store)
session.close()


## compute number of decoys already being computed in this session
decoys = 0
for task in tasks:
    start, end = task.instance.split('--')
    decoys += int(end) - int(start)
logger.debug("Total no. of decoys already scheduled for computation: %d", decoys)


## build input file list

inputs_ = [ ]
for path in inputs:
    if not os.path.exists(path):
        logger.error("Cannot access input path '%s' - aborting.", path)
        sys.exit(1)
    else:
        # make paths absolute
        if not os.path.isabs(path):
            path = os.path.join(os.getcwd(), path)
        inputs_.append(path)
inputs = inputs_
logger.debug("Gathered input files: '%s'" % str.join("', '", inputs))


## compute job list
            
# add jobs to the session, until we are computing the specified number of decoys
# XXX: if the number of requested decoys is lowered, we should cancel jobs!
if decoys < options.total_decoys:
    if decoys > 0:
        logger.info("Already computing %d decoys for '%s', requested %d more.",
                    decoys, input, options.total_decoys - decoys)
    for nr in range(decoys, options.total_decoys, options.decoys_per_job):
        instance = ("%d--%d" 
                    % (nr, min(options.total_decoys, 
                               nr + options.decoys_per_job)))
        arguments = [ '-out:nstruct', str(options.decoys_per_job) ]
        tasks.append(gc3libs.application.rosetta.RosettaApplication(
            options.protocol,
            inputs,
            outputs,
            flags_file = flags_file,
            # set computational requirements
            requested_memory = options.memory_per_core,
            requested_cores = options.ncores,
            requested_walltime = options.walltime,
            # Rosetta-specific data
            arguments = arguments,
            # set job output directory
            output_dir = (
                options.output
                .replace('PATH', os.path.dirname(session_file_name) or os.getcwd())
                .replace('INSTANCE', instance)
                .replace('DATE', time.strftime('%Y-%m-%d', time.localtime(time.time())))
                .replace('TIME', time.strftime('%H:%M', time.localtime(time.time())))
                ),
            # grosetta-specific data
            instance = instance,
            ))


## iterate through job list, updating state and acting accordingly


def zerodict():
    """
    A dictionary that automatically creates keys 
    with value 0 on first reference.
    """
    def zero(): 
        return 0
    return gc3libs.utils.defaultdict(zero)

def compute_stats(tasks):
    """
    Return a dictionary mapping each state name into the count of
    jobs in that state. In addition, the following keys are defined:

      * `ok`:  count of TERMINATED jobs with return code 0

      * `failed`: count of TERMINATED jobs with nonzero return code
    """
    result = zerodict()
    for task in tasks:
        state = task.execution.state
        result[state] += 1
        if state == gc3libs.Run.State.TERMINATED:
            # need to distinguish failed jobs from successful ones
            if task.execution.returncode == 0:
                result['ok'] += 1
            else:
                result['failed'] += 1
    return result

def pprint(tasks, output=sys.stdout, session=None):
    """
    Output a summary table to stream `output`.
    """
    if len(tasks) == 0:
        if session is not None:
            print ("There are no jobs in session file '%s'." % session)
        else:
            print ("There are no jobs in session file.")
    else:
        output.write("%-15s  %-18s  %-s\n" 
                     % ("Decoys Nr.", "State (JobID)", "Info"))
        output.write(80 * "=" + '\n')
        for task in tasks:
            output.write("%-15s  %-18s  %-s\n" % 
                         (task.instance, 
                          ('%s (%s)' % (task.execution.state, task.persistent_id)), 
                          task.execution.info))

# create a `Core` instance to interface with the Grid middleware
grid = gc3libs.core.Core(*gc3libs.core.import_config([
            gc3libs.Default.CONFIG_FILE_LOCATION
            ]))

# create an `Engine` instance to manage the job list; we'll call its
# `progress` method in the main loop
engine = gc3libs.core.Engine(grid, tasks, store,
                             max_in_flight = options.max_running)

def main(tasks):
    # advance all jobs
    engine.progress()
    # write updated jobs to session file
    try:
        session = file(session_file_name, "wb")
        save(tasks, session, store)
        session.close()
    except IOError, ex:
        logger.error("Cannot save job status to session file '%s': %s"
                     % (session_file_name, str(ex)))
    ## print results to user
    stats = compute_stats(tasks)
    if options.list:
        pprint(tasks, sys.stdout)
    else:
        print ("Status of jobs in the '%s' session:" 
               % os.path.basename(session_file_name))
        total = len(tasks)
        if total > 0:
            for state in sorted(stats.keys()):
                print ("  %-10s  %d/%d (%.1f%%)"
                       % (state, stats[state], total, 
                          (100.0 * stats[state] / total)))
        else:
            print ("  No jobs in this session.")
    ## compute exitcode based on the running status of jobs
    rc = 0
    if stats['failed'] > 0:
        rc |= 2
    if stats[gc3libs.Run.State.RUNNING] > 0 or stats[gc3libs.Run.State.SUBMITTED] > 0:
        rc |= 4
    if stats[gc3libs.Run.State.NEW] > 0:
        rc |= 8
    return rc

rc = main(tasks)
if options.wait > 0:
    try:
        while rc > 3:
            time.sleep(options.wait)
            rc = main(tasks)
    except KeyboardInterrupt: # gracefully intercept Ctrl+C
        pass

if options.boolean_exitcode:
    rc &= 1
sys.exit(rc)
