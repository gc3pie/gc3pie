#! /usr/bin/env python
#
#   ggamess.py -- Front-end script for submitting multiple GAMESS jobs to SMSCG.
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
Front-end script for submitting multiple GAMESS jobs to SMSCG.

Exitcode tracks job status; use the "-b" option to get a 0/1 exit code.
The exitcode is a bitfield; the 4 least-significant bits have the following
meaning:
   ===    ============================================================
   Bit    Meaning
   ===    ============================================================
   0      Set if a fatal error occurred: `ggamess` could not complete
   1      Set if there are jobs in `FAILED` state
   2      Set if there are jobs in `RUNNING` or `SUBMITTED` state
   3      Set if there are jobs in `NEW` state
   ===    ============================================================
This boils down to the following rules:
   * exitcode == 0: all jobs are `DONE`, no further `ggamess` action
   * exitcode == 1: an error interrupted `ggamess` execution
   * exitcode == 2: all jobs finished, but some are in `FAILED` state
   * exitcode > 3: run `ggamess` again to progress jobs

See the output of ``ggamess --help`` for program usage instructions.
"""
# summary of user-visible changes
__changelog__ = """
  2010-12-20:
    * Initial release, forked off the ``ggamess`` sources.
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
import random
import shutil
import tarfile
import time


## interface to Gc3libs

import gc3libs
from gc3libs.application.gamess import GamessApplication
import gc3libs.core
import gc3libs.Default
import gc3libs.persistence
import gc3libs.utils



## parse command-line

PROG = os.path.basename(sys.argv[0])

cmdline = OptionParser("%s [options] INPUTDIR [INPUTDIR ...]" % PROG,
                       description="""
Scan the specified INPUTDIR directories recursively for '.inp' files,
and submit a GAMESS job for each input file found; job progress is
monitored and, when a job is done, its '.out' and '.dat' file are
retrieved back into the same directory where the '.inp' file is (this
can be overridden with the '-o' option).

The `ggamess` command keeps a record of jobs (submitted, executed and
pending) in a session file (set name with the '-s' option); at each
invocation of the command, the status of all recorded jobs is updated,
output from finished jobs is collected, and a summary table of all
known jobs is printed.  New jobs are added to the session if new input
files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; `ggamess` will delay submission
of newly-created jobs so that this limit is never exceeded.
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
                   " for each GAMESS job. NUM must be a whole number."
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
                   " NAME is replaced by the input file name;"
                   " DATE is replaced by the submission date in ISO format (YYYY-MM-DD);"
                   " TIME is replaced by the submission time formatted as HH:MM."
                   )
cmdline.add_option("-r", "--resource", action="store", dest="resource_name", metavar="STRING",
                   default=None, help='Select resource destination')
cmdline.add_option("-s", "--session", dest="session", 
                   default=os.path.join(os.getcwd(), 'ggamess.csv'),
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
                   help="Each GAMESS job will run for at most DURATION time"
                   " (default: %default hours), after which it"
                   " will be killed and considered failed. DURATION can be a whole"
                   " number, expressing duration in hours, or a string of the form HH:MM,"
                   " specifying that a job can last at most HH hours and MM minutes."
                   )
(options, args) = cmdline.parse_args()

# set up logging
loglevel = max(1, logging.ERROR - 10 * options.verbose)
gc3libs.configure_logger(loglevel, "ggamess")
logger = logging.getLogger("gc3.ggamess")
logger.setLevel(loglevel)
logger.propagate = True

# consistency check
if options.max_running < 1:
    cmdline.error("Argument to option -J/--max-running must be a positive integer.")
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
# The easiest way to do so is to append 'NAME' to the `output_dir`
# (if it's not already there).
if not 'NAME' in options.output:
    options.output = os.path.join(options.output, 'NAME')


## create/retrieve session

def load(session, store):
    """
    Load all jobs from a previously-saved session file.
    The `session` argument can be any file-like object suitable
    for passing to Python's stdlib `csv.DictReader`.
    """
    result = [ ]
    for row in csv.DictReader(session,  # FIXME: field list must match `job` attributes!
                              ['instance', 'persistent_id', 'state', 'info']):
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
                       ['instance', 'persistent_id', 'state', 'info'], 
                       extrasaction='ignore').writerow(task)

# create a `Persistence` instance to save/load jobs

store = gc3libs.persistence.FilesystemStore(idfactory=gc3libs.persistence.JobIdFactory)

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


## build input file set

old_inputs = set([ task.inp_file_path for task in tasks ])

new_inputs = set()
for path in args:
    if os.path.isdir(path):
        # recursively scan for .inp files
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                if filename.endswith('.inp'):
                    # like `inputs.append(dirpath + filename)` but make path absolute
                    inp = os.path.realpath(os.path.join(dirpath, filename))
                    if inp not in old_inputs:
                        new_inputs.add(inp)
    elif os.path.exists(path):
        inp = os.path.realpath(path)
        if inp not in old_inputs:
            new_inputs.add(inp)
    elif not path.endswith(".inp") and os.path.exists(path + ".inp"):
        inp = os.path.realpath(path + '.inp')
        if inp not in old_inputs:
            new_inputs.add(inp)
    else:
        logger.error("Cannot access input path '%s' - ignoring it.", path)
logger.debug("Gathered input files: '%s'" % str.join("', '", new_inputs))


## compute job list
            
# pre-allocate Job IDs
if len(new_inputs) > 0:
    gc3libs.persistence.Id.reserve(len(new_inputs))

# add new jobs to the session
random.seed()
for inp in new_inputs:
    inp_file_name = os.path.splitext(os.path.basename(inp))[0]
    tasks.append(GamessApplication(
        inp,
        # set computational requirements
        requested_memory = options.memory_per_core,
        requested_cores = options.ncores,
        requested_walltime = options.walltime,
        # set job output directory
        output_dir = (
            options.output
            .replace('PATH', os.path.dirname(inp) or os.getcwd())
            .replace('NAME', inp_file_name)
            .replace('DATE', time.strftime('%Y-%m-%d', time.localtime(time.time())))
            .replace('TIME', time.strftime('%H:%M', time.localtime(time.time())))
            # # use a temporary download directory; the
            # # GGamessApplication.postprocess() method will move files
            # # to the correct final destination
            # + "/tmp.%x" % random.randint(0, sys.maxint)
            ),
        # ggamess-specific data
        instance = inp_file_name,
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
                     % ("Input file", "State (JobID)", "Info"))
        output.write(80 * "=" + '\n')
        for task in tasks:
            output.write("%-15s  %-18s  %-s\n" % 
                         (task.instance, 
                          ('%s (%s)' % (task.execution.state, task.persistent_id)), 
                          task.execution.info))

# create a `Core` instance to interface with the Grid middleware
grid = gc3libs.core.Core(*gc3libs.core.import_config(
        gc3libs.Default.CONFIG_FILE_LOCATIONS
        ))

if options.resource_name:
    grid.select_resource(options.resource_name)
    gc3libs.log.info("Retained only resources: %s (restricted by command-line option '-r %s')",
                      str.join(",", [res['name'] for res in grid._resources]),
                      options.resource_name)

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
