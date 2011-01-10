#! /usr/bin/env python
#
#   gdocking.py -- Front-end script for submitting ROSETTA `docking_protocol` jobs to SMSCG.
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
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
# summary of user-visible changes
__changelog__ = """
  2010-12-20:
    * Renamed to ``gdocking``; default session file is not ``gdocking.csv``.
  2010-09-21:
    * Do not collect output FASC and PDB files into a single tar file. 
      (Get old behavior back with the '-t' option)
    * Do not compress PDB files in Rosetta output. 
      (Get the old behavior back with the '-z' option)
  2010-08-09:
    * Exitcode tracks job status; use the "-b" option to get the old behavior back.
      The new exitcode is a bitfield; the 4 least-significant bits have the following
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
    * when all jobs are finished, exit `grosetta` even if the "-C" option is given
    * Print only summary of job statuses; use the "-l" option to get the long listing
  2010-07-26:
    * Default output directory is now './' (should be less surprising to users).
    * FASC and PDBs are now collected in the output directory.
  2010-07-15:
    * After successful retrieval of job information, reorder output files so that:
      - for each sumitted job, there is a corresponding ``input.N--M.fasc`` file,
        in the same directory as the input ".pdb" file;
      - all decoys belonging to the same input ".pdb" file are collected into 
        a single ``input.decoys.tar`` file (in the same dir as the input ".pdb" file);
      - output from grid jobs is kept untouched in the "job.XXX/" directories.
    * Compress PDB files by default, and prefix them with a "source filename + N--M" prefix
    * Number of computed decoys can now be increased from the command line:
      if `grosetta` is called with different '-P' and '-p' options, it will
      add new jobs to the list so that the total number of decoys per input file
      (including already-submitted ones) is up to the new total.
    * New '-N' command-line option to discard old session contents and start a new session afresh.
  2010-07-14:
    * Default session file is now './grosetta.csv', so it's not hidden to users.
"""
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
import gc3libs.collections
import gc3libs.Default
import gc3libs.core
import gc3libs.utils



## parse command-line

PROG = os.path.basename(sys.argv[0])

cmdline = OptionParser("%s [options] [INPUT ...]" % PROG,
                       description="""
Compute decoys of specified '.pdb' files by running several
Rosetta 'docking_protocol' instances in parallel.

The `grosetta` command keeps a record of jobs (submitted, executed and
pending) in a session file; at each invocation of the command, the
status of all recorded jobs is updated, output from finished jobs is
collected, and a summary table of all known jobs is printed.

If any INPUT argument is specified on the command line, `grosetta`
appends new jobs to the session file, up to the quantity needed
to compute the requested number of decoys.  Each of the INPUT
parameters can be either a single '.pdb' file, or a directory, which
is recursively scanned for '.pdb' files.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; `grosetta` will delay submission
of newly-created jobs so that this limit is never exceeded.
""")
cmdline.add_option("-b", action="store_true", dest="boolean_exitcode", default=False,
                   help="Set exitcode to 0 or 1, depending on whether this program"
                   " executed correctly or not, regardless of the submitted jobs status."
                   )
cmdline.add_option("-C", "--continuous", type="int", dest="wait", default=0,
                   metavar="INTERVAL",
                   help="Keep running, monitoring jobs and possibly submitting new ones or"
                   " fetching results every INTERVAL seconds."
                   )
cmdline.add_option("-c", "--cpu-cores", dest="ncores", type="int", default=1, # 1 core
                   metavar="NUM",
                   help="Require the specified number of CPU cores (default: %default)"
                   " for each Rosetta 'docking_protocol' job. NUM must be a whole number."
                   )
cmdline.add_option("-f", "--flags-file", dest="flags_file", 
                   default=os.path.join(gc3libs.Default.RCDIR, 'docking_protocol.flags'),
                   metavar="PATH",
                   help="Pass the specified flags file to Rosetta 'docking_protocol'"
                   " Default: '~/.gc3/docking_protocol.flags'"
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
                   " NAME is replaced by the input file name (w/out the '.pdb' extension);"
                   " PATH is replaced by the directory where the input file resides;"
                   " INSTANCE is replaced by the sequential no. of the ROSETTA job;"
                   " DATE is replaced by the submission date in ISO format (YYYY-MM-DD);"
                   " TIME is replaced by the submission time formatted as HH:MM."
                   )
cmdline.add_option("-P", "--decoys-per-file", type="int", dest="decoys_per_file", 
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
                   default=os.path.join(os.getcwd(), 'gdocking.csv'),
                   metavar="FILE",
                   help="Use FILE to store the index of running jobs (default: '%default')."
                   " Any input files specified on the command line will be added to the existing"
                   " session.  Any jobs already in the session will be monitored and"
                   " their output will be fetched if the jobs are done."
                   )
cmdline.add_option("-t", "--tarfile", dest="tarfile", default=False, action="store_true",
                   help="Collect all output PDB and FASC/SC files into a single '.tar' file,"
                   " located in the output directory (see the '-o' option)."
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
cmdline.add_option("-z", "--compress-pdb", dest="compress", default=False, action="store_true",
                   help="Compress '.pdb' output files with `gzip`."
                   )
(options, args) = cmdline.parse_args()

# set up logging
loglevel = max(1, logging.ERROR - 10 * options.verbose)
gc3libs.configure_logger(loglevel)
logger = logging.getLogger("gc3.gdocking")
logger.setLevel(loglevel)
logger.propagate = True

# consistency check
if options.max_running < 1:
    cmdline.error("Argument to option -J/--max-running must be a positive integer.")
if options.decoys_per_file < 1:
    cmdline.error("Argument to option -P/--decoys-per-file must be a positive integer.")
if options.decoys_per_job < 1:
    cmdline.error("Argument to option -p/--decoys-per-job must be a positive integer.")
if options.wait < 0: 
    cmdline.error("Argument to option -C/--continuous must be a positive integer.")

if not os.path.isabs(options.flags_file):
    options.flags_file = os.path.join(os.getcwd(), options.flags_file)
if not os.path.exists(options.flags_file):
    cmdline.error("Flags file '%s' does not exist." % options.flags_file)
logger.info("Using flags file '%s'", options.flags_file)

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
    options.output = os.path.join(options.output, 'NAME.INSTANCE')

## interface to GC3Libs

class GRosettaApplication(gc3libs.application.rosetta.RosettaDockingApplication):
    """
    Augment a `RosettaDockingApplication` with state transition
    methods that implement job status reporting for the UI, and data
    post-processing.
    """
    def __init__(self, pdb_file_path, native_file_path=None, 
                 number_of_decoys_to_create=1, flags_file=None, **kw):
        gc3libs.application.rosetta.RosettaDockingApplication.__init__(
            self, pdb_file_path, native_file_path, 
            number_of_decoys_to_create, flags_file, 
            **kw)
        # save pdb_file_path for later processing
        self.pdb_file_path = pdb_file_path
        # define additional attributes
        self.computed = 0 # number of decoys actually computed by this job

    def postprocess(self, output_dir):
        # work directory is the parent of the download directory
        work_dir = os.path.dirname(output_dir)
        # move around output files so they're easier to preprocess:
        #   1. All '.fasc' files land in the same directory as the input '.pdb' file
        #   2. All generated '.pdb'/'.pdb.gz' files are collected in a '.decoys.tar'
        #   3. Anything else is left as-is
        input_name = os.path.basename(self.pdb_file_path)
        input_name_sans = os.path.splitext(input_name)[0]
        output_tar_filename = os.path.join(output_dir, 'docking_protocol.tar.gz')
        # count: 'protocols.jobdist.main: Finished 1brs.0--1.1brs_0002 in 149 seconds.'
        if os.path.exists(output_tar_filename):
            output_tar = tarfile.open(output_tar_filename, 'r:gz')
            # single tar file holding all decoy .PDB files
            pdbs_tarfile_path = os.path.join(work_dir, input_name_sans) + '.decoys.tar'
            if options.tarfile:
                if not os.path.exists(pdbs_tarfile_path):
                    pdbs = tarfile.open(pdbs_tarfile_path, 'w')
                else:
                    pdbs = tarfile.open(pdbs_tarfile_path, 'a')
            for entry in output_tar:
                if (entry.name.endswith('.fasc') or entry.name.endswith('.sc')):
                    filename, extension = os.path.splitext(entry.name)
                    scoring_file_name = (os.path.join(work_dir, input_name_sans) 
                                      + '.' + self.instance + extension)
                    src = output_tar.extractfile(entry)
                    dst = open(scoring_file_name, 'wb')
                    dst.write(src.read())
                    dst.close()
                    src.close()
                elif (options.tarfile and 
                      (entry.name.endswith('.pdb.gz') or entry.name.endswith('.pdb'))):
                    src = output_tar.extractfile(entry)
                    dst = tarfile.TarInfo(entry.name)
                    dst.size = entry.size
                    dst.type = entry.type
                    dst.mode = entry.mode
                    dst.mtime = entry.mtime
                    dst.uid = os.getuid()
                    dst.gid = os.getgid()
                    dst.uname = pwd.getpwuid(os.getuid()).pw_name
                    dst.gname = grp.getgrgid(os.getgid()).gr_name
                    if hasattr(entry, 'pax_headers'):
                        dst.pax_headers = entry.pax_headers
                    pdbs.addfile(dst, src)
                    src.close()
            if options.tarfile:
                pdbs.close()
        else: # no `docking_protocol.tar.gz` file
            self.info = ("No 'docking_protocol.tar.gz' file found.")

        
## create/retrieve session

def load(session, store):
    """
    Load all jobs from a previously-saved session file.
    The `session` argument can be any file-like object suitable
    for passing to Python's stdlib `csv.DictReader`.
    """
    result = [ ]
    for row in csv.DictReader(session,  # FIXME: field list must match `job` attributes!
                              ['id', 'persistent_id', 'state', 'info', 'computed']):
        if row['id'].strip() == '':
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
        csv.DictWriter(session, ['id', 'persistent_id', 'state', 'info', 'computed'], 
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


## compute number of decoys already entered for each input file
def zerodict():
    """
    A dictionary that automatically creates keys 
    with value 0 on first reference.
    """
    def zero(): 
        return 0
    return gc3libs.utils.defaultdict(zero)

decoys = zerodict()
for task in tasks:
    start, end = task.instance.split('--')
    input = task.pdb_file_path
    decoys[input] += int(end) - int(start) + 1
    logger.debug("Task '%s': total no. of decoys being computed for input '%s': %d", 
                 task.persistent_id, task.pdb_file_path, decoys[input])


## build input file list

inputs = set([ task.pdb_file_path for task in tasks ])
for path in args:
    if os.path.isdir(path):
        # recursively scan for .pdb files
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                if filename.endswith('.pdb'):
                    # like `inputs.append(dirpath + filename)` but make path absolute
                    inputs.append(os.path.realpath(os.path.join(dirpath, filename)))
    elif os.path.exists(path):
        inputs.add(path)
    elif not path.endswith(".pdb") and os.path.exists(path + ".pdb"):
        inputs.add(path + '.pdb')
    else:
        logger.error("Cannot access input path '%s' - ignoring it.", path)
logger.debug("Gathered input files: '%s'" % str.join("', '", inputs))


## compute job list
            
for input in inputs:
    if decoys[input] < options.decoys_per_file-1:
        if decoys[input] > 0:
            logger.info("Already computing %d decoys for '%s', requested %d more.",
                        decoys[input], input, options.decoys_per_file - 1 - decoys[input])
        for nr in range(decoys[input], options.decoys_per_file, options.decoys_per_job):
            instance = ("%d--%d" 
                        % (nr, min(options.decoys_per_file - 1, 
                                   nr + options.decoys_per_job - 1)))
            prefix = os.path.splitext(os.path.basename(input))[0] + '.' + instance + '.'
            arguments = [ '-out:prefix', prefix ]
            if options.compress:
                arguments.append('-out:pdb_gz') # compress PDB output files
            tasks.append(GRosettaApplication(
                input, # == pdb_file_path
                flags_file = options.flags_file,
                # set computational requirements
                requested_memory = options.memory_per_core,
                requested_cores = options.ncores,
                requested_walltime = options.walltime,
                # Rosetta-specific data
                number_of_decoys_to_create = options.decoys_per_job,
                arguments = arguments,
                # set job output directory
                output_dir = (
                    options.output
                    .replace('NAME', os.path.basename(input))
                    .replace('PATH', os.path.dirname(input) or os.getcwd())
                    .replace('INSTANCE', instance)
                    .replace('DATE', time.strftime('%Y-%m-%d', time.localtime(time.time())))
                    .replace('TIME', time.strftime('%H:%M', time.localtime(time.time())))
                    ),
                # grosetta-specific data
                id = prefix[:-1], # except the trailing '.'
                input = input,
                instance = instance,
                ))


## iterate through job list, updating state and acting accordingly


# create a `Core` instance to interface with the Grid middleware
grid = gc3libs.core.Core(*gc3libs.core.import_config([gc3libs.Default.CONFIG_FILE_LOCATION]))

# create an `Engine` instance to manage the job list; we'll call its
# `progress` method in the main loop
engine = gc3libs.core.Engine(grid, tasks, store,
                             max_in_flight = options.max_running)

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
        output.write("%-15s  %-15s  %-18s  %-s\n" 
                     % ("Input file name", "Decoys Nr.", "State (JobID)", "Info"))
        output.write(80 * "=" + '\n')
        for task in tasks:
            output.write("%-15s  %-15s  %-18s  %-s\n" % 
                         (os.path.basename(task.pdb_file_path), task.instance, 
                          ('%s (%s)' % (task.execution.state, task.persistent_id)), 
                          task.execution.info))

def main(tasks):
    # advance all jobs
    engine.progress()
    # write updated jobs to session file
    try:
        session = file(session_file_name, "wb")
        save(tasks, session, store)
        session.close()
    except IOError, x:
        logger.error("Cannot save job status to session file '%s': %s"
                     % (session_file_name, str(x)))
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
