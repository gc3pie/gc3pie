#! /usr/bin/env python
#
#   gcodeml.py -- Front-end script for submitting multiple CODEML jobs to SMSCG.
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
Front-end script for submitting multiple CODEML jobs to SMSCG.

Exitcode tracks job status; use the "-b" option to get a 0/1 exit code.
The exitcode is a bitfield; the 4 least-significant bits have the following
meaning:
   ===    ============================================================
   Bit    Meaning
   ===    ============================================================
   0      Set if a fatal error occurred: `gcodeml` could not complete
   1      Set if there are jobs in `FAILED` state
   2      Set if there are jobs in `RUNNING` or `SUBMITTED` state
   3      Set if there are jobs in `NEW` state
   ===    ============================================================
This boils down to the following rules:
   * exitcode == 0: all jobs are `DONE`, no further `gcodeml` action
   * exitcode == 1: an error interrupted `gcodeml` execution
   * exitcode == 2: all jobs finished, but some are in `FAILED` state
   * exitcode > 3: run `gcodeml` again to progress jobs

See the output of ``gcodeml --help`` for program usage instructions.
"""
# summary of user-visible changes
__changelog__ = """
  2010-12-20:
    * Initial release, forked off the ``ggamess`` sources.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__docformat__ = 'reStructuredText'

import csv
import fnmatch
import logging
import operator
import os
import os.path
from optparse import OptionParser
import re
import sys
import random
import shutil
import tarfile
import time


## interface to Gc3libs

import gc3libs
#from gc3libs.application.codeml import CodemlApplication
import gc3libs.core
import gc3libs.Default
import gc3libs.persistence
import gc3libs.utils



## parse command-line

PROG = os.path.basename(sys.argv[0])

cmdline = OptionParser("%s [options] INPUTDIR [INPUTDIR ...]" % PROG,
                       description="""
Scan the specified INPUTDIR directories recursively for '.ctl' files,
and submit a CODEML job for each input file found; job progress is
monitored and, when a job is done, its '.mlc' file is retrieved back
into the same directory where the '.ctl' file is (this can be
overridden with the '-o' option).

The `gcodeml` command keeps a record of jobs (submitted, executed and
pending) in a session file (set name with the '-s' option); at each
invocation of the command, the status of all recorded jobs is updated,
output from finished jobs is collected, and a summary table of all
known jobs is printed.  New jobs are added to the session if new input
files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; `gcodeml` will delay submission
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
                   " for each CODEML job. NUM must be a whole number."
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
                   default=os.path.join(os.getcwd(), 'gcodeml.csv'),
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
                   help="Each CODEML job will run for at most DURATION time"
                   " (default: %default hours), after which it"
                   " will be killed and considered failed. DURATION can be a whole"
                   " number, expressing duration in hours, or a string of the form HH:MM,"
                   " specifying that a job can last at most HH hours and MM minutes."
                   )
(options, args) = cmdline.parse_args()

# set up logging
loglevel = max(1, logging.ERROR - 10 * options.verbose)
gc3libs.configure_logger(loglevel, "gcodeml")
logger = logging.getLogger("gc3.gcodeml")
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


## The CODEML application

# split an line 'key = value' around the middle '=' and ignore spaces
_assignment_re = re.compile('\s* = \s*', re.X)

class CodemlApplication(gc3libs.Application):
    """
    Run a CODEML job with the specified '.ctl' files.
    
    The given '.ctl' input files are parsed and the '.phy' and
    '.nwk' files mentioned therein are added to the list of files
    to be copied to the execution site.
    """
    
    def __init__(self, *ctls, **kw):
        # need to send the binary and the PERL driver script
        inputs = [ 'codeml.pl', 'codeml' ]
        # for each '.ctl' file, extract the referenced "seqfile" and
        # "treefile" and add them to the input list
        for ctl in ctls:
            try:
                # try getting the seqfile/treefile path before we
                # append the '.ctl' file to inputs; if they cannot be
                # found, we do not append the '.ctl' either...
                for path in CodemlApplication.seqfile_and_treefile(ctl).values():
                    if path not in inputs:
                        inputs.append(path)
                inputs.append(ctl)
            # if the phy/nwk files are not found,
            # `seqfile_and_treefile` raises an exception; catch it
            # here and ignore the '.ctl' file as well.
            except RuntimeError, ex:
                logger.warning("Cannot find seqfile and/or treefile referenced in '%s'"
                               " - ignoring this input." % ctl)
        gc3libs.Application.__init__(
            self,
            executable = 'codeml.pl',
            arguments = [ os.path.basename(ctl) for ctl in ctls ],
            inputs = inputs,
            outputs = [ (os.path.basename(path)[:-4] + '.mlc')
                        for path in ctls ],
            stdout = 'stdout.txt',
            stderr = 'stderr.txt',
            # an estimation of wall-clock time requirements can be
            # derived from the '.phy' input file, use it to set the
            # `required_walltime` attribute, so we do not risk jobs
            # being killed because they exceed allotted running time
            #required_walltime = ...,
            **kw
            )

    # aux function to get thw seqfile and treefile paths
    @staticmethod
    def seqfile_and_treefile(ctl_path):
        """
        Return full path to the seqfile and treefile referenced in
        the '.ctl' file given as arguments.
        """
        dirname = os.path.dirname(ctl_path)
        def abspath(filename):
            if os.path.isabs(filename):
                return filename
            else:
                return os.path.realpath(os.path.join(dirname, filename))
        result = { }
        ctl = open(ctl_path, 'r')
        for line in ctl.readlines():
            # remove comments (from '*' to end-of line)
            line = line.split('*')[0]
            # remove leading and trailing whitespace
            line = line.strip()
            # ignore empty lines
            if len(line) == 0:
                continue
            key, value = _assignment_re.split(line, maxsplit=1)
            if key in [ 'seqfile', 'treefile' ]:
                result[key] = abspath(value)
            # shortcut: if we already have both 'seqfile' and
            # 'treefile', there's no need for scanning the file
            # any more.
            if len(result) == 2:
                ctl.close()
                return result
        # if we get to this point, the ``seqfile = ...`` and
        # ``treefile = ...`` lines were not found; signal this to the
        # caller by raising an exception
        ctl.close()
        raise RuntimeError("Could not extract path to seqfile and/or treefile from '%s'"
                           % ctl_path)


    def postprocess(self, download_dir):
        """
        Set the exit code of a `CodemlApplication` job by inspecting its
        ``.mlc`` output files.

        An output file is valid iff its last line of each output file
        reads ``Time used: HH:M``.

        The exit status of the whole job is set to one of these values:

        *  0 -- all files processed successfully
        *  1 -- some files were *not* processed successfully
        *  2 -- no files processed successfully
        * 127 -- the ``codeml`` application did not run at all.
         
        """
        # XXX: when should we consider an application "successful" here?
        # In the Rosetta ``docking_protocol`` application, the aim is to get
        # at least *some* decoys generated: as long as there are a few decoys
        # in the output, we do not care about the rest.  Is this approach ok
        # in Codeml/Selectome as well?
        
        # Except for "signal 125" (submission to batch system failed),
        # any other error condition may result in some output files having
        # been computed/generated, so let us continue in those cases and
        # not care about the exit signal...
        if self.execution.signal == 125:
            # submission failed, job did not run at all
            self.execution.exitcode = 127
            return

        total = len(self.outputs)
        # form full-path to the output files
        outputs = [ os.path.join(download_dir, filename) 
                    for filename in fnmatch.filter(os.listdir(download_dir), '*.mlc') ]
        if len(outputs) == 0:
            # no output retrieved, did ``codeml`` run at all?
            self.execution.exitcode = 127
            return
        # count the number of successfully processed files
        failed = 0
        for mlc in outputs:
            output_file = open(mlc, 'r')
            last_line = output_file.readlines()[-1]
            output_file.close()
            if not last_line.startswith('Time used: '):
                failed += 1
        # set exit code and informational message
        if failed == 0:
            self.execution.exitcode = 0
            self.info = "All files processed successfully, output downloaded to '%s'" % download_dir
        elif failed < total:
            self.execution.exitcode = 1
            self.info = "Some files *not* processed successfully, output downloaded to '%s'" % download_dir
        else:
            self.execution.exitcode = 2
            self.info = "No files processed successfully, output downloaded to '%s'" % download_dir
        return


## create/retrieve session

def load(session, store):
    """
    Load all jobs from a previously-saved session file.
    The `session` argument can be any file-like object suitable
    for passing to Python's stdlib `csv.DictReader`.
    """
    result = [ ]
    for row in csv.DictReader(session,  # FIXME: field list must match `job` attributes!
                              ['job_name', 'persistent_id', 'state', 'info']):
        if row['job_name'].strip() == '':
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
                       ['job_name', 'persistent_id', 'state', 'info'], 
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

old_inputs = set([ (task.job_name + '.ctl') for task in tasks ])

new_inputs = [ ]
def _maybe_add_to_inputs(ctl):
    name = os.path.basename(ctl)
    if name in old_inputs:
        logger.debug("Input file '%s' not new - skipping it." % ctl)
    elif ((ctl not in new_inputs) 
          and (os.path.basename(ctl) not in old_inputs)):
        new_inputs.append(os.path.realpath(ctl))

for path in args:
    if os.path.isdir(path):
        # recursively scan for .ctl files
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                if filename.endswith('.ctl'):
                    _maybe_add_to_inputs(os.path.join(dirpath, filename))
    elif path.endswith('.ctl') and os.path.exists(path):
        _maybe_add_to_inputs(path)
    elif not path.endswith('.ctl') and os.path.exists(path + '.ctl'):
        _maybe_add_to_inputs(os.path.realpath(path + '.ctl'))
    else:
        logger.error("Cannot access input path '%s' - ignoring it.", path)
logger.debug("Gathered input files: '%s'" % str.join("', '", [t[0] for t in new_inputs]))


## compute job list
            
# pre-allocate Job IDs
if len(new_inputs) > 0:
    gc3libs.persistence.Id.reserve(len(new_inputs))

# add new jobs to the session
random.seed()
for ctl in new_inputs:
    # job name is the input file basename, minus '.ctl' extension
    job_name = os.path.splitext(os.path.basename(ctl))[0]
    # create a new CodemlApplication
    tasks.append(CodemlApplication(
        ctl,
        # set computational requirements
        requested_memory = options.memory_per_core,
        requested_cores = options.ncores,
        requested_walltime = options.walltime,
        # set job output directory
        output_dir = (
            options.output
            .replace('PATH', os.path.dirname(ctl) or os.getcwd())
            .replace('NAME', job_name)
            .replace('DATE', time.strftime('%Y-%m-%d', time.localtime(time.time())))
            .replace('TIME', time.strftime('%H:%M', time.localtime(time.time())))
            ),
        job_name = job_name,
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
                     % ("Job name", "State (JobID)", "Info"))
        output.write(80 * "=" + '\n')
        for task in tasks:
            output.write("%-15s  %-18s  %-s\n" % 
                         (os.path.basename(task.job_name),
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
