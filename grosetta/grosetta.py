#! /usr/bin/env python
#
#   grosetta.py -- Front-end script for submitting ROSETTA jobs to SMSCG.
#
#   Copyright (C) 2010 GC3, University of Zurich
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
  2010-08-09:
    * Exitcode tracks job status; use the "-b" option to get the old behavior back.
      The new exitcode is a bitfield; the 4 least-significant bits have the following
      meaning:
         Bit    Meaning
         ===    ============================================================
         0      Set if a fatal error occurred: `grosetta` could not complete
         1      Set if there are jobs in `FAILED` state
         2      Set if there are jobs in `RUNNING` or `SUBMITTED` state
         3      Set if there are jobs in `NEW` state
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

import gc3utils
import gc3utils.Default
import gc3utils.gcli
from gc3utils.Job import Job as Gc3utilsJob
import gc3utils.utils


## interface to Gc3Utils

from gc3utils.Application import RosettaDockingApplication

## common code with gmtkn24 -- will be migrated into gc3libs someday

def zerodict():
    """
    A dictionary that automatically creates keys 
    with value 0 on first reference.
    """
    def zero(): 
        return 0
    return gc3utils.utils.defaultdict(zero)

class Job(Gc3utilsJob):
    """
    A small extension to `gc3utils.Job.Job`, with a few convenience
    extensions.
    """
    def __init__(self, **kw):
        kw.setdefault('log', list())
        kw.setdefault('state', None) # user-visible status 
        kw.setdefault('status', -1)  # gc3utils status (internal use only)
        kw.setdefault('timestamp', gc3utils.utils.defaultdict(time.time))
        Gc3utilsJob.__init__(self, **kw)
    def is_valid(self):
        # override validity checks -- this will not be a valid `Gc3utilsJob`
        # object until Grid.submit() is called on it ...
        return True

    def set_info(self, msg):
        self.info = msg
        self.log.append(msg)

    def set_state(self, state):
        if state != self.state:
            self.state = state
            epoch = time.time()
            self.timestamp[state] = epoch
            if state == 'NEW':
                self.created = epoch
            self.set_info(state.capitalize() + ' at ' + time.asctime(time.localtime(epoch)))
        

def _get_state_from_gc3utils_job_status(status):
    """
    Convert a gc3utils.Job status code into a readable state description.
    """
    try:
        return {
           -1:'NEW', # XXX: local addition!
            1:'FINISHING',
            2:'RUNNING',
            3:'FAILED',
            4:'SUBMITTED',
            5:'DONE',
            6:'DELETED',
            7:'UNKNOWN',
            }[status]
    except KeyError:
        return 'UNKNOWN'

class Grid(object):
    """
    An interface to job lifecycle management.
    """
    def __init__(self, config_file=gc3utils.Default.CONFIG_FILE_LOCATION, default_output_dir=None):
        self.mw = gc3utils.gcli.Gcli(*gc3utils.gcli.import_config(config_file))
        self.default_output_dir = default_output_dir

    def save(self, job):
        """
        Save a job using gc3utils' persistence mechanism.
        """
        # `unique_token` is added by gc3utils.gcli.gsub() upon
        # successful submission, so it may lack on newly-created objects
        if job.has_key('unique_token'):
            job.jobid = job.unique_token
        # update state so that it is correctly saved, but do not use
        # set_state() so the job history is not altered; if
        # `job.status` is not present, then the job has not yet been
        # submitted and its state should default to "NEW"
        job.state = _get_state_from_gc3utils_job_status(job.get('status', -1)) # XXX: should be gc3utils.Job.JOB_STATE_NEW
        gc3utils.Job.persist_job(job)

    def submit(self, application, job=None):
        """
        Submit an instance of the given `application`, and store it
        into `job` (or a new instance of the `Job` class if `job` is
        `None`).  After successful submission, persist job to
        permanent storage.  Return (string) id of submitted job.
        """
        job = self.mw.gsub(application, job)
        return job.unique_token

    def update_state(self, job):
        """
        Update running status of `job`.  In case update fails, state
        is set to 'UNKNOWN'.  Return job state.
        """
        st = self.mw.gstat(job) # `gstat` returns list(!?)
        if len(st) == 0:
            job.set_state('UNKNOWN')
            job.set_info("Could not update job status.")
        else:
            job.set_state(_get_state_from_gc3utils_job_status(job.status))
        return job.state

    def get_output(self, job, output_dir=None):
        """
        Retrieve job's output files into `output_dir`.
        If `output_dir` is `None` (default), then use
        `self.output_dir`. 
        """
        # `job_local_dir` is where gc3utils will retrieve the output
        if output_dir is not None:
            job.job_local_dir = output_dir
        elif self.default_output_dir is not None:
            job.job_local_dir = self.default_output_dir
        else:
            job.job_local_dir = os.getcwd()
        self.mw.gget(job)
        job.output_retrieved_to = os.path.join(job.job_local_dir, job.jobid)
        # XXX: Need to reset the `output_processing_done` attribute, 
        # but the `persist_job` fn in gc3utils.Job will only do an update
        # to the persisted file, so that deleting an attribute does not work.
        # We work this around by setting the value to `None`, since this is 
        # what we use as sentinel value later on, but gc3utils.Job.persist_job()
        # definitely needs fixing
        if job.has_key('output_processing_done'):
            job.output_processing_done = None

    def progress(self, job, can_submit=True, can_retrieve=True):
        """
        Update the job's state and take appropriate action;
        return the (possibly changed) job state.

        If optional argument `can_submit` is `True` (default), will
        try to submit jobs in state ``NEW``.  If optional argument
        `can_retrieve` is `False` (default), will try to fetch job
        results back.
        """
        # update status of SUBMITTED/RUNNING jobs before launching new ones, otherwise
        # we would be checking the status of some jobs twice...
        if job.state == 'SUBMITTED' or job.state == 'RUNNING':
            # update state 
            try:
                self.update_state(job)
            except Exception, x:
                logger.error("Ignoring error in updating state of job '%s': %s: %s"
                             % (job.id, x.__class__.__name__, str(x)),
                             exc_info=True)
        if job.state == 'NEW' and can_submit:
            # try to submit; go to 'SUBMITTED' if successful, 'FAILED' if not
            try:
                self.submit(job.application, job)
                job.set_state('SUBMITTED')
            except Exception, x:
                logger.error("Error in submitting job '%s': %s: %s"
                             % (job.id, x.__class__.__name__, str(x)))
                job.set_state('NEW')
                job.set_info("Submission failed: %s" % str(x))
        if can_retrieve and job.state == 'FINISHING':
            # get output; go to 'DONE' if successful, ignore errors so we retry next time
            try:
                self.get_output(job, job.output_dir)
                job.set_state('DONE')
                job.set_info("Results retrieved into directory '%s'" 
                             % os.path.join(job.output_dir, job.jobid))
            except Exception, x:
                logger.error("Got error in fetching output of job '%s': %s: %s" 
                             % (job.id, x.__class__.__name__, str(x)), exc_info=True)
        if can_retrieve and (job.state == 'FAILED') and not job.get('output_retrieved_to', None):
            # try to get output, ignore errors as there might be no output
            try:
                self.get_output(job, job.output_dir)
                job.status = gc3utils.Job.JOB_STATE_FAILED # patch
                job.set_info("Output retrieved into directory '%s/%s'" 
                             % (job.output_dir, job.jobid))
            except Exception, x:
                logger.error("Got error in fetching output of job '%s': %s: %s" 
                             % (job.id, x.__class__.__name__, str(x)), exc_info=True)
                job.set_info("No output could be retrieved.")
        self.save(job)
        return job.state


class JobCollection(dict):
    """
    A collection of `Job` objects, indexed and accessible by `(input,
    instance)` pair.
    """
    def __init__(self, **kw):
        self.default_job_initializer = kw

    def add(self, job):
        """Add a `Job` instance to the collection."""
        self[job.id] = job
    def __iadd__(self, job):
        self.add(job)
        return self

    def remove(self, job):
        """Remove a `Job` instance from the collection."""
        del self[job.id]

    def load(self, session):
        """
        Load all jobs from a previously-saved session file.
        The `session` argument can be any file-like object suitable
        for passing to Python's stdlib `csv.DictReader`.
        """
        for row in csv.DictReader(session,  # FIXME: field list must match `job` attributes!
                                  ['id', 'jobid', 'state', 'info', 'history']):
            if row['id'].strip() == '':
                # invalid row, skip
                continue 
            id = row['id']
            if not self.has_key(id):
                self[id] = Job(unique_token=row['jobid'], ** self.default_job_initializer)
            job = self[id]
            # update state etc.
            job.update(row)
            # resurrect saved state
            job.update(gc3utils.Job.get_job(job.jobid))
            # convert 'history' into a list
            job.log = job.history.split("; ")
            # get back timestamps of various events
            for event in job.log:
                if event.upper().startswith('CREATED'):
                    job.created = time.mktime(time.strptime(event.split(' ',2)[2]))
                if event.upper().startswith('SUBMITTED'):
                    job.timestamp['SUBMITTED'] = time.mktime(time.strptime(event.split(' ',2)[2]))
                if event.upper().startswith('RUNNING'):
                    job.timestamp['RUNNING'] = time.mktime(time.strptime(event.split(' ',2)[2]))
                if event.upper().startswith('FINISHING'):
                    job.timestamp['FINISHING'] = time.mktime(time.strptime(event.split(' ',2)[2]))
                if event.upper().startswith('DONE'):
                    job.timestamp['DONE'] = time.mktime(time.strptime(event.split(' ',2)[2]))

    def save(self, session):
        """
        Save all jobs into a given session file.  The `session`
        argument can be any file-like object suitable for passing to
        Python's standard library `csv.DictWriter`.
        """
        for job in self.values():
            job.history = str.join("; ", job.log)
            csv.DictWriter(session, ['id', 'jobid', 'state', 'info', 'history'], 
                           extrasaction='ignore').writerow(job)

    def stats(self):
        """
        Return a dictionary mapping each state name into the count of
        jobs in that state; an additional key 'total' maps to the
        total number of jobs in this collection.
        """
        result = zerodict()
        for job in self.values():
            result[job.state] += 1
        return result

    # XXX: this is not part of the to-be gc3libs class `JobCollection`:
    # this is UI-specific code, that shall go into a derived class, 
    # owned by the `GRosetta` code.
    def pprint(self, output=sys.stdout, session=None):
        """
        Output a summary table to stream `output`.
        """
        if len(self) == 0:
            if session is not None:
                print ("There are no jobs in session file '%s'." % session)
            else:
                print ("There are no jobs in session file.")
        else:
            output.write("%-15s  %-15s  %-18s  %-s\n" 
                         % ("Input file name", "Decoys Nr.", "State (JobID)", "Info"))
            output.write(80 * "=" + '\n')
            for job in self.values():
                output.write("%-15s  %-15s  %-18s  %-s\n" % 
                             (os.path.basename(job.input), job.instance, 
                              ('%s (%s)' % (job.state, job.jobid)), job.info))


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
                   default=os.path.join(gc3utils.Default.RCDIR, 'docking_protocol.flags'),
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
                   default=os.path.join(os.getcwd(), 'grosetta.csv'),
                   metavar="FILE",
                   help="Use FILE to store the status of running jobs (default: '%default')."
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
(options, args) = cmdline.parse_args()

# set up logging
logging.basicConfig(level=max(1, logging.ERROR - 10 * options.verbose),
                    format='%(name)s: %(message)s')
logger = logging.getLogger(PROG)
gc3utils.log.setLevel(max(1, (5-options.verbose)*10))

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
# FIXME
options.walltime = int(options.wctime / 3600)


## create/retrieve session

jobs = JobCollection()
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
jobs.load(session)
session.close()

# compute number of decoys already entered for each input file
decoys = zerodict()
for job in jobs.values():
    start, end = job.instance.split('--')
    decoys[job.input] = max(decoys[job.input], int(end))
    logger.debug("Job '%s': total computed decoys for input '%s': %d", job.id, job.input, decoys[job.input])


## build input file list

inputs = set([ job.input for job in jobs.values() ])
for path in args:
    if os.path.isdir(path):
        # recursively scan for .pdb files
        for dirpath, dirnames, filenames in os.walk(inputs):
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
logger.debug("Gathered input file list %s" % inputs)


## compute job list
            
for input in inputs:
    if decoys[input] < options.decoys_per_file-1:
        logger.info("Already computing %d decoys for '%s', requested %d more.",
                    decoys[input], input, options.decoys_per_file - 1 - decoys[input])
        for nr in range(decoys[input], options.decoys_per_file, options.decoys_per_job):
            instance = ("%d--%d" 
                        % (nr, min(options.decoys_per_file - 1, 
                                   nr + options.decoys_per_job - 1)))
            prefix = os.path.splitext(os.path.basename(input))[0] + '.' + instance + '.'
            jobs += Job(
                id = prefix[:-1], # except the trailing '.'
                input = input,
                instance = instance,
                application = RosettaDockingApplication(
                    input, 
                    flags_file = options.flags_file,
                    # set computational requirements
                    requested_memory = options.memory_per_core,
                    requested_cores = options.ncores,
                    requested_walltime = options.walltime,
                    # Rosetta-specific data
                    number_of_decoys_to_create = options.decoys_per_job,
                    arguments = [ "-out:pdb_gz", # compress PDB output files
                                  "-out:prefix", prefix ],
                    ),
                state = 'NEW',
                created = time.time(),
                # set job output directory
                output_dir = (
                    options.output
                    .replace('NAME', os.path.basename(input))
                    .replace('PATH', os.path.dirname(input) or os.getcwd())
                    .replace('INSTANCE', instance)
                    .replace('DATE', time.strftime('%Y-%m-%d', time.localtime(time.time())))
                    .replace('TIME', time.strftime('%H:%M', time.localtime(time.time())))
                    ),
                )


## iterate through job list, updating state and acting accordingly


def main(jobs):
    grid = Grid(default_output_dir=options.output)
    # build table
    in_flight_count = 0
    can_submit = True
    can_retrieve = True
    for job in jobs.values():
        state = grid.progress(job, can_submit, can_retrieve)
        if state in [ 'SUBMITTED', 'RUNNING' ]:
            in_flight_count += 1
            if in_flight_count > options.max_running:
                can_submit = False
        if job.get('output_retrieved_to', None) and not job.get('output_processing_done', False):
            # move around output files so they're easier to preprocess:
            #   1. All '.fasc' files land in the same directory as the input '.pdb' file
            #   2. All generated '.pdb'/'.pdb.gz' files are collected in a '.decoys.tar'
            #   3. Anything else is left as-is
            input_name = os.path.basename(job.input)
            input_name_sans = os.path.splitext(input_name)[0]
            output_tar = tarfile.open(os.path.join(job.output_retrieved_to, 
                                                   'docking_protocol.tar.gz'), 'r:gz')
            pdbs_tarfile_path = os.path.join(job.output_dir, input_name_sans) + '.decoys.tar'
            if not os.path.exists(pdbs_tarfile_path):
                pdbs = tarfile.open(pdbs_tarfile_path, 'w')
            else:
                pdbs = tarfile.open(pdbs_tarfile_path, 'a')
            for entry in output_tar:
                if entry.name.endswith('.fasc'):
                    fasc_file_name = (os.path.join(job.output_dir, input_name_sans) 
                                      + '.' + job.instance + '.fasc')
                    src = output_tar.extractfile(entry)
                    dst = open(fasc_file_name, 'wb')
                    dst.write(src.read())
                    dst.close()
                    src.close()
                elif entry.name.endswith('.pdb.gz'): #or entry.name.endswith('.pdb'):
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
            pdbs.close()
            job.output_processing_done = True
        if state == 'DONE':
            # nothing more to do - remove from job list if more than 1 day old
            if (time.time() - job.timestamp['DONE']) > 24*60*60:
                jobs.remove(job)
        if job.state == 'FAILED':
            # what should we do?
            # just keep the job around for a while and then remove it?
            if (time.time() - job.timestamp['FAILED']) > 3*24*60*60:
                jobs.remove(job)
    # write updated jobs to session file
    try:
        session = file(session_file_name, "wb")
        jobs.save(session)
        session.close()
    except IOError, x:
        logger.error("Cannot save job status to session file '%s': %s"
                     % (session_file_name, str(x)))
    ## print results to user
    if options.list:
        jobs.pprint(sys.stdout)
    else:
        print ("Status of jobs in the '%s' session:" 
               % os.path.basename(session_file_name))
        stats = jobs.stats()
        total = len(jobs)
        for state in sorted(stats.keys()):
            print ("  %-10s  %d/%d (%.1f%%)"
                   % (state, stats[state], total, 
                      (100.0 * stats[state] / total)))
    ## compute exitcode based on the running status of jobs
    stats = jobs.stats()
    rc = 0
    if stats['FAILED'] > 0:
        rc |= 2
    if in_flight_count > 0: # there are jobs in 'RUNNING' and 'SUBMITTED' state
        rc |= 4
    if stats['NEW'] > 0:
        rc |= 8
    return rc

rc = main(jobs)
if options.wait > 0:
    try:
        while rc > 3:
            time.sleep(options.wait)
            rc = main(jobs)
    except KeyboardInterrupt: # gracefully intercept Ctrl+C
        pass

sys.exit(rc)
