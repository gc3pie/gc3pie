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

import gc3utils
import gc3utils.Default
import gc3utils.gcli
import gc3utils.utils


## defaults

rcdir = os.path.expanduser('~/.gc3')


## interface to Gc3Utils

from gc3utils.Application import RosettaApplication

class RosettaDockingApplication(RosettaApplication):
    """
    Specialized `Application` class for executing a single run of the
    Rosetta "docking_protocol" application.
    """
    def __init__(self, pdb_file_path, native_file_path=None, 
                 number_of_decoys_to_create=1, flag_file_path=None, **kw):
        pdb_file_name = os.path.basename(pdb_file_path)
        pdb_file_dir = os.path.dirname(pdb_file_path)
        pbd_file_name_sans = os.path.splitext(pdb_file_name)[0]
        if native_file_path is None:
            native_file_path = pdb_file_path
        def get_and_remove(D, k, d):
            if D.has_key(k):
                result = D[k]
                del D[k]
                return result
            else:
                return d
        RosettaApplication.__init__(
            self,
            application = 'docking_protocol',
            inputs = { 
                "-in:file:s":pdb_file_path,
                "-in:file:native":native_file_path,
                },
            outputs = [ pdb_file_name+'.fasc' ],
            flags_file = flag_file_path,
            arguments = [ 
                "-out:file:o", pdb_file_name,
                "-out:nstruct", number_of_decoys_to_create,
                ] + get_and_remove(kw, 'arguments', []),
            job_local_dir = get_and_remove(kw, 'job_local_dir', pdb_file_dir),
            **kw)


## job control

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

def _get_state_from_gc3utils_job_status(status):
    """
    Convert a gc3utils.Job status code into a readable state description.
    """
    try:
        return {
            1:'EXECUTED',
            2:'RUNNING',
            3:'FAILED',
            4:'SUBMITTED',
            5:'DONE',
            6:'DELETED',
            7:'UNKNOWN',
            }[status]
    except KeyError:
        return 'UNKNOWN'

class Job(Struct):
    """
    Instances of this class live a double life as (1) dictionary-like
    records storing information about a particular computational job
    instance; (2) interfaces to Grid middleware for managing the
    life-cycle of a computational job.
    """

    def __init__(self, **kw):
        Struct.__init__(self, **kw)
        self.mw = gc3utils.gcli.Gcli(*gc3utils.utils.import_config(gc3utils.Default.CONFIG_FILE_LOCATION))
        self.options = kw['options']
        self.app = RosettaDockingApplication(
            self.input,
            #arguments = self.options.application_arguments,
            #job_local_dir = self.options.output
            requested_memory = self.options.memory_per_core,
            requested_cores = self.options.ncores,
            requested_walltime = self.options.walltime,
            )
        
    def submit(self):
        j = self.mw.gsub(self.app)
        gc3utils.utils.persist_job(j)
        self.jobid = j.unique_token

    def get_state(self):
        j = self.mw.gstat(gc3utils.utils.get_job(self.jobid))[0] # `gstat` madness
        gc3utils.utils.persist_job(j)
        self.state = _get_state_from_gc3utils_job_status(j.status)
        return self.state

    def get_output(self):
        j = gc3utils.utils.get_job(self.jobid)
        if self.has_key('job_local_dir'):
            j.job_local_dir = self.job_local_dir
        self.mw.gget(j)
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
cmdline.add_option("-w", "--wall-clock-time", dest="wctime", default=str(8*60*60), # 8 hrs
                   metavar="DURATION",
                   help="Each SMSCG job will run for at most DURATION time, after which it"
                   " will be killed and considered failed. DURATION can be a whole"
                   " number, expressing duration in seconds, or a string of the form HH:MM,"
                   " specifying that a job can last at most HH hours and MM minutes."
                   )
cmdline.add_option("-m", "--memory-per-core", dest="memory_per_core", type="int", default=2, # 2 GB
                   metavar="GIGABYTES",
                   help="Require that at least GIGABYTES (a whole number)"
                        " are available to each execution core.")
cmdline.add_option("-c", "--cpu-cores", dest="ncores", type="int", default=1, # 1 core
                   metavar="NUM",
                   help="Require the specified number of CPU cores for each SMSCG job."
                        " NUM must be a whole number."
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
cmdline.add_option("-C", "--continuous", type="int", dest="wait", default=0,
                   metavar="INTERVAL",
                   help="Keep running, monitoring jobs and possibly submitting new ones or"
                   " fetching results every INTERVAL seconds."
                   )
cmdline.add_option("-v", "--verbose", type="int", dest="verbose", default=0,
                   metavar="LEVEL",
                   help="Increase program verbosity"
                   " (default is 0; any higher number may spoil screen output).",
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
    cmdline.error("Argument to option -C/--continuous must be a positive integer.")

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
# FIXME
options.walltime = int(options.wctime / 3600)

# set verbosity
gc3utils.log.setLevel(max(1, (5-options.verbose)*10))


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

logging.debug("Gathered input file list %s" % inputs)


## compute job list

jobs = dict() # use (NAME,INSTANCE) as primary key
for input in inputs:
    for nr in range(0, options.passes_per_file, options.passes_per_job):
        instance = ("%d--%d" % (nr, nr + options.passes_per_job))
        jobs[input, instance] = Job(
            input = input,
            instance = instance,
            state = 'NEW',
            timestamp = time.time(),
            jobid = None,
            options = options,
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
                          ['input', 'instance', 'jobid', 'state', 'reached_on']):
    # convert 'reached_on'; back to UNIX epoch
    row['timestamp'] = time.mktime(time.strptime(row['reached_on']))
    if jobs.has_key((row['input'], row['instance'])):
        # update state etc.
        jobs[row['input'], row['instance']].update(row)
    else:
        jobs[row['input'], row['instance']] = Job(options=options, **row)


## iterate through job list, updating state and acting accordingly

def main(jobs):
    # print table header
    # build table
    in_flight_count = 0
    for job in jobs.values():
        # must update status of SUBMITTED/RUNNING jobs before launching new ones
        if job.state == 'SUBMITTED' or job.state == 'RUNNING':
            # update state 
            try:
                old_state = job.state
                state = job.get_state()
                if state != old_state:
                    job.state = state
                    job.timestamp = time.time()
                if state in [ 'SUBMITTED', 'RUNNING' ]:
                    in_flight_count += 1
            except Exception, x:
                logging.error("Ignoring error in updating state of job '%s.%s': %s: %s"
                              % (job.input, job.instance, x.__class__.__name__, str(x)),
                              exc_info=True)
        if job.state == 'NEW' and in_flight_count < options.max_running:
            # try to submit; go to 'SUBMITTED' if successful, 'FAILED' if not
            try:
                job.submit()
                job.state = 'SUBMITTED'
                job.submit_timestamp = time.time()
                job.timestamp = time.time()
            except Exception, x:
                logging.error("Error in submitting job '%s.%s': %s: %s"
                              % (job.input, job.instance, x.__class__.__name__, str(x)))
                job.state = 'FAILED'
        if job.state == 'EXECUTED':
            # get output; go to 'DONE' if successful, 'FAILED' if not
            try:
                # FIXME: temporary fix, should persist `submit_timestamp`!
                if not job.has_key('submit_timestamp'):
                    job.submit_timestamp = time.localtime(job.timestamp)
                # set job output directory
                output_dir = (options.output
                              .replace('NAME', os.path.basename(job.input))
                              .replace('PATH', os.path.dirname(job.input) or '.')
                              .replace('INSTANCE', job.instance)
                              .replace('DATE', time.strftime('%Y-%m-%d', job.submit_timestamp))
                              .replace('TIME', time.strftime('%H:%M', job.submit_timestamp))
                              )
                # `job_local_dir` is where gc3utils will retrieve the output
                job.job_local_dir = output_dir
                job.get_output()
                job.state = 'DONE'
                job.timestamp = time.time()
                logging.info("Retrieved output of job %s.%s into directory '%s'" 
                             % (job.input, job.instance, output_dir))
            except Exception, x:
                logging.error("Got error in updating state of job '%s.%s': %s: %s"
                              % (job.input, job.instance, x.__class__.__name__, str(x)))
        if job.state == 'DONE':
            # nothing more to do - remove from job list if more than 1 day old
            if (time.time() - job.timestamp) > 24*60*60:
                del jobs[job.input, job.instance]
        if job.state == 'FAILED':
            # what should we do?
            # just keep the job around for a while and then remove it?
            if (time.time() - job.timestamp) > 3*24*60*60:
                del jobs[job.input, job.instance]
        job.reached_on = time.ctime(job.timestamp)
    if len(jobs) == 0:
        print ("There are no jobs in session file '%s'." % options.session)
    else:
        # write updated jobs to session file
        csv.DictWriter(session, ['input', 'instance', 'jobid', 'state', 'reached_on'], 
                       extrasaction='ignore').writerows(jobs.values())
        # pretty-print table of jobs
        print ("%-15s  %-15s  %-18s  %-s" % ("Input file name", "Instance count", "State (JobID)", "Reached on"))
        print (80 * "=")
        for job in jobs.values():
            print ("%-15s  %-15s  %-18s  %-s" % 
                   (job.input, job.instance, ('%s (%s)' % (job.state, job.jobid)), job.reached_on))


main(jobs)
if options.wait > 0:
    try:
        while True:
            time.sleep(options.wait)
            main(jobs)
    except KeyboardInterrupt: # gracefully intercept Ctrl+C
        pass

sys.exit(0)
