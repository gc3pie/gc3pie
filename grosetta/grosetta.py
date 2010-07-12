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
        pdb_file_name_sans = os.path.splitext(pdb_file_name)[0]
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
            outputs = [
                pdb_file_name_sans + '.fasc',
                pdb_file_name_sans + '.sc',
                ],
            flags_file = flag_file_path,
            arguments = [ 
                "-out:file:o", pdb_file_name_sans,
                "-out:nstruct", number_of_decoys_to_create,
                ] + get_and_remove(kw, 'arguments', []),
            job_local_dir = get_and_remove(kw, 'job_local_dir', pdb_file_dir),
            **kw)


## job control

class Struct(dict):
    """
    A `dict`-like object, whose keys can be accessed with the usual
    '[...]' lookup syntax, or with the '.' get attribute syntax.

    Examples::

      >>> a = Struct()
      >>> a['x'] = 1
      >>> a.x
      1
      >>> a.y = 2
      >>> a['y']
      2
    """
    def __init__(self, **kw):
        self.update(kw)
    def __setattr__(self, key, val):
        self[key] = val
    def __getattr__(self, key):
        return self[key]
    def __hasattr__(self, key):
        return self.has_key(key)


def _get_state_from_gc3utils_job_status(status):
    """
    Convert a gc3utils.Job status code into a readable state description.
    """
    try:
        return {
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

class Job(Struct):
    """
    Instances of this class live a double life as (1) dictionary-like
    records storing information about a particular computational job
    instance; (2) interfaces to Grid middleware for managing the
    life-cycle of a computational job.
    """

    def __init__(self, **kw):
        kw.setdefault('log', list())
        kw.setdefault('timestamp', gc3utils.utils.defaultdict(time.time))
        Struct.__init__(self, **kw)
        self.mw = gc3utils.gcli.Gcli(*gc3utils.utils.import_config(gc3utils.Default.CONFIG_FILE_LOCATION))
        options = kw['options']
        self.app = RosettaDockingApplication(
            self.input,
            number_of_decoys_to_create=options.decoys_per_job,
            flags_file_path=options.flags_file_path,
            #arguments = self.options.application_arguments,
            #job_local_dir = self.options.output
            requested_memory = options.memory_per_core,
            requested_cores = options.ncores,
            requested_walltime = options.walltime,
            )

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
        
    def submit(self):
        j = self.mw.gsub(self.app)
        gc3utils.utils.persist_job(j)
        self.jobid = j.unique_token

    def update_state(self):
        j = self.mw.gstat(gc3utils.utils.get_job(self.jobid))[0] # `gstat` madness
        gc3utils.utils.persist_job(j)
        self.set_state(_get_state_from_gc3utils_job_status(j.status))
        return self.state

    def get_output(self, output_dir):
        j = gc3utils.utils.get_job(self.jobid)
        # `job_local_dir` is where gc3utils will retrieve the output
        j.job_local_dir = output_dir
        self.mw.gget(j)
        gc3utils.utils.persist_job(j)


## parse command-line


cmdline = OptionParser("grosetta [options] [INPUT ...]",
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
cmdline.add_option("-f", "--flags-file", dest="flags_file_path", default=None,
                   metavar="PATH",
                   help="Pass the specified flags file to Rosetta 'docking_protocol'"
                   " Default: '~/.gc3/docking_protocol.flags'"
                   )
cmdline.add_option("-J", "--max-running", type="int", dest="max_running", default=50,
                   metavar="NUM",
                   help="Allow no more than NUM concurrent jobs (default: %default)"
                   " to be in SUBMITTED or RUNNING state."
                   )
cmdline.add_option("-m", "--memory-per-core", dest="memory_per_core", type="int", default=2, # 2 GB
                   metavar="GIGABYTES",
                   help="Require that at least GIGABYTES (a whole number)"
                        " are available to each execution core. (Default: %default)")
cmdline.add_option("-o", "--output", dest="output", default='PATH/',
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
                   default=10000,
                   metavar="NUM",
                   help="Compute NUM decoys per input file (default: %default)."
                   )
cmdline.add_option("-p", "--decoys-per-job", type="int", dest="decoys_per_job", 
                   default=15,
                   metavar="NUM",
                   help="Compute NUM decoys in a single job (default: %default)."
                   " This parameter should be tuned so that the running time"
                   " of a single job does not exceed the maximum wall-clock time."
                   )
cmdline.add_option("-s", "--session", dest="session", 
                   default=os.path.join(rcdir, 'grosetta.csv'),
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

# consistency check
if options.max_running < 1:
    cmdline.error("Argument to option -J/--max-running must be a positive integer.")
if options.decoys_per_file < 1:
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
    for nr in range(0, options.decoys_per_file, options.decoys_per_job):
        instance = ("%d--%d" % (nr, nr + options.decoys_per_job - 1))
        jobs[input, instance] = Job(
            input = input,
            instance = instance,
            state = 'NEW',
            created = time.time(),
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
                          ['input', 'instance', 'jobid', 'state', 'info', 'history']):
    if row['input'].strip() == '':
        # invalid row, skip
        continue 
    id = (row['input'], row['instance'])
    if jobs.has_key(id):
        # update state etc.
        jobs[id].update(row)
    else:
        jobs[id] = Job(options=options, **row)
    job = jobs[id]
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
session.close()


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
                state = job.update_state()
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
                job.set_state('SUBMITTED')
            except Exception, x:
                logging.error("Error in submitting job '%s.%s': %s: %s"
                              % (job.input, job.instance, x.__class__.__name__, str(x)))
                job.set_state('FAILED')
                job.set_info("Submission failed: %s" % str(x))
        if job.state == 'FINISHING':
            # get output; go to 'DONE' if successful, 'FAILED' if not
            try:
                # FIXME: temporary fix, should persist `created`!
                if not job.has_key('created'):
                    job.created = time.localtime(time.time())
                # set job output directory
                output_dir = (options.output
                              .replace('NAME', os.path.basename(job.input))
                              .replace('PATH', os.path.dirname(job.input) or '.')
                              .replace('INSTANCE', job.instance)
                              .replace('DATE', time.strftime('%Y-%m-%d', job.created))
                              .replace('TIME', time.strftime('%H:%M', job.created))
                              )
                job.get_output(output_dir)
                job.set_state('DONE')
                job.set_info("Results retrieved into directory '%s'" % output_dir)
            except Exception, x:
                logging.error("Got error in updating state of job '%s.%s': %s: %s"
                              % (job.input, job.instance, x.__class__.__name__, str(x)))
        if job.state == 'DONE':
            # nothing more to do - remove from job list if more than 1 day old
            if (time.time() - job.timestamp['DONE']) > 24*60*60:
                del jobs[job.input, job.instance]
        if job.state == 'FAILED':
            # what should we do?
            # just keep the job around for a while and then remove it?
            if (time.time() - job.timestamp['FAILED']) > 3*24*60*60:
                del jobs[job.input, job.instance]
    if len(jobs) == 0:
        print ("There are no jobs in session file '%s'." % options.session)
    else:
        # write updated jobs to session file
        try:
            session = file(session_file_name, "wb")
            for job in jobs.values():
                job.history = str.join("; ", job.log)
                csv.DictWriter(session, ['input', 'instance', 'jobid', 'state', 'info', 'history'], 
                               extrasaction='ignore').writerow(job)
            session.close()
        except IOError, x:
            logging.error("Cannot save job status to session file '%s': %s"
                          % (session_file_name, str(x)))
        # pretty-print table of jobs
        print ("%-15s  %-15s  %-18s  %-s" % ("Input file name", "Instance count", "State (JobID)", "Info"))
        print (80 * "=")
        for job in jobs.values():
            print ("%-15s  %-15s  %-18s  %-s" % 
                   (job.input, job.instance, ('%s (%s)' % (job.state, job.jobid)), job.info))


main(jobs)
if options.wait > 0:
    try:
        while True:
            time.sleep(options.wait)
            main(jobs)
    except KeyboardInterrupt: # gracefully intercept Ctrl+C
        pass

sys.exit(0)
