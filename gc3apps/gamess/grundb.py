#! /usr/bin/env python
#
"""
GrunDB is an interface for starting GAMESS analyses of molecules from
the online GAMESS.UZH database (http://ocikbgtw.uzh.ch/gamess.uzh) on
the Grid resources from the Swiss National Infrastructure SMSCG and
local compute clusters.

Given a template GAMESS input file, GRunDB will launch a GAMESS job
for each molecule of the chosen subset(s) of the GAMESS.UZH database,
manage the job lifecycle, and finally print out a comparison table
of stoichiomery reference data (from the database) and the same
quantitites as computed by GAMESS.

GRunDB is a Linux command-line program and is structured so to
interoperate with other programs from the GC3Utils suite, giving users
flexibility in managing the computational job lifecycle.
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__changelog__ = '''
   * 2010-09-08: Use new GAMESS.UZH location at http://ocikbgtw.uzh.ch/gamess.uzh
'''
__docformat__ = 'reStructuredText'


from __future__ import absolute_import, print_function
import csv
from cStringIO import StringIO
import itertools
import logging
import os
import os.path
import re
import shutil
import sys
import time
import urlparse

from optparse import OptionParser

# for the `GamessDb` class
from mechanize import Browser, HTTPError
from BeautifulSoup import BeautifulSoup

# interface to gc3utils
import gc3utils
from gc3utils.Application import GamessApplication
import gc3utils.defaults
import gc3utils.gcli
from gc3utils.Job import Job as Gc3utilsJob
import gc3utils.utils


PROG = os.path.splitext(os.path.basename(sys.argv[0]))[0]

## interact with the online DB

def grouped(iterable, pattern, container=tuple):
    """
    Iterate over elements in `iterable`, grouping them into
    batches: the `n`-th batch has length given by the `n`-th
    item of `pattern`.  Each batch is cast into an object
    of type `container`.

    Examples::

      >>> l = [0,1,2,3,4,5]
      >>> list(grouped(l, [1,2,3]))
      [0, (1, 2), (3, 4, 5)]
    """
    iterable = iter(iterable) # need a real iterator for tuples and lists
    for l in pattern:
        yield container(itertools.islice(iterable, l))


class CsvTable(object):
    def __init__(self, stream):
        header = stream.readline()
        self._dialect = csv.Sniffer().sniff(header)
        self._data = stream
        # analyze header and deduce data grouping
        hdrs = csv.reader(StringIO(header), dialect=self._dialect).next()
        self._groupnames = [ ]
        self._grouping = [ ]
        width = None
        for hdr in hdrs:
            if width is None:
                width = 1
            elif hdr == '':
                width += 1
            else:
                self._groupnames.append(prev_hdr)
                self._grouping.append(width)
                width = 1
            if hdr != '':
                prev_hdr = hdr
        self._groupnames.append(hdr)
        self._grouping.append(width)
    def rows_as_dict(self):
        for raw in csv.reader(self._data, dialect=self._dialect):
            yield dict([ (k,v) for k,v
                         in itertools.izip(self._groupnames,
                                           grouped(raw, self._grouping)) ])


class GamessDb(object):
    """
    Interact with the online web pages of the GAMESS.UZH DB.
    """
    BASE_URL = 'http://ocikbgtw.uzh.ch/gamess.uzh/'
    def __init__(self):
        # initialization
        self._browser = Browser()
        self._browser.set_handle_robots(False)
        self._subsets = self._list_subsets()


    def _list_subsets(self):
        """Return dictionary mapping GAMESS.UZH subset names to download URLs."""
        html = BeautifulSoup(self._browser.open(GamessDb.BASE_URL))
        links = html.findAll(name="a", attrs={'class':"mapitem"})
        result = { }
        for a in links:
            if a.string is not None:
                name = a.string
                result[name] = urlparse.urljoin(GamessDb.BASE_URL, a['href'])
        return result


    def list(self):
        """Return dictionary mapping GAMESS.UZH subset names to download URLs."""
        return self._subsets


    _illegal_chars_re = re.compile(r'[\s/&*`$~"' "'" ']+')
    _inp_filename_re = re.compile(r'\.inp$', re.I)

    def get_geometries(self, subset, output_dir='geometries'):
        """
        Download geometry files for the specified GAMESS.UZH subset,
        and save them into a `output_dir` subdirectory of the current
        working directory.

        Return list of extracted molecules/filenames.
        """
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        # download all links pointing to ".inp" files
        subset_url = self._subsets[subset]
        html = BeautifulSoup(self._browser.open(subset_url))
        links = html.findAll(name="a", attrs={'class':"mapitem"})
        molecules = [ ]
        for a in links:
            if a.string is not None:
                name = GamessDb._illegal_chars_re.sub('_', a.string)
                # ignore links that don't look like `.inp` files ...
                if not GamessDb._inp_filename_re.search(name):
                    continue
                # mechanize.retrieve always uses temp files
                (filename, headers) = self._browser.retrieve(urlparse.urljoin(subset_url, a['href']))
                shutil.copy(filename, os.path.join(output_dir, name))
                molecules.append(os.path.splitext(name)[0])
        logger.info("%s geometries downloaded into file '%s'", subset, output_dir)
        return molecules


    def get_reference_data(self, subset):
        """
        Iterate over stoichiometry reference data in a given GAMESS.UZH
        subset.  Each returned value is a pair `(r, d)`, where `r` is
        a dictionary mapping compound names (string) to their
        stoichiometric coefficient (integer), and `d` is a (float)
        number representing the total energy.
        """
        subset_url = self._subsets[subset]
        subset_page = self._browser.open(subset_url)
        refdata_csv = self._browser.follow_link(text="Direct data download")
        table = CsvTable(refdata_csv)
        for row in table.rows_as_dict():
            reactants = row['Systems']
            if len(reactants) == 0:
                continue # ignore null rows
            qtys = row['Stoichiometry']
            refdata = float(row['Ref.'][0])
            reaction = { }
            for n,sy in enumerate(reactants):
                if sy.strip() == '' or qtys[n].strip() == '':
                    continue # skip null fields
                reaction[sy] = int(qtys[n])
            yield (reaction, refdata)


### THE FOLLOWING CODE IS COMMON WITH `grosetta`
### (AND SHOULD BE KEPT IN SYNC WITH IT)
### It will be merged into `gc3utils` someday.

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
    def __init__(self, **extra_args):
        extra_args.setdefault('log', list())
        extra_args.setdefault('state', None) # user-visible status
        extra_args.setdefault('status', -1)  # gc3utils status (internal use only)
        extra_args.setdefault('timestamp', gc3utils.utils.defaultdict(time.time))
        Gc3utilsJob.__init__(self, **extra_args)
    def is_valid(self):
        # override validity checks -- this will not be a valid `Gc3utilsJob`
        # object until Grid.submit() is called on it ...
        return True

    def set_info(self, msg):
        self.info = msg
        self.history.append(msg)

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
        cfg = gc3libs.config.Config(config_file)
        self.mw = gc3utils.gcli.Gcli(cfg)
        self.default_output_dir = default_output_dir

    def save(self, job):
        """
        Save a job using gc3utils' persistence mechanism.
        """
        # update state so that it is correctly saved, but do not use set_state()
        # so the job history is not altered
        job.state = _get_state_from_gc3utils_job_status(job.status)
        job.jobid = job.unique_token # XXX
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
        if job.state == 'SUBMITTED' or job.state == 'RUNNING' or job.state == 'UNKNOWN':
            # update state
            try:
                self.update_state(job)
            except Exception, x:
                logger.error("Ignoring error in updating state of job '%s': %s: %s"
                              % (job.id, x.__class__.__name__, str(x)),
                              exc_info=True)
        if can_submit and job.state == 'NEW':
            # try to submit; go to 'SUBMITTED' if successful, 'FAILED' if not
            try:
                self.submit(job.application, job)
                job.set_state('SUBMITTED')
            except Exception, x:
                logger.error("Error in submitting job '%s': %s: %s"
                              % (job.id, x.__class__.__name__, str(x)))
                sys.excepthook(* sys.exc_info())
                job.set_state('NEW')
                job.set_info("Submission failed: %s" % str(x))
        if can_retrieve and job.state == 'FINISHING':
            # get output; go to 'DONE' if successful, ignore errors so we retry next time
            try:
                self.get_output(job, job.output_dir)
                job.set_state('DONE')
                job.set_info("Results retrieved into directory '%s'" % job.output_dir)
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
    def __init__(self, **extra_args):
        self.default_job_initializer = extra_args

    def add(self, job):
        """Add a `Job` instance to the collection."""
        self[job.id] = job
    def __iadd__(self, job):
        self.add(job)
        return self

    def remove(self, job):
        """Remove a `Job` instance from the collection."""
        del self[job.id()]

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
        jobs in that state.
        """
        result = zerodict()
        for job in self.values():
            result[job.state] += 1
        return result

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
            output.write("%-15s  %-18s  %-s\n"
                         % ("Input file name", "State (JobID)", "Info"))
            output.write(78 * "=" + '\n')
            for job in sorted(self.values()):
                output.write("%-15s  %-18s  %-s\n" %
                             (os.path.basename(job.id), ('%s (%s)' % (job.state, job.jobid)), job.info))

### END OF COMMON CODE PART


## GMTKN24's main routines

def _load_jobs(session):
    jobs = JobCollection()
    try:
        session_file_name = os.path.realpath(session)
        if os.path.exists(session_file_name):
            session = file(session_file_name, "r+b")
        else:
            session = file(session_file_name, "w+b")
    except IOError, x:
        raise IOError("Cannot open session file '%s' in read/write mode: %s."
                      % (session, str(x)))
    jobs.load(session)
    session.close()
    return jobs

def add_extension(filename, ext):
    if not os.path.isabs(filename):
        filename = os.path.abspath(filename)
    if filename.endswith(ext):
        return filename
    if ext.startswith('.'):
        return filename + ext
    else:
        return filename + '.' + ext

def read_file_contents(filename):
    file = open(filename, 'r')
    contents = file.read()
    file.close()
    return contents

def new(subset, session, template_file_name):
    """
    Download geometry files of the specified GAMESS.UZH subset, prefix
    them with the given template, and append corresponding jobs to the
    session file.
    """
    grid = Grid()
    session_file_name = add_extension(session, 'csv')
    logger.info("Will save jobs status to file '%s'", session_file_name)
    session_inp_dir = add_extension(session, '.inp.d')
    if not os.path.exists(session_inp_dir):
        os.mkdir(session_inp_dir)
    session_out_dir = add_extension(session, '.out.d')
    if not os.path.exists(session_out_dir):
        os.mkdir(session_out_dir)
    template = read_file_contents(template_file_name)
    # download geometries
    subset_inp_dir = os.path.join(session_inp_dir, subset)
    if not os.path.exists(subset_inp_dir):
        os.mkdir(subset_inp_dir)
    logger.info("Downloading %s geometries into '%s' ...", subset, subset_inp_dir)
    molecules = GamessDb().get_geometries(subset, subset_inp_dir)
    # prefix them with the GAMESS file snippet
    for name in molecules:
        dest_file_name = os.path.join(subset_inp_dir, name + '.inp')
        geometry = read_file_contents(dest_file_name)
        inp = open(dest_file_name, 'w')
        inp.write(template)
        inp.write(geometry)
        inp.close()
    # open session file
    logger.info("Loading session file '%s' ...", session_file_name)
    jobs = _load_jobs(session_file_name)
    logger.info("Loaded %d jobs from session file.", len(jobs))
    # append new jobs
    for name in molecules:
        inp_file_name = os.path.join(subset_inp_dir, name + '.inp')
        # XXX: order of the following statements *is* important!
        new_job = Job(
            id = name,
            application = GamessApplication(
                inp_file_name,
                # set computational requirements
                requested_memory = options.memory_per_core,
                requested_cores = options.ncores,
                requested_walltime = options.walltime,
                ),
            job_local_dir = session_out_dir, # XXX: apparently required by gc3utils
            # GMTKN24-specific data
            molecule = name,
            subset = subset,
            session = session_file_name,
            output_dir = session_out_dir,
            )
        new_job.set_state('NEW')
        grid.save(new_job)
        jobs += new_job
        logger.info("Created new GAMESS job with input file '%s'", inp_file_name)
    # save session
    session = open(session_file_name, 'wb')
    jobs.save(session)
    session.close()
    # display summary
    jobs.pprint(sys.stdout, session_file_name)


def grep1(filename, re):
    """
    Return first line in `filename` that matches `re`.
    Return `re.match` object, or `None` if no line matched.
    """
    file = open(filename, 'r')
    for line in file:
        match = re.search(line)
        if match:
            file.close()
            return match
    file.close()
    return None


_whitespace_re = re.compile(r'\s+', re.X)
def prettify(text):
    return _whitespace_re.sub(' ', text.capitalize())


def progress(session):
    """
    Update status of all jobs in session; submit new jobs; retrieve
    output of finished jobs.
    """
    grid = Grid()
    session_file_name = add_extension(session, 'csv')
    jobs = _load_jobs(session_file_name)
    logger.info("Loaded %d jobs from session file '%s'",
                len(jobs), session_file_name)
    final_energy_re = re.compile(r'FINAL [-\s]+ [A-Z0-9_-]+ \s+ ENERGY \s* (IS|=) \s* '
                                 r'(?P<energy>[+-]?[0-9]*(\.[0-9]*)?) \s* [A-Z0-9\s]*', re.X)
    termination_re = re.compile(r'EXECUTION \s+ OF \s+ GAMESS \s+ TERMINATED \s+-?(?P<gamess_outcome>NORMALLY|ABNORMALLY)-?'
                                r'|ddikick.x: .+ (exited|quit) \s+ (?P<ddikick_outcome>gracefully|unexpectedly)', re.X)
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
            gamess_output = os.path.join(job.output_retrieved_to, job.molecule + '.out')
            # determine job exit status
            try:
                termination = grep1(gamess_output, termination_re)
            except IOError, ex:
                termination = None # skip next `if`
                job.set_state('FAILED')
                job.set_info('Could not read GAMESS output file: %s' % str(ex))
            if termination:
                outcome = termination.group('gamess_outcome') or termination.group('ddikick_outcome')
                if outcome == 'ABNORMALLY' or outcome == 'unexpectedly':
                    job.set_state('FAILED')
                    job.set_info(prettify(termination.group(0)))
                elif outcome == 'NORMALLY' and job.state != 'DONE':
                    job.set_info("GAMESS terminated normally, but some error occurred in the Grid middleware;"
                                 " use the 'ginfo %s' command to inspect." % job.jobid)
                else:
                    job.set_info(prettify(termination.group(0)))
            # get 'FINAL ENERGY' from file
            if job.state == 'DONE':
                final_energy_line = grep1(gamess_output, final_energy_re)
                if final_energy_line:
                    # prettify GAMESS' output line
                    job.set_info(prettify(final_energy_line.group(0)))
                    # record energy in compute-ready form
                    job.energy = float(final_energy_line.group('energy'))
                    if abs(job.energy) < 0.01:
                        job.set_info(job.info +
                                     ' (WARNING: energy below threshold, computation might be incorrect)')
                else:
                    job.set_info(job.info + ' (WARNING: could not parse GAMESS output file)')
            job.output_processing_done = True
            grid.save(job)
    # write updated jobs to session file
    try:
        session_file = file(session_file_name, "wb")
        jobs.save(session_file)
        session_file.close()
    except IOError, x:
        logger.error("Cannot save job status to session file '%s': %s"
                     % (session_file_name, str(x)))
    # print results to user
    jobs.pprint(sys.stdout)
    # when all jobs are done, compute and output ref. data
    stats = jobs.stats()
    if stats['DONE'] == len(jobs):
        # get subsets of all jobs in session
        subsets = set()
        for job in jobs.values():
            subsets.add(job.subset)
        # map each molecule to its computed energy
        energy = dict([ (job.molecule, job.energy)
                        for job in jobs.values() ])
        print ("")
        print ("STOICHIOMETRY DATA")
        print ("")
        print ("%-40s  %-12s  (%-s; %-s)"
               % ("Reaction", "Comp. energy", "Ref. data", "deviation"))
        print (78 * "=")
        for subset in sorted(subsets):
            # print subset name, centered
            print ((78 - len(subset)) / 2) * ' ' + subset
            print (78 * "-")
            # print reaction data
            for reaction,refdata in GamessDb().get_reference_data(subset):
                # compute corresponding energy
                computed_energy = sum([ (627.509*qty*energy[sy]) for sy,qty in reaction.items() ])
                deviation = computed_energy - refdata
                print ("%-40s  %+.2f  (%+.2f; %+.2f)"
                       % (
                        # symbolic reaction
                        str.join(' + ',
                             [ ("%d*%s" % (qty, sy)) for sy,qty in reaction.items() ]),
                        # numerical data
                        computed_energy, refdata, deviation)
                       )


## parse command-line options

cmdline = OptionParser(PROG + " [options] ACTION [SUBSET] [SESSION]",
                       description="""
Interface for starting a GAMESS run on all molecules in named subsets
of the GAMESS.UZH online database.  The actual behavior of this script
depends on the ACTION pargument.

If ACTION is "new", then the molecules and data from the specified
SUBSET of GAMESS.UZH are downloaded, and a '.csv' spreadsheet named after
SESSION is created (if it does not exist) to track job statuses. The
SUBSET argument is required; SESSION, if omitted, will be given a
default value.

If ACTION is "progress", the status of running GAMESS jobs is updated,
new jobs are submitted, and the output of finished jobs is retrieved.
The SESSION argument is required; only the GAMESS jobs relative to the
specified SESSION are updated.

If ACTION is "abort", all running GAMESS jobs (in the specified
SESSION) are killed.

If ACTION is "list", a list of all available subsets is printed on the
standard output.

""")
cmdline.add_option("-c", "--cpu-cores", dest="ncores", type="int", default=8, # 8 cores
                   metavar="NUM",
                   help="Require the specified number of CPU cores for each GAMESS job."
                        " NUM must be a whole number.  Default: %default"
                   )
cmdline.add_option("-J", "--max-running", type="int", dest="max_running", default=20,
                   metavar="NUM",
                   help="Allow no more than NUM concurrent jobs in the grid."
                   " Default: %default"
                   )
cmdline.add_option("-m", "--memory-per-core", dest="memory_per_core", type="int", default=2, # 2 GB
                   metavar="GIGABYTES",
                   help="Require that at least GIGABYTES (a whole number; default %default)"
                        " are available to each execution core."
                   )
cmdline.add_option("-t", "--template", dest="template", metavar="TEMPLATE",
                   default=os.path.join(gc3utils.defaults.RCDIR, "grundb.inp.template"),
                   help="Prefix geometry data with the contents of the specified file,"
                   " to build the full GAMESS '.inp' file (Default: '%default')"
                   )
cmdline.add_option("-v", "--verbose", dest="verbose", action="count", default=0,
                   help="Verbosely report about program operation and progress."
                   )
cmdline.add_option("-w", "--wall-clock-time", dest="wctime", default=str(24), # 24 hrs
                   metavar="DURATION",
                   help="Each GAMESS job will run for at most DURATION hours (default: %default),"
                   " after which it will be killed and considered failed. DURATION can be a whole"
                   " number, expressing duration in hours, or a string of the form HH:MM,"
                   " specifying that a job can last at most HH hours and MM minutes."
                   )
(options, args) = cmdline.parse_args()

# consistency check
if options.max_running < 1:
    cmdline.error("Argument to option -J/--max-running must be a positive integer.")

n = options.wctime.count(":")
if 0 == n: # wctime expressed in seconds
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


## main

logging.basicConfig(level=max(1, logging.ERROR - 10 * options.verbose),
                    format='%(name)s: %(message)s')
logger = logging.getLogger(PROG)
gc3utils.log.setLevel(max(1, (5-options.verbose)*10))


def get_required_argument(args, argno=1, argname='SUBSET'):
    try:
        return args[argno]
    except:
        logger.critical("Missing required argument %s."
                        " Aborting now; type '%s --help' to get usage help.",
                        argname, PROG)
        raise
def get_optional_argument(args, argno=2, argname="SESSION",
                          default=('Run.%s' % time.strftime('%Y-%m-%d.%H.%M'))):
    try:
        return args[argno]
    except:
        logger.info("Missing argument %s, using default '%s'", argname, default)
        return default


if "__main__" == __name__:
    if len(args) < 1:
        logger.critical("%s: Need at least the ACTION argument."
                        " Type '%s --help' to get usage help."
                        % (PROG, PROG))
        sys.exit(1)

    #try:
    if 'new' == args[0]:
        subset = get_required_argument(args, 1, "SUBSET")
        session = get_optional_argument(args, 2, "SESSION")
        if subset == 'ALL':
            subsets = GamessDb().list().keys()
        else:
            subsets = subset.split(',')
        for subset in subsets:
            new(subset, session, options.template)

    elif 'progress' == args[0]:
        session = get_required_argument(args, 1, "SESSION")
        progress(session)

    elif 'abort' == args[0]:
        session = get_optional_argument(args, 1, "SESSION")
        abort(session)

    elif 'refdata' == args[0]:
        subset = get_required_argument(args, 1, "SUBSET")
        for r,d in GamessDb().get_reference_data(subset):
            print ("%s = %.3f"
                   % (str.join(' + ',
                               [ ("%d*%s" % (qty, sy)) for sy,qty in r.items() ]),
                      d))

    elif 'download' == args[0]:
        subset = get_required_argument(args, 1, "SUBSET")
        GamessDb().get_geometries(subset)

    elif 'list' == args[0]:
        print ("Available subsets of GMTKN24:")
        ls = GamessDb().list()
        for name, url in ls.items():
            print ("  %s --> %s" % (name, url))

    elif 'doctest' == args[0]:
        import doctest
        doctest.testmod(name="gmtkn24",
                        optionflags=doctest.NORMALIZE_WHITESPACE)

    else:
        logger.critical("Unknown ACTION word '%s'."
                        " Type '%s --help' to get usage help."
                        % (args[0], PROG))
    sys.exit(1)

    #except HTTPError, x:
    #    logger.critical("HTTP error %d requesting page: %s" % (x.code, x.msg))
    #    sys.exit(1)
