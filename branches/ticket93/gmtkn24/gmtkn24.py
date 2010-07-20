#! /usr/bin/env python
#
"""
An interface for starting GAMESS analyses of molecules in the online
GMTKN24 database (http://toc.uni-muenster.de/GMTKN/GMTKNmain.html).
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__changelog__ = '''
'''
__docformat__ = 'reStructuredText'


import csv
import itertools
import logging
import os
import os.path
import re
import sys
import time

from optparse import OptionParser

# for the `Gmtkn24` class
import gamess
from mechanize import Browser, HTTPError
from BeautifulSoup import BeautifulSoup
from zipfile import ZipFile

# interface to gc3utils
import gc3utils
from gc3utils.Application import GamessApplication
import gc3utils.Default
import gc3utils.gcli
from gc3utils.Job import Job as Gc3utilsJob
import gc3utils.utils


PROG = os.path.splitext(os.path.basename(sys.argv[0]))[0]

## parse HTML tables

class HtmlTable(object):
    def __init__(self, html):
        """
        Initialize the `HtmlTable` object from HTML source.
        The `html` argument can be either the text source of
        an HTML fragment, or a BeautifulSource `Tag` object.
        """
        self._headers, self._rows = self.extract_html_table(html)

    def rows_as_dict(self):
        """
        Iterate over table rows, representing each row as a dictionary
        mapping header names into corresponding cell values.
        """
        for row in self._rows:
            yield dict([ (th[0], row[n])
                         for n,th in enumerate(self._headers) ])

    @staticmethod
    def extract_html_table(html):
        """
        Return the table in `html` text as a list of rows, each row being
        a tuple of the values found in table cells.
        """
        if not isinstance(html, BeautifulSoup):
            html = BeautifulSoup(html)
        table = html.find('table')
        # extract headers
        headers = [ ]
        for th in table.findAll('th'):
            text = str.join('', th.find(text=True)).strip()
            if th.has_key('colspan'):
                span = int(th['colspan'])
            else:
                span = 1
            headers.append((text, span))
        # extract rows and group cells according to the TH spans
        spans = [ s for (t,s) in headers ]
        rows = [ list(grouped(row, spans)) 
                      for row in HtmlTable.extract_html_table_rows(table) ]
        # all done
        return (headers, rows)

    @staticmethod
    def extract_html_table_rows(table):
        """
        Return list of rows (each row being a `tuple`) extracted 
        from HTML `Tag` element `table`.
        """
        return [ tuple([ str.join('', td.findAll(text=True)).strip()
                         for td in tr.findAll('td') ])
                 for tr in table.findAll('tr') ]

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


## interact with the online DB

class Gmtkn24(object):
    """
    Interact with the online web pages of GMTKN24.
    """
    BASE_URL = 'http://toc.uni-muenster.de/GMTKN/'
    def __init__(self):
        # initialization
        self._browser = Browser()
        self._browser.set_handle_robots(False)
        self._subsets = self._list_subsets()

    _subset_link_re = re.compile("The (.+) subset")
    def _list_subsets(self):
        """Return dictionary mapping GMTKN24 subset names to download URLs."""
        html = BeautifulSoup(self._browser.open(Gmtkn24.BASE_URL + 'GMTKNmain.html'))
        links = html.findAll(name="a")
        result = { }
        for a in links:
            if a.string is not None:
                match = Gmtkn24._subset_link_re.match(a.string)
                if match is not None:
                    # if a subset has several names, keep only the first one
                    name = match.group(1).split(' ')[0]
                    result[name] = Gmtkn24.BASE_URL + a['href']
        return result

    def list(self):
        """Return dictionary mapping GMTKN24 subset names to download URLs."""
        return self._subsets

    def get_geometries(self, subset, output_dir='geometries'):
        """
        Download geometry files for the specified GMTKN24 subset,
        and save them into the 'geometries/' subdirectory of the
        current working directory.

        Return list of extracted molecules/filenames.
        """
        subset_url = self._subsets[subset]
        page = self._browser.open(subset_url)
        # must download the zip to a local file -- zipfiles are not stream-friendly ...
        geometries_url = self._browser.click_link(text="Geometries")
        (filename, headers) = self._browser.retrieve(geometries_url)
        logger.info("%s geometries downloaded into file '%s'", subset, filename)
        # extract geometries into the `geometries/` folder
        geometries_zip = ZipFile(filename, 'r')
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        names = geometries_zip.namelist()
        extracted = list()
        for name in names:
            # skip the `README` file
            if name.endswith("README"):
                continue
            # zipfile's `extract` method preserves full pathname, 
            # so let's get the data from the archive and write
            # it in the file WE want...
            content = geometries_zip.read(name)
            output_name = os.path.basename(name)
            output_path = os.path.join(output_dir, output_name)
            output = open(output_path, 'w')
            output.write(content)
            output.close()
            extracted.append(output_name)
            logger.info("Extracted geometry file '%s'", os.path.basename(name))
        geometries_zip.close()
        return extracted

    def get_reference_data(self, subset):
        """
        Iterate over stoichiometry reference data in a given GMTKN24
        subset.  Each returned value is a pair `(r, d)`, where `r` is
        a dictionary mapping compound names (string) to their
        stoichiometric coefficient (integer), and `d` is a (float)
        number representing the total energy.
        """
        subset_url = self._subsets[subset]
        subset_page = self._browser.open(subset_url)
        refdata_page = self._browser.follow_link(text="Reference data")
        table = HtmlTable(refdata_page.read())
        for row in table.rows_as_dict():
            reactants = row['Systems']
            if len(reactants) == 0:
                continue # ignore null rows
            qtys = row['Stoichiometry']
            refdata = float(row['Ref.'][0])
            reaction = { }
            for n,sy in enumerate(reactants):
                if qtys[n] == '':
                    continue # skip null fields
                reaction[sy] = int(qtys[n])
            yield (reaction, refdata)


## imported from grosetta

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
        if job.state == 'SUBMITTED' or job.state == 'RUNNING':
            # update state 
            try:
                self.update_state(job)
            except Exception, x:
                logger.error("Ignoring error in updating state of job '%s.%s': %s: %s"
                              % (job.input, job.instance, x.__class__.__name__, str(x)),
                              exc_info=True)
        if job.state == 'NEW' and can_submit:
            # try to submit; go to 'SUBMITTED' if successful, 'FAILED' if not
            try:
                self.submit(job.application, job)
                job.set_state('SUBMITTED')
            except Exception, x:
                logger.error("Error in submitting job '%s': %s: %s"
                              % (job.id, x.__class__.__name__, str(x)))
                sys.excepthook(* sys.exc_info())
                job.set_state('FAILED')
                job.set_info("Submission failed: %s" % str(x))
        if job.state == 'FINISHING' and can_retrieve:
            # get output; go to 'DONE' if successful, 'FAILED' if not
            try:
                # FIXME: temporary fix, should persist `created`!
                if not job.has_key('created'):
                    job.created = time.localtime(time.time())
                self.get_output(job, job.output_dir)
                job.set_state('DONE')
                job.set_info("Results retrieved into directory '%s'" % job.output_dir)
            except Exception, x:
                logger.error("Got error in updating state of job '%s.%s': %s: %s"
                              % (job.input, job.instance, x.__class__.__name__, str(x)), 
                              exc_info=True)
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
        jobs in that state; an additional key 'total' maps to the
        total number of jobs in this collection.
        """
        result = zerodict()
        for job in self.values():
            result[job.state] += 1
        result['total'] = len(self)
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
            for job in self.values():
                output.write("%-15s  %-18s  %-s\n" % 
                             (os.path.basename(job.id), ('%s (%s)' % (job.state, job.jobid)), job.info))


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
    if not filename.startswith('/'):
        filename = os.path.join(os.getcwd(), filename)
    if filename.endswith(ext):
        return filename
    if ext.startswith('.'):
        return filename + ext
    else:
        return filename + '.' + ext

def new(subset, session, template):
    """
    Download geometry files of the specified GMTKN24 subset, convert them 
    to GAMESS input files using the given template, and append corresponding
    jobs to the session file.
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
    # download geometries
    logger.info("Downloading %s geometries into '%s' ...", subset, session_inp_dir)
    molecules = Gmtkn24().get_geometries(subset, session_inp_dir)
    # convert to GAMESS .inp format
    for molecule in molecules:
        logger.info("Generating GAMESS input file '%s' from geometry '%s/%s' ...",
                    session_inp_dir + '/' + molecule + '.inp', session_inp_dir, molecule)
        gamess.turbomol_to_gamess(session_inp_dir + '/' + molecule,
                                  session_inp_dir + '/' + molecule + '.inp', 
                                  template)
    # open session file
    logger.info("Loading session file '%s' ...", session_file_name)
    jobs = _load_jobs(session_file_name)
    logger.info("Loaded %d jobs from session file.", len(jobs))
    # append new jobs
    for molecule in molecules:
        # XXX: order of the following statements *is* important!
        new_job = Job(
            id = molecule,
            application = GamessApplication(
                session_inp_dir + '/' + molecule + '.inp',
                # set computational requirements
                requested_memory = options.memory_per_core,
                requested_cores = options.ncores,
                requested_walltime = options.walltime,
                ),
            job_local_dir = session_out_dir, # XXX: apparently required by gc3utils
            # GMTKN24-specific data
            molecule = molecule,
            subset = subset,
            session = session_file_name,
            output_dir = session_out_dir,
            )
        new_job.set_state('NEW')
        grid.save(new_job)
        jobs += new_job
        logger.info("Created new GAMESS job with input file '%s'", 
                    session_inp_dir + '/' + molecule + '.inp')
    # save session
    session = open(session_file_name, 'wb')
    jobs.save(session)
    session.close()
    # display summary
    jobs.pprint(sys.stdout, session_file_name)


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
    final_energy_re = re.compile(r'FINAL +[A-Z0-9_-]+ +ENERGY IS +([+-]?[0-9]*(\.[0-9]*)?) *[A-Z0-9 ]*')
    whitespace_re = re.compile(r'\s+', re.X)
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
            # get 'FINAL ENERGY' from file
            gamess_output = open(os.path.join(job.output_retrieved_to, job.molecule + '.out'), 'r')
            for line in gamess_output:
                match = final_energy_re.search(line)
                if match:
                    # prettify GAMESS' output line
                    job.set_info(whitespace_re.sub(' ', match.group(0).capitalize()))
                    # record energy in compute-ready form
                    job.energy = float(match.group(1))
                    break
            gamess_output.close()
            job.output_processing_done = True
        if job.state == 'FAILED':
            # what should we do?
            pass
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
        print ()
        print ("STOICHIOMETRY DATA")
        print ()
        print ("%-40s  %-6.4f  (%-6.4f)" 
               % ("Reaction", "Computed energy", "Ref. data"))                
        print (78 * "=")
        for subset in subsets:
            for reaction,refdata in Gmtkn24().get_reference_data(subset):
                print ("%-40s  %-6.4f  (%-6.4f)" 
                       % (
                        # symbolic reaction
                        str.join(' + ', 
                                 [ ("%d*%s" % (qty, sy)) for sy,qty in reaction.items() ]), 
                        # compute corresponding energy
                        sum([ (qty*energy[sy]) for sy,qty in reaction.items() ]),
                        # print ref. data from GMTKN24
                        refdata)
                       )


## parse command-line options

cmdline = OptionParser(PROG + " [options] ACTION [SUBSET] [SESSION]",
                       description="""
Interface for starting a GAMESS sweep-run on named subsets of the
GMTKN24 online database.  The actual behavior of this script depends
on the ACTION pargument.

If ACTION is "new", then the molecules and data from the specified
SUBSET of GMTKN24 are downloaded, and a '.csv' spreadsheet named after
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
                   default=os.path.join(gc3utils.Default.RCDIR, "gmtkn24.inp.template"),
                   help="Use the specified template file for constructing a GAMESS '.inp'"
                   " input file from GMTKN24 molecule geometries.  (Default: '%default')"
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
                          default=('Run.%s' % time.strftime('%Y-%m-%d.%H.%M', time.localtime()))):
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

    try:
        if 'new' == args[0]:
            subset = get_required_argument(args, 1, "SUBSET")
            session = get_optional_argument(args, 2, "SESSION")
            new(subset, session, options.template)

        elif 'progress' == args[0]:
            session = get_required_argument(args, 1, "SESSION")
            progress(session)

        elif 'abort' == args[0]:
            session = get_optional_argument(args, 1, "SESSION")
            abort(session)

        elif 'refdata' == args[0]:
            subset = get_required_argument(args, 1, "SUBSET")
            for r,d in Gmtkn24().get_reference_data(subset):
                print ("%s = %.3f" 
                       % (str.join(' + ', 
                                   [ ("%d*%s" % (qty, sy)) for sy,qty in r.items() ]), 
                          d))

        elif 'download' == args[0]:
            subset = get_required_argument(args, 1, "SUBSET")
            Gmtkn24().get_geometries(subset)

        elif 'list' == args[0]:
            print ("Available subsets of GMTKN24:")
            ls = Gmtkn24().list()
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

    except HTTPError, x:
        logger.critical("HTTP error %d requesting page: %s" % (x.code, x.msg))
        sys.exit(1)
