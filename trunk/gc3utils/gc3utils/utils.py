import sys
import os
import commands
import logging
import logging.handlers
import tempfile
import getpass
import re
import md5
import time
import ConfigParser
import shutil
import getpass
import smtplib
import subprocess
from email.mime.text import MIMEText
sys.path.append('/opt/nordugrid/lib/python2.4/site-packages')
import warnings
warnings.simplefilter("ignore")
from arclib import *
import Exceptions
import Job
import Default
import gc3utils

# __all__ = ["configure_logging","check_inputfile","readConfig","check_qgms_version","dirname","inputname","inputfilename","create_unique_token"]

# ================================================================
#
#                     Generic functions
#
# ================================================================


def sumfile(fobj):
    """
    Returns an md5 hash for an object with read() method.
    Stolen from http://code.activestate.com/recipes/266486/
    """
    m = md5.new()
    while True:
        d = fobj.read(8096)
        if not d:
            break
        m.update(d)
    return m.hexdigest()


def md5sum(fname):
    # Returns an md5 hash for file fname, or stdin if fname is \"-\"."""

    if ( fname == "-" ):
        ret = sumfile(sys.stdin)
    else:
        try:
            f = file(fname, 'rb')
        except:
            logging.critical('Failed to open [ %s ]')
            f.close()
            raise
        ret = sumfile(f)
        f.close()
    return ret


def create_unique_token(inputfile, clustername):
    """create a unique job token based on md5sum, timestamp, clustername, and jobname"""
    try:
        inputmd5 = md5sum(inputfile)
        inname = inputname(inputfile)
        timestamp = str(time.time())
        unique_token = inname + "-" + timestamp + "-" + inputmd5 + "-" + clustername
        return unique_token
    except:
        logging.debug('Failed crating unique token')
        raise Exception('failed crating unique token')

def dirname(rawinput):
    """Return the dirname of the input file."""
    logging.debug('Checking dirname from [ %s ]',rawinput)

    dirname = os.path.dirname(rawinput)

    if not dirname:
        dirname = '.'

#    todo: figure out if this is a desirable outcome.  i.e. do we want dirname to be empty, or do a pwd and find out what the current dir is, or keep the "./".  I suppose this could make a difference to some of the behavior of the scripts, such as copying files around and such.

    return dirname


def inputname(rawinput):
    """
    Remove the .inp & full path from the input file and set variables to indicate the difference.

    There are 2 reasons for this:
    - Users can submit a job using the syntax "gsub exam01.inp" or "gsub exam01" and both will work.
    - Sometimes it is useful to differentiate between the the job name "exam01" and the input file "exam01.inp"

    Return the name of the input.
    """
    logging.debug('Checking inputname from [ %s ]',rawinput)

    basename = os.path.basename(rawinput)
    pattern = re.compile('.inp$')
    inputname = re.sub(pattern, '', basename)
    return inputname


def inputfilename(rawinput):
    """
    Attach the .inp suffix to the inputname so we have a complete filename again.

    Return the name of the input file.
    """
    logging.debug('Checking inputfilename from [ %s ]',rawinput)

    inputfilename = os.path.basename(rawinput)
    return inputfilename


def same_input_already_run(input_object):
    """Check the database to see if this input file is run already."""
#    todo: create this function
    pass


def check_inputfile(inputfile_fullpath):
    """
    Perform various checks on the inputfile.
    Right now we just make sure it exists.  In the future it could include checks for:

    - is this a valid gamess input
    - estimate runtime
    - etc.
    """
    logging.debug('checking\t\t\t[ %s ]',inputfile_fullpath)

    if os.path.isfile(inputfile_fullpath):
        return True
    else:
        return False

def check_jobdir(jobdir):
    """
    Perform various checks on the jobdir.
    Right now we just make sure it exists.  In the future it could include checks for:

    - are the files inside valid
    - etc.
    """

    if os.path.isdir(jobdir):
        return True
    else:
        return False


def configure_logger(verbosity, logger_tag, log_file_name):
    if ( verbosity > 5):
        logging_level = 10
    else:
        logging_level = (( 6 - verbosity) * 10)

    log = logging.getLogger(logger_tag)
    log.setLevel(logging_level)
    handler = logging.handlers.RotatingFileHandler(log_file_name, maxBytes=200, backupCount=5)
    log.addHandler(handler)
    return log

def configure_logging(verbosity):
    """Configure logging service."""

    if ( verbosity > 5):
        logging_level = 10
    else:
        logging_level = (( 6 - verbosity) * 10)

    logging.basicConfig(level=logging_level, format='%(asctime)s %(levelname)-8s %(message)s')

    return

def check_qgms_version(minimum_version):
    """
    This will check that the qgms script is an acceptably new version.
    This function could also be exanded to make sure gamess is installed and working, and if not recompile it first.
    """
    # todo: fill out this function.
    # todo: add checks to verify gamess is working?

    current_version = 0.1

    # todo: write some function that goes out and determines version

    if minimum_version < current_version:
        logging.error('qgms script version is too old.  Please update it and resubmit.')
        return False

    return True


def read_config(config_file_location):

    resource_list = []
    defaults = {}

    try:
        _configFileLocation = os.path.expandvars(config_file_location)
        if ( os.path.exists(_configFileLocation) & os.path.isfile(_configFileLocation) ):
            # Config File exists; read it
            config = ConfigParser.ConfigParser()
            config.readfp(open(_configFileLocation))
            defaults = config.defaults()

            _resources = config.sections()
            for _resource in _resources:
                _option_list = config.options(_resource)
                _resource_options = {}
                for _option in _option_list:
                    _resource_options[_option] = config.get(_resource,_option)
                _resource_options['name'] = _resource
                resource_list.append(_resource_options)
#                resource_list[_resource] = _resource_options

            logging.debug('readConfig resource_list lenght of [ %d ]',len(resource_list))
            return [defaults,resource_list]
        else:
            logging.error('config file [%s] not found or not readable ',_configFileLocation)
            raise Exception('config file not found')
    except:
        logging.error('Exception in readConfig')
        raise

def obtain_file_lock(joblist_location, joblist_lock):
    # Obtain lock
    lock_obtained = False
    retries = 3
    default_wait_time = 1


    # if joblist_location does not exist, create it
    if not os.path.exists(joblist_location):
        open(joblist_location, 'w').close()
        logging.debug(joblist_location + ' did not exist.  created it.')


    logging.debug('trying creating lock for %s in %s',joblist_location,joblist_lock)    

    while lock_obtained == False:
        if ( retries > 0 ):
            try:
                os.link(joblist_location,joblist_lock)
                lock_obtained = True
                break
            except OSError:
                # lock already created; wait
                logging.debug('Lock already created; retry later [ %d ]',retries)
                time.sleep(default_wait_time)
                retries = retries - 1
            except:
                logging.error('failed obtaining lock due to %s',sys.exc_info()[1])
                raise
        else:
            logging.error('could not obtain lock for updating list of jobs')
            break

    return lock_obtained

def release_file_lock(joblist_lock):
    try:
        os.remove(joblist_lock)
        return True
    except:
        logging.debug('Failed removing lock due to %s',sys.exc_info()[1])
        return False

def send_email(_to,_from,_subject,_msg):
    try:
        _message = MIMEText(_msg)
        _message['Subject'] = _subject
        _message['From'] = _from
        _message['To'] = _to
        
        s = smtplib.SMTP()
        s.connect()
        s.sendmail(_from,[_to],_message.as_string())
        s.quit()
        
    except:
        logging.error('Failed sending email [ %s ]',sys.exc_info()[1])

def check_grid_authentication():
    try:
        c = Certificate(PROXY)
        if ( c.IsExpired() ):
            raise
        return True
    except:
        return False

def check_user_certificate():
    try:
        c = Certificate(USERCERT)
        if ( c.IsExpired() ):
            raise
        return True
    except:
        return False

def renew_grid_credential(_aaiUserName):
    VOMSPROXYINIT = ['voms-proxy-init','-valid','24:00','-voms','smscg','-q','-pwstdin']
    SLCSINFO = "openssl x509 -noout -checkend 3600 -in ~/.globus/usercert.pem"
    SLCSINIT = "slcs-init --idp uzh.ch"
#    AAI_CREDENTIAL_REPO = "$HOME/.gc3/aai_credential"
                            
    try:
        if ( _aaiUserName is None ):
            # Go interactive
            _aaiUserName = raw_input('Insert AAI/Switch username for user '+getpass.getuser()+': ')
        # UserName set, go interactive asking password
        input_passwd = getpass.getpass('Insert AAI/Switch password for user '+_aaiUserName+' : ')
        logging.debug('Checking slcs status')
        if ( check_user_certificate() != True ):
            # Failed because slcs credential expired
            # trying renew slcs
            # this should be an interactiave command
            logging.debug('Checking slcs status\t\t[ failed ]')
            logging.debug('Initializing slcs')
            retval = commands.getstatusoutput(SLCSINIT+" -u "+_aaiUserName+" -p "+input_passwd+" -k "+input_passwd)
            if ( retval[0] != 0 ):
                logging.critical("failed renewing slcs: %s",retval[1])
                raise Exceptions.SLCSException('failed slcs-init')
                
        logging.info('Initializing slcs\t\t\t[ ok ]')
                
        # Try renew voms credential
        # Another interactive command
        
        logging.debug('Initializing voms-proxy')
        
        p1 = subprocess.Popen(['echo',input_passwd],stdout=subprocess.PIPE)
        p2 = subprocess.Popen(VOMSPROXYINIT,stdin=p1.stdout,stdout=subprocess.PIPE)
        p2.communicate()
        if ( p2.returncode != 0 ):
            # Failed renewing voms credential
            # FATAL ERROR
            logging.critical("Initializing voms-proxy\t\t[ failed ]")
            raise Exceptions.VOMSException('failed voms-proxy-init')

        logging.info('Initializing voms-proxy\t\t[ ok ]')
        logging.info('check_authentication\t\t\t\t[ ok ]')
        
        # disposing content of passord variable
        input_passwd = None
        return True
    except:
        logging.error('Check grid credential failed  [ %s ]',sys.exc_info()[1])
        # Return False or raise exception ?
        raise
    
def display_job_status(job_list):
    if len(job_list) > 0:
        #sys.stdout.write("Job id\t\t\t\t\t\t Status \t Resource\n")
        #sys.stdout.write("======================================================================================================\n")
        for _job in job_list:
            _status_string = ""
            if _job.status is Job.JOB_STATE_FINISHED:
                _status_string = 'FINISHED'
            elif _job.status is Job.JOB_STATE_RUNNING:
                _status_string = 'RUNNING'
            elif _job.status is Job.JOB_STATE_FAILED:
                _status_string = 'FAILED'
            elif _job.status is Job.JOB_STATE_SUBMITTED:
                _status_string = 'SUBMITTED'
            elif _job.status is Job.JOB_STATE_COMPLETED:
                _status_string = 'COMPLETED'
            else:
                gc3utils.log.error('job status [ %d ] setting to Unknown',_job.status)
                _status_string = 'UNKNOWN'
 
            sys.stdout.write(_job.unique_token+'\t'+_status_string)
            sys.stdout.write('\n')
            sys.stdout.flush()

def get_job(unique_token):
    return get_job_filesystem(unique_token)

def get_job_filesystem(unique_token):

    # initialize Job object
    _job = Job.Job(status=Job.JOB_STATE_UNKNOWN)

    # get lrms_jobid
    try:
        _fileHandle = open(unique_token+'/'+Default.JOB_FILE)
        _job_info = re.split('\t',_fileHandle.read())
        _job.insert('resource_name',_job_info[0])
        _job.insert('lrms_jobid',_job_info[1])
        _job.insert('unique_token',unique_token)

        gc3utils.log.debug('Job: %s resource_name: %s lrms_jobid: %s',unique_token,_job.resource_name,_job.lrms_jobid)
        gc3utils.log.info('Created Job instance\t[ok]')

        return _job
    except:
        raise Exceptions.JobRetrieveError(sys.exc_info()[1])
