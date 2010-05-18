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
#import smtplib
import subprocess
#from email.mime.text import MIMEText
sys.path.append('/opt/nordugrid/lib/python2.4/site-packages')
import warnings
warnings.simplefilter("ignore")
from arclib import *
import Exceptions
import Job
import Default
import gc3utils


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
            gc3utils.log.critical('Failed to open [ %s ]')
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
        gc3utils.log.debug('Failed crating unique token')
        raise Exception('failed crating unique token')

def dirname(rawinput):
    """Return the dirname of the input file."""
    gc3utils.log.debug('Checking dirname from [ %s ]',rawinput)

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
    gc3utils.log.debug('Checking inputname from [ %s ]',rawinput)

    basename = os.path.basename(rawinput)
    pattern = re.compile('.inp$')
    inputname = re.sub(pattern, '', basename)
    return inputname


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
    gc3utils.log.debug('checking\t\t\t[ %s ]',inputfile_fullpath)

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

def configure_logger(verbosity, log_file_name):
    if ( verbosity > 5):
        logging_level = 10
    else:
        logging_level = (( 6 - verbosity) * 10)

    gc3utils.log.setLevel(logging_level)
    handler = logging.handlers.RotatingFileHandler(log_file_name, maxBytes=200, backupCount=5)
    gc3utils.log.addHandler(handler)


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
        gc3utils.log.error('qgms script version is too old.  Please update it and resubmit.')
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

            gc3utils.log.debug('readConfig resource_list length of [ %d ]',len(resource_list))
            return [defaults,resource_list]
        else:
            gc3utils.log.error('config file [%s] not found or not readable ',_configFileLocation)
            raise Exception('config file not found')
    except:
        gc3utils.log.error('Exception in readConfig')
        raise

def obtain_file_lock(joblist_location, joblist_lock):
    # Obtain lock
    lock_obtained = False
    retries = 3
    default_wait_time = 1


    # if joblist_location does not exist, create it
    if not os.path.exists(joblist_location):
        open(joblist_location, 'w').close()
        gc3utils.log.debug(joblist_location + ' did not exist.  created it.')


    gc3utils.log.debug('trying creating lock for %s in %s',joblist_location,joblist_lock)    

    while lock_obtained == False:
        if ( retries > 0 ):
            try:
                os.link(joblist_location,joblist_lock)
                lock_obtained = True
                break
            except OSError:
                # lock already created; wait
                gc3utils.log.debug('Lock already created; retry later [ %d ]',retries)
                time.sleep(default_wait_time)
                retries = retries - 1
            except:
                gc3utils.log.error('failed obtaining lock due to %s',sys.exc_info()[1])
                raise
        else:
            gc3utils.log.error('could not obtain lock for updating list of jobs')
            break

    return lock_obtained

def release_file_lock(joblist_lock):
    try:
        os.remove(joblist_lock)
        return True
    except:
        gc3utils.log.debug('Failed removing lock due to %s',sys.exc_info()[1])
        return False

#def send_email(_to,_from,_subject,_msg):
#    try:
#        _message = MIMEText(_msg)
#        _message['Subject'] = _subject
#        _message['From'] = _from
#        _message['To'] = _to
        
#        s = smtplib.SMTP()
#        s.connect()
#        s.sendmail(_from,[_to],_message.as_string())
#        s.quit()
        
#    except:
#        logging.error('Failed sending email [ %s ]',sys.exc_info()[1])

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
        gc3utils.log.debug('Checking slcs status')
        if ( check_user_certificate() != True ):
            # Failed because slcs credential expired
            # trying renew slcs
            # this should be an interactiave command
            gc3utils.log.debug('Checking slcs status\t\t[ failed ]')
            gc3utils.log.debug('Initializing slcs')
            retval = commands.getstatusoutput(SLCSINIT+" -u "+_aaiUserName+" -p "+input_passwd+" -k "+input_passwd)
            if ( retval[0] != 0 ):
                gc3utils.log.critical("failed renewing slcs: %s",retval[1])
                raise Exceptions.SLCSException('failed slcs-init')
                
        gc3utils.log.info('Initializing slcs\t\t\t[ ok ]')
                
        # Try renew voms credential
        # Another interactive command
        
        gc3utils.log.debug('Initializing voms-proxy')
        
        p1 = subprocess.Popen(['echo',input_passwd],stdout=subprocess.PIPE)
        p2 = subprocess.Popen(VOMSPROXYINIT,stdin=p1.stdout,stdout=subprocess.PIPE)
        p2.communicate()
        if ( p2.returncode != 0 ):
            # Failed renewing voms credential
            # FATAL ERROR
            gc3utils.log.critical("Initializing voms-proxy\t\t[ failed ]")
            raise Exceptions.VOMSException('failed voms-proxy-init')

        gc3utils.log.info('Initializing voms-proxy\t\t[ ok ]')
        gc3utils.log.info('check_authentication\t\t\t\t[ ok ]')
        
        # disposing content of passord variable
        input_passwd = None
        return True
    except:
        gc3utils.log.error('Check grid credential failed  [ %s ]',sys.exc_info()[1])
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
                gc3utils.log.error('job status [ %s ] setting to Unknown',_job.status)
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
