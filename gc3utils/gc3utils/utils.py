import sys
import os
import os.path
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
from lockfile import FileLock
import shelve


    

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
    """
    Returns an md5 hash for file fname, or stdin if fname is \"-\".
    """

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


def progressive_number():
    """
    Return a positive integer, whose value is guaranteed to
    be monotonically increasing across different invocations
    of this function, and also across separate instances of the
    calling program.

    Example::

      >>> n = progressive_number()
      >>> m = progressive_number()
      >>> m > n
      True

    After every invocation of this function, the returned number
    is stored into the file ``~/.gc3/next_id.txt``.

    *Note:* as file-level locking is used to serialize access to the
    counter file, this function may block (default timeout: 30
    seconds) while trying to acquire the lock, or raise an exception
    if this fails.

    """
    # FIXME: should use global config value for directory
    id_filename = os.path.expanduser("~/.gc3/next_id.txt")
    # ``FileLock`` requires that the to-be-locked file exists; if it
    # does not, we create an empty one (and avoid overwriting any
    # content, in case another process is also writing to it).  There
    # is thus no race condition here, as we attempt to lock the file
    # anyway, and this will stop concurrent processes.
    if not os.path.exists(id_filename):
        open(id_filename, "a").close()
    lock = FileLock(id_filename, threaded=False) 
    lock.acquire(timeout=30) # XXX: can raise 'LockTimeout'
    id_file = open(id_filename, 'r+')
    id = int(id_file.read(8) or "0", 16)
    id +=1 
    id_file.seek(0)
    id_file.write("%08x -- DO NOT REMOVE OR ALTER THIS FILE: it is used internally by the gc3utils\n" % id)
    id_file.close()
    lock.release()
    return id


def create_unique_token():
    """
    Return a "unique job identifier" (a string).  Job identifiers are 
    temporally unique: no job identifier will (ever) be re-used,
    even in different invocations of the program.

    Currently, the unique job identifier has the form "job.XXX" where
    "XXX" is a decimal number.  
    """
    return "job.%d" % progressive_number()


def dirname(rawinput):
    """
    Return the dirname of the input file.
    """

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
    """
    Configure the gc3utils logger.

    - Input is the logging level and a filename to use.
    - Returns nothing.
    """

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
    """
    Read configuration file.
    """

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
    """
    Lock a file.
    """

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
    """
    Release locked file.
    """

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

def job_status_to_string(job_status):
    _status_string = ""
    if job_status is Job.JOB_STATE_FINISHED:
        _status_string = 'FINISHED'
    elif job_status is Job.JOB_STATE_RUNNING:
        _status_string = 'RUNNING'
    elif job_status is Job.JOB_STATE_FAILED:
        _status_string = 'FAILED'
    elif job_status is Job.JOB_STATE_SUBMITTED:
        _status_string = 'SUBMITTED'
    elif job_status is Job.JOB_STATE_COMPLETED:
        _status_string = 'COMPLETED'
    elif job_status is Job.JOB_STATE_HOLD:
        _status_string = 'HOLD'
    elif job_status is Job.JOB_STATE_READY:
        _status_string = 'READY'
    elif job_status is Job.JOB_STATE_WAIT:
        _status_string = 'WAIT'
    elif job_status is Job.JOB_STATE_OUTPUT:
        _status_string = 'OUTPUT'
    elif job_status is Job.JOB_STATE_UNREACHABLE:
        _status_string = 'UNREACHABLE'
    elif job_status is Job.JOB_STATE_NOTIFIED:
        _status_string = 'NOTIFIED'
    elif job_status is Job.JOB_STATE_ERROR:
        _status_string = 'ERROR'
    else:
        gc3utils.log.error('job status [ %s ] setting to Unknown',job_status)
        _status_string = 'UNKNOWN'
    return _status_string


def display_job_status(job_list,job_status_filter):
    if len(job_list) > 0:
        sys.stdout.write("Job id\t\t\t\t\t Status\n")
        sys.stdout.write("-------------------------------------------------\n")
        for _job in job_list:

            gc3utils.log.debug('displaying job status %d',_job.status)

            if job_status_filter > 0:
                if not _job.status == job_status_filter:
                    continue
            _status_string = job_status_to_string(_job.status)

 
            sys.stdout.write(_job.unique_token+'\t'+_status_string)
            sys.stdout.write('\n')
            sys.stdout.flush()

def get_job(unique_token):
    return get_job_filesystem(unique_token)

def get_job_filesystem(unique_token):

    handler = None
    gc3utils.log.debug('retrieving job from %s',Default.JOBS_DIR+'/'+unique_token)

    try:
        handler = shelve.open(Default.JOBS_DIR+'/'+unique_token)
        job = Job.Job(unique_token)
        job.update(handler)
        handler.close()
        if job.is_valid():
            return job
        else:
            raise Exceptions.JobRetrieveError('Failed retrieving job from filesystem')
    except:
        if handler:
            handler.close()
        raise

def create_job_folder_filesystem(job_folder_location,unique_token):
    try:
        # create_unique_token
        unique_id = job_folder_location + '/' + unique_token
        while os.path.exists(unique_id):
            unique_id = unique_id + '_' + os.getgid()

        gc3utils.log.debug('creating folder for job session: %s',unique_id)
        os.makedirs(unique_id)
    except:
        gc3utils.log.error('Failed creating job on filesystem')
        raise

def persist_job_filesystem(job_obj):

    handler = None
    gc3utils.log.debug('dumping job in %s',Default.JOBS_DIR+'/'+job_obj.unique_token)

    try:
        handler = shelve.open(Default.JOBS_DIR+'/'+job_obj.unique_token)
        handler.update(job_obj)
        handler.close()
    except:
        if handler:
            handler.close()
        raise

def prepare_job_dir(_download_dir):
    if os.path.isdir(_download_dir):
        # directory exists; move it to .1
        os.rename(_download_dir,_download_dir + "_" + create_unique_token())

    os.makedirs(_download_dir)

#def mark_completed_job(job_obj):
    #                # Job finished; results retrieved; writing .finished file
#    try:
#        gc3utils.log.debug('Creating finished file')
#        open(unique_token+"/"+self.defaults['lrms_finished'],'w').close()
#    except:
#        gc3utils.log.error('Failed creating finished file [ %s ]',sys.exc_info()[1])
#        # Should handle the exception differently ?
#    
#    gc3utils.log.debug('Removing jobid from joblist file')
#    # Removing jobid from joblist file
#    try:
#        default_joblist_location = os.path.expandvars(default_joblist_location)
#        default_joblist_lock = os.path.expandvars(default_joblist_lock)
#        
#        if ( obtain_file_lock(default_joblist_location,default_joblist_lock) ):
#            _newFileHandle = tempfile.NamedTemporaryFile(suffix=".xrsl",prefix="gridgames_arc_")
#            
#            _oldFileHandle  = open(default_joblist_location)
#            _oldFileHandle.seek(0)
#            for line in _oldFileHandle:
#    598#                            gc3utils.log.debug('checking %s with %s',line,unique_token)
#    599#                            if ( not unique_token in line ):
#    600#                                gc3utils.log.debug('writing line')
#    601#                                _newFileHandle.write(line)
#    602#
#    603#                        _oldFileHandle.close()
#    604#
#    605#                        os.remove(default_joblist_location)
#    606#
#    607#                        _newFileHandle.seek(0)
#    608#
#    609#                        gc3utils.log.debug('replacing joblist file with %s',_newFileHandle.name)
#    610#                        os.system("cp "+_newFileHandle.name+" "+default_joblist_location)
#    611#
#    612#                        _newFileHandle.close()
#    613#
#    614#                    else:
#    615#                        raise Exception('Failed obtain lock')
#    616#                except:
#    617#                    gc3utils.log.error('Failed updating joblist file in %s',default_joblist_location)
#    618#                    gc3utils.log.debug('Exception %s',sys.exc_info()[1])
#    619#
#    620#                # release lock
#    621#                if ( (not release_file_lock(default_joblist_lock)) & (os.path.isfile(default_joblist_lock)) ):
#    622#                    gc3utils.log.error('Failed removing lock file')
    
if __name__ == '__main__':
    import doctest
    doctest.testmod()
