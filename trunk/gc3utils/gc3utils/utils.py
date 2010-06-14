import sys
import os
import os.path
import commands
import logging
import logging.handlers
import tempfile
import getpass
import re
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
from Exceptions import *
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


def dirname(pathname):
    """
    Same as `os.path.dirname` but return `.` in case of path names with no directory component.
    """
    dirname = os.path.dirname(pathname)
    if not dirname:
        dirname = '.'
    # FIXME: figure out if this is a desirable outcome.  i.e. do we want dirname to be empty, or do a pwd and find out what the current dir is, or keep the "./".  I suppose this could make a difference to some of the behavior of the scripts, such as copying files around and such.
    return dirname


def inputname(pathname):
    """
    Return the file name, with `.inp` extension and the directory name stripped out.

    There are 2 reasons for this:
    - Users can submit a job using the syntax "gsub exam01.inp" or "gsub exam01" and both will work.
    - Sometimes it is useful to differentiate between the the job name "exam01" and the input file "exam01.inp"
    """
    return os.path.splitext(input_file_name(pathname))[0]
#    basename = os.path.basename(pathname)
#    filename, ext = os.path.splitext(basename)
    # FIXME: should raise exception if `ext` is not ".inp"
#    return filename

def input_file_name(pathname):
    return  os.path.basename(pathname)

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


def configuration_file_exists(filename, template_filename=None):
    """
    Return `True` if a file with the specified name exists in the 
    configuration directory.  If not, try to copy the template file
    over and then return `False`; in case the copy operations fails, 
    a `NoConfigurationFile` exception is raised.

    If parameter `filename` is not an absolute path, it is interpreted
    as relative to `gc3utils.Default.RCDIR`; if `template_filename` is
    `None`, then it is assumed to be the same as `filename`.
    """
    if template_filename is None:
        template_filename = os.path.basename(filename)
    if not os.path.isabs(filename):
        filename = os.path.join(Default.RCDIR, filename)
    if os.path.exists(filename):
        return True
    else:
        try:
            # copy sample config file 
            if not os.path.exists(dirname(filename)):
                os.makedirs(dirname(filename))
            from pkg_resources import Requirement, resource_filename
            sample_config = resource_filename(Requirement.parse("gc3utils"), 
                                              "gc3utils/etc/" + template_filename)
            import shutil
            shutil.copyfile(sample_config, filename)
            return False
        except IOError, x:
            gc3utils.log.critical("CRITICAL ERROR: Failed copying configuration file: %s" % x)
            raise NoConfigurationFile("No configuration file '%s' was found, and an attempt to create it failed. Aborting." % filename)
        except ImportError:
            raise NoConfigurationFile("No configuration file '%s' was found. Aborting." % filename)
    

def from_template(template, **kw):
    """
    Return the contents of `template`, substituting all occurrences
    of Python formatting directives '%(key)s' with the corresponding values 
    taken from dictionary `kw`.

    If `template` is an object providing a `read()` method, that is
    used to gather the template contents; else, if a file named
    `template` exists, the template contents are read from it;
    otherwise, `template` is treated like a string providing the
    template contents itself.
    """
    if hasattr(template, 'read') and callable(template.read):
        template_contents = template.read()
    elif os.path.exists(template):
        template_file = file(template, 'r')
        template_contents = template_file.read()
        template_file.close()
    else:
        # treat `template` as a string
        template_contents = template
    # substitute `kw` into `t` and return it
    return (template_contents % kw)


# === Configuration File
def import_config(config_file_location):
    (default_val,resources_vals) = read_config(config_file_location)
    return (get_defaults(default_val),get_resources(resources_vals))

def get_defaults(defaults):
    # Create an default object for the defaults
    # defaults is a list[] of values
    try:
        # Create default values
        default = gc3utils.Default.Default(defaults)
    except:
        gc3utils.log.critical('Failed loading default values')
        raise
        
    return default
    

def get_resources(resources_list):
    # build Resource objects from the list returned from read_config
    #        and match with selectd_resource from comand line
    #        (optional) if not options.resource_name is None:
    resources = []
    
    try:
        for resource in resources_list:
            gc3utils.log.debug('creating instance of Resource object... ')

            try:
                tmpres = gc3utils.Resource.Resource(resource)
            except:
                gc3utils.log.error("rejecting resource '%s'",resource['name'])
                #                gc3utils.log.warning("Resource '%s' failed validity test - rejecting it.",
                #                                     resource['name'])

                continue
#            tmpres = gc3utils.Resource.Resource()
                
#            tmpres.update(resource)
            #            for items in resource:
            #                gc3utils.log.debug('Updating with %s %s',items,resource[items])
            #                tmpres.insert(items,resource[items])
            
            gc3utils.log.debug('Checking resource type %s',resource['type'])
            if resource['type'] == 'arc':
                tmpres.type = gc3utils.Default.ARC_LRMS
            elif resource['type'] == 'ssh_sge':
                tmpres.type = gc3utils.Default.SGE_LRMS
            else:
                gc3utils.log.error('No valid resource type %s',resource['type'])
                continue
            
            gc3utils.log.debug('checking validity with %s',str(tmpres.is_valid()))
            
            resources.append(tmpres)
    except:
        gc3utils.log.critical('failed creating Resource list')
        raise
    
    return resources

                                
def read_config(config_file_location):
    """
    Read configuration file.
    """

    resource_list = []
    defaults = {}

#    print 'mike_debug 100'
#    print config_file_location

    _configFileLocation = os.path.expandvars(config_file_location)
    if not configuration_file_exists(_configFileLocation, "gc3utils.conf.example"):
        # warn user
        raise NoConfigurationFile("No configuration file '%s' was found; a sample one has been copied in that location; please edit it and define resources before you try running gc3utils commands again." % _configFileLocation)

    # Config File exists; read it
    config = ConfigParser.ConfigParser()
    try:
        config_file = open(_configFileLocation)
        config.readfp(config_file)
    except:
        raise NoConfigurationFile("Configuration file '%s' is unreadable or malformed. Aborting." 
                                  % _configFileLocation)

    defaults = config.defaults()

    _resources = config.sections()
    for _resource in _resources:
        _option_list = config.options(_resource)
        _resource_options = {}
        for _option in _option_list:
            _resource_options[_option] = config.get(_resource,_option)
        _resource_options['name'] = _resource
        resource_list.append(_resource_options)

    gc3utils.log.debug('readConfig resource_list length of [ %d ]',len(resource_list))
    return [defaults,resource_list]

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
                raise SLCSException('failed slcs-init')
                
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
            raise VOMSException('failed voms-proxy-init')

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
    try:
        return {
#            Job.JOB_STATE_HOLD:    'HOLD',
#            Job.JOB_STATE_WAIT:    'WAITING',
#            Job.JOB_STATE_READY:   'READY',
#            Job.JOB_STATE_ERROR:   'ERROR',
            Job.JOB_STATE_FAILED:  'FAILED',
#            Job.JOB_STATE_OUTPUT:  'OUTPUTTING',
            Job.JOB_STATE_RUNNING: 'RUNNING',
            Job.JOB_STATE_FINISHED:'FINISHED',
#            Job.JOB_STATE_NOTIFIED:'NOTIFIED',
            Job.JOB_STATE_SUBMITTED:'SUBMITTED',
            Job.JOB_STATE_COMPLETED:'COMPLETED',
            Job.JOB_STATE_DELETED: 'DELETED'
            }[job_status]
    except KeyError:
        gc3utils.log.error('job status code %s unknown', job_status)
        return 'UNKNOWN'


def get_job(unique_token):
    return get_job_filesystem(unique_token)

def get_job_filesystem(unique_token):

    handler = None
    gc3utils.log.debug('retrieving job from %s',Default.JOBS_DIR+'/'+unique_token)

    try:
        if not os.path.exists(Default.JOBS_DIR+'/'+unique_token):
            raise JobRetrieveError('Job not found')
        handler = shelve.open(Default.JOBS_DIR+'/'+unique_token)
        job = Job.Job(handler) 
        handler.close()
        if job.is_valid():
            return job
        else:
            raise JobRetrieveError('Failed retrieving job from filesystem')
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


def persist_job(job_obj):
    return persist_job_filesystem(job_obj)

def persist_job_filesystem(job_obj):

    handler = None
    gc3utils.log.debug('dumping job in %s',Default.JOBS_DIR+'/'+job_obj.unique_token)
    if not os.path.exists(Default.JOBS_DIR):
        try:
            os.makedirs(Default.JOBS_DIR)
        except Exception, x:
            # raise same exception but add context message
            gc3utils.log.error("Could not create jobs directory '%s': %s" 
                               % (Default.JOBS_DIR, x))
            raise
    try:
        handler = shelve.open(Default.JOBS_DIR+'/'+job_obj.unique_token)
        handler.update(job_obj)
        handler.close()
    except Exception, x:
        gc3utils.log.error("Could not persist job %s to '%s': %s" 
                           % (job_obj.unique_token, Default.JOBS_DIR, x))
        if handler:
            handler.close()
        raise

def clean_job(unique_token):
    return clean_job_filesystem(unique_token)

def clean_job_filesystem(unique_token):
    if os.path.isfile(Default.JOBS_DIR+'/'+unique_token):
        os.remove(Default.JOBS_DIR+'/'+unique_token)
    return 0

def prepare_job_dir(_download_dir):
    try:
        if os.path.isdir(_download_dir):
            # directory exists; move it to .1
            os.rename(_download_dir,_download_dir + "_" + create_unique_token())

        os.makedirs(_download_dir)
        return True

    except:
        gc3utils.log.error('Failed creating folder %s ' % _download_dir)
        gc3utils.log.debug('%s %s',sys.exc_info()[0], sys.exc_info()[1])
        return False

if __name__ == '__main__':
    import doctest
    doctest.testmod()
