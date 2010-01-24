import sys
import os
import commands
import logging
import tempfile
import getpass
import re
import md5
import time
import ConfigParser
import shutil
import getpass

# -----------------------------------------------------
# Interface design
#

class LRMS:

    def __init__(self, resource):
        pass
    
    def check_authentication(self):
        pass
    
    def submit_job(self, unique_token, application, input_file):
        # return LRMS specific lrms_jobid
        # stages input files if necessary
        # dumps submission stdout to lrms_log string
        pass
    
    def check_status(self,lrms_jobid):
        pass
    
    def get_results(self,lrms_jobid,job_dir):
        pass

# -----------------------------------------------------
# ARC lrms
#
    
class ArcLrms(LRMS):

    isValid = 0
    VOMSPROXYINFO = "grid-proxy-info -exists -valid 1:10"
    VOMSPROXYINIT = "voms-proxy-init -q -voms smscg"
    SLCSINFO = "openssl x509 -noout -checkend 3600 -in ~/.globus/usercert.pem"
    SLCSINIT = "slcs-init --idp uzh.ch"
    GAMESS_XRSL_TEMPLATE = "~/.gc3/gamess_template.xrsl"
    AAI_CREDENTIAL_REPO = "~/.gc3/aai_credential"
    resource = []

    def __init__(self, resource):
        # check first that manadtory fields are defined
        # if resource['frontend'] == "" means access to the entire arc based infrastructure
        if (resource['type'] == "arc"):
            self.resource = resource
            # shall we really set hardcoded defaults ?
            if ( 'cores' not in self.resource ):
                self.resource['cores'] = "1"
            if ( 'memory' not in self.resource ):
                self.resource['memory'] = "1000"
            if ( 'walltime' not in self.resource ):
                self.resource['walltime'] = "12"
            self.isValid = 1

    def check_authentication(self):
        try:
            logging.debug('Checking voms-proxy status')
            retval = commands.getstatusoutput(self.VOMSPROXYINFO)
            if ( retval[0] != 0 ):
                # Probably failed because of expired credential
                # Checking slcs first

                logging.debug('Checking voms-proxy status\t\t[ failed ]:\n\t%s',retval[1])

                # Getting AAI username
                _aaiUserName = None

                self.AAI_CREDENTIAL_REPO = os.path.expandvars(self.AAI_CREDENTIAL_REPO)
                if ( os.path.exists(self.AAI_CREDENTIAL_REPO) & os.path.isfile(self.AAI_CREDENTIAL_REPO) ):
                    logging.debug('Opening AAI credential file in %s',self.AAI_CREDENTIAL_REPO)
                    _fileHandle = open(self.AAI_CREDENTIAL_REPO,'r')
                    _aaiUserName = _fileHandle.read()
                    logging.debug('_aaiUserName: %s',_aaiUserName)
                    _aaiUserName = _aaiUserName.rstrip("\n")
                    logging.debug('_aaiUserName: %s',_aaiUserName)

                if ( _aaiUserName is None ):
                    _aaiUserName = raw_input('Insert AAI/Switch username for user '+getpass.getuser()+': ')

                input_passwd = getpass.getpass('Insert AAI/Switch password for user '+_aaiUserName+' : ')

                logging.debug('Checking slcs status')
                retval = commands.getstatusoutput(self.SLCSINFO)
                if ( retval[0] != 0 ):
                    # Failed because slcs credential expired
                    # trying renew slcs 
                    # this should be an interactiave command
                    logging.debug('Checking slcs status\t\t[ failed ]\n\t%s',retval[1])
                    logging.debug('Initializing slcs')
                    retval = commands.getstatusoutput(self.SLCSINIT+" -u "+_aaiUserName+" -p "+input_passwd+" -k "+input_passwd)
                    if ( retval[0] != 0 ):
                        # Failed renewing slcs
                        logging.critical("failed renewing slcs: %s",retval[1])
                        return False

                    logging.info('Initializing slcs\t\t\t[ ok ]')
                    
                # Try renew voms credential
                # Another interactive command

                logging.debug('Initializing voms-proxy')
                retval = commands.getstatusoutput("echo \'"+input_passwd+"\' | "+self.VOMSPROXYINIT+" -pwstdin")
                if ( retval[0] != 0 ):
                    # Failed renewing voms credential
                    # FATAL ERROR
                    logging.critical("Initializing voms-proxy\t\t[ failed]\n\t%s",retval[1])
                    return False
                logging.info('Initializing voms-proxy\t\t[ ok ]')
            logging.info('check_authentication\t\t\t\t[ ok ]')
                
            return True
        except:
            raise Exception('failed in check_authentication')


    def submit_job(self, unique_token, application, input_file):
        try:
            # Initialize xrsl from template
            self.GAMESS_XRSL_TEMPLATE = os.path.expandvars(self.GAMESS_XRSL_TEMPLATE)
                
            if ( os.path.exists(self.GAMESS_XRSL_TEMPLATE) & os.path.isfile(self.GAMESS_XRSL_TEMPLATE) ):

                _file_handle = tempfile.NamedTemporaryFile(suffix=".xrsl",prefix="gridgames_arc_")
                logging.debug('tmp file %s',_file_handle.name)

                # getting information from input_file
                _file_name = os.path.basename(input_file)
                _file_name = _file_name.split(".")[0]
                _file_name_path = os.path.dirname(input_file)
                logging.debug('Input file path %s dirpath %s',_file_name,_file_name_path)
                
                _command = "sed -e 's|CORES|"+self.resource['cores']+"|g' -e 's|INPUT_FILE_NAME|"+_file_name+"|g' -e 's|INPUT_FILE_PATH|"+_file_name_path+"|g' -e 's|MEMORY|"+self.resource['memory']+"|g'  -e 's|WALLTIME|"+self.resource['walltime']+"|g'  < "+self.GAMESS_XRSL_TEMPLATE+" > "+_file_handle.name

                # Cleaning up
                _file_handle.close()

                logging.debug('preparing SED command: %s',_command)
                retval = commands.getstatusoutput(_command)

                if ( retval[0] != 0 ):
                    # Failed somehow
                    logging.error("Create XRSL\t\t[ failed ]")
                    logging.debug(retval[1])
                    # Shall we dump anyway into lrms_log befor raising ?
                    raise Exception('failed creating submission file')


                logging.debug('checking resource [ %s ]',self.resource['frontend'])
                # Ready for real submission
                if ( self.resource['frontend'] == "" ):
                    # frontend not defined; use the entire arc-based infrastructure
                    _command = "ngsub -d2 -f "+_file_handle.name
                else:
                    _command = "ngsub -d2 -c "+self.resource['frontend']+" -f "+_file_handle.name

                logging.debug('Running ARC command [ %s ]',_command)
            
                retval = commands.getstatusoutput(_command)

                jobid_pattern = "Job submitted with jobid: "

                if ( ( retval[0] != 0 ) | ( jobid_pattern not in retval[1] ) ):
                    # Failed somehow
                    logging.error("ngsub command\t\t[ failed ]")
                    logging.debug(retval[1])
                    raise Exception('failed submitting to LRMS')

                # assuming submit successfull
                logging.debug("ngsub command\t\t[ ok ]")
                logging.debug(retval[1])

                # Extracting ARC jobid
                lrms_jobid = re.split(jobid_pattern,retval[1])[1]
                logging.debug('Job submitted with jobid: %s',lrms_jobid)

                return [lrms_jobid,retval[1]]

            else:
                logging.critical('XRSL file not found %s',self.GAMESS_XRSL_TEMPLATE)
                raise Exception('template file for submission scritp not found')
        except:
            logging.critical('Failure in submitting')
            raise

    def check_status(self, lrms_jobid):
#        submitted_list = ['ACCEPTING','SUBMITTING','PREPARING']
        running_list = ['INLRMS:Q','INLRMS:R','EXECUTED', 'ACCEPTING','SUBMITTING','PREPARING']
        finished_list = ['FINISHED', 'FAILED']
#        failed_list = ['FAILED']
        try:
            # Ready for real submission
            _command = "ngstat "+lrms_jobid

            logging.debug('Running ARC command [ %s ]',_command)

            retval = commands.getstatusoutput(_command)
            # jobstatusunknown_pattern = "This job was only very recently"
            jobstatusunknown_pattern = "Job information not found"
            jobstatusremoved_pattern = "Job information not found"
            jobstatusok_pattern = "Status: "
            jobexitcode_pattern = "Exit Code: "
            if ( retval[0] != 0 ):
                # | ( jobstatus_pattern not in retval[1] ) ):
                # Failed somehow
                logging.error("ngstat command\t\t[ failed ]")
                logging.debug(retval[1])
                raise Exception('failed checking status to LRMS')

            if ( jobstatusunknown_pattern in retval[1] ):
                jobstatus = "Status: RUNNING"
#            elif ( jobstatusremoved_pattern in retval[1] ):
#                jobstatus = "Status: FINISHED"
            elif ( jobstatusok_pattern in retval[1] ):

                # Extracting ARC job status
                lrms_jobstatus = re.split(jobstatusok_pattern,retval[1])[1]
                lrms_jobstatus = re.split("\n",lrms_jobstatus)[0]

                logging.debug('lrms_jobstatus\t\t\t[ %s ]',lrms_jobstatus)

                if ( lrms_jobstatus in running_list ):
                    jobstatus = "Status: RUNNING"
                elif ( lrms_jobstatus in finished_list ):
                    jobstatus = "Status: FINISHED"
#                if ( lrms_jobstatus in submitted_list ):
#                    jobstatus = "Status: SUBMITTED"
#                elif ( lrms_jobstatus in running_list ):
#                    jobstatus = "Status: RUNNING"
#                elif ( ( lrms_jobstatus in finished_list ) | ( lrms_jobstatus in failed_list )):
#                    lrms_exitcode = re.split(jobexitcode_pattern,retval[1])[1]
#                    lrms_exitcode = re.split("\n",lrms_exitcode)[0]
#                    jobstatus = "Status: FINISHED\nExit Code: "+lrms_exitcode
                else:
                    jobstatus = "Status: [ "+lrms_jobstatus+" ]"

            return [jobstatus,retval[1]]

        except:
            logging.critical('Failure in checking status')
            raise

    def get_results(self,lrms_jobid,job_dir):
        try:
            result_location_pattern="Results stored at "
            
            _command = "ngget -keep -s FINISHED -d 2 -dir "+job_dir+" "+lrms_jobid

            logging.debug('Running ARC command [ %s ]',_command)

            job_results_retrieved_pattern = "successfuly downloaded: 0"

            retval = commands.getstatusoutput(_command)
            if ( ( retval[0] != 0 ) ):
                # Failed somehow
                logging.error("ngget command\t\t[ failed ]")
                logging.debug(retval[1])
                raise Exception('failed getting results from LRMS')

            if ( result_location_pattern in retval[1] ):
                _result_location_folder = re.split(result_location_pattern,retval[1])[1]
                _result_location_folder = re.split("\n",_result_location_folder)[0]
                logging.debug('Moving result data from [ %s ]',_result_location_folder)
                if ( os.path.isdir(_result_location_folder) ):
                    retval = commands.getstatusoutput("cp -ap "+_result_location_folder+"/* "+job_dir)
                    if ( retval[0] != 0 ):
                        logging.error('Failed copying results data from [ %s ] to [ %s ]',_result_location_folder,job_dir)
                    else:
                        logging.info('Copying results\t\t[ ok ]')
                        logging.debug('Removing [ %s ]',_result_location_folder)
                        shutil.rmtree(_result_location_folder)
                logging.info('get_results\t\t\t[ ok ]')
                return [True,retval[1]]
            else:
                return [False,retval[1]]
        except:
            logging.critical('Failure in retrieving results')
            raise

# -----------------------------------------------------
# SSH lrms
#

class SshLrms(LRMS):

    ssh_location = "/usr/bin/ssh"
    scp_location = "/usr/bin/scp"
    rsync_location = "/usr/bin/rsync"
    ssh_options = "-o ConnectTimeout=30"
    rsync_options= '"-e ssh ' + ssh_options + '"'
    resource = []
    
    isValid = 0
    def __init__(self, resource):
        if (resource['type'] == "ssh"):
            self.resource = resource
            # shall we really set hardcoded defaults ?
            if ( 'cores' not in self.resource ):
                self.resource['cores'] = "1"
            if ( 'memory' not in self.resource ):
                self.resource['memory'] = "1000"
            if ( 'walltime' not in self.resource ):
                self.resource['walltime'] = "12"
            self.isValid = 1


    """Here are the common functions needed in every Resource Class."""
    
    def check_authentication(self):
        """Make sure ssh to server works."""
        # We can make the assumption the local username is the same on hte remote host, whatever it is
        # We can also assume local username has passwordless ssh access to resources (or ssh-agent running)

        
        try:
            # ssh username@frontend date 
            # ssh -o ConnectTimeout=1 idesl2.uzh.ch uname -a
            testcommand = "uname -a"
            _command = self.ssh_location + " " + self.ssh_options + " " + self.resource['username'] + "@" + self.resource['frontend'] + " " + testcommand
            logging.debug('check_authentication _command: ' + _command)

            retval = commands.getstatusoutput(_command)
            if ( retval[0] != 0 ):
                raise Exception('failed in check_authentication')

            return True
        except:
            raise


    def submit_job(self, unique_token, application, input_file):
        """Submit a job."""

# todo : fix this:
#    def submit_job(self,application,inputfile,outputfile,cores,memory):


        # example: ssh mpackard@ocikbpra.uzh.ch 'cd unique_token ; $gamess_location -n cores input_file 

        # dump stdout+stderr to unique_token/lrms_log

        try:

            # copy input first
            try:
                self.copy_input(input_file, unique_token, self.resource['username'], self.resource['frontend'])
            except:
                raise

            # then try to submit it to the local queueing system 
            _submit_command = "%s %s@%s 'cd ~/%s; %s/qgms -n %s %s'" % (self.ssh_location, self.resource['username'], self.resource['frontend'], unique_token, self.resource['gamess_location'], self.resource['ncores'], input_file)
	
            logging.debug('submit _submit_command: ' + _submit_command)

            retval = commands.getstatusoutput(_submit_command)

            if ( retval[0] != 0 ):
                logging.critical("_submit_command failed")
                raise 

            lrms_jobid = self.get_qsub_jobid(retval[1])

            logging.debug('Job submitted with jobid: %s',lrms_jobid)
            return [lrms_jobid,retval[1]]

        except:
            logging.critical('Failure in submitting')
            raise


    def check_status(self, lrms_jobid):
        """Check status of a job."""

        try:
            # then check the lrms_jobid with qstat
            _testcommand = 'qstat -j %s' % lrms_jobid

            _command = self.ssh_location + " " + self.ssh_options + " " + self.resource['username'] + "@" + self.resource['frontend'] + " '" + _testcommand + "'"
            logging.debug('check_status _command: ' + _command)

            retval = (commands.getstatusoutput(_command))

            # for some reason we have to use os.WEXITSTATUS to get the real exit code here
            _realretval = str(os.WEXITSTATUS(retval[0]))
            
            logging.debug('check_status _real_retval: ' + _realretval)

            if ( _realretval == '1' ):
                jobstatus = "Status: FINISHED"
            else: 
                jobstatus = "Status: RUNNING"
        
            return [jobstatus,retval[1]]
    
        except:
            logging.critical('Failure in checking status')
            raise


    def get_results(self,lrms_jobid,unique_token):
        """Retrieve results of a job."""

        # todo: - parse settings to figure out what output files should be copied back (assume gamess for now)

        try:
	        jobname = unique_token.split('-')[0]

                full_path_to_remote_unique_id = os.path.expandvars('$HOME'+'/'+unique_token)
                full_path_to_local_unique_id = unique_token

	        # first copy the normal gamess output
	        remote_file = '%s/%s.o%s' % (full_path_to_remote_unique_id, jobname, lrms_jobid)
	        local_file = '%s/%s.out' % (full_path_to_local_unique_id, jobname)
	        # todo : check options
	        retval = self.copyback_file(remote_file, local_file)
	        if ( retval[0] != 0 ):
	            logging.critical('could not retrieve gamess output: ' + local_file)
	        else:
	            logging.debug('retrieved: ' + local_file)
	
	        # then copy the special output files
		    suffixes = ('.dat', '.cosmo', '.irc')
		    for suffix in suffixes:
		        remote_file = '%s/%s.o%s%s' % (full_path_to_remote_unique_id, jobname, lrms_jobid, suffix)
		        local_file = '%s/%s%s' % (full_path_to_local_unique_id, jobname, suffix)
		        # todo : check options
	            retval = self.copyback_file(remote_file, local_file)
	            if ( retval[0] != 0 ):
	                logging.critical('did not retrieve: ' + local_file)
	            else:
	                logging.debug('retrieved: ' + local_file)

            # now try to clean up 
            # todo : clean up  

                return [True, retval[1]]

        except:
            logging.critical('Failure in retrieving results')
            raise
            

    """Below are the functions needed only for the SshLrms class."""

    def get_qsub_jobid(self, _output):
        """Parse the qsub output for the local jobid."""
        # cd unique_token
        # lrms_jobid = grep something from output
        # todo : something with _output
        # todo : make this actually do something
        # lrms_jobid = _output.pull_out_the_number
        lrms_jobid = re.split(" ",_output)[2]

        logging.debug('get_qsub_jobid jobid: ' + lrms_jobid)

        return lrms_jobid



    def copy_input(self, input_file, unique_token, username, frontend):
        """Try to create remote directory named unique_token, then copy input_file there."""
        
        # todo: change the name of this method (so it is less confusing with copy out vs copy back?
        # todo: add a switch that tries rsync first, then falls back to ssh.
        # todo: generalize the rsync vs scp and have both copy_input and copyback_file use 1 method

        # first mkdir 
        _mkdir_command = "%s %s@%s mkdir ~/%s" % ( self.ssh_location, username, frontend, unique_token )
        logging.debug('copy_input _mkdir_command: ' + _mkdir_command)

        retval = commands.getstatusoutput(_mkdir_command)

        if ( retval[0] != 0 ):
            logging.critical('Failed to mkdir ~/%s on %s' % unique_token, frontend)
            raise retval[1]

        # then copy the input file

        _copyinput_command = "%s %s %s@%s:~/%s" % ( self.scp_location, input_file, username, frontend, unique_token )
        logging.debug('copy_input _copyinput_command: ' + _copyinput_command)

        retval = commands.getstatusoutput(_copyinput_command)

        if ( retval[0] != 0 ):
            logging.critical('Failed to copy %s to %s' % input_file, frontend)
            raise retval[1]

        # todo: add a check here that compares the md5 of the copied file to the md5 in the unique_token

        return True


    def copyback_file(self, remote_file, local_file):
        """Copy a file back via rsync or scp.  Prefer rsync."""

        # if rsync is available, use it 
        # if not, use scp
        # if not, fail


        # try rsync, then scp
        if os.path.isfile(self.rsync_location):
            _method = self.rsync_location
            _method_options = self.rsync_options
        elif os.path.isfile(self.scp_location):
            _method = self.scp_location
            _method_options = self.scp_options
        else:
            logging.critical('could not locate a suitable copy executable.')
            return False

        # define command
        _command = '%s %s %s@%s:%s %s' % (
                _method,
                _method_options, 
                self.resource['username'], 
                self.resource['frontend'], 
                remote_file,
                local_file)

        logging.debug('copyback_file _command: ' + _command)

        # do the copy
        retval = (commands.getstatusoutput(_command))
                                
        # for some reason we have to use os.WEXITSTATUS to get the real exit code here
        _realretval = str(os.WEXITSTATUS(retval[0]))
        logging.debug('copyback_file _real_retval: ' + _realretval)
        logging.debug(retval[1])

        # check exit status and return
        if ( _realretval != 0 ):
            logging.critical('command failed: %s ' % (_command))

        return retval



# ================================================================
#
#                     Generic functions
#
# ================================================================


def sumfile(fobj):
    """Returns an md5 hash for an object with read() method."""
    """Stolen from http://code.activestate.com/recipes/266486/"""
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
            sys.exit(1)
#            return 'Failed to open file'
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


def configure_logging(verbosity):
    """Configure logging service."""

    if ( verbosity > 5):
        logging_level = 10
    else:
        logging_level = (( 6 - verbosity) * 10)

    logging.basicConfig(level=logging_level, format='%(asctime)s %(levelname)-8s %(message)s')

    return


# Not usefull
def parse_commandline_jobdir_only(args):
    """
    Parse command line arguments.
    In this case there is only 1: a valid job dir.
    """

    numargs = len(args) - 1

    logging.debug('arguments: ' + str(args[1:]))
    logging.debug('number of arguments: ' + str(numargs))

    # Check number of arguments
    if numargs != 1 :
        logging.critical('Usage: gstat job_dir')
        logging.critical('Incorrect # of arguments. Exiting.')
        sys.exit(1)

    jobdir = str(args[1])

    return jobdir


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


def readConfig(config_file_location):

    resource_list = {}
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
                _resource_options['resource_name'] = _resource
                resource_list[_resource] = _resource_options

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

