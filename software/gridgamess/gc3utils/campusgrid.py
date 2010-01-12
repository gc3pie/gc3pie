import sys
import os
import commands
import logging
import tempfile
import getpass
import re
import md5
import time

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
    
    def get_results(lrms_jobid):
        pass

# ----------------------------------------
    
class ArcLrms(LRMS):

    isValid = 0
    VOMSPROXYINFO = "grid-proxy-info -exists -valid 1:10"
    VOMSPROXYINIT = "voms-proxy-init -q -voms smscg"
    SLCSINFO = "openssl x509 -noout -checkend 3600 -in ~/.globus/usercert.pem"
    SLCSINIT = "slcs-init --idp uzh.ch"
    GAMESS_XRSL_TEMPLATE = "$HOME/.gc3/gamess_template.xrsl"
    AAI_CREDENTIAL_REPO = "$HOME/.gc3/aai_credential"
    resource = []

    def __init__(self, resource):
        if ( (resource['frontend'] != "") & (resource['type'] == "arc") ):
            self.resource = resource
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
                        return 1

                    logging.info('Initializing slcs\t\t\t[ ok ]')
                    
                # Try renew voms credential
                # Another interactive command

                logging.debug('Initializing voms-proxy')
                retval = commands.getstatusoutput("echo \'"+input_passwd+"\' | "+self.VOMSPROXYINIT+" -pwstdin")
                if ( retval[0] != 0 ):
                    # Failed renewing voms credential
                    # FATAL ERROR
                    logging.critical("Initializing voms-proxy\t\t[ failed]\n\t%s",retval[1])
                    return 1
                logging.info('Initializing voms-proxy\t\t[ ok ]')
            logging.info('check_authentication\t\t\t\t[ ok ]')
                
            return 0
        except:
            raise


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
                    raise

                # Ready for real submission
                _command = "ngsub -d2 -c "+self.resource['frontend']+" -f "+_file_handle.name

                retval = commands.getstatusoutput(_command)

                jobid_pattern = "Job submitted with jobid: "

                if ( ( retval[0] != 0 ) | ( jobid_pattern not in retval[1] ) ):
                    # Failed somehow
                    logging.error("ngsub command\t\t[ failed ]")
                    logging.debug(retval[1])
                    raise

                # assuming submit successfull
                logging.debug("ngsub command\t\t[ ok ]")
                logging.debug(retval[1])

                # Extracting ARC jobid
                lrms_jobid = re.split(jobid_pattern,retval[1])[1]
                logging.debug('Job submitted with jobid: %s',lrms_jobid)

                return [lrms_jobid,retval[1]]

            else:
                logging.critical('XRSL file not found %s',self.GAMESS_XRSL_TEMPLATE)
                raise
        except:
            logging.critical('Failure in submitting')
            raise

    def check_status(self, lrms_jobid):
        submitted_list = ['ACCEPTING','SUBMITTING']
        running_list = ['INLRMS:Q','INLRMS:R','EXECUTED']
        finished_list = ['FINISHED']
        
        try:
            # Ready for real submission
            _command = "ngstat "+lrms_jobid

            retval = commands.getstatusoutput(_command)
            jobstatus_pattern = "Status: "
            jobexitcode_pattern = "Exit Code: "
            if ( ( retval[0] != 0 ) | ( jobstatus_pattern not in retval[1] ) ):
                # Failed somehow
                logging.error("ngstat command\t\t[ failed ]")
                logging.debug(retval[1])
                raise

            # Extracting ARC job status
            lrms_jobstatus = re.split(jobstatus_pattern,retval[1])[1]
            lrms_jobstatus = re.split("\n",lrms_jobstatus)[0]

            logging.debug('lrms_jobstatus\t\t\t[ %s ]',lrms_jobstatus)

            if ( lrms_jobstatus in submitted_list ):
                jobstatus = "Status: SUBMITTED"
            elif ( lrms_jobstatus in running_list ):
                jobstatus = "Status: RUNNING"
            elif ( lrms_jobstatus in finished_list ):
                lrms_exitcode = re.split(jobexitcode_pattern,retval[1])[1]
                jobstatus = "Status: FINISHED\nExit Code: "+lrms_exitcode
            else:
                jobstatus = "Status: [ "+lrms_jobstatus+" ]"

            return jobstatus

        except:
            logging.critical('Failure in checking status')
            raise
                                
# ----------------------------------------



class SshLrms(LRMS):
    
    isValid = 0
    def __init__(self, resource):
        if ( (resource['frontend'] != "") & (resource['type'] == "ssh") ):
            self.resource = resource
            if ( 'cores' not in resource ):
                self.resource['cores'] = "1"
            if ( 'memory' not in resource ):
                self.resource['memory'] = "1000"
            if ( 'walltime' not in resource ):
                self.resource['walltime'] = "12"
            self.isValid = 1

    """Here are the common functions needed in every Resource Class."""

    def check_authentication(username, frontend):
        """Make sure ssh to server works."""
        # ssh username@frontend date 
        # ssh -o ConnectTimeout=1 idesl2.uzh.ch uname -a
        testcommand = "uname -a"
        cmd = ssh_location + " " + ssh_options + " " + username + "@" + frontend + " " + testcommand
        logging.debug('check_authentication cmd: ' + cmd)

        try:
            os.system(cmd)
        except:
            command_failed(cmd, "the connection test to " + frontend + "failed.")

        return


    def submit2(input, unique_token, application, lrms_log):
# todo compare & contrast
        return 

    def submit(self,application,inputfile,outputfile,cores,memory):

        """Submit a job."""

        # dump stdout+stderr to unique_token/lrms_log
        # should look something like this when done:
        """ ssh mpackard@ocikbpra.uzh.ch 'cd unique_token ; $qgms_location -n cores input_file """
# todo remove :
#        cmd = ssh_location + username + "@" + frontend + "'cd ' + unique_token + " ; $" + qgms_location + " -n " + ncores + " " + input + "'"
        cmd = "%s %s@%s 'cd %s; $%s -n %s %s'" % (ssh_location, username, frontend, jobdir, qgms_location, ncores, input)
        logging.debug('submit cmd: ' + cmd)

        try:
#            os.system(cmd)
# todo uncomment when the cmd looks good 
            print cmd
        except: 
            command_failed(cmd, "the submission to " + frontend + "failed.")
            sys.exit(1)
            
        lrms_jobid = get_qsub_jobid(output)

        return lrms_jobid

    def check_status(unique_token):
        """Check status of a job."""
        cmd = ssh_location + " " + ssh_options + " " + username + "@" + frontend + " " + testcommand
        logging.debug('check_status cmd: ' + cmd)
        return 

    def get_results(unique_token):
        """Retrieve results of a job."""
# todo remove :
#        cmd = scp_location + username + "@" + frontend + "'cd ' + unique_token + " ; $" + qgms_location + " -n " + ncores + " " + input + "'" 
        cmd = "%s %s@%s 'cd %s; %s -n %s %s'" % (scp_location, username, frontend, jobdir, qgms_location, ncores, input)
        logging.debug('get_results cmd: ' + cmd)

        finishedfile = unique_token + ".finished"

        if os.path.isfile(finishedfile): 
            print "Job is already finished.  Exiting."
            sys.exit(1)


        """
        Next steps:
        - parse settings to figure out what output files should be copied back (assume gamess for now)

        """

        lrms_jobid = 12345

        # now try to copy back all the files with the right suffixes

        suffixes = ('dat', 'cosmo', 'irc')
        for suffix in suffixes:
            options = [ssh_options, identity, resource_frontend, jobdir_fullpath, jobname, lrms_jobid, suffix, jobdir]
        # todo : check options
            copyback(options)


        # now try to clean up 
        # todo : clean up  


        # if all is well, touch the finishedfile
        open(finishedfile, 'w').close() 
        
        return


    """Below are the special functions needed only for this class."""

    def get_qsub_jobid(output):
        """Parse the qsub output for the local jobid."""
        return lrms_jobid

    def command_failed(cmd, custom_message):
        """This just prints out a common fail header."""
        print "This command failed:"
        print cmd
        print custom_message + "  Exiting."
        sys.exit(1)
        return 

    def copy_input(input, unique_token, frontend):
        """Try to create remote directory named unique_token, then copy input_file there."""
        
        # todo : do we need this?

        # ssh username@frontend:~unique_token
        # try rsync first
        # if not try scp
        # if not fail
        return

        def copyback(options):
            """Copy a file back via rsync or scp.  Prefer rsync."""

        ssh_options = "-o ConnectTimeout=30"
        ssh_location = "/usr/bin/ssh"
        scp_location = "/usr/bin/scp"
        rsync_location = "/usr/bin/rsync"

        # if rsync is available, use it 
        # if not, use scp
        # if not, fail

#           if os.path.isfile(rsync_location):
#               options.insert(0, rsync_location)
#               tup = tuple(options)
#               cmd = '%s -e ssh %s %s@%s:%s/%s.o%s.%s %s' % (tup)
#           elif os.path.isfile(scp_location):
#               options.insert(0, scp_location)
#               tup = tuple(options)
#               cmd = '%s %s %s@%s:%s/%s.o%s.%s %s' % (tup)
#        else:
#            logging.critical('Copyback failed.  Exiting.')
#            sys.exit(1)

        logging.debug('copyback cmd: ' + cmd)
        return cmd


# todo: check that this stuff is ok to remove - mike
#check_authentication
#copy_input
#submit_job -> returns .lrms_jobid
#copy_input if necessary
#parse .lrms_output for .lrms_jobid
#check_status
#get_results


#
# Here are some generic functions that everyone can use.


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
    """Returns an md5 hash for file fname, or stdin if fname is "-"."""
    """Stolen from http://code.activestate.com/recipes/266486/"""
    if fname == '-':
        ret = sumfile(sys.stdin)
    else:
        try:
            f = file(fname, 'rb')
        except:
            print 'Failed to open file.  Exiting.'
            sys.exit(1)
#            return 'Failed to open file'
        ret = sumfile(f)
        f.close()
    return ret


def create_unique_token(inputfile, clustername):
    """create a unique job token based on md5sum, timestamp, clustername, and jobname"""
    inputmd5 = md5sum(inputfile)
    inname = inputname(inputfile)
    timestamp = str(time.time())
    unique_token = inname + "-" + timestamp + "-" + inputmd5 + "-" + clustername
    return unique_token



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
        logging_level = (( 6 - options.verbosity) * 10)

    logging.basicConfig(level=logging_level, format='%(asctime)s %(levelname)-8s %(message)s')

    return


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
        print "qgms script version is too old.  Please update it and resubmit."
        sys.exit(1)

    return


