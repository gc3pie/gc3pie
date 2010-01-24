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
import utils
import LRMS

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
