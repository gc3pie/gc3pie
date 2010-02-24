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
from utils import *
from LRMS import LRMS


# -----------------------------------------------------
# SSH lrms
#

class SshLrms(LRMS):

    ssh_location = "/usr/bin/ssh"
    scp_location = "/usr/bin/scp"
    rsync_location = "/usr/bin/rsync"
    ssh_options = "-o ConnectTimeout=30"
    rsync_options= '-e "ssh ' + ssh_options + '"'
    resource = []
    
    isValid = 0
    def __init__(self, resource):
        if (resource['type'] == "ssh"):
            self.resource = resource
            self.isValid = 1
            
            self.resource['ncores'] = int(self.resource['ncores'])
            self.resource['memory_per_core'] = int(self.resource['memory_per_core']) * 1000
            self.resource['walltime'] = int(self.resource['walltime'])
            if (self.resource['walltime'] > 0 ):
                # convert from hours to minutes
                self.resource['walltime'] = self.resource['walltime'] * 60
                
            logging.debug('Init resource %s with %d cores, %d walltime, %d memory',self.resource['resource_name'],self.resource['ncores'],self.resource['walltime'],self.resource['memory_per_core'])
            
    """Here are the common functions needed in every Resource Class."""
    
    def check_authentication(self):
        """Make sure ssh to server works."""
        # We can make the assumption the local username is the same on the remote host, whatever it is
        # We can also assume local username has passwordless ssh access to resources (or ssh-agent running)
        
        try:
            # ssh username@frontend date 
            # ssh -o ConnectTimeout=1 idesl2.uzh.ch uname -a
            testcommand = "uname -a"
            _command = self.ssh_location + " " + self.ssh_options + " " + self.resource['username'] + "@" + self.resource['frontend'] + " " + testcommand
            logging.debug('check_authentication _command: ' + _command)

            retval = commands.getstatusoutput(_command)
            if ( retval[0] != 0 ):
                raise Exception('failed [ %d ]',retval[0])

            return True

        except:
            raise Exception('failed in check_authentication')



    def submit_job(self, unique_token, application, input_file):
        """Submit a job.

        On the backend, the command will look something like this:
        # ssh user@remote_frontend 'cd unique_token ; $gamess_location -n cores input_file'
        """

# todo : fix this:
#    def submit_job(self,application,inputfile,outputfile,cores,memory):


        # example: ssh mpackard@ocikbpra.uzh.ch 'cd unique_token ; $gamess_location -n cores input_file 

        # dump stdout+stderr to unique_token/lrms_log
	
	# homogenize the input
	_inputfilename = inputfilename(input_file)

        try:

            # copy input first
            try:
                self.copy_input(input_file, unique_token, self.resource['username'], self.resource['frontend'])
            except:
                raise

            # then try to submit it to the local queueing system 
            _submit_command = "%s %s@%s 'cd ~/%s; %s/qgms -n %s %s'" % (self.ssh_location, self.resource['username'], self.resource['frontend'], unique_token, self.resource['gamess_location'], self.resource['ncores'], _inputfilename)
	
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

        _unique_token = os.path.basename(unique_token)


        try:
            jobname = _unique_token.split('-')[0]

            # todo: this expandvars $home is not going to work on clusters where home is different.  fix.
            full_path_to_remote_unique_id = os.path.expandvars('$HOME'+'/'+_unique_token)
            full_path_to_local_unique_id = _unique_token

            # create a list of lists 
            # each element in the outer list is itself a list
            # each inner list has 2 elements, a remote file location and a local file location
            # i.e. [copy_from, copy_to]
             
            copyfiles_list = []

	        # first add the output file
            remote_file = '%s/%s.o%s' % (full_path_to_remote_unique_id, jobname, lrms_jobid)
            local_file = '%s/%s.out' % (full_path_to_local_unique_id, jobname)
            remote2local_list = [remote_file, local_file]
            copyfiles_list.append(remote2local_list)

            # then add the rest of the special output files
            cp_suffixes = ('.dat', \
                '.cosmo', \
                '.irc')
            for suffix in cp_suffixes:
                remote_file = '%s/%s.o%s%s' % (full_path_to_remote_unique_id, jobname, lrms_jobid, suffix) 
                local_file = '%s/%s%s' % (full_path_to_local_unique_id, jobname, suffix)
                remote2local_list = [remote_file, local_file]
                copyfiles_list.append(remote2local_list)

            logging.debug('copyfiles_list: ' + str(copyfiles_list))

            for elem in copyfiles_list:
                remote_file = elem[0]
                local_file = elem[1] 
                
                # if we already have the file, don't copy again
                if ( os.path.exists(local_file) & os.path.isfile(local_file) ):
                    logging.debug(local_file + " already copied.  skipping.")
                    continue
                else:
	                # todo : check options
                    retval = self.copyback_file(remote_file, local_file)
                    if ( retval[0] != 0 ):
                        logging.critical('could not retrieve gamess output: ' + local_file)
                    else:
                        logging.debug('retrieved: ' + local_file)
	

            # now try to clean up the remote files 

            purgefiles_list = []

            # now try to clean up 
            rm_suffixes = ('.inp', 
                '.o'+lrms_jobid, 
                '.o'+lrms_jobid+'.dat', 
                '.o'+lrms_jobid+'.inp', 
                '.po'+lrms_jobid, 
                '.qsub')

            # create list of remote files to remove
            for suffix in rm_suffixes:
                logging.debug('rm_suffix: ' + suffix)
                remote_file = '%s/%s%s' % (full_path_to_remote_unique_id, jobname, suffix)
		        # todo : check options
                if ( self.purge_remotefile(remote_file) != 0 ):
                    logging.critical('did not purge remote file: ' + remote_file)
                else:
                    logging.debug('purged remote file: ' + remote_file)

            # create list of remote files to remove
            for suffix in rm_suffixes:
                logging.debug('rm_suffix: ' + suffix)
                remote_file = "%s/%s%s" % (full_path_to_remote_unique_id, jobname, suffix)
                purgefiles_list.append(remote_file)
		        # todo : check options 

            purgefiles_string = " ".join(purgefiles_list)

            logging.debug('remote_files to remove: ' + purgefiles_string)
            try:
                self.purge_remotefile(purgefiles_string)
                logging.debug('purged remote file(s): ' + purgefiles_string)
            except:
                raise


            # remove full_path_to_remote_unique_id
            if ( self.purge_remotedir(full_path_to_remote_unique_id) != 0 ):
                logging.critical('did not purge remote dir: ' + full_path_to_remote_unique_id)
            else:
                logging.debug('purged remote dir: ' + full_path_to_remote_unique_id)
            
            # todo : check clean up  

        except:
            logging.critical('Failure in retrieving results')
            raise

        return [True, retval[1]]
            

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
            _method_options = self.ssh_options
        else:
            logging.critical('could not locate a suitable copy executable.')
            return False

        # define command
        _command = '%s %s %s@%s:%s %s' % ( \
                _method, \
                _method_options, \
                self.resource['username'], \
                self.resource['frontend'], \
                remote_file, \
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

    def purge_remotefile(self, remote_files):
        """Remove a remote file."""

        logging.debug("remote_files: " + remote_files)

        _method = self.ssh_location
        _method_options = self.ssh_options

        # define command
        _command = '%s %s %s@%s rm %s' % ( \
                _method, \
                _method_options, \
                self.resource['username'], \
                self.resource['frontend'], \
                remote_files)

        logging.debug('purge_remotefile _command: ' + _command)

        # do the remove
        retval = (commands.getstatusoutput(_command))

        # for some reason we have to use os.WEXITSTATUS to get the real exit code here
        _realretval = str(os.WEXITSTATUS(retval[0]))
        logging.debug('purge_remotefile _real_retval: ' + _realretval)
        logging.debug(retval[1])

        # check exit status and return
        if ( _realretval != 0 ):
            logging.critical('command failed: %s ' % (_command))

        return retval

    def purge_remotedir(self, remote_dir):
        """Remove a remote directory."""

        _method = self.ssh_location
        _method_options = self.ssh_options

        # define command
        _command = '%s %s %s@%s rmdir %s' % ( \
                _method, \
                _method_options, \
                self.resource['username'], \
                self.resource['frontend'], \
                remote_dir)

        logging.debug('purge_remotedir _command: ' + _command)

        # do the remove
        retval = (commands.getstatusoutput(_command))

        # for some reason we have to use os.WEXITSTATUS to get the real exit code here
        _realretval = str(os.WEXITSTATUS(retval[0]))
        logging.debug('purge_remotedir _real_retval: ' + _realretval)
        logging.debug(retval[1])

        # check exit status and return
        if ( _realretval != 0 ):
            logging.critical('command failed: %s ' % (_command))

        return retval

