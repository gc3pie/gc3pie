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
import paramiko
from utils import *
from LRMS import LRMS


# -----------------------------------------------------
# SSH lrms
#

class SshLrms(LRMS):

    resource = []
    
    isValid = 0
    def __init__(self, resource):
        if (resource['type'] == "ssh"):
            self.resource = resource
            # should we really set hardcoded defaults ?
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

        ssh, sftp = self.connect_ssh(self.resource['frontend'])
        _command = "uname -a"
        logging.debug('check_authentication command: ' + _command)

        try:
            stdin, stdout, stderr = ssh.exec_command(_command)
            out = stdout.read()
            err = stderr.read()
            logging.debug('check_authentication command stdout: ' + out)
            logging.debug('check_authentication command stderr: ' + err)
            ssh.close()

        except:
            ssh.close()
            raise
            logging.critical('check_authentication failed')
            return False

        return True

    def submit_job(self, unique_token, application, input_file):
        """Submit a job.

        On the backend, the command will look something like this:
        # ssh user@remote_frontend 'cd unique_token ; $gamess_location -n cores input_file'
        """

	    # homogenize the input
        _inputfilename = inputfilename(input_file)

        # establish ssh connection
        try:
            ssh, sftp = self.connect_ssh(self.resource['frontend'])

        except:
            ssh.close()
            raise

        # make remote unique_token dir 
        _remotepath = unique_token
        try:
            sftp.mkdir(_remotepath)
        except Exception, e:
            ssh.close()
            logging.critical(e)
            logging.critical('copy_input mkdir failed')
            raise

        # then copy the input file to it
        _localpath = input_file
        _remotepath = unique_token + '/' + inputfilename(input_file)
        try:
            sftp.put(_localpath, _remotepath)
        except:
            ssh.close()
            logging.critical('copy_input put failed')
            raise

        # then try to submit it to the local queueing system 
        _submit_command = 'cd ~/%s && %s/qgms -n %s %s' % (unique_token, self.resource['gamess_location'], self.resource['ncores'], _inputfilename)
	
        logging.debug('submit _submit_command: ' + _submit_command)

        try:
            stdin, stdout, stderr = ssh.exec_command(_submit_command)
            out = stdout.read()
            err = stderr.read()

            logging.debug("_submit_command stdout:" + out)
            logging.debug("_submit_command stderr:" + err)

        except:
            ssh.close()
            logging.critical("_submit_command failed")
            raise
            return False

        lrms_jobid = self.get_qsub_jobid(out)
        logging.debug('Job submitted with jobid: %s',lrms_jobid)

        ssh.close()

        return (lrms_jobid,out)


    def check_status(self, lrms_jobid):
        """Check status of a job."""

        try:
            # open ssh connection
            ssh, sftp = self.connect_ssh(self.resource['frontend'])

            # then check the lrms_jobid with qstat
            testcommand = 'qstat -j %s' % lrms_jobid

            stdin, stdout, stderr = ssh.exec_command(testcommand)
            out = stdout.read()
            err = stderr.read()
            
            logging.debug('check_status command stdout:' + out)
            logging.debug('check_status command stderr:' + err)

            # todo : this test could be much better; fix if statement between possible qstat outputs

            teststring = "Following jobs do not exist:"

            if teststring in err:
                jobstatus = "Status: FINISHED"
            else: 
                jobstatus = "Status: RUNNING"
        
    
        except Exception, e:
            ssh.close()
            logging.critical('Failure in checking status')
            raise e

        ssh.close()
        return (jobstatus,err)

    def get_results(self,lrms_jobid,unique_token):
        """Retrieve results of a job."""

        # todo: - parse settings to figure out what output files should be copied back (assume gamess for now)

        _unique_token = os.path.basename(unique_token)


        try:
            jobname = _unique_token.split('-')[0]            

            # create a list of lists 
            # each element in the outer list is itself a list
            # each inner list has 2 elements, a remote file location and a local file location
            # i.e. [copy_from, copy_to]
             
            ssh, sftp = self.connect_ssh(self.resource['frontend'])
            
            # Get the paths to the files on the remote and local machines
            stdin, stdout, stderr = ssh.exec_command('echo $HOME')
            remote_home = stdout.read().strip()
            full_path_to_remote_unique_id = remote_home+'/'+_unique_token
            full_path_to_local_unique_id = _unique_token
            
            copyfiles_list = []

	        # first add the output file
            remote_file = '%s/%s.o%s' % (full_path_to_remote_unique_id, jobname, lrms_jobid)
            local_file = '%s/%s.stdout' % (full_path_to_local_unique_id, jobname)
            remote2local_list = [remote_file, local_file]
            copyfiles_list.append(remote2local_list)

	        # .po file
            remote_file = '%s/%s.po%s' % (full_path_to_remote_unique_id, jobname, lrms_jobid)
            local_file = '%s/%s.po' % (full_path_to_local_unique_id, jobname)
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

            # try to copy back files
            for elem in copyfiles_list:
                remote_file = elem[0]
                local_file = elem[1] 
                
                # if we already have the file, don't copy again
                if ( os.path.exists(local_file) & os.path.isfile(local_file) ):
                    logging.debug(local_file + " already copied.  skipping.")
                    continue
                else:
	                # todo : check options
                    try:
                        sftp.get(remote_file, local_file)
                        logging.debug('retrieved: ' + local_file)
                    except Exception, e:
                        # todo : figure out how to check for existance of file before trying to copy
                        logging.debug('could not retrieve gamess output: ' + local_file)

            # now try to clean up the remote files 

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
                # try to remove them
                try:
                    sftp.remove(remote_file)
                    logging.debug('purged remote file: ' + remote_file)
                except Exception, e:
                    # todo : figure out how to check for existance of file before trying to remove
                    logging.debug('did not purge remote file: ' + remote_file)

                
            # now try to remove the directory itself
            # todo : add checks for directory sanity
            # i.e. that it's not just /home
            # i.e. that it is empty first
            try:
                sftp.rmdir(full_path_to_remote_unique_id)
                logging.debug('purged remote directory: ' + full_path_to_remote_unique_id)
            except Exception, e:
                ssh.close()
                logging.critical('did not purge remote directory: ' + full_path_to_remote_unique_id)
                raise e

            # todo : check clean up  

        except Exception, e:
            ssh.close()
            logging.critical('Failure in retrieving results')
            raise e

        ssh.close()

        # we don't have a return code or lrms_log for this situation, but we need to return them, so just make something fake
        dummy_output = 'nothing'
        return [True,dummy_output]
            

    """Below are the functions needed only for the SshLrms class."""

    def get_qsub_jobid(self, output):
        """Parse the qsub output for the local jobid."""
        # todo : something with _output
        # todo : make this actually do something

        logging.debug('get_qsub_jobid raw output: ' + output)
        try:
            lrms_jobid = re.split(" ",output)[2]
        except Exception, e:
            logging.critical('could not get jobid from output')
            raise e
            lrms_jobid = False

        logging.debug('get_qsub_jobid jobid: ' + lrms_jobid)
        return lrms_jobid


    def connect_ssh(self,host):
        """Create an ssh connection."""
        # todo : add fancier key handling and password asking stuff
        try:
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            ssh.connect(host,timeout=30,username=self.resource['username'])
            sftp = ssh.open_sftp()
            return ssh, sftp

        except:
            ssh.close()
            raise


