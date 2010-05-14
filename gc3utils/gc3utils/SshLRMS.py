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

    # todo : say why
    isValid = 0
    _resource = None

    def __init__(self, resource):

        gc3utils.log = logging.getLogger('gc3utils')

        if resource.type == Default.SGE_LRMS:
            self._resource = resource
            self.isValid = 1
            
            self._resource.ncores = int(self._resource.ncores)
            self._resource.max_memory_per_core = int(self._resource.max_memory_per_core) * 1000
            self._resource.walltime = int(self._resource.walltime)
            if self._resource.walltime > 0:
                # Convert from hours to minutes
                self._resource.walltime = self._resource.walltime * 60

    """Here are the common functions needed in every Resource Class."""

    def CheckAuthentication(self):
        """
        Make sure ssh to server works.
        We assume the user has already set up passwordless ssh access to the resource. 
        """

        ssh, sftp = self.connect_ssh(self._resource.frontend)
        _command = "uname -a"
        log.debug('CheckAuthentication command: ' + _command)

        try:
            stdin, stdout, stderr = ssh.exec_command(_command)
            out = stdout.read()
            err = stderr.read()
            gc3utils.log.debug('CheckAuthentication command stdout: ' + out)
            gc3utils.log.debug('CheckAuthentication command stderr: ' + err)
            ssh.close()

        except:
            ssh.close()
            raise
            gc3utils.log.critical('CheckAuthentication failed')
            return False

        return True

#    def submit_job(self, unique_token, application, input_file):
    def submit_job(self, application):
        """
        Submit a job.

        On the backend, the command will look something like this:
        # ssh user@remote_frontend 'cd unique_token ; $gamess_location -n cores input_file'
        """

	# getting information from input_file
        _file_name = os.path.basename(application.input_file_name)
        _file_name_path = os.path.dirname(application.input_file_name)
        _file_name = _file_name.split(".inp")[0]
    	gc3utils.log.debug('Input file path %s dirpath %s from %s',_file_name,_file_name_path,application.input_file_name)

        # Establish an ssh connection.
        try:
            (ssh, sftp) = self.connect_ssh(self._resource.frontend)
        except:
            raise

        # Create the remote unique_token directory. 
        _remotepath = unique_token
        try:
            sftp.mkdir(_remotepath)
        except Exception, e:
            ssh.close()
            gc3utils.log.critical(e)
            gc3utils.log.critical('copy_input mkdir failed')
            raise

        # Copy the input file to remote unique_token directory.
        _localpath = input_file
        _remotepath = unique_token + '/' + _file_name
        try:
            sftp.put(_localpath, _remotepath)
        except:
            ssh.close()
            gc3utils.log.critical('copy_input put failed')
            raise


        # Build up the SGE submit command.
        _submit_command = 'cd ~/\'%s\' && %s/qgms -n %s' % (unique_token, self._resource.gamess_location, self._resource.ncores)

        # If walltime is provided, convert to seconds and add to the SGE submit command.
        _walltime_in_seconds = int(self._resource.walltime)*3600
        if ( _walltime_in_seconds > 0 ):
            _submit_command = _submit_command + ' -t %i ' % _walltime_in_seconds

        gc3utils.log.debug('submit _walltime_in_seconds: ' + str(_walltime_in_seconds))
#        gc3utils.log.debug('submit _walltime_in_seconds: ' + str(self._resource.walltime))

        # Add the input file name to the SGE submit command.
        _submit_command = _submit_command + ' \'%s\'' % _file_name
        
        gc3utils.log.debug('submit _submit_command: ' + _submit_command)

        # Try to submit it to the local queueing system.
        try:
            stdin, stdout, stderr = ssh.exec_command(_submit_command)
            out = stdout.read()
            err = stderr.read()

            gc3utils.log.debug("_submit_command stdout:" + out)
            gc3utils.log.debug("_submit_command stderr:" + err)

        except:
            ssh.close()
            gc3utils.log.critical("_submit_command failed")
            raise
            return False

        lrms_jobid = self.get_qsub_jobid(out)
        gc3utils.log.debug('Job submitted with jobid: %s',lrms_jobid)

        ssh.close()

        return (lrms_jobid,out)


    def check_status(self, lrms_jobid):
        """Check status of a job."""

        try:
            # open ssh connection
            ssh, sftp = self.connect_ssh(self._resource.frontend)

            # then check the lrms_jobid with qstat
            testcommand = 'qstat -j %s' % lrms_jobid

            stdin, stdout, stderr = ssh.exec_command(testcommand)
            out = stdout.read()
            err = stderr.read()
            
            gc3utils.log.debug('CheckStatus command stdout:' + out)
            gc3utils.log.debug('CheckStatus command stderr:' + err)

            # todo : this test could be much better; fix if statement between possible qstat outputs

            teststring = "Following jobs do not exist:"

            if teststring in err:
                jobstatus = "Status: FINISHED"
            else: 
                jobstatus = "Status: RUNNING"
        
    
        except Exception, e:
            ssh.close()
            gc3utils.log.critical('Failure in checking status')
            raise e

        ssh.close()
        return (jobstatus,err)

    def get_results(self,lrms_jobid,unique_token):
        """Retrieve results of a job."""

        # todo: - parse settings to figure out what output files should be copied back (assume gamess for now)

        _unique_token = os.path.basename(unique_token)


        try:
            ''' Make sure we handle the situation when there is a '-' in the file name
            We know the job name will be three '-' from the back of the unique_token
            Example:
            G-P-G.rst.restart_0-1268147228.33-fa72c57af2bbe092a0b9d95ee95aad22-schrodinger
            '''
            jobname =  '-'.join( _unique_token.split('-')[0:-3])
           
            # Create a list of lists.
            # Each element in the outer list is itself a list.
            # Each inner list has 2 elements, a remote file location and a local file location.
            # i.e. [copy_from, copy_to]
             
            ssh, sftp = self.connect_ssh(self._resource.frontend)
            
            # Get the paths to the files on the remote and local machines.
            stdin, stdout, stderr = ssh.exec_command('echo $HOME')
            remote_home = stdout.read().strip()
            full_path_to_remote_unique_id = remote_home+'/'+_unique_token
            full_path_to_local_unique_id = unique_token
            
            copyfiles_list = []

	        # First add the output file.
            remote_file = '%s/%s.o%s' % (full_path_to_remote_unique_id, jobname, lrms_jobid)
            local_file = '%s/%s.stdout' % (full_path_to_local_unique_id, jobname)
            remote2local_list = [remote_file, local_file]
            copyfiles_list.append(remote2local_list)

	        # .po file
            remote_file = '%s/%s.po%s' % (full_path_to_remote_unique_id, jobname, lrms_jobid)
            # The arc gget uses stderr not po for the suffix. We therefore need to rename the file
            local_file = '%s/%s.stderr' % (full_path_to_local_unique_id, jobname)
            remote2local_list = [remote_file, local_file]
            copyfiles_list.append(remote2local_list)

            # Then add the rest of the special output files.
            cp_suffixes = ('.dat', \
                '.cosmo', \
                '.irc')
            for suffix in cp_suffixes:
                remote_file = '%s/%s.o%s%s' % (full_path_to_remote_unique_id, jobname, lrms_jobid, suffix) 
                local_file = '%s/%s%s' % (full_path_to_local_unique_id, jobname, suffix)
                remote2local_list = [remote_file, local_file]
                copyfiles_list.append(remote2local_list)

            gc3utils.log.debug('copyfiles_list: ' + str(copyfiles_list))

            # todo : add checksums to make sure the files are the same.  raise exception if not
            # todo : combine copy & remove steps

            # try to copy back files
            for elem in copyfiles_list:
                remote_file = elem[0]
                local_file = elem[1] 
                
                # if we already have the file, don't copy again
                if ( os.path.exists(local_file) & os.path.isfile(local_file) ):
                    gc3utils.log.debug(local_file + " already copied.  skipping.")
                    continue
                else:
	                # todo : check options
                    try:
                        sftp.get(remote_file, local_file)
                        gc3utils.log.debug('retrieved: ' + local_file)
                    except Exception, e:
                        # todo : figure out how to check for existance of file before trying to copy
                        gc3utils.log.debug('could not retrieve gamess output: ' + local_file)

            # Now try to clean up the remote files.

            rm_suffixes = ('.inp', 
                '.o'+lrms_jobid, 
                '.o'+lrms_jobid+'.dat', 
                '.o'+lrms_jobid+'.inp', 
                '.po'+lrms_jobid, 
                '.qsub')

            # Create a list of remote files to remove.
            for suffix in rm_suffixes:
                gc3utils.log.debug('rm_suffix: ' + suffix)
                remote_file = '%s/%s%s' % (full_path_to_remote_unique_id, jobname, suffix)
		        # todo : check options
                # try to remove them
                try:
                    sftp.remove(remote_file)
                    gc3utils.log.debug('purged remote file: ' + remote_file)
                except Exception, e:
                    # todo : figure out how to check for existance of file before trying to remove
                    gc3utils.log.debug('did not purge remote file: ' + remote_file)

                
            # Now try to remove the directory itself.
            # todo : add checks for directory sanity
            # i.e. that it's not just /home
            # i.e. that it is empty first
            try:
                sftp.rmdir(full_path_to_remote_unique_id)
                gc3utils.log.debug('purged remote directory: ' + full_path_to_remote_unique_id)
            except Exception, e:
                ssh.close()
                gc3utils.log.critical('did not purge remote directory: ' + full_path_to_remote_unique_id)
                raise e

            # todo : check clean up  

        except Exception, e:
            ssh.close()
            gc3utils.log.critical('Failure in retrieving results')
            raise e

        ssh.close()

        # We don't have a return code or lrms_log for this situation, but we need to return them, so just make something fake.
        dummy_output = 'nothing'
        return [True,dummy_output]

    def list_jobs(self, shortview):
        """List status of jobs."""

        try:

            # print the header
            if shortview == False:
                # long view
                print "%-50s %-20s %-10s" % ("[unique_token]","[name]","[status]")
            else:
                # short view
                print "%-20s %-10s" % ("[name]","[status]")
            
            # look in current directory for jobdirs
            jobdirs = []
            dirlist = os.listdir("./")
            for dir in dirlist:
                if os.path.isdir(dir) == True:
                    if os.path.exists(dir + "/.lrms_jobid") and os.path.exists(dir + "/.lrms_log"):
                        gc3utils.log.debug(dir + "is a jobdir")
                        jobdirs.append(dir)

            # Break down unique_token into separate variables.
            for dir in jobdirs:
                unique_token = dir
                name =  '-'.join( _unique_token.split('-')[0:-3])
                status = CheckStatus(unique_toke)

                if shortview == False:
                    # long view
                    sys.stdout.write('%-20s %-10s' % (name, status))
                else:
                    # short view
                    sys.stdout.write('%-50s %-20s %-10s' % (unique_token, filename, size))

            sys.stdout.write('Jobs listed.\n')
            sys.stdout.flush
            
        except Exception, e:
            gc3utils.log.critical('Failed to list jobs.')
            raise e

        return 
            

    def KillJob(self, lrms_jobid):
        """Kill job."""

        ssh, sftp = self.connect_ssh(self._resource.frontend)

        # Kill the job on the remote queueing system.
        
        try:
            stdin, stdout, stderr = ssh.exec_command('qdel ' + lrms_jobid)
            out = stdout.read()
            err = stderr.read()

            gc3utils.log.debug("_submit_command stdout:" + out)
            gc3utils.log.debug("_submit_command stderr:" + err)
            
        except Exception, e:
            ssh.close()
            gc3utils.log.critical('Failed to kill job: ' + unique_token)
            raise e
        
        ssh.close()

        return (0,out)
            
    """Below are the functions needed only for the SshLrms class."""

    def get_qsub_jobid(self, output):
        """Parse the qsub output for the local jobid."""
        # todo : something with _output
        # todo : make this actually do something

        gc3utils.log.debug('get_qsub_jobid raw output: ' + output)
        try:
            lrms_jobid = re.split(" ",output)[2]
        except Exception, e:
            gc3utils.log.critical('could not get jobid from output')
            raise e
            lrms_jobid = False

        gc3utils.log.debug('get_qsub_jobid jobid: ' + lrms_jobid)
        return lrms_jobid


    def connect_ssh(self,host):
        """Create an ssh connection."""
        # todo : add fancier key handling and password asking stuff

        try:
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            ssh.connect(host,timeout=30,username=self._resource.username)
            sftp = ssh.open_sftp()
            return ssh, sftp

        except paramiko.SSHException:
            ssh.close()
            gc3utils.log.critical('Could not create ssh connection to ', host, '.')
            raise


    def GetResourceStatus(self):
            gc3utils.log.debug("Returning information of local resoruce")
            return Resource(resource_name=self._resource.resource_name,total_cores=self._resource.ncores,memory_per_core=self._resource.memory_per_core)
