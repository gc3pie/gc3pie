import sys
import os
import re

import Exceptions 
import commands
import getpass
import logging
import paramiko
import sge
import utils

from LRMS import LRMS
from utils import *


def _qgms_job_name(filename):
    """
    Return the batch system job name used to submit GAMESS on input
    file name `filename`.
    """
    if filename.endswith('.inp'):
        return os.path.splitext(filename)[0]
    else:
        return filename


# -----------------------------------------------------
# SSH lrms
#

class SshLrms(LRMS):

    # todo : say why
    isValid = 0
    _resource = None

    def __init__(self, resource):

        gc3utils.log = logging.getLogger('gc3utils')

        if (resource.has_key('type') 
            and resource.type == Default.SGE_LRMS 
            and resource.has_key('ssh_username')):
            self._resource = resource
            self.isValid = 1

            # TBCK: does Ssh really needs this ?
            self._resource.ncores = int(self._resource.ncores)
            self._resource.max_memory_per_core = int(self._resource.max_memory_per_core) * 1000
            self._resource.walltime = int(self._resource.walltime)
            if self._resource.walltime > 0:
                # Convert from hours to minutes
                self._resource.walltime = self._resource.walltime * 60
        else:
            gc3utils.log.warning("Cannot create LRMS instance for resource '%s': either 'type' is not 'ssh_sge', or 'ssh_username' is missing.")

    def is_valid(self):
        return self.isValid

    def submit_job(self, application):
        """
        Submit a job.

        On the backend, the command will look something like this:
        # ssh user@remote_frontend 'cd unique_token ; $gamess_location -n cores input_file'
        """
        # Establish an ssh connection.
        (ssh, sftp) = self._connect_ssh(self._resource.frontend,self._resource.ssh_username)

        # Create the remote unique_token directory. 
        try:
            _command = 'mkdir -p $HOME/.gc3utils_jobs; mktemp -p $HOME/.gc3utils_jobs -d lrms_job.XXXXXXXXXX'
            exit_code, stdout, stderr = self._execute_command(ssh, _command)
            if exit_code == 0:
                ssh_remote_folder = stdout.split('\n')[0]
            else:
                raise paramiko.SSHException('Failed while executing remote command')
        except:
            gc3utils.log.critical("Failed creating remote temporary folder: command '%s' returned exit code %d)"
                                  % (_command, exit_code))

            if not ssh  is None:
                ssh.close()
            raise

        # Copy the input file to remote directory.
        for input in application.inputs.items():
            local_path, remote_path = input
            remote_path = os.path.join(ssh_remote_folder, remote_path)

            gc3utils.log.debug("Transferring file '%s' to '%s'" % (local_path, remote_path))
            try:
                sftp.put(local_path, remote_path)
            except:
                gc3utils.log.critical("Copying input file '%s' to remote cluster '%s' failed",
                                      local_path, self._resource.frontend)
                if not ssh  is None:
                    ssh.close()
                raise

        # Build up the SGE submit command.
        submit_command = ('cd %s && qsub -cwd -S /bin/sh %s' 
                          % (ssh_remote_folder, application.cmdline(self._resource)))

        # Try to submit it to the local queueing system.
        try:
            gc3utils.log.debug('Running submit command: ' + submit_command)
            exit_code, stdout, stderr = self._execute_command(ssh, submit_command)
            ssh.close()
            if exit_code != 0:
                gc3utils.log.critical("Failed executing remote command '%s'; exit status %d"
                                      % (submit_command,exit_code))
                gc3utils.log.debug('remote command returned stdout: %s' % stdout)
                gc3utils.log.debug('remote command returned stderr: %s' % stderr)
                raise paramiko.SSHException('Failed executing remote command')

            lrms_jobid = self._get_qsub_jobid(stdout)
            gc3utils.log.debug('Job submitted with jobid: %s',lrms_jobid)

            job_log = "\nstdout:\n" + stdout + "\nstderr:\n" + stderr

            job = Job.Job(lrms_jobid=lrms_jobid,
                          status=Job.JOB_STATE_SUBMITTED,
                          resource_name=self._resource.name,
                          log=job_log)
            job.remote_ssh_folder = ssh_remote_folder

            # remember outputs for later ref
            job.outputs = dict(application.outputs)
            job.lrms_job_name = _qgms_job_name(_input_file_name) # XXX: GAMESS-specific!
            return job

        except:
            if not ssh  is None:
                ssh.close()
            gc3utils.log.critical("Failure submitting job to resource '%s' - see log file for errors"
                                  % self._resource.name)
            raise


    def check_status(self, job):
        """Check status of a job."""

        mapping = {
            'qname':'queue',
            'jobname':'job_name',
            'slots':'cpu_count',
            'exit_status':'exit_code',
            'failed':'system_failed',
            'cpu':'used_cpu_time',
            'ru_wallclock':'used_walltime',
            'maxvmem':'used_memory'
            }
        try:
            # open ssh connection
            ssh, sftp = self._connect_ssh(self._resource.frontend,self._resource.ssh_username)

            # then check the lrms_jobid with qstat
            #           testcommand = 'qstat -j %s' % job.lrms_jobid
            _command = 'qstat | grep -i %s' % job.lrms_jobid
            #            _command = 'qstat -j %s' % job.lrms_jobid

            gc3utils.log.debug('checking remote job status with %s' % _command)

            exit_code, stdout, stderr = self._execute_command(ssh, _command)
            if exit_code != 0:
                #            finished_job_patter = 'Following jobs do not exist'
                #            if finished_job_patter in stderr:

                # maybe the job is finished
                
                # collect accounting information
                #job.status = Job.JOB_STATE_FINISHED

                _command = 'qacct -j %s | grep -v ===' % job.lrms_jobid

                gc3utils.log.debug('no job information found. trying with %s' % _command)
                
                exit_code, stdout, stderr = self._execute_command(ssh, _command)
                if exit_code != 0:
                    gc3utils.log.critical('Failed executing remote command: %s. exit status %d' % (_command,exit_code))
                    gc3utils.log.debug('remote command returned stdout: %s' % stdout)
                    gc3utils.log.debug('remote command returned stderr: %s' % stderr)
                    raise paramiko.SSHException('Failed executing remote command')
                else:
                    # parse stdout and update job obect with detailed accounting information

                    gc3utils.log.debug('parsing stdout to get job accounting information')
                    #gc3utils.log.debug('stdout: %s' % stdout)

                    for line in stdout.split('\n'):
                        if line != '':
                            key, value = line.split(' ', 1)
                            value = value.strip()

                            if mapping.has_key(key):
                                job_key = mapping[key]
                                job[job_key] = str(value)
                            else:
                                gc3utils.log.debug('Dropping Job information %s of value %s ' % (str(key),str(value)))

                    job.status = Job.JOB_STATE_FINISHED
                    # job_obj.cluster = arc_job.cluster
                    # job_obj.cpu_count = arc_job.cpu_count
                    # job_obj.exitcode = arc_job.exitcode
                    # job_obj.job_name = arc_job.job_name
                    # job_obj.queue = arc_job.queue
                    # job_obj.queue_rank = arc_job.queue_rank
                    # job_obj.requested_cpu_time = arc_job.requested_cpu_time
                    # job_obj.requested_wall_time = arc_job.requested_wall_time
                    # job_obj.sstderr = arc_job.sstderr
                    # job_obj.sstdout = arc_job.sstdout
                    # job_obj.sstdin = arc_job.sstdin
                    # job_obj.used_cpu_time = arc_job.used_cpu_time
                    # job_obj.used_wall_time = arc_job.used_wall_time
                    # job_obj.used_memory = arc_job.used_memory
                    
            else:
                # this is extremely fragile
                #gc3utils.log.debug('parsing %s' % stdout)
                # stdout_results = re.split('\n',stdout)
                #gc3utils.log.debug('parsing %s' % stdout_results)

                job_status = stdout.split('\n')[0].split()[4]
                    
                #                stdout_results = [ value for value in stdout_results if value != '' ]
                #                gc3utils.log.debug('parsing %s' % stdout_results)
                #job_status = stdout_results[4]

                gc3utils.log.debug('inspecting lrms job status %s' % job_status)
                if 'r' in job_status or 'R' in job_status or 't' in job_status or 'q' in job_status:
                    job.status = Job.JOB_STATE_RUNNING
                elif job_status == 'd' or job_status == 'E':
                    job.status = Job.JOB_STATE_FAILED
                elif job.status == 's' or job.status == 'S' or job.status == 'T' or job.status == 'w':
                    # use JOB_STATE_UNKNOWN for the time being
                    # we need to introduce an additional stated that clarifies a sysadmin internvention is needed
                    job.status = Job.JOB_STATE_UNKNOWN

                # need to set detailed job status information
                #gc3utils.log.debug('marked job status: %s' % job.status)

            ssh.close()
            
            return job
        
        except:
            if not ssh  is None:
                ssh.close()
            gc3utils.log.critical('Failure in checking status')
            raise


    def cancel_job(self, job_obj):
        try:
            ssh, sftp = self._connect_ssh(self._resource.frontend,self._resource.ssh_username)
            _command = 'qdel '+job_obj.lrms_jobid

            exit_code, stdout, stderr = self._execute_command(ssh, _command)

            if exit_code != 0:
                gc3utils.log.critical('Failed executing remote command: %s. exit status %d' % (_command,exit_code))
                gc3utils.log.debug('remote command returned stdout: %s' % stdout)
                gc3utils.log.debug('remote command returned stderr: %s' % stderr)
                raise paramiko.SSHException('Failed executing remote command')

            ssh.close()
            return job_obj

        except:
            if not ssh  is None:
                ssh.close()
            gc3utils.log.critical('Failure in checking status')
            raise
        


    def get_results(self,job):
        """Retrieve results of a job."""

        try:
            ''' 
            Make sure we handle the situation when there is a '-' in the file name
            We know the job name will be three '-' from the back of the unique_token
            Example:
            G-P-G.rst.restart_0-1268147228.33-fa72c57af2bbe092a0b9d95ee95aad22-schrodinger
            '''
            #jobname =  '-'.join( job.unique_token.split('-')[0:-3])

            # Create a list of lists.
            # Each element in the outer list is itself a list.
            # Each inner list has 2 elements, a remote file location and a local file location.
            # i.e. [copy_from, copy_to]

            gc3utils.log.debug("Connecting to cluster frontend '%s' as user '%s' via SSH ...", 
                               self._resource.frontend, self._resource.ssh_username)
            ssh, sftp = self._connect_ssh(self._resource.frontend,self._resource.ssh_username)
            
            # todo : test that this copying works for both full and relative paths.

            # Get the paths to the files on the remote and local machines.
            #stdin, stdout, stderr = ssh.exec_command('echo $HOME')
            #_remote_home = stdout.read().strip()
            #_full_path_to_remote_unique_id = _remote_home+'/'+job.unique_token
            #_full_path_to_local_unique_id = job.unique_token

            # If the dir no longer exists, exit.
            # todo : maybe change the status to something else
            try:
                files_list = sftp.listdir(job.remote_ssh_folder)
            except Exception, x:
                gc3utils.log.error("Could not read remote job directory '%s': " 
                                   % job.remote_ssh_folder, exc_info=True)
                if not ssh  is None:
                   ssh.close()
                #raise
                job.status = Job.JOB_STATE_FAILED
                return job

            if job.has_key('job_local_dir'):
                _download_dir = job.job_local_dir + '/' + job.unique_token
            else:
                _download_dir = Default.JOB_FOLDER_LOCATION + '/' + job.unique_token
                
            # Prepare/Clean download dir
            if gc3utils.utils.prepare_job_dir(_download_dir) is False:
                # failed creating local folder
                raise Exception('failed creating local folder')

            gc3utils.log.debug('downloading job into %s',_download_dir)

            # copy back all files, renaming them to adhere to the ArcLRMS convention
            try: 
                jobname = job.lrms_job_name
                gc3utils.log.debug("Recorded job name is '%s'" % jobname)
            except KeyError:
                # no job name was set, empty string should be safe for following code
                jobname = ''
                gc3utils.log.warning("No recorded job name; will not be able to rename files accordingly")
            filename_map = { 
                # XXX: SGE-specific?
                ('/%s.o%s' % (jobname, job.lrms_jobid)) : ('%s.stdout' % jobname),
                ('/%s.e%s' % (jobname, job.lrms_jobid)) : ('%s.stderr' % jobname),
                # the following is definitely GAMESS-specific
                ('/%s.o%s.dat' % (jobname, job.lrms_jobid)) : ('%s.dat' % jobname),
                ('/%s.o%s.inp' % (jobname, job.lrms_jobid)) : ('%s.inp' % jobname),
                }
            # copy back all files
            gc3utils.log.debug("Downloading job output into '%s' ...",_download_dir)
            for remote_path, local_path in job.outputs.items():
                remote_path =  job.remote_ssh_folder +'/' + remote_path
                # default to keep same file name ...
                local_path = _download_dir +'/' + local_path
                # ... but override if it's a known one
                for r,l in filename_map.items():
                    if remote_path.endswith(r):
                        local_path = _download_dir + '/' + l
                gc3utils.log.debug("Downloading remote file '%s' to local file '%s'", 
                                   remote_path, local_path)
                try:
                    if not os.path.exists(local_path):
                        gc3utils.log.debug("Copying remote '%s' to local '%s'", remote_path, local_path)
                        sftp.get(remote_path, local_path)
                    else:
                        gc3utils.log.info("Local file '%s' already exists; will not be overwritten!",
                                          local_path)
                except:
                    gc3utils.log.debug('Could not copy remote file: ' + remote_path)
                    raise
            # `qgms` submits GAMESS jobs with `-j y`, i.e., stdout and stderr are
            # collected into the same file; make jobname.stderr a link to jobname.stdout
            # in case some program relies on its existence
            if not os.path.exists(_download_dir + '/' + jobname + '.stderr'):
                os.symlink(_download_dir + '/' + jobname + '.stdout',
                           _download_dir + '/' + jobname + '.stderr')

            # cleanup remote folder
            _command = "rm -rf '%s'" % job.remote_ssh_folder
            exit_code, stdout, stderr = self._execute_command(ssh, _command)
            if exit_code != 0:
                gc3utils.log.error('Failed while removing remote folder %s' % job.remote_ssh_folder)
                gc3utils.log.debug('error: %s' % stderr)
            
            # set job status to COMPLETED
            job.download_dir = _download_dir
            job.status = Job.JOB_STATE_COMPLETED

            ssh.close()
            return job

        except: 
            if not ssh  is None:
                ssh.close()
            gc3utils.log.critical('Failure in retrieving results')
            gc3utils.log.debug('%s %s',sys.exc_info()[0], sys.exc_info()[1])
            raise 



    def get_resource_status(self):

        username = self._resource.ssh_username
        gc3utils.log.debug("Establishing SSH connection to '%s' as user '%s' ...", 
                           self._resource.frontend, username)
        ssh, sftp = self._connect_ssh(self._resource.frontend, username)
        # FIXME: should check `exit_code` and `stderr`
        gc3utils.log.debug("Running `qstat -U %s`...", username)
        exit_code, qstat_stdout, stderr = self._execute_command(ssh, "qstat -U %s" % username)
        gc3utils.log.debug("Running `qstat -F -U %s`...", username)
        exit_code, qstat_F_stdout, stderr = self._execute_command(ssh, "qstat -F -U %s" % username)
        ssh.close()
        sftp.close()

        gc3utils.log.debug("Computing updated values for total/available slots ...")
        (total_running, self._resource.queued, 
         self._resource.user_run, self._resource.user_queued) = sge.count_jobs(qstat_stdout, username)
        slots = sge.compute_nr_of_slots(qstat_F_stdout)
        self._resource.free_slots = int(slots['global']['available'])
        self._resource.used_quota = -1

        gc3utils.log.info("Updated resource '%s' status:"
                          " free slots: %d,"
                          " own running jobs: %d,"
                          " own queued jobs: %d,"
                          " total queued jobs: %d",
                          self._resource.name,
                          self._resource.free_slots,
                          self._resource.user_run,
                          self._resource.user_queued,
                          self._resource.queued,
                          )
        return self._resource

        
     ## Below are the functions needed only for the SshLrms -class.

    def _get_qsub_jobid(self, output):
        """Parse the qsub output for the local jobid."""
        # todo : something with _output
        # todo : make this actually do something

        gc3utils.log.debug('get_qsub_jobid raw output: ' + output)
        try:
            lrms_jobid = output.split(" ")[2]
            gc3utils.log.debug('get_qsub_jobid jobid: ' + lrms_jobid)
            return lrms_jobid

        except:
            gc3utils.log.critical('could not get jobid from output')
            lrms_jobid = False
            raise Exceptions.SshSubmitException

    def _execute_command(self, ssh, command):
        """
        Returns tuple: exit_status, stdout, stderr
        """
        try:
            stdin_stream, stdout_stream, stderr_stream = ssh.exec_command(command)
            output = stdout_stream.read()
            errors = stderr_stream.read()
            exitcode = stdout_stream.channel.recv_exit_status()
            return exitcode, output, errors
        except:
            gc3utils.log.error('Failed while executing remote command: %s' % command)
            raise
                
    def _connect_ssh(self,host,username):
        """Create an ssh connection."""
        # todo : add fancier key handling and password asking stuff

        try:
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            ssh.connect(host,timeout=30,username=username, allow_agent=True)
            sftp = ssh.open_sftp()
            return ssh, sftp

        except paramiko.SSHException, x:
            if not ssh  is None:
               ssh.close()
            gc3utils.log.critical("Could not create ssh connection to '%s': %s: %s", 
                                  host, x.__class__.__name__, str(x))
            raise


