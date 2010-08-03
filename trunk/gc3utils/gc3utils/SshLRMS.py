import commands
import getpass
import logging
import os
import paramiko
import random
import re
import sys
import tempfile

import gc3utils
import Application
import Default
import Exceptions 
import Job
from LRMS import LRMS
import sge
import utils


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
            gc3utils.log.warning("Cannot create LRMS instance for resource '%s':"
                                 " either 'type' is not 'ssh_sge', or 'ssh_username' is missing.",
                                 resource.name)

    def is_valid(self):
        return self.isValid

    def submit_job(self, application, job=None):
        """
        Submit a job running an instance of the given `application`.
        Return the `job` object, modified to refer to the submitted computational job,
        or a new instance of the `Job` class if `job` is `None` (default).

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

        def run_ssh_command(command, msg=None):
            """Run the specified command and raise an exception if it failed."""
            gc3utils.log.debug(msg or ("Running remote command '%s' ..." % command))
            exit_code, stdout, stderr = self._execute_command(ssh, command)
            if exit_code != 0:
                gc3utils.log.critical("Failed executing remote command '%s'; exit status %d"
                                      % (command, exit_code))
                gc3utils.log.debug('remote command returned stdout: %s' % stdout)
                gc3utils.log.debug('remote command returned stderr: %s' % stderr)
                raise paramiko.SSHException("Failed executing remote command '%s'" % command)
            return stdout, stderr

        try:
            # Try to submit it to the local queueing system.
            qsub, script = application.qsub(self._resource)
            if script is not None:
                # save script to a temporary file and submit that one instead
                local_script_file = tempfile.NamedTemporaryFile()
                local_script_file.write(script)
                local_script_file.flush()
                if application.has_key('application_tag'):
                    script_name = '%s.%x.sh' % (application.application_tag, 
                                                random.randint(sys.maxint))
                else:
                    script_name = 'script.%x.sh' % (random.randint(sys.maxint))
                # upload script to remote location
                sftp.put(local_script_file.name, script_name)
                # cleanup
                local_script_file.close()
                if os.path.exists(local_script_file.name):
                    os.unlink(local_script_file.name)
                # submit it
                qsub += ' ' + script_name
            stdout, stderr = run_ssh_command("/bin/sh -c 'cd %s && %s'" % (ssh_remote_folder, qsub))
            lrms_jobid = sge.get_qsub_jobid(stdout)
            gc3utils.log.debug('Job submitted with jobid: %s',lrms_jobid)
            ssh.close()

            job_log = "\nstdout:\n" + stdout + "\nstderr:\n" + stderr

            if job is None:
                job = Job.Job()
            job.lrms_jobid = lrms_jobid
            job.status = Job.JOB_STATE_SUBMITTED
            job.resource_name = self._resource.name
            job.log = job_log
            job.remote_ssh_folder = ssh_remote_folder

            # remember outputs for later ref
            job.outputs = dict(application.outputs)
            if isinstance(application, Application.GamessApplication):
                # XXX: very qgms/GAMESS-specific!
                job.lrms_job_name = _qgms_job_name(utils.first(application.inputs.values()))
            return job

        except:
            if ssh is not None:
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
            _command = "qstat | egrep  '^ +%s'" % job.lrms_jobid
            gc3utils.log.debug("checking remote job status with '%s'" % _command)
            exit_code, stdout, stderr = self._execute_command(ssh, _command)
            if exit_code == 0:
                # parse `qstat` output
                job_status = stdout.split('\n')[0].split()[4]
                gc3utils.log.debug("translating SGE's `qstat` code '%s' to gc3utils.Job status" % job_status)
                if 'qw' in job_status:
                    job.status = Job.JOB_STATE_SUBMITTED
                elif 'r' in job_status or 'R' in job_status or 't' in job_status:
                    job.status = Job.JOB_STATE_RUNNING
                elif job_status == 'd':
                    job.status = Job.JOB_STATE_DELETED
                elif job_status == 'E':
                    job.status = Job.JOB_STATE_FAILED
                elif job.status == 's' or job.status == 'S' or job.status == 'T' or 'qh' in job.status:
                    # use JOB_STATE_UNKNOWN for the time being
                    # we need to introduce an additional stated that clarifies a sysadmin internvention is needed
                    job.status = Job.JOB_STATE_UNKNOWN
            else:
                # jobs disappear from `qstat` output as soon as they are finished;
                # we rely on `qacct` to provide information on a finished job
                _command = 'qacct -j %s' % job.lrms_jobid
                gc3utils.log.debug("`qstat` returned no job information; trying with '%s'" % _command)
                exit_code, stdout, stderr = self._execute_command(ssh, _command)
                if exit_code == 0:
                    # parse stdout and update job obect with detailed accounting information
                    gc3utils.log.debug('parsing stdout to get job accounting information')
                    for line in stdout.split('\n'):
                        # skip empty and header lines
                        line = line.strip()
                        if line == '' or '===' in line:
                            continue
                        # extract key/value pairs from `qacct` output
                        key, value = line.split(' ', 1)
                        value = value.strip()
                        try:
                            job[mapping[key]] = value
                        except KeyError:
                            gc3utils.log.debug("Ignoring job information '%s=%s'"
                                               " -- no mapping defined to gc3utils.Job attributes." 
                                               % (key,value))
                    job.status = Job.JOB_STATE_FINISHED
                else:
                    # `qacct` failed as well...
                    gc3utils.log.critical("Failed executing remote command: '%s'; exit status %d" 
                                          % (_command,exit_code))
                    gc3utils.log.debug('remote command returned stdout: %s' % stdout)
                    gc3utils.log.debug('remote command returned stderr: %s' % stderr)
                    raise paramiko.SSHException('Failed executing remote command')
                    
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
            if gc3utils.Job.prepare_job_dir(_download_dir) is False:
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


