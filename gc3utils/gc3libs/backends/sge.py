#! /usr/bin/env python
#
"""
Job control on SGE clusters (possibly connecting to the front-end via SSH).
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
#
# Includes parts adapted from the ``bzr`` code, which is
# copyright (C) 2005, 2006, 2007, 2008, 2009 Canonical Ltd
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__docformat__ = 'reStructuredText'
__version__ = '$Revision$'


import os
import paramiko
import random
import re
import sys
import tempfile
import time

import gc3libs
from gc3libs.application import Application
from gc3libs.application.gamess import GamessApplication
from gc3libs.backends import LRMS
import gc3libs.Default as Default
import gc3libs.Exceptions as Exceptions 
import gc3libs.Job as Job
import gc3libs.utils as utils # first, defaultdict, to_bytes

import transport

def _int_floor(s):
    return int(float(s))

# `_convert` is a `dict` instance, mapping key names to functions
# that parse a value from a string into a Python native type.
_convert = {
    'slots':         int,
    'slots_used':    int,
    'slots_resv':    int,
    'slots_total':   int,
    'load_avg':      float,
    'load_short':    float,
    'load_medium':   float,
    'load_long':     float,
    'np_load_avg':   float,
    'np_load_short': float,
    'np_load_medium':float,
    'np_load_long':  float,
    'num_proc':      _int_floor, # SGE considers `num_proc` a floating-point value...
    'swap_free':     utils.to_bytes,
    'swap_total':    utils.to_bytes,
    'swap_used':     utils.to_bytes,
    'mem_free':      utils.to_bytes,
    'mem_used':      utils.to_bytes,
    'mem_total':     utils.to_bytes,
    'virtual_free':  utils.to_bytes,
    'virtual_used':  utils.to_bytes,
    'virtual_total': utils.to_bytes,
}
def _parse_value(key, value):
    try:
        return _convert[key](value)
    except:
        return value


def parse_qstat_f(qstat_output):
    """
    Parse SGE's ``qstat -F`` output (as contained in string `qstat_output`)
    and return a `dict` instance, mapping each queue name to its attributes.
    """
    # a job report line starts with a numeric job ID
    _job_line_re = re.compile(r'^[0-9]+ \s+', re.X)
    # queue report header line starts with queuename@hostname 
    _queue_header_re = re.compile(r'^([a-z0-9\._-]+)@([a-z0-9\.-]+) \s+ ([BIPCTN]+) \s+ ([0-9]+)?/?([0-9]+)/([0-9]+)', 
                                  re.I|re.X)
    # property lines always have the form 'xx:propname=value'
    _property_line_re = re.compile(r'^[a-z]{2}:([a-z_]+)=(.+)', re.I|re.X)
    def dzdict():
        def zdict():
            return utils.defaultdict(lambda: 0)
        return utils.defaultdict(zdict)
    result = utils.defaultdict(dzdict)
    qname = None
    for line in qstat_output.split('\n'):
        # strip leading and trailing whitespace
        line = line.strip()
        # is this a queue header?
        match = _queue_header_re.match(line)
        if match:
            qname, hostname, kind, slots_resv, slots_used, slots_total = match.groups()
            if 'B' not in kind:
                continue # ignore non-batch queues
            # Some versions of SGE do not have a "reserved" digit in the slots column, so
            # slots_resv will be set to None.  For our purposes it is better that it is 0.
            if slots_resv == None:
                slots_resv = 0
            # key names are taken from 'qstat -xml' output
            result[qname][hostname]['slots_resv'] = _parse_value('slots_resv', slots_resv)
            result[qname][hostname]['slots_used'] = _parse_value('slots_used', slots_used)
            result[qname][hostname]['slots_total'] = _parse_value('slots_total', slots_total)
        # is this a property line?
        match = _property_line_re.match(line)
        if match:
            key, value = match.groups()
            result[qname][hostname][key] = _parse_value(key, value)
    return result


def compute_nr_of_slots(qstat_output):
    """
    Compute the number of total, free, and used/reserved slots from
    the output of SGE's ``qstat -F``.

    Return a dictionary instance, mapping each host name into a
    dictionary instance, mapping the strings ``total``, ``available``,
    and ``unavailable`` to (respectively) the the total number of
    slots on the host, the number of free slots on the host, and the
    number of used+reserved slots on the host.

    Cluster-wide totals are associated with key ``global``.

    **Note:** The 'available slots' computation carried out by this
    function is unreliable: there is indeed no notion of a 'global' or
    even 'per-host' number of 'free' slots in SGE.  Slot numbers can
    be computed per-queue, but a host can belong in different queues
    at the same time; therefore the number of 'free' slots available
    to a job actually depends on the queue it is submitted to.  Since
    SGE does not force users to submit explicitly to a queue, rather
    encourages use of a sort of 'implicit' routing queue, there is no
    way to compute the number of free slots, as this entirely depends
    on how local policies will map a job to the available queues.
    """
    qstat = parse_qstat_f(qstat_output)
    def zero_initializer():
        return 0
    def dict_with_zero_initializer():
        return utils.defaultdict(zero_initializer)
    result = utils.defaultdict(dict_with_zero_initializer)
    for q in qstat.iterkeys():
        for host in qstat[q].iterkeys():
            r = result[host]
            s = qstat[q][host]
            r['total'] = max(s['slots_total'], r['total'])
            r['unavailable'] = max(s['slots_used'] + s['slots_resv'], 
                                   r['unavailable'])
    # compute available slots by subtracting the number of used+reserved from the total
    g = result['global']
    for host in result.iterkeys():
        r = result[host]
        r['available'] = r['total'] - r['unavailable']
        # update cluster-wide ('global') totals
        g['total'] += r['total']
        g['unavailable'] += r['unavailable']
        g['available'] += r['available']
    return result


def parse_qhost_f(qhost_output):
    """
    Parse SGE's ``qhost -F`` output (as contained in string `qhost_output`)
    and return a `dict` instance, mapping each host name to its attributes.
    """
    result = utils.defaultdict(dict)
    n = 0
    for line in qhost_output.split('\n'):
        # skip header lines
        n += 1
        if n < 3:
            continue
        # property lines start with TAB
        if line.startswith(' '):
            key, value = line.split('=')
            ignored, key = key.split(':')
            result[hostname][key] = _parse_value(key, value)
        # host lines begin at column 0
        else:
            hostname = line.split(' ')[0]
    return result
    
    
def count_jobs(qstat_output, whoami):
    """
    Parse SGE's ``qstat`` output (as contained in string `qstat_output`)
    and return a quadruple `(R, Q, r, q)` where:
      * `R` is the total number of running jobs in the SGE cell (from any user);
      * `Q` is the total number of queued jobs in the SGE cell (from any user);
      * `r` is the number of running jobs submitted by user `whoami`;
      * `q` is the number of queued jobs submitted by user `whoami`
    """
    total_running = 0
    total_queued = 0
    own_running = 0
    own_queued = 0
    n = 0
    for line in qstat_output.split('\n'):
        # skip header lines
        n += 1
        if n < 3:
            continue
        # remove leading and trailing whitespace
        line = line.strip()
        if len(line) == 0:
            continue
        jid, prio, name, user, state, rest = re.split(r'\s+', line, 5)
        # skip in error/hold/suspended/deleted state
        if (('E' in state) or ('h' in state) or ('T' in state) 
            or ('s' in state) or ('S' in state) or ('d' in state)):
            continue
        if 'q' in state:
            total_queued += 1
            if user == whoami:
                own_queued += 1
        if 'r' in state:
            total_running += 1
            if user == whoami:
                own_running += 1
    return (total_running, total_queued, own_running, own_queued)
        

_qsub_jobid_re = re.compile(r"Your job (?P<jobid>\d+) .+ has been submitted", re.I)

def get_qsub_jobid(qsub_output):
    """Parse the qsub output for the local jobid."""
    for line in qsub_output.split('\n'):
        match = _qsub_jobid_re.match(line)
        if match:
            return match.group('jobid')
    raise Exceptions.InternalError("Could not extract jobid from qsub output '%s'" 
                                   % qsub_output.rstrip())

def _qgms_job_name(filename):
    """
    Return the batch system job name used to submit GAMESS on input
    file name `filename`.
    """
    if filename.endswith('.inp'):
        return os.path.splitext(filename)[0]
    else:
        return filename


class SgeLrms(LRMS):
    """
    Job control on SGE clusters (possibly by connecting via SSH to a submit node).
    """
    def __init__(self, resource, auths):
        """
        Create an `SgeLRMS` instance from a `Resource` object.

        For a `Resource` object `r` to be a valid `SgeLRMS` construction
        parameter, the following conditions must be met:
          * `r.type` must have value `Default.SGE_LRMS`;
          * `r.frontend` must be a string, containing the FQDN of an SGE cluster submit node;
          * `r.auth_type` must be a valid key to pass to `Authorization.get()`.
        """
        # XXX: should these be `InternalError` instead?
        assert (resource.has_key('type') and resource.type == Default.SGE_LRMS), \
            "SgeLRMS.__init__(): called with a resource parameter that does not have 'type' equal to 'ssh_sge'"
        assert resource.has_key('name'), \
            "SgeLRMS.__init__(): passed a resource parameter without a 'name' attribute."

        if not resource.has_key('frontend'):
            raise ConfigurationError("Resource '%s' has type 'ssh_sge' but no 'frontend' attribute." 
                                     % resource.name)

        self._resource = resource
        # set defaults
        self._resource.setdefault('sge_accounting_delay', 15)

        auth = auths.get(resource.authorization_type)

        self._ssh_username = auth.username

        # for this backend transport object is mandatory.
        if resource.has_key('transport'):
            if resource.transport == 'local':
                self.transport = transport.LocalTransport()
            elif resource.transport == 'ssh':
                self.transport = transport.SshTransport(self._resource.frontend,username=self._ssh_username)
            else:
                raise transport.TransportError('Unknown transport %s', resource.transport)
        else:
            raise LRMSException('Invalid resource description: missing transport')
        
        # XXX: does Ssh really needs this ?
        self._resource.ncores = int(self._resource.ncores)
        self._resource.max_memory_per_core = int(self._resource.max_memory_per_core) * 1000
        self._resource.max_walltime = int(self._resource.max_walltime)
        if self._resource.max_walltime > 0:
            # Convert from hours to minutes
            self._resource.max_walltime = self._resource.max_walltime * 60

        self.isValid = 1


    def _is_transport_open(self):
        pass

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
        
        self.transport.connect()

        ## Establish an ssh connection.
        #(ssh, sftp) = self._connect_ssh(self._resource.frontend,self._ssh_username)

        # Create the remote unique_token directory. 
        try:
            _command = 'mkdir -p $HOME/.gc3utils_jobs; mktemp -p $HOME/.gc3utils_jobs -d lrms_job.XXXXXXXXXX'
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code == 0:
                ssh_remote_folder = stdout.split('\n')[0]
            else:
                raise LRMSError('Failed while executing remote command')
        except:
            gc3libs.log.critical("Failed creating remote temporary folder: command '%s' returned exit code %d, stderr %s)"
                                 % (_command, exit_code, stderr))
            self.transport.close()
            raise

        # Copy the input file to remote directory.
        for input in application.inputs.items():
            local_path, remote_path = input
            remote_path = os.path.join(ssh_remote_folder, remote_path)

            gc3libs.log.debug("Transferring file '%s' to '%s'" % (local_path, remote_path))
            try:
                self.transport.put(local_path, remote_path)
            except:
                gc3libs.log.critical("Copying input file '%s' to remote cluster '%s' failed",
                                      local_path, self._resource.frontend)
                self.transport.close()
                raise

        #def run_ssh_command(command, msg=None):
        #    """Run the specified command and raise an exception if it failed."""
        #    gc3libs.log.debug(msg or ("Running remote command '%s' ..." % command))
        #    exit_code, stdout, stderr = self._execute_command(ssh, command)
        #    if exit_code != 0:
        #        gc3libs.log.critical("Failed executing remote command '%s'; exit status %d"
        #                              % (command, exit_code))
        #        gc3libs.log.debug('remote command returned stdout: %s' % stdout)
        #        gc3libs.log.debug('remote command returned stderr: %s' % stderr)
        #        raise paramiko.SSHException("Failed executing remote command '%s'" % command)
        #    return stdout, stderr

        try:
            # Try to submit it to the local queueing system.
            qsub, script = application.qsub(self._resource)
            if script is not None:
                # save script to a temporary file and submit that one instead
                local_script_file = tempfile.NamedTemporaryFile()
                local_script_file.write(script)
                local_script_file.flush()
                script_name = '%s.%x.sh' % (application.get('application_tag', 'script'), 
                                            random.randint(0, sys.maxint))
                # upload script to remote location
                self.transport.put(local_script_file.name,
                                   os.path.join(ssh_remote_folder, script_name))
                # cleanup
                local_script_file.close()
                if os.path.exists(local_script_file.name):
                    os.unlink(local_script_file.name)
                # submit it
                qsub += ' ' + script_name
            exitcode, stdout, stderr = self.transport.execute_command("/bin/sh -c 'cd %s && %s'" % (ssh_remote_folder, qsub))
            lrms_jobid = get_qsub_jobid(stdout)
            gc3libs.log.debug('Job submitted with jobid: %s',lrms_jobid)
            self.transport.close()

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
            if isinstance(application, GamessApplication):
                # XXX: very qgms/GAMESS-specific!
                job.lrms_job_name = _qgms_job_name(utils.first(application.inputs.values()))


            # add submssion time reference
            job.submission_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

            return job

        except:
            self.transport.close()
            gc3libs.log.critical("Failure submitting job to resource '%s' - see log file for errors"
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
            'maxvmem':'used_memory',
            'end_time':'completion_time',
            'qsub_time':'submission_time',
            'maxvmem':'used_memory',
            }
        try:
            self.transport.connect()
            # open ssh connection
            # ssh, sftp = self._connect_ssh(self._resource.frontend,self._ssh_username)

            # then check the lrms_jobid with qstat
            _command = "qstat | egrep  '^ *%s'" % job.lrms_jobid
            gc3libs.log.debug("checking remote job status with '%s'" % _command)
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code == 0:
                # parse `qstat` output
                job_status = stdout.split()[4]
                gc3libs.log.debug("translating SGE's `qstat` code '%s' to gc3utils.Job status" % job_status)
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
                gc3libs.log.debug("`qstat` returned no job information; trying with '%s'" % _command)
                exit_code, stdout, stderr = self.transport.execute_command(_command)
                if exit_code == 0:
                    # parse stdout and update job obect with detailed accounting information
                    gc3libs.log.debug('parsing stdout to get job accounting information')
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
                            gc3libs.log.debug("Ignoring job information '%s=%s'"
                                               " -- no mapping defined to gc3utils.Job attributes." 
                                               % (key,value))

                    gc3libs.log.debug('Normalizing data')
                    # Need to mormalize dates
                    if job.has_key('submission_time'):
                        gc3libs.log.debug('submission_time: %s',job.submission_time)
                        job.submission_time = self._date_normalize(job.submission_time)
                    if job.has_key('completion_time'):
                        gc3libs.log.debug('completion_time: %s',job.completion_time)
                        job.completion_time = self._date_normalize(job.completion_time)
                                                                                                    
                    job.status = Job.JOB_STATE_FINISHED
                else:
                    # `qacct` failed as well...
                    try:
                        if (time.time() - job.sge_qstat_failed_at) > self._resource.sge_accounting_delay:
                            # accounting info should be there, if it's not then job is definitely lost
                            gc3libs.log.critical("Failed executing remote command: '%s'; exit status %d" 
                                                  % (_command,exit_code))
                            gc3libs.log.debug('remote command returned stdout: %s' % stdout)
                            gc3libs.log.debug('remote command returned stderr: %s' % stderr)
                            raise paramiko.SSHException("Failed executing remote command: '%s'; exit status %d" 
                                                        % (_command,exit_code))
                        else:
                            # do nothing, let's try later...
                            pass
                    except AttributeError:
                        # this is the first time `qstat` fails, record a timestamp and retry later
                        job.sge_qstat_failed_at = time.time()

            # explicitly set stdout and stderr
            # Note: stdout and stderr are always considered as merged
            job.stdout_filename = job.lrms_job_name + '.o' + job.lrms_jobid
            job.stderr_filename = job.lrms_job_name + '.o' + job.lrms_jobid
            
            self.transport.close()
            
            return job
        
        except:
            self.transport.close()
            gc3libs.log.critical('Failure in checking status')
            raise


    def cancel_job(self, job_obj):
        try:
            
            self.transport.connect()

            _command = 'qdel '+job_obj.lrms_jobid

            exit_code, stdout, stderr = self.transport.execute_command(_command)

            if exit_code != 0:
                # It is possible that 'qdel' fails because job has been already completed
                # thus the cancel_job behaviour should be to 
                gc3libs.log.error('Failed executing remote command: %s. exit status %d' % (_command,exit_code))
                gc3libs.log.debug('remote command returned stdout: %s' % stdout)
                gc3libs.log.debug('remote command returned stderr: %s' % stderr)
                if exit_code == 127:
                    # failed executing remote command
                    raise LRMSError('Failed executing remote command')

            self.transport.close()
            return job_obj

        except:
            self.transport.close()
            gc3libs.log.critical('Failure in checking status')
            raise
        


    def get_results(self, job):
        """Retrieve results of a job."""
        
        gc3libs.log.debug("Connecting to cluster frontend '%s' as user '%s' via SSH ...", 
                           self._resource.frontend, self._ssh_username)
        try:

            self.transport.connect()

            try:
                files_list = self.transport.listdir(job.remote_ssh_folder)
            except Exception, x:
                gc3libs.log.error("Could not read remote job directory '%s': " 
                                   % job.remote_ssh_folder, exc_info=True)
                gc3libs.log.debug("Error type %s, %s" % (sys.exc_info()[0], sys.exc_info()[1]))
                self.transport.close()
                #raise
                job.status = Job.JOB_STATE_FAILED
                return job

            if job.has_key('job_local_dir'):
                _download_dir = job.job_local_dir + '/' + job.unique_token
            else:
                _download_dir = Default.JOB_FOLDER_LOCATION + '/' + job.unique_token
                
            # Prepare/Clean download dir
            if Job.prepare_job_dir(_download_dir) is False:
                # failed creating local folder
                raise Exception("failed creating download folder '%s'" % _download_dir)

            # copy back all files, renaming them to adhere to the ArcLRMS convention
            try: 
                jobname = job.lrms_job_name
                gc3libs.log.debug("Recorded job name is '%s'" % jobname)
            except KeyError:
                # no job name was set, empty string should be safe for following code
                jobname = ''
                gc3libs.log.warning("No recorded job name; will not be able to rename files accordingly")
            filename_map = { 
                # XXX: SGE-specific?
                ('%s.out' % jobname) : ('%s.o%s' % (jobname, job.lrms_jobid)),
                ('%s.err' % jobname) : ('%s.e%s' % (jobname, job.lrms_jobid)),
                # the following is definitely GAMESS-specific
                ('%s.cosmo' % jobname) : ('%s.o%s.cosmo' % (jobname, job.lrms_jobid)),
                ('%s.dat'   % jobname) : ('%s.o%s.dat'   % (jobname, job.lrms_jobid)),
                ('%s.inp'   % jobname) : ('%s.o%s.inp'   % (jobname, job.lrms_jobid)),
                ('%s.irc'   % jobname) : ('%s.o%s.irc'   % (jobname, job.lrms_jobid)),
                }
            # copy back all files
            gc3libs.log.debug("Downloading job output into '%s' ...",_download_dir)
            for remote_path, local_path in job.outputs.items():
                try:
                    # override the remote name if it's a known variable one...
                    remote_path = os.path.join(job.remote_ssh_folder, filename_map[remote_path])
                except KeyError:
                    # ...but keep it if it's not a known one
                    remote_path = os.path.join(job.remote_ssh_folder, remote_path)
                local_path = os.path.join(_download_dir, local_path)
                gc3libs.log.debug("Downloading remote file '%s' to local file '%s'", 
                                   remote_path, local_path)
                try:
                    if not os.path.exists(local_path):
                        gc3libs.log.debug("Copying remote '%s' to local '%s'", remote_path, local_path)
                        self.transport.get(remote_path, local_path)
                    else:
                        gc3libs.log.info("Local file '%s' already exists; will not be overwritten!",
                                          local_path)
                except:
                    gc3libs.log.error('Could not copy remote file: ' + remote_path)
                    raise
            # `qgms` submits GAMESS jobs with `-j y`, i.e., stdout and stderr are
            # collected into the same file; make jobname.stderr a link to jobname.stdout
            # in case some program relies on its existence
            if not os.path.exists(_download_dir + '/' + jobname + '.err'):
                os.symlink(_download_dir + '/' + jobname + '.out',
                           _download_dir + '/' + jobname + '.err')

            # cleanup remote folder
            try:
                self.transport.remove_tree(job.remote_ssh_folder)
            except:
                gc3libs.log.error('Failed while removing remote folder %s. Error type %s, %s' 
                                  % (job.remote_ssh_folder, sys.exc_info()[0], sys.exc_info()[1]))
            
            # set job status to COMPLETED
            job.download_dir = _download_dir
            job.status = Job.JOB_STATE_COMPLETED

            self.transport.close()
            return job

        except: 
            self.transport.close()
            gc3libs.log.critical('Failure in retrieving results')
            gc3libs.log.debug('%s %s',sys.exc_info()[0], sys.exc_info()[1])
            raise 


    def tail(self, job_obj, filename, offset=0, buffer_size=None):
        """
        tail allows to get a snapshot of any valid file created by the job
        """

        try:

            self.transport.connect()

            # Sanitize offset
            if int(offset) < 1024:
                offset = 0

            # Sanitize buffer_size
            if  not buffer_size:
                buffer_size = -1

            # reference to remote file
            _remote_filename = job_obj.remote_ssh_folder + '/' + filename

            # create temp file
            _tmp_filehandle = tempfile.NamedTemporaryFile(mode='w+b', suffix='.tmp', prefix='gc3_')

            # remote_handler = sftp.open(_remote_filename, mode='r', bufsize=-1)
            remote_handler = self.transport.open(_remote_filename, mode='r', bufsize=-1)

            remote_handler.seek(offset)
            _tmp_filehandle.write(remote_handler.read(buffer_size))

            gc3libs.log.debug('Done')

            _tmp_filehandle.file.flush()
            _tmp_filehandle.file.seek(0)
            
            self.transport.close()
        
            return _tmp_filehandle
        except:
            self.transport.close()
            gc3libs.log.critical('Failure in reading remote file %s', filename)
            gc3libs.log.debug('%s %s',sys.exc_info()[0], sys.exc_info()[1])
            raise
                
    def get_resource_status(self):

        try:

            self.transport.connect()

            username = self._ssh_username
            gc3libs.log.debug("Running `qstat -U %s`...", username)
            _command = "qstat -U "+username
            exit_code, qstat_stdout, stderr = self.transport.execute_command(_command)

            gc3libs.log.debug("Running `qstat -F -U %s`...", username)
            _command = "qstat -F -U "+username
            exit_code, qstat_F_stdout, stderr = self.transport.execute_command(_command)

            self.transport.close()

            gc3libs.log.debug("Computing updated values for total/available slots ...")
            (total_running, self._resource.queued, 
             self._resource.user_run, self._resource.user_queued) = count_jobs(qstat_stdout, username)
            slots = compute_nr_of_slots(qstat_F_stdout)
            self._resource.free_slots = int(slots['global']['available'])
            self._resource.used_quota = -1

            gc3libs.log.info("Updated resource '%s' status:"
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

        except:
            self.transport.close()
            gc3libs.log.critical('Failure while querying remote LRMS')
            gc3libs.log.debug('%s %s',sys.exc_info()[0], sys.exc_info()[1])
            raise
        
     ## Below are the functions needed only for the SshLrms -class.

    def _date_normalize(self, date_string):
        # Example format: Wed Aug 25 15:41:30 2010
        t = time.strptime(date_string,"%a %b %d %H:%M:%S %Y")
        # Temporarly adapted to return a string representation
        return time.strftime("%Y-%m-%d %H:%M:%S", t)

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="sge",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
