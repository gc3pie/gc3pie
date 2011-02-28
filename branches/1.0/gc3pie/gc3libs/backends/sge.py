#! /usr/bin/env python
#
"""
Job control on SGE clusters (possibly connecting to the front-end via SSH).
"""
# Copyright (C) 2009-2011 GC3, University of Zurich. All rights reserved.
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
__version__ = '1.0rc2 (SVN $Revision$)'


import os
#import paramiko
import random
import re
import sys
import tempfile
import time

from gc3libs import log, Run
from gc3libs.backends import LRMS
import gc3libs.exceptions
import gc3libs.utils as utils # first, defaultdict, to_bytes
from gc3libs.utils import same_docstring_as

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
            if slots_resv is None:
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
        

_qsub_jobid_re = re.compile(r'Your job (?P<jobid>\d+) \("(?P<jobname>.+)"\) has been submitted', re.I)

def get_qsub_jobid(qsub_output):
    """Parse the ``qsub`` output for the local jobid."""
    for line in qsub_output.split('\n'):
        match = _qsub_jobid_re.match(line)
        if match:
            return (match.group('jobid'), match.group('jobname'))
    raise gc3libs.exceptions.InternalError("Could not extract jobid from qsub output '%s'" 
                        % qsub_output.rstrip())


# FIXME: does not work: parsing dates is locale-dependent,
# so this won't work if the date was printed on a computer
# that has a different locale than this one.
#
def _job_info_normalize(self, job):
    if job.haskey('used_cputime'):
        # convert from string to int. Also convert from float representation to int
        job.used_cputime =  int(job.used_cputime.split('.')[0])

    if job.haskey('used_memory'):
        # convert from MB to KiB. Remove 'M' or'G' charater at the end.
        job.used_memory = int(mem[:len(mem)-1]) * 1024

def _sge_filename_mapping(job_name, lrms_jobid, file_name):
    return {
        # XXX: SGE-specific?
        ('%s.out' % job_name) : ('%s.o%s' % (job_name, lrms_jobid)),
        ('%s.err' % job_name) : ('%s.e%s' % (job_name, lrms_jobid)),
        # the following is definitely GAMESS-specific
        ('%s.cosmo' % job_name) : ('%s.o%s.cosmo' % (job_name, lrms_jobid)),
        ('%s.dat'   % job_name) : ('%s.o%s.dat'   % (job_name, lrms_jobid)),
        ('%s.inp'   % job_name) : ('%s.o%s.inp'   % (job_name, lrms_jobid)),
        ('%s.irc'   % job_name) : ('%s.o%s.irc'   % (job_name, lrms_jobid)),
        }[file_name]


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
          * `r.auth` must be a valid key to pass to `Auth.get()`.
        """
        # XXX: should these be `InternalError` instead?
        assert resource.type == gc3libs.Default.SGE_LRMS, \
            "SgeLRMS.__init__(): Failed. Resource type expected 'sge'. Received '%s'" \
            % resource.type

        # checking mandatory resource attributes
        resource.name
        resource.frontend
        resource.transport

        self._resource = resource

        # set defaults
        self._resource.setdefault('sge_accounting_delay', 15)
        auth = auths.get(resource.auth)

        self._ssh_username = auth.username

        if resource.transport == 'local':
            self.transport = transport.LocalTransport()
        elif resource.transport == 'ssh':
            self.transport = transport.SshTransport(self._resource.frontend, 
                                                    username=self._ssh_username)
        else:
            raise gc3libs.exceptions.TransportError("Unknown transport '%s'", resource.transport)
        
        # XXX: does Ssh really needs this ?
        self._resource.ncores = int(self._resource.ncores)
        self._resource.max_memory_per_core = int(self._resource.max_memory_per_core) * 1000
        self._resource.max_walltime = int(self._resource.max_walltime)
        if self._resource.max_walltime > 0:
            # Convert from hours to minutes
            self._resource.max_walltime = self._resource.max_walltime * 60

        self.isValid = 1


    def is_valid(self):
        return self.isValid


    @same_docstring_as(LRMS.submit_job)
    def submit_job(self, app):
        job = app.execution
        # Create the remote directory. 
        try:
            self.transport.connect()

            _command = 'mkdir -p $HOME/.gc3pie_jobs; mktemp -p $HOME/.gc3pie_jobs -d lrms_job.XXXXXXXXXX'
            log.info("Creating remote temporary folder: command '%s' " % _command)
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code == 0:
                ssh_remote_folder = stdout.split('\n')[0]
            else:
                raise LRMSError("Failed while executing command '%s' on resource '%s'. exit code %d, stderr %s."
                                % (_command, self._resource, exit_code, stderr))
        except gc3libs.exceptions.TransportError, x:
            raise
        except:
            self.transport.close()
            raise

        # Copy the input file to remote directory.
        for input in app.inputs.items():
            local_path, remote_path = input
            remote_path = os.path.join(ssh_remote_folder, remote_path)
            remote_parent = os.path.dirname(remote_path)

            try:
                if remote_parent not in ['', '.']:
                    log.debug("Making remote directory '%s'" % remote_parent)
                    self.transport.makedirs(remote_parent)
                log.debug("Transferring file '%s' to '%s'" % (local_path, remote_path))
                self.transport.put(local_path, remote_path)
            except:
                log.critical("Copying input file '%s' to remote cluster '%s' failed",
                                      local_path, self._resource.frontend)
                self.transport.close()
                raise

        try:
            # Try to submit it to the local queueing system.
            qsub, script = app.qsub(self._resource)
            if script is not None:
                # save script to a temporary file and submit that one instead
                local_script_file = tempfile.NamedTemporaryFile()
                local_script_file.write(script)
                local_script_file.flush()
                script_name = '%s.%x.sh' % (app.get('application_tag', 'script'), 
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
            exitcode, stdout, stderr = self.transport.execute_command("/bin/sh -c 'cd %s && %s'" 
                                                                      % (ssh_remote_folder, qsub))
            jobid, jobname = get_qsub_jobid(stdout)
            log.debug('Job submitted with jobid: %s', jobid)
            self.transport.close()

            job.lrms_jobid = jobid
            job.lrms_jobname = jobname
            if app.has_key('stdout'):
                job.stdout_filename = app.stdout
            else:
                job.stdout_filename = '%s.o%s' % (jobname, jobid)
            if app.join:
                job.stderr_filename = job.stdout_filename
            else:
                if app.has_key('stderr'):
                    job.stderr_filename = app.stderr
                else:
                    job.stderr_filename = '%s.e%s' % (jobname, jobid)
            job.log.append('Submitted to SGE @ %s with jobid %s' 
                           % (self._resource.name, jobid))
            job.log.append("SGE `qsub` output:\n"
                           "  === stdout ===\n%s"
                           "  === stderr ===\n%s"
                           "  === end ===\n" 
                           % (stdout, stderr), 'sge', 'qsub')
            job.ssh_remote_folder = ssh_remote_folder

            return job

        except:
            self.transport.close()
            log.critical("Failure submitting job to resource '%s' - see log file for errors"
                                  % self._resource.name)
            raise


    @same_docstring_as(LRMS.update_job_state)
    def update_job_state(self, app):
        # check that passed object obeys contract
        try:
            job = app.execution
            job.lrms_jobid
        except AttributeError, ex:
            # `job` has no `lrms_jobid`: object is invalid
            raise gc3libs.exceptions.InvalidArgument("Job object is invalid: %s" % str(ex))

        def map_sge_names_to_local_ones(name):
            return 'sge_' + name

        mapping = {
            'qname':         'queue',
            'jobname':       'job_name',
            'slots':         'cores',
            'exit_status':   'exit_code',
            'failed':        'sge_system_failed',
            'cpu':           'used_cpu_time',
            'ru_wallclock':  'used_walltime',
            'maxvmem':       'used_memory',
            'end_time':      'sge_completion_time',
            'qsub_time':     'sge_submission_time',
            }

        try:
            self.transport.connect()

            # check the lrms_jobid with qstat
            _command = "qstat | egrep  '^ *%s'" % job.lrms_jobid
            log.debug("checking remote job status with '%s'" % _command)
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code == 0:
                # parse `qstat` output
                job_status = stdout.split()[4]
                log.debug("translating SGE's `qstat` code '%s' to gc3libs.Job state" % job_status)
                if 'qw' in job_status:
                    state = Run.State.SUBMITTED
                elif 'r' in job_status or 'R' in job_status or 't' in job_status:
                    state = Run.State.RUNNING
                elif job_status in ['s', 'S', 'T'] or 'qh' in job_status:
                    state = Run.State.STOPPED
                elif job_status == 'E':
                    state = Run.State.TERMINATED
                else:
                    log.warning("unknown SGE job status '%s', returning `UNKNOWN`", job_status)
                    state = Run.State.UNKNOWN
            else:
                # jobs disappear from `qstat` output as soon as they are finished;
                # we rely on `qacct` to provide information on a finished job
                _command = 'qacct -j %s' % job.lrms_jobid
                log.debug("`qstat` returned no job information; trying with '%s'" % _command)
                exit_code, stdout, stderr = self.transport.execute_command(_command)
                if exit_code == 0:
                    # parse stdout and update job obect with detailed accounting information
                    log.debug('parsing stdout to get job accounting information')
                    for line in stdout.split('\n'):
                        # skip empty and header lines
                        line = line.strip()
                        if line == '' or '===' in line:
                            continue
                        # extract key/value pairs from `qacct` output
                        key, value = line.split(' ', 1)
                        value = value.strip()
                        try:
                            # job[map_sge_names_to_local_ones(key)] = value
                            # job[mapping[key]] =  _parse_value(key, value)
                            job[mapping[key]] = value
                            #self.no

                            if key == 'exit_status':
                                job.returncode = int(value)
                            elif key == 'failed':
                                failure = int(value)
                                if failure != 0:
                                    # XXX: is exit_status significant? should we reset it to -1?
                                    job.signal = Job.Signals.RemoteError
                        except KeyError:
                            log.debug("Ignoring job information '%s=%s'"
                                               " -- no mapping defined to gc3utils.Job attributes." 
                                               % (key,value))

                    # FIXME: parsing dates is locale-dependent; if the
                    # locale of the local computer and the SGE
                    # front-end server do not match, this will blow
                    # up.  Disabling it for now, until we can find a
                    # way to force both locales to be the same.  (RM,
                    # 2010-11-15)
                    #
                    # log.debug('Normalizing data')
                    # # Need to mormalize dates
                    # if job.has_key('submission_time'):
                    #     log.debug('submission_time: %s',job.submission_time)
                    #     job.submission_time = _date_normalize(job.submission_time)
                    # if job.has_key('completion_time'):
                    #     log.debug('completion_time: %s',job.completion_time)
                    #     job.completion_time = _date_normalize(job.completion_time)
                                                                                                    
                    state = Run.State.TERMINATED
                else:
                    # `qacct` failed as well...
                    try:
                        if (time.time() - job.sge_qstat_failed_at) > self._resource.sge_accounting_delay:
                            # accounting info should be there, if it's not then job is definitely lost
                            log.critical("Failed executing remote command: '%s'; exit status %d" 
                                                  % (_command,exit_code))
                            log.debug('remote command returned stdout: %s' % stdout)
                            log.debug('remote command returned stderr: %s' % stderr)
                            raise paramiko.SSHException("Failed executing remote command: '%s'; exit status %d" 
                                                        % (_command,exit_code))
                        else:
                            # do nothing, let's try later...
                            pass
                    except AttributeError:
                        # this is the first time `qstat` fails, record a timestamp and retry later
                        job.sge_qstat_failed_at = time.time()

        except Exception, ex:
            self.transport.close()
            log.error("Error in querying SGE resource '%s': %s: %s",
                              self._resource.name, ex.__class__.__name__, str(ex))
            raise
        
        self.transport.close()

        job.state = state
        return state


    @same_docstring_as(LRMS.cancel_job)
    def cancel_job(self, app):
        job = app.execution
        try:
            self.transport.connect()

            _command = 'qdel '+job.lrms_jobid
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code != 0:
                # It is possible that 'qdel' fails because job has been already completed
                # thus the cancel_job behaviour should be to 
                log.error('Failed executing remote command: %s. exit status %d' % (_command,exit_code))
                log.debug('remote command returned stdout: %s' % stdout)
                log.debug('remote command returned stderr: %s' % stderr)
                if exit_code == 127:
                    # failed executing remote command
                    raise LRMSError('Failed executing remote command')

            self.transport.close()
            return job

        except:
            self.transport.close()
            log.critical('Failure in checking status')
            raise
        


    @same_docstring_as(LRMS.free)
    def free(self, app):

        job = app.execution
        try:
            log.debug("Connecting to cluster frontend '%s' as user '%s' via SSH ...", 
                           self._resource.frontend, self._ssh_username)
            self.transport.connect()
            self.transport.remove_tree(job.ssh_remote_folder)
        except:
            log.warning("Failed removing remote folder '%s': %s: %s" 
                        % (job.ssh_remote_folder, sys.exc_info()[0], sys.exc_info()[1]))
        return


    @same_docstring_as(LRMS.get_results)
    def get_results(self, app, download_dir, overwrite=False):

        job = app.execution
        try:
            log.debug("Connecting to cluster frontend '%s' as user '%s' via SSH ...", 
                           self._resource.frontend, self._ssh_username)
            self.transport.connect()

            # XXX: why do we list the remote dir? `file_list` is not used ever after...
            try:
                files_list = self.transport.listdir(job.ssh_remote_folder)
            except Exception, x:
                self.transport.close()
                log.error("Could not read remote job directory '%s': %s: %s" 
                                  % (job.ssh_remote_folder, x.__class__.__name__, str(x)), 
                                  exc_info=True)
                return

            # copy back all files, renaming them to adhere to the ArcLRMS convention
            log.debug("Downloading job output into '%s' ...", download_dir)
            for remote_path, local_path in app.outputs.items():
                try:
                    # override the remote name if it's a known variable one...
                    remote_path = os.path.join(job.ssh_remote_folder, 
                                               _sge_filename_mapping(job.lrms_jobname, 
                                                                     job.lrms_jobid, remote_path))
                    # remote_path = os.path.join(job.ssh_remote_folder, filename_map[remote_path])
                except KeyError:
                    # ...but keep it if it's not a known one
                    remote_path = os.path.join(job.ssh_remote_folder, remote_path)
                local_path = os.path.join(download_dir, local_path)
                log.debug("Downloading remote file '%s' to local file '%s'", 
                                   remote_path, local_path)
                try:
                    if not os.path.exists(local_path) or overwrite:
                        log.debug("Copying remote '%s' to local '%s'", remote_path, local_path)
                        self.transport.get(remote_path, local_path)
                    else:
                        log.info("Local file '%s' already exists; will not be overwritten!",
                                 local_path)
                except:
                    log.error('Could not copy remote file: ' + remote_path)
                    # FIXME: should we set `job.signal` to
                    # `Job.Signals.DataStagingError`?  Does not seem a
                    # good idea: What if one file is missing out of
                    # several good ones?  Fetching output could be
                    # attempted in any case, and (re)setting
                    # `job.signal` here could mask the true error
                    # cause.
                    raise

            # make jobname.stderr a link to jobname.stdout in case
            # some program relies on its existence
            if not os.path.exists(os.path.join(download_dir, job.stderr_filename)):
                os.symlink(os.path.join(download_dir, job.stdout_filename),
                           os.path.join(download_dir, job.stderr_filename))

            self.transport.close()
            return # XXX: should we return list of downloaded files?

        except: 
            self.transport.close()
            raise 


    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        job = app.execution
        assert job.has_key('ssh_remote_folder'), \
            "Missing attribute `ssh_remote_folder` on `Job` instance passed to `SgeLrms.peek`."

        if size is None:
            size = sys.maxint

        _filename_mapping = _sge_filename_mapping(job.lrms_jobname, job.lrms_jobid, remote_filename)
        _remote_filename = os.path.join(job.ssh_remote_folder, _filename_mapping)

        try:
            self.transport.connect()
            remote_handler = self.transport.open(_remote_filename, mode='r', bufsize=-1)
            remote_handler.seek(offset)
            data = remote_handler.read(size)
            self.transport.close()
        except Exception, ex:
            self.transport.close()
            log.error("Could not read remote file '%s': %s: %s",
                              _remote_filename, ex.__class__.__name__, str(ex))

        try:
            local_file.write(data)
        except (TypeError, AttributeError):
            output_file = open(local_file, 'w+b')
            output_file.write(data)
            output_file.close()
        log.debug('... Done.')

                
    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        try:
            self.transport.connect()

            username = self._ssh_username
            log.debug("Running `qstat -U %s`...", username)
            _command = "qstat -U "+username
            exit_code, qstat_stdout, stderr = self.transport.execute_command(_command)

            log.debug("Running `qstat -F -U %s`...", username)
            _command = "qstat -F -U "+username
            exit_code, qstat_F_stdout, stderr = self.transport.execute_command(_command)

            self.transport.close()

            log.debug("Computing updated values for total/available slots ...")
            (total_running, self._resource.queued, 
             self._resource.user_run, self._resource.user_queued) = count_jobs(qstat_stdout, username)
            slots = compute_nr_of_slots(qstat_F_stdout)
            self._resource.free_slots = int(slots['global']['available'])
            self._resource.used_quota = -1

            log.info("Updated resource '%s' status:"
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
            log.error('Failure while querying remote LRMS')
            log.debug('%s %s',sys.exc_info()[0], sys.exc_info()[1])
            raise
        


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="sge",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
