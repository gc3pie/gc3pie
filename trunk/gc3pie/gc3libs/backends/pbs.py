#! /usr/bin/env python
#
"""
Job control on PBS/Torque clusters (possibly connecting to the front-end via SSH).
"""
# Copyright (C) 2009-2012 GC3, University of Zurich. All rights reserved.
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
__version__ = 'development version (SVN $Revision$)'


import os
import posixpath
import random
import re
import sys
import tempfile
import time

from gc3libs.compat.collections import defaultdict

from gc3libs import log, Run
from gc3libs.backends import LRMS
import gc3libs.exceptions
import gc3libs.utils as utils # first, to_bytes
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
    'num_proc':      _int_floor, # PBS/Torque considers `num_proc` a floating-point value...
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
    Parse PBS/Torque's ``qstat -f`` output (as contained in string `qstat_output`)
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
            return defaultdict(lambda: 0)
        return defaultdict(zdict)
    result = defaultdict(dzdict)
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
            # Some versions of PBS/Torque do not have a "reserved" digit in the slots column, so
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
    the output of PBS/Torque's ``qstat -f``.

    Return a dictionary instance, mapping each host name into a
    dictionary instance, mapping the strings ``total``, ``available``,
    and ``unavailable`` to (respectively) the the total number of
    slots on the host, the number of free slots on the host, and the
    number of used+reserved slots on the host.

    Cluster-wide totals are associated with key ``global``.

    **Note:** The 'available slots' computation carried out by this
    function is unreliable: there is indeed no notion of a 'global' or
    even 'per-host' number of 'free' slots in PBS/Torque.  Slot numbers can
    be computed per-queue, but a host can belong in different queues
    at the same time; therefore the number of 'free' slots available
    to a job actually depends on the queue it is submitted to.  Since
    PBS/Torque does not force users to submit explicitly to a queue, rather
    encourages use of a sort of 'implicit' routing queue, there is no
    way to compute the number of free slots, as this entirely depends
    on how local policies will map a job to the available queues.
    """
    qstat = parse_qstat_f(qstat_output)
    def zero_initializer():
        return 0
    def dict_with_zero_initializer():
        return defaultdict(zero_initializer)
    result = defaultdict(dict_with_zero_initializer)
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
    Parse PBS/Torque's ``qhost -f`` output (as contained in string `qhost_output`)
    and return a `dict` instance, mapping each host name to its attributes.
    """
    result = defaultdict(dict)
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
    Parse PBS/Torque's ``qstat`` output (as contained in string `qstat_output`)
    and return a quadruple `(R, Q, r, q)` where:

      * `R` is the total number of running jobs in the PBS/Torque cell (from any user);
      * `Q` is the total number of queued jobs in the PBS/Torque cell (from any user);
      * `r` is the number of running jobs submitted by user `whoami`;
      * `q` is the number of queued jobs submitted by user `whoami`
    """
    total_running = 0
    total_queued = 0
    own_running = 0
    own_queued = 0
    n = 0
    _qstat_line_re = re.compile(r'^(?P<jobid>\d+)[^\d]+\s+'
                                '(?P<jobname>[^\s]+)\s+'
                                '(?P<username>[^\s]+)\s+'
                                '(?P<time_used>[^\s]+)\s+'
                                '(?P<state>[^\s]+)\s+'
                                '(?P<queue>[^\s]+)')
    for line in qstat_output.split('\n'):
        # import pdb; pdb.set_trace()
        log.info("Output line: %s" %  line)
        m = _qstat_line_re.match(line)
        if not m: continue
        if m.group('state') in ['R']:
            total_running += 1
            if m.group('username') == whoami:
                own_running += 1
        elif m.group('state') in ['Q']: 
            total_queued += 1
            if m.group('username') == whoami:
                own_queued += 1
        log.info("running: %d, queued: %d" % (total_running, total_queued))

    return (total_running, total_queued, own_running, own_queued)
        

_qsub_jobid_re = re.compile(r'(?P<jobname>(?P<jobid>\d+).*)', re.I)

def get_qsub_jobid(qsub_output):
    """Parse the ``qsub`` output for the local jobid."""
    for line in qsub_output.split('\n'):
        match = _qsub_jobid_re.match(line)
        if match:
            return (match.group('jobid'), match.group('jobname'))
    raise gc3libs.exceptions.InternalError("Could not extract jobid from qsub output '%s'" 
                        % qsub_output.rstrip())


def _job_info_normalize(self, job):
    if job.haskey('used_cputime'):
        # convert from string to int. Also convert from float representation to int
        job.used_cputime =  int(job.used_cputime.split('.')[0])

    if job.haskey('used_memory'):
        # store used memory in MiB
        job.used_memory = utils.to_bytes(mem + 'B') / 1024


# FIXME: I think this function is completely wrong and only exists to
# support GAMESS' ``qgms``, which does not allow users to specify the
# name of STDOUT/STDERR files.  When we have a standard flexible
# submission mechanism for all applications, we should remove it!
def _sge_filename_mapping(jobname, jobid, file_name):
    """
    Map STDOUT/STDERR filenames (as recorded in `Application.outputs`)
    to PBS/Torque/OGS default STDOUT/STDERR file names (e.g.,
    ``<jobname>.o<jobid>``).
    """
    try:
        return {
            # XXX: PBS/Torque-specific?
            ('%s.out' % jobname) : ('%s.o%s' % (jobname, jobid)),
            ('%s.err' % jobname) : ('%s.e%s' % (jobname, jobid)),
            # FIXME: the following is definitely GAMESS-specific
            ('%s.cosmo' % jobname) : ('%s.o%s.cosmo' % (jobname, jobid)),
            ('%s.dat'   % jobname) : ('%s.o%s.dat'   % (jobname, jobid)),
            ('%s.inp'   % jobname) : ('%s.o%s.inp'   % (jobname, jobid)),
            ('%s.irc'   % jobname) : ('%s.o%s.irc'   % (jobname, jobid)),
            }[file_name]
    except KeyError:
        return file_name


def _make_remote_and_local_path_pair(transport, job, remote_relpath, local_root_dir, local_relpath):
    """
    Return list of (remote_path, local_path) pairs corresponding to 
    """
    # see https://github.com/fabric/fabric/issues/306 about why it is
    # correct to use `posixpath.join` for remote paths (instead of `os.path.join`)
    remote_path = posixpath.join(job.ssh_remote_folder,
                                 _sge_filename_mapping(job.lrms_jobname, job.lrms_jobid,
                                                       remote_relpath))
    local_path = os.path.join(local_root_dir, local_relpath)
    if transport.isdir(remote_path):
        # recurse, accumulating results
        result = [ ]
        for entry in transport.listdir(remote_path):
            result += _make_remote_and_local_path_pair(
                transport, job,
                posixpath.join(remote_relpath, entry),
                local_path, entry)
        return result
    else:
        return [(remote_path, local_path)]



class PbsLrms(LRMS):
    """
    Job control on PBS/Torque clusters (possibly by connecting via SSH to a submit node).
    """
    def __init__(self, resource, auths):
        """
        Create an `PbsLRMS` instance from a `Resource` object.

        For a `Resource` object `r` to be a valid `PbsLRMS` construction
        parameter, the following conditions must be met:
          * `r.type` must have value `Default.PBS_LRMS`;
          * `r.frontend` must be a string, containing the FQDN of an PBS/Torque cluster submit node;
          * `r.auth` must be a valid key to pass to `Auth.get()`.
        """
        # XXX: should these be `InternalError` instead?
        assert resource.type == gc3libs.Default.PBS_LRMS, \
            "PbsLRMS.__init__(): Failed. Resource type expected 'sge'. Received '%s'" \
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
                raise gc3libs.exceptions.LRMSError("Failed while executing command '%s' on resource '%s';"
                                " exit code: %d, stderr: '%s'."
                                % (_command, self._resource, exit_code, stderr))
        except gc3libs.exceptions.TransportError, x:
            raise
        except:
            # self.transport.close()
            raise

        # Copy the input file to remote directory.
        for local_path,remote_path in app.inputs.items():
            remote_path = os.path.join(ssh_remote_folder, remote_path)
            remote_parent = os.path.dirname(remote_path)
            try:
                if remote_parent not in ['', '.']:
                    log.debug("Making remote directory '%s'" % remote_parent)
                    self.transport.makedirs(remote_parent)
                log.debug("Transferring file '%s' to '%s'" % (local_path.path, remote_path))
                self.transport.put(local_path.path, remote_path)
                # preserve execute permission on input files
                if os.access(local_path.path, os.X_OK):
                    self.transport.chmod(remote_path, 0755)
            except:
                log.critical("Copying input file '%s' to remote cluster '%s' failed",
                                      local_path.path, self._resource.frontend)
                # self.transport.close()
                raise

        if app.executable.startswith('./'):
            gc3libs.log.debug("Making remote path '%s' executable.",
                              app.executable)
            self.transport.chmod(os.path.join(ssh_remote_folder,
                                              app.executable), 0755)
        
        try:
            # Try to submit it to the local queueing system.
            qsub, script = app.pbs_qsub(self._resource)
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
                if 'queue' in self._resource:
                    qsub += " -q %s" % self._resource['queue']
                # submit it
                qsub += ' ' + script_name
            exit_code, stdout, stderr = self.transport.execute_command("/bin/sh -c 'cd %s && %s'" 
                                                                      % (ssh_remote_folder, qsub))

            if exit_code != 0:
                raise gc3libs.exceptions.LRMSError("Failed while executing command '%s' on resource '%s';"
                                " exit code: %d, stderr: '%s'."
                                % (_command, self._resource, exit_code, stderr))
            
            jobid, jobname = get_qsub_jobid(stdout)
            log.debug('Job submitted with jobid: %s', jobid)
            # self.transport.close()

            job.execution_target = self._resource.frontend
            
            job.lrms_jobid = jobid
            job.lrms_jobname = jobname
            if 'stdout' in app:
                job.stdout_filename = app.stdout
            else:
                job.stdout_filename = '%s.o%s' % (jobname, jobid)
            if app.join:
                job.stderr_filename = job.stdout_filename
            else:
                if 'stderr' in app:
                    job.stderr_filename = app.stderr
                else:
                    job.stderr_filename = '%s.e%s' % (jobname, jobid)
            job.log.append('Submitted to PBS/Torque @ %s with jobid %s' 
                           % (self._resource.name, jobid))
            job.log.append("PBS/Torque `qsub` output:\n"
                           "  === stdout ===\n%s"
                           "  === stderr ===\n%s"
                           "  === end ===\n" 
                           % (stdout, stderr), 'sge', 'qsub')
            job.ssh_remote_folder = ssh_remote_folder

            return job

        except:
            # self.transport.close()
            log.critical("Failure submitting job to resource '%s' - see log file for errors"
                                  % self._resource.name)
            raise


    @same_docstring_as(LRMS.update_job_state)
    def update_job_state(self, app):
        # check that passed object obeys contract
        _tracejob_last_re = re.compile('(?P<end_time>\d+/\d+/\d+\s+\d+:\d+:\d+)\s+.\s+Exit_status=(?P<exit_status>\d+)\s+'
                                  'resources_used.cput=(?P<used_cputime>[^ ]+)\s+'
                                  'resources_used.mem=(?P<mem>[^ ]+)\s+'
                                  'resources_used.vmem=(?P<vmem>[^ ]+)\s+'
                                  'resources_used.walltime=(?P<walltime>[^ ]+)')
        _tracejob_queued_re = re.compile('(?P<submission_time>\d+/\d+/\d+\s+\d+:\d+:\d+)\s+.\s+'
                                         'Job Queued at request of .*job name =\s*(?P<jobname>[^,]+),'
                                         '\s+queue =\s*(?P<qname>[^,]+)')
        
        try:
            job = app.execution
            job.lrms_jobid
        except AttributeError, ex:
            # `job` has no `lrms_jobid`: object is invalid
            raise gc3libs.exceptions.InvalidArgument("Job object is invalid: %s" % str(ex))

        try:
            self.transport.connect()

            # check the lrms_jobid with qstat
            _command = "qstat %s | grep ^%s" % (job.lrms_jobid,job.lrms_jobid)
            log.debug("checking remote job status with '%s'" % _command)
            exit_code, stdout, stderr = self.transport.execute_command(_command)
            if exit_code == 0:
                # parse `qstat` output
                job_status = stdout.split()[4]
                log.debug("translating PBS/Torque's `qstat` code '%s' to gc3libs.Run.State" % job_status)
                if job_status in ['Q', 'W']:
                    state = Run.State.SUBMITTED
                elif job_status in ['R']:
                    state = Run.State.RUNNING
                elif job_status in ['S', 'H', 'T'] or 'qh' in job_status:
                    state = Run.State.STOPPED
                elif job_status in ['C', 'E']: 
                    state = Run.State.TERMINATING 
                else:
                    log.warning("unknown PBS/Torque job status '%s', returning `UNKNOWN`", job_status)
                    state = Run.State.UNKNOWN
            else:
                # jobs disappear from `qstat` output as soon as they are finished;
                # we rely on `qacct` to provide information on a finished job
                _command = 'tracejob %s' % job.lrms_jobid
                log.debug("`qstat` returned no job information; trying with '%s'" % _command)
                exit_code, stdout, stderr = self.transport.execute_command(_command)
                if exit_code == 0:
                    # parse stdout and update job obect with detailed accounting information
                    for line in stdout.split('\n'):
                        # skip empty and header lines
                        if _tracejob_last_re.match(line):
                            job.update(_tracejob_last_re.match(line).groupdict())
                            job.returncode = int(job['exit_status'])
                            job.completion_time = job['end_time']
                        elif _tracejob_queued_re.match(line):
                            job.update(_tracejob_queued_re.match(line).groupdict())
                            job.submission_time = job['submission_time']
                        else:
                            continue
                        
                    # FIXME: parsing dates is locale-dependent; if the
                    # locale of the local computer and the PBS/Torque
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
                                                                                                    
                    state = Run.State.TERMINATING
                else:
                    # `qacct` failed as well...
                    try:
                        if (time.time() - job.sge_qstat_failed_at) > self._resource.sge_accounting_delay:
                            # accounting info should be there, if it's not then job is definitely lost
                            log.critical("Failed executing remote command: '%s'; exit status %d" 
                                                  % (_command,exit_code))
                            log.debug("Remote command returned stdout: %s" % stdout)
                            log.debug("remote command returned stderr: %s" % stderr)
                            raise paramiko.SSHException("Failed executing remote command: '%s'; exit status %d" 
                                                        % (_command,exit_code))
                        else:
                            # do nothing, let's try later...
                            pass
                    except AttributeError:
                        # this is the first time `qstat` fails, record a timestamp and retry later
                        job.pbs_qstat_failed_at = time.time()

        except Exception, ex:
            # self.transport.close()
            log.error("Error in querying PBS/Torque resource '%s': %s: %s",
                              self._resource.name, ex.__class__.__name__, str(ex))
            raise
        
        # self.transport.close()

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
                log.debug("remote command returned stdout: %s" % stdout)
                log.debug("remote command returned stderr: %s" % stderr)
                if exit_code == 127:
                    # failed executing remote command
                    raise gc3libs.exceptions.LRMSError('Failed executing remote command')

            # self.transport.close()
            return job

        except:
            # self.transport.close()
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
        if app.output_base_url is not None:
            raise gc3libs.exceptions.UnrecoverableDataStagingError(
                "Retrieval of output files to non-local destinations"
                " is not supported in the PBS/Torque backend (yet).")

        job = app.execution
        try:
            self.transport.connect()
            # Make list of files to copy, in the form of (remote_path, local_path) pairs.
            # This entails walking the `Application.outputs` list to expand wildcards
            # and directory references.
            stageout = [ ]
            for remote_relpath, local_url in app.outputs.iteritems():
                local_relpath = local_url.path
                if remote_relpath == gc3libs.ANY_OUTPUT:
                    remote_relpath = ''
                    local_relpath = ''
                stageout += _make_remote_and_local_path_pair(
                    self.transport, job, remote_relpath, download_dir, local_relpath)

            # copy back all files, renaming them to adhere to the ArcLRMS convention
            log.debug("Downloading job output into '%s' ...", download_dir)
            for remote_path, local_path in stageout:
                log.debug("Downloading remote file '%s' to local file '%s'",
                          remote_path, local_path)
                if (overwrite
                    or not os.path.exists(local_path)
                    or os.path.isdir(local_path)):
                    log.debug("Copying remote '%s' to local '%s'"
                              % (remote_path, local_path))
                    # ignore missing files (this is what ARC does too)
                    self.transport.get(remote_path, local_path,
                                       ignore_nonexisting=True)
                else:
                    log.info("Local file '%s' already exists;"
                             " will not be overwritten!",
                             local_path)

            # self.transport.close()
            return # XXX: should we return list of downloaded files?

        except: 
            # self.transport.close()
            raise 


    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        job = app.execution
        assert job.has_key('ssh_remote_folder'), \
            "Missing attribute `ssh_remote_folder` on `Job` instance passed to `PbsLrms.peek`."

        if size is None:
            size = sys.maxint

        _filename_mapping = _sge_filename_mapping(job.lrms_jobname, job.lrms_jobid, remote_filename)
        _remote_filename = os.path.join(job.ssh_remote_folder, _filename_mapping)

        try:
            self.transport.connect()
            remote_handler = self.transport.open(_remote_filename, mode='r', bufsize=-1)
            remote_handler.seek(offset)
            data = remote_handler.read(size)
            # self.transport.close()
        except Exception, ex:
            # self.transport.close()
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
            log.debug("Running `qstat -a`...")
            _command = "qstat "
            exit_code, qstat_stdout, stderr = self.transport.execute_command(_command)

            # self.transport.close()

            log.debug("Computing updated values for total/available slots ...")
            (total_running, self._resource.queued, 
             self._resource.user_run, self._resource.user_queued) = count_jobs(qstat_stdout, username)
            # slots = compute_nr_of_slots(qstat_stdout)
            # self._resource.free_slots = int(slots['global']['available'])
            self._resource.total_run = total_running
            self._resource.free_slots = -1
            self._resource.used_quota = -1

            log.info("Updated resource '%s' status:"
                     " free slots: %d,"
                     " total running: %d,"
                     " own running jobs: %d,"
                     " own queued jobs: %d,"
                     " total queued jobs: %d",
                     self._resource.name,
                     self._resource.free_slots,
                     self._resource.total_run,
                     self._resource.user_run,
                     self._resource.user_queued,
                     self._resource.queued,
                     )
            return self._resource

        except Exception, ex:
            # self.transport.close()
            log.error("Error querying remote LRMS, see debug log for details.")
            log.debug("Error querying LRMS: %s: %s",
                      ex.__class__.__name__, str(ex))
            raise
        
    @same_docstring_as(LRMS.validate_data)
    def validate_data(self, data_file_list):
        """
        Supported protocols: file
        """
        for url in data_file_list:
            if not url.scheme in ['file']:
                return False
        return True

    @same_docstring_as(LRMS.validate_data)
    def close(self):
        self.transport.close()
        
## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="sge",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
