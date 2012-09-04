#! /usr/bin/env python
#
"""
Job control on SGE clusters (possibly connecting to the front-end via SSH).
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


from getpass import getuser
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

import batch

_qsub_jobid_re = re.compile(r'Your job (?P<jobid>\d+) \("(?P<jobname>.+)"\) has been submitted', re.I)

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
    Parse SGE's ``qhost -F`` output (as contained in string `qhost_output`)
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
    to SGE/OGS default STDOUT/STDERR file names (e.g.,
    ``<jobname>.o<jobid>``).
    """
    try:
        return {
            # XXX: SGE-specific?
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


class SgeLrms(batch.BatchSystem):
    """
    Job control on SGE clusters (possibly by connecting via SSH to a submit node).
    """

    _batchsys_name = 'Grid Engine'

    def __init__(self, name,
                 # this are inherited from the base LRMS class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime,
                 auth, # ignored if `transport` is 'local'
                 # these are inherited from the `BatchSystem` class
                 frontend, transport,
                 accounting_delay = 15,
                 **extra_args):

        # init base class
        batch.BatchSystem.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth,
            frontend, transport, accounting_delay, **extra_args)

        self.qsub = self._get_command_argv('qsub')

        # GridEngine commands
        self._qacct = self._get_command('qacct')
        self._qdel = self._get_command('qdel')
        self._qstat = self._get_command('qstat')


    def _submit_command(self, app):
        sub_argv, app_argv = app.qsub_sge(self)
        return (str.join(' ', sub_argv), str.join(' ', app_argv))

    def _parse_submit_output(self, output):
        """Parse the ``qsub`` output for the local jobid."""
        return self.get_jobid_from_submit_output(output, _qsub_jobid_re)

    def _stat_command(self, job):
        return ("%s | egrep  '^ *%s'" % (self._qstat, job.lrms_jobid))

    def _parse_stat_output(self, stdout):
        job_status = stdout.split()[4]
        log.debug("translating SGE's `qstat` code '%s' to gc3libs.Run.State" % job_status)

        jobstatus = dict()
        if job_status in ['s', 'S', 'T'] or job_status.startswith('h'):
            jobstatus['state'] = Run.State.STOPPED
        elif 'qw' in job_status:
            jobstatus['state'] = Run.State.SUBMITTED
        elif 'r' in job_status or 'R' in job_status or 't' in job_status:
            jobstatus['state'] = Run.State.RUNNING
        elif job_status == 'E': # error condition
            jobstatus['state'] = Run.State.TERMINATING
        else:
            log.warning("unknown SGE job status '%s', returning `UNKNOWN`", job_status)
            jobstatus['state'] = Run.State.UNKNOWN
        return jobstatus

    def _acct_command(self, job):
        return ("%s -j %s" % (self._qacct, job.lrms_jobid))

    def _parse_acct_output(self, stdout):
        jobstatus = dict()
        mapping = {
            'qname':         'queue',
            'jobname':       'job_name',
            'slots':         'cores',
            'exit_status':   'exit_status',
            'failed':        'sge_system_failed',
            'cpu':           'used_cpu_time',
            'ru_wallclock':  'used_walltime',
            'maxvmem':       'used_memory',
            'end_time':      'sge_completion_time',
            'qsub_time':     'sge_submission_time',
            }

        for line in stdout.split('\n'):
            # skip empty and header lines
            line = line.strip()
            if line == '' or '===' in line:
                continue
            # extract key/value pairs from `qacct` output
            key, value = line.split(' ', 1)
            value = value.strip()
            try:
                jobstatus[mapping[key]] = value

                if key == 'failed':
                    # value may be, e.g., "100 : assumedly after job"
                    failure = int(value.split()[0])
            except KeyError:
                log.debug("Ignoring job information '%s=%s';"
                          " no mapping defined for it"
                          " in 'gc3libs/backends/sge.py'."
                          % (key,value))

        return jobstatus

    def _cancel_command(self, jobid):
        return ("%s %s" % (self._qdel, jobid))

    @same_docstring_as(LRMS.get_resource_status)
    @LRMS.authenticated
    def get_resource_status(self):
        try:
            self.transport.connect()

            _command = ("%s -U %s" % (self._qstat, self._username))
            log.debug("Running `%s`...", _command)
            exit_code, qstat_stdout, stderr = self.transport.execute_command(_command)
            if exit_code != 0:
                # cannot continue
                raise gc3libs.exceptions.LRMSError(
                    "SGE backend failed executing '%s':"
                    "exit code: %d; stdout: '%s'; stderr: '%s'." %
                    (_command, exit_code, stdout, stderr))

            _command = ("%s -F -U %s" % (self._qstat, self._username))
            log.debug("Running `%s`...", _command)
            exit_code, qstat_F_stdout, stderr = self.transport.execute_command(_command)
            if exit_code != 0:
                # cannot continue
                raise gc3libs.exceptions.LRMSError(
                    "SGE backend failed executing '%s':"
                    "exit code: %d; stdout: '%s'; stderr: '%s'." %
                    (_command, exit_code, stdout, stderr))

            (total_running, self.queued,
             self.user_run, self.user_queued) = count_jobs(qstat_stdout, self._username)
            slots = compute_nr_of_slots(qstat_F_stdout)
            self.free_slots = int(slots['global']['available'])
            self.used_quota = -1

            log.info("Updated resource '%s' status:"
                     " free slots: %d,"
                     " own running jobs: %d,"
                     " own queued jobs: %d,"
                     " total queued jobs: %d",
                     self.name,
                     self.free_slots,
                     self.user_run,
                     self.user_queued,
                     self.queued,
                     )
            return self

        except Exception, ex:
            log.error("Error querying remote LRMS, see debug log for details.")
            log.debug("Error querying LRMS: %s: %s",
                      ex.__class__.__name__, str(ex))
            raise

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="sge",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
