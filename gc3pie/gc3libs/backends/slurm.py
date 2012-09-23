#! /usr/bin/env python
#
"""
Job control on SLURM clusters (possibly connecting to the front-end via SSH).
"""
# Copyright (C) 2012 GC3, University of Zurich. All rights reserved.
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


import datetime
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
import gc3libs.backends.batch as batch
import gc3libs.exceptions
from gc3libs.quantity import Memory, kB, MB, GB, Duration, seconds, minutes, hours
import gc3libs.backends.transport as transport
import gc3libs.utils as utils # first, to_bytes
from gc3libs.utils import same_docstring_as


# environmental variables:
#        SLURM_TIME_FORMAT   Specify the format used to report time stamps. A value of standard, the default value, generates  output  in  the  form  "year-month-dateThour:minute:second".

# stat cmd: squeue --noheader --format='%i|%T|%r|%R'  -j jobid1,jobid2,...
#   %i: job id
#   %T: Job  state,  extended  form: PENDING, RUNNING, SUSPENDED, CANCELLED, COMPLETING, COMPLETED, CONFIGURING, FAILED, TIMEOUT, PREEMPTED, and NODE_FAIL.
#   %R: For pending jobs: the reason a job is waiting for execution is printed within parenthesis. For terminated jobs with failure: an  explanation  as to why the job failed is printed within parenthesis.  For all other job states: the list of allocate nodes.
#   %r: reason a job is in its current state


## data for parsing SLURM commands output

# regexps for extracting relevant strings

# `sbatch` examples:
#
# $ sbatch -N4 myscript
# salloc: Granted job allocation 65537
#
# $ sbatch -N4 <<EOF
# > #!/bin/sh
# > srun hostname |sort
# > EOF
# sbatch: Submitted batch job 65541
#
_sbatch_jobid_re = re.compile(
    r'(sbatch:\s*)?(Granted job allocation|Submitted batch job) (?P<jobid>\d+)')

# `squeue` examples:
#
#    $ squeue --noheader --format='%i|%T|%r|%R' -j 2,3
#    2|PENDING|Resources|(Resources)
#    3|PENDING|Resources|(Resources)
#


## code

def count_jobs(squeue_output, whoami):
    """
    Parse SLURM's ``squeue`` output and return a quadruple `(R, Q, r,
    q)` where:

      * `R` is the total number of running jobs (from any user);
      * `Q` is the total number of queued jobs (from any user);
      * `r` is the number of running jobs submitted by user `whoami`;
      * `q` is the number of queued jobs submitted by user `whoami`

    The `squeue_output` must contain the results of an invocation of
    ``squeue --noheader --format='%i:%T:%u:%U:%r:%R'``.
    """
    total_running = 0
    total_queued = 0
    own_running = 0
    own_queued = 0
    for line in squeue_output.split('\n'):
        if line == '':
            continue
        # the choice of format string makes it easy to parse squeue output
        jobid, state, username, uid, reason, nodelist = line.split(':')
        if state in ['RUNNING', 'COMPLETING']:
            total_running += 1
            if username == whoami:
                own_running += 1
        # XXX: State CONFIGURING is described in the squeue(1) man
        # page as "Job has been allocated resources, but are waiting
        # for them to become ready for use (e.g. booting).".  Should
        # it be classified as "running" instead?
        elif state in ['PENDING', 'CONFIGURING']:
            total_queued += 1
            if username == whoami:
                own_queued += 1
    return (total_running, total_queued, own_running, own_queued)


class SlurmLrms(batch.BatchSystem):
    """
    Job control on SLURM clusters (possibly by connecting via SSH to a submit node).
    """

    _batchsys_name = 'SLURM'

    def __init__(self, name,
                 # this are inherited from the base LRMS class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime,
                 auth, # ignored if `transport` is 'local'
                 # these are inherited from `BatchSystem`
                 frontend, transport,
                 accounting_delay = 15,
                 # these are specific to this backend
                 **extra_args):

        # init base class
        batch.BatchSystem.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth,
            frontend, transport, accounting_delay, **extra_args)

        # backend-specific setup
        self.sbatch = self._get_command_argv('sbatch')

        # SLURM commands
        self._scancel = self._get_command('scancel')
        self._squeue = self._get_command('squeue')
        self._sacct = self._get_command('sacct')


    def _parse_submit_output(self, output):
        return self.get_jobid_from_submit_output(output, _sbatch_jobid_re)

    # submit cmd: sbatch --job-name="jobname" --mem-per-cpu="MBs" --input="filename" --output="filename" --no-requeue -n "number of slots" --cpus-per-task=1 --time="minutes" script.sh
    #
    # You can only submit scripts with `sbatch`, attempts to directly run a command fail with this error message:
    #
    #    $ sbatch sleep 180
    #    sbatch: error: This does not look like a batch script.  The first
    #    sbatch: error: line must start with #! followed by the path to an interpreter.
    #    sbatch: error: For instance: #!/bin/sh
    #
    # If the server is not running or unreachable, the following error is displayed:
    #
    #    $ sbatch -n 1 --no-requeue /tmp/sleep.sh
    #    sbatch: error: Batch job submission failed: Unable to contact slurm controller (connect failure)
    #
    # Note: SLURM has a flexible way of assigning CPUs/cores/etc. to a job; here we treat all execution slots as equal and map 1 task to 1 core/thread.
    #
    # Acceptable time formats include "minutes",  "minutes:seconds", "hours:minutes:seconds", "days-hours", "days-hours:minutes" and "days-hours:minutes:seconds".
    #
    def _submit_command(self, app):
        sbatch_argv, app_argv = app.sbatch(self)
        return (str.join(' ', sbatch_argv), str.join(' ', app_argv))


    # stat cmd: squeue --noheader --format='%i|%T|%u|%U|%r|%R'  -j jobid1,jobid2,...
    #   %i: job id
    #   %T: Job  state,  extended  form: PENDING, RUNNING, SUSPENDED, CANCELLED, COMPLETING, COMPLETED, CONFIGURING, FAILED, TIMEOUT, PREEMPTED, and NODE_FAIL.
    #   %R: For pending jobs: the reason a job is waiting for execution is printed within parenthesis. For terminated jobs with failure: an  explanation  as to why the job failed is printed within parenthesis.  For all other job states: the list of allocate nodes.
    #   %r: reason a job is in its current state
    #   %u: username of the submitting user
    #   %U: numeric UID of the submitting user
    #
    def _stat_command(self, job):
        return "%s --noheader -o %%i:%%T:%%r -j %s" % (self._squeue, job.lrms_jobid)

    def _parse_stat_output(self, stdout):
        """
        Receive the output of ``squeue --noheader -o %i:%T:%r and parse it.
        """
        jobstatus = dict()
        if stdout.strip() == '':
            # if stdout is empty and `squeue -j` exitcode is 0, then the job has recently completed;
            # if the job has been removed from the controllers' memory, then `squeue -j` exits with code 1
            jobstatus['state'] = Run.State.TERMINATING
        else:
            # parse stdout
            jobid, state, reason = stdout.split(':')
            log.debug("translating SLURM's state '%s' to gc3libs.Run.State" % state)
            if state in ['PENDING', 'CONFIGURING']:
                # XXX: see above for a discussion of whether 'CONFIGURING'
                # should be grouped with 'RUNNING' or not; here it's
                # likely the correct choice to group it with 'PENDING' as
                # the "configuring" phase may last a few minutes during
                # which the job is not yet really running.
                jobstatus['state'] = Run.State.SUBMITTED
            elif state in ['RUNNING', 'COMPLETING']:
                jobstatus['state'] =  Run.State.RUNNING
            elif state in ['SUSPENDED']:
                jobstatus['state'] = Run.State.STOPPED
            elif state in ['COMPLETED', 'CANCELLED', 'FAILED', 'NODE_FAIL', 'PREEMPTED', 'TIMEOUT']:
                jobstatus['state'] = Run.State.TERMINATING
            else:
                jobstatus['state'] = Run.State.UNKNOWN
        return jobstatus


    # acct cmd: sacct
    #
    # Just call it with the space-separated list of job IDs.
    #
    # If SLURM accounting is disabled (default), then `sacct` outputs an error message:
    #
    #    $ sacct 2
    #    SLURM accounting storage is disabled
    #
    def _acct_command(self, job):
        return  '%s %s' % (self._sacct, job.lrms_jobid)

    def _parse_acct_output(self, stdout):
        jobstatus = dict()
        log.warning("The SLURM backend (resource '%s') cannot yet parse the resource usage records.", self.name)
        # set all common resource usage attributes to `None`
        jobstatus['duration'] = None
        jobstatus['exitcode'] = None
        jobstatus['max_used_memory'] = None
        jobstatus['used_cpu_time'] = None
        return jobstatus


    # kill cmd: scancel
    #
    # Just call it with the space-separated list of job IDs.
    #
    # - on successful cancellation, `scancel` emits no output::
    #
    #    $ scancel 2
    #
    # - for non-existing job IDs, `scancel` outputs an error message:
    #
    #    $ scancel 15
    #    scancel: error: Kill job error on job id 15: Invalid job id specified
    #
    def _cancel_command(self, jobid):
        return ("%s %s" % (self._scancel, jobid))


    @same_docstring_as(LRMS.get_resource_status)
    @LRMS.authenticated
    def get_resource_status(self):
        self._resource.updated = False
        try:
            self.transport.connect()

            _command = ("%s --noheader -o %%i|%%T|%%u|%%U|%%r|%%R" % self._squeue)
            log.debug("Running `%s`...", _command)
            exit_code, qstat_stdout, stderr = self.transport.execute_command(_command)
            if exit_code != 0:
                # cannot continue
                raise gc3libs.exceptions.LRMSError(
                    "SLURM backend failed executing '%s':"
                    " exit code: %d; stdout: '%s', stderr: '%s'"
                    % (_command, exit_code, stdout, stderr))

            log.debug("Computing updated values for total/available slots ...")
            (total_running, self.queued,
             self.user_run, self.user_queued) = count_jobs(qstat_stdout, username)
            self.total_run = total_running
            self.free_slots = -1
            self.used_quota = -1

            log.info("Updated resource '%s' status:"
                     " free slots: %d,"
                     " total running: %d,"
                     " own running jobs: %d,"
                     " own queued jobs: %d,"
                     " total queued jobs: %d",
                     self.name,
                     self.free_slots,
                     self.total_run,
                     self.user_run,
                     self.user_queued,
                     self.queued,
                     )
            return self

        except Exception, ex:
            # self.transport.close()
            log.error("Error querying remote LRMS, see debug log for details.")
            log.debug("Error querying LRMS: %s: %s",
                      ex.__class__.__name__, str(ex), exc_info=True)
            raise


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="slurm",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
