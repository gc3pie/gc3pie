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
__version__ = '2.0.0-rc4 version (SVN $Revision$)'


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


    # acct cmd: sacct --noheader --parsable --format jobid,ncpus,cputimeraw,elapsed,submit,eligible,reserved,start,end,exitcode,maxrss,maxvmsize,totalcpu -j JOBID
    #
    # where:
    #   * jobid:      Job ID
    #   * alloccpus:  Count of allocated processors
    #   * cputimeraw: CPU time in seconds
    #   * elapsed:    Job duration, as [DD-][hh:]mm:ss
    #   * submit:     Time the job was submitted, as MM/DD-hh:mm:ss (timestamp in UTC) or ISO8601 format depending on compilation option
    #   * eligible:   When the job became eligible to run
    #   * reserved:   Difference between `start` and `eligible`
    #   * start:      Job start time, as MM/DD-hh:mm:ss (timestamp in UTC) or ISO8601 format depending on compilation option
    #   * end:        Termination time of the job, as MM/DD-hh:mm:ss (timestamp in UTC) or ISO8601 format depending on compilation option
    #   * exitcode:   exit code, ':', killing signal number
    #   * maxrss:     The maximum RSS across all tasks
    #   * maxvmsize:  The maximum virtual memory used across all tasks
    #   * totalcpu:   Total CPU time used by the job (does not include child processes, if any)
    #
    # Examples:
    #
    #    $ sudo sacct --noheader --parsable --format jobid,ncpus,cputimeraw,elapsed,submit,eligible,reserved,start,end,exitcode,maxrss,maxvmsize,totalcpu -j 14
    #    14|1|10|00:00:10|2012-09-23T23:38:41|2012-09-23T23:38:41|49710-06:28:06|2012-09-23T23:38:31|2012-09-23T23:38:41|0:0|||00:00:00|
    #
    #    $ env SLURM_TIME_FORMAT=standard sacct -S 0901 --noheader --parsable --format jobid,ncpus,cputimeraw,elapsed,submit,eligible,reserved,start,end,exitcode,maxrss,maxvmsize,totalcpu
    #    449002|32|128|00:00:04|2012-09-04T11:09:26|2012-09-04T11:09:26|00:00:12|2012-09-04T11:09:38|2012-09-04T11:09:42|0:0|||00:00:00|
    #    449018|64|1472|00:00:23|2012-09-04T11:18:06|2012-09-04T11:18:06|00:00:18|2012-09-04T11:18:24|2012-09-04T11:18:47|0:0|||00:01.452|
    #    449018.batch|1|23|00:00:23|2012-09-04T11:18:24|2012-09-04T11:18:24|00:00:-02|2012-09-04T11:18:24|2012-09-04T11:18:47|0:0|7884K|49184K|00:01.452|
    #    449057|3200|659200|00:03:26|2012-09-04T11:19:45|2012-09-04T11:19:45|00:00:12|2012-09-04T11:19:57|2012-09-04T11:23:23|0:0|||00:01.620|
    #    449057.batch|1|206|00:03:26|2012-09-04T11:19:57|2012-09-04T11:19:57|00:00:-02|2012-09-04T11:19:57|2012-09-04T11:23:23|0:0|7896K|49184K|00:01.620|
    #    449217|3200|0|00:00:00|2012-09-04T11:42:58|2012-09-04T11:42:58|00:00:05|2012-09-04T11:43:03|2012-09-04T11:43:03|0:0|||00:00.720|
    #    449217.batch|1|272|00:04:32|2012-09-03T17:22:53|2012-09-03T17:22:53|00:00:-02|2012-09-03T17:22:53|2012-09-03T17:27:25|0:0|7484K|47820K|00:00.720|
    #    449218|3200|790400|00:04:07|2012-09-04T11:43:09|2012-09-04T11:43:09|00:25:34|2012-09-04T12:08:43|2012-09-04T12:12:50|0:0|||00:01.620|
    #    449218.batch|1|67774|18:49:34|2012-09-03T17:23:16|2012-09-03T17:23:16|00:00:-02|2012-09-03T17:23:16|2012-09-04T12:12:50|0:0|10448K|71172K|00:01.620|
    #    450069|0|0|00:00:00|2012-09-05T19:03:56|2012-09-05T19:03:56|00:00:00|2012-09-05T19:03:56|2012-09-05T19:03:56|0:1|||00:00:00|
    #    450070|0|0|00:00:00|2012-09-05T19:04:01|2012-09-05T19:04:01|00:00:00|2012-09-05T19:04:01|2012-09-05T19:04:01|0:1|||00:00:00|
    #    450085|32|0|00:00:00|2012-09-05T19:06:31|2012-09-05T19:06:31|00:00:01|2012-09-05T19:06:32|2012-09-05T19:06:32|0:0|||00:00:00|
    #    450086|32|0|00:00:00|2012-09-05T19:06:34|2012-09-05T19:06:34|00:00:01|2012-09-05T19:06:35|2012-09-05T19:06:35|0:0|||00:00:00|
    #    450087|32|64|00:00:02|2012-09-05T19:06:36|2012-09-05T19:06:36|00:00:03|2012-09-05T19:06:39|2012-09-05T19:06:41|0:0|||00:00:00|
    #    450089|44800|0|00:00:00|2012-09-05T19:06:46|2012-09-05T19:06:46|00:00:10|2012-09-05T19:06:56|2012-09-05T19:06:56|0:0|||00:00:00|
    #    450090|44800|313600|00:00:07|2012-09-05T19:07:01|2012-09-05T19:07:01|00:00:07|2012-09-05T19:07:08|2012-09-05T19:07:15|0:0|||00:00:00|
    #    450133|32|2176|00:01:08|2012-09-05T19:12:57|2012-09-05T19:12:57|00:00:00|2012-09-05T19:12:57|2012-09-05T19:14:05|0:0|||00:00.324|
    #    450133.batch|1|68|00:01:08|2012-09-05T19:12:57|2012-09-05T19:12:57|00:00:-02|2012-09-05T19:12:57|2012-09-05T19:14:05|0:0|7768K|49136K|00:00.324|
    #    450134|47872|0|00:00:00|2012-09-05T19:15:14|2012-09-05T19:15:14|00:01:19|2012-09-05T19:16:33|2012-09-05T19:16:33|0:0|||00:00:00|
    #    450135|47584|0|00:00:00|2012-09-05T19:16:54|2012-09-05T19:16:54|01:07:08|2012-09-05T20:24:02|2012-09-05T20:24:02|0:0|||00:00:00|
    #    450136|47584|0|00:00:00|2012-09-05T19:17:18|2012-09-05T19:17:18|00:28:29|2012-09-05T19:45:47|2012-09-05T19:45:47|0:0|||00:00:00|
    #    450139|9600|3945600|00:06:51|2012-09-05T19:28:53|2012-09-05T19:28:53|00:00:28|2012-09-05T19:29:21|2012-09-05T19:36:12|0:0|||00:00:00|
    #    450141|640|33920|00:00:53|2012-09-05T19:36:30|2012-09-05T19:36:30|00:00:05|2012-09-05T19:36:35|2012-09-05T19:37:28|0:0|||00:00:00|
    #    450142|0|0|00:00:00|2012-09-05T19:45:41|2012-09-05T19:45:41|00:29:58|2012-09-05T20:15:39|2012-09-05T20:15:39|0:0|||00:00:00|
    #    450183|3200|748800|00:03:54|2012-09-05T20:01:01|2012-09-05T20:01:01|00:00:22|2012-09-05T20:01:23|2012-09-05T20:05:17|0:0|||00:01.628|
    #    450183.batch|1|234|00:03:54|2012-09-05T20:01:23|2012-09-05T20:01:23|00:00:-02|2012-09-05T20:01:23|2012-09-05T20:05:17|0:0|7904K|49184K|00:01.628|
    #    450184|6400|3001600|00:07:49|2012-09-05T20:06:22|2012-09-05T20:06:22|00:00:11|2012-09-05T20:06:33|2012-09-05T20:14:22|0:0|||00:01.828|
    #    450184.batch|1|469|00:07:49|2012-09-05T20:06:33|2012-09-05T20:06:33|00:00:-02|2012-09-05T20:06:33|2012-09-05T20:14:22|0:0|8940K|62988K|00:01.828|
    #    450185|9600|2937600|00:05:06|2012-09-05T20:06:47|2012-09-05T20:06:47|00:06:28|2012-09-05T20:13:15|2012-09-05T20:18:21|0:0|||00:02.004|
    #    450185.batch|1|306|00:05:06|2012-09-05T20:13:15|2012-09-05T20:13:15|00:00:-02|2012-09-05T20:13:15|2012-09-05T20:18:21|0:0|7904K|49184K|00:02.004|
    #    450186|12800|0|00:00:00|2012-09-05T20:06:57|2012-09-05T20:06:57|00:07:54|2012-09-05T20:14:51|2012-09-05T20:14:51|0:0|||00:00:00|
    #    450187|0|0|00:00:00|2012-09-05T20:19:18|2012-09-05T20:19:18|00:02:04|2012-09-05T20:21:22|2012-09-05T20:21:22|0:0|||00:00:00|
    #    450188|0|0|00:00:00|2012-09-05T20:22:30|2012-09-05T20:22:30|00:00:10|2012-09-05T20:22:40|2012-09-05T20:22:40|0:0|||00:00:00|
    #    459281|32|64|00:00:02|2012-09-12T09:16:46|2012-09-12T09:16:46|00:00:00|2012-09-12T09:16:46|2012-09-12T09:16:48|0:0|||00:00:00|
    #    459307|32|2144|00:01:07|2012-09-12T09:23:45|2012-09-12T09:23:45|00:00:01|2012-09-12T09:23:46|2012-09-12T09:24:53|0:0|||00:00.320|
    #    459307.batch|1|67|00:01:07|2012-09-12T09:23:46|2012-09-12T09:23:46|00:00:-02|2012-09-12T09:23:46|2012-09-12T09:24:53|0:0|7740K|49020K|00:00.320|
    #    459308|27264|0|00:00:00|2012-09-12T09:30:32|2012-09-12T09:30:32|00:03:05|2012-09-12T09:33:37|2012-09-12T09:33:37|0:0|||00:00:00|
    #    459309|25600|0|00:00:00|2012-09-12T09:32:07|2012-09-12T09:32:07|00:01:30|2012-09-12T09:33:37|2012-09-12T09:33:37|0:0|||00:00:00|
    #    459310|25600|2278400|00:01:29|2012-09-12T09:33:45|2012-09-12T09:33:45|00:01:36|2012-09-12T09:35:21|2012-09-12T09:36:50|0:0|||00:01.208|
    #    459310.batch|1|89|00:01:29|2012-09-12T09:35:21|2012-09-12T09:35:21|00:00:-02|2012-09-12T09:35:21|2012-09-12T09:36:50|0:0|8748K|50232K|00:01.208|
    #    459311|47744|4487936|00:01:34|2012-09-12T09:38:18|2012-09-12T09:38:18|00:05:27|2012-09-12T09:43:45|2012-09-12T09:45:19|0:0|||00:01.964|
    #    459311.batch|1|94|00:01:34|2012-09-12T09:43:45|2012-09-12T09:43:45|00:00:-02|2012-09-12T09:43:45|2012-09-12T09:45:19|0:0|9.50M|51088K|00:01.964|
    #    459312|47744|4487936|00:01:34|2012-09-12T09:54:52|2012-09-12T09:54:52|00:00:01|2012-09-12T09:54:53|2012-09-12T09:56:27|0:0|||00:01.964|
    #    459312.batch|1|94|00:01:34|2012-09-12T09:54:53|2012-09-12T09:54:53|00:00:-02|2012-09-12T09:54:53|2012-09-12T09:56:27|0:0|9.50M|51088K|00:01.964|
    #    459549|32|0|00:00:00|2012-09-12T11:58:14|2012-09-12T11:58:14|00:00:10|2012-09-12T11:58:24|2012-09-12T11:58:24|0:0|||00:00:00|
    #    459550|64|192|00:00:03|2012-09-12T11:58:27|2012-09-12T11:58:27|00:00:25|2012-09-12T11:58:52|2012-09-12T11:58:55|0:0|||00:00:00|
    #    459566|32|96|00:00:03|2012-09-12T12:01:58|2012-09-12T12:01:58|00:00:01|2012-09-12T12:01:59|2012-09-12T12:02:02|0:0|||00:00.064|
    #    459566.batch|1|3|00:00:03|2012-09-12T12:01:59|2012-09-12T12:01:59|00:00:-02|2012-09-12T12:01:59|2012-09-12T12:02:02|0:0|11468K|163664K|00:00.064|
    #    459569|32|2336|00:01:13|2012-09-12T12:02:26|2012-09-12T12:02:26|00:00:01|2012-09-12T12:02:27|2012-09-12T12:03:40|0:1|||00:00.004|
    #    459569.batch|1|73|00:01:13|2012-09-12T12:02:27|2012-09-12T12:02:27|00:00:-02|2012-09-12T12:02:27|2012-09-12T12:03:40|0:15|27664K|234196K|00:00.004|
    #    459574|32|32|00:00:01|2012-09-12T12:04:53|2012-09-12T12:04:53|00:00:00|2012-09-12T12:04:53|2012-09-12T12:04:54|0:0|||00:00.232|
    #    459574.batch|1|1|00:00:01|2012-09-12T12:04:53|2012-09-12T12:04:53|00:00:-02|2012-09-12T12:04:53|2012-09-12T12:04:54|0:0|0|0|00:00.232|
    #    459578|32|320|00:00:10|2012-09-12T12:06:53|2012-09-12T12:06:53|00:00:00|2012-09-12T12:06:53|2012-09-12T12:07:03|0:0|||00:00.004|
    #    459578.batch|1|11|00:00:11|2012-09-12T12:06:53|2012-09-12T12:06:53|00:00:-02|2012-09-12T12:06:53|2012-09-12T12:07:04|0:15|28740K|174496K|00:00.004|
    #        #
    # Warning: the `SLURM_TIME_FORMAT` environment variable influences how the times are reported,
    # so it should always be set to `standard` to get ISO8601 reporting.  See below for an example
    # of non-standard (SLURM_TIME_FORMAT=relative) report:
    #
    #    $ sacct --noheader --parsable --format jobid,ncpus,cputimeraw,elapsed,submit,eligible,reserved,start,end,exitcode,maxrss,maxvmsize,totalcpu -S 0901
    #    449002|32|128|00:00:04|4 Sep 11:09|4 Sep 11:09|00:00:12|4 Sep 11:09|4 Sep 11:09|0:0|||00:00:00|
    #    449018|64|1472|00:00:23|4 Sep 11:18|4 Sep 11:18|00:00:18|4 Sep 11:18|4 Sep 11:18|0:0|||00:01.452|
    #    449018.batch|1|23|00:00:23|4 Sep 11:18|4 Sep 11:18|00:00:-02|4 Sep 11:18|4 Sep 11:18|0:0|7884K|49184K|00:01.452|
    #    449057|3200|659200|00:03:26|4 Sep 11:19|4 Sep 11:19|00:00:12|4 Sep 11:19|4 Sep 11:23|0:0|||00:01.620|
    #    449057.batch|1|206|00:03:26|4 Sep 11:19|4 Sep 11:19|00:00:-02|4 Sep 11:19|4 Sep 11:23|0:0|7896K|49184K|00:01.620|
    #    449217|3200|0|00:00:00|4 Sep 11:42|4 Sep 11:42|00:00:05|4 Sep 11:43|4 Sep 11:43|0:0|||00:00.720|
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
        self.updated = False
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
