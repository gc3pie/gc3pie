#! /usr/bin/env python
#
"""
Job control using ``libarcclient``.  (Which can submit to all
EMI-supported resources.)
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
__version__ = 'development version (SVN $Revision$)'


import sys

import itertools
import os
import time
import tempfile

import arc

from gc3libs import log, Run
from gc3libs.backends import LRMS
import gc3libs.exceptions
from gc3libs.utils import *
from gc3libs.Resource import Resource


class Arc1Lrms(LRMS):
    """
    Manage jobs through ``libarcclient``.
    """
    def __init__(self,resource, auths):
        # Normalize resource types
        assert resource.type == gc3libs.Default.ARC1_LRMS, \
            "ArcLRMS.__init__(): Failed. Resource type expected '%'. Received '%s'" \
            % (gc3libs.Default.ARC1_LRMS, resource.type)

        self._resource = resource

        self.auths = auths

        self._resource.ncores = int(self._resource.ncores)
        self._resource.max_memory_per_core = int(self._resource.max_memory_per_core) * 1000
        self._resource.max_walltime = int(self._resource.max_walltime)
        if self._resource.max_walltime > 0:
            # Convert from hours to minutes
            self._resource.max_walltime = self._resource.max_walltime * 60
            
        self._queues_cache_time = gc3libs.Default.ARC_CACHE_TIME # XXX: should it be configurable?

        # XXX: do we need a way to setup non-default UserConfig?
        self._usercfg = arc.UserConfig("", "")

        # set up libarcclient logging
        arc_rootlogger = arc.Logger_getRootLogger()
        arc_logger = arc.Logger(arc_rootlogger, self._resource.name)
        arc_logger_dest = arc.LogStream(sys.stderr) # or open(os.devnull, 'w')
        arc_rootlogger.addDestination(arc_logger_dest)
        arc_rootlogger.setThreshold(arc.DEBUG) # or .VERBOSE, .INFO, .WARNING, .ERROR
        
        self.isValid = 1

    def is_valid(self):
        return self.isValid


    @same_docstring_as(LRMS.cancel_job)
    def cancel_job(self, app):
        self.auths.get(self._resource.auth)
        controller, job = self._get_job_and_controller(app.execution.lrms_jobid)
        controller.Cancel(job)


    # ARC refreshes the InfoSys every 30 seconds by default;
    # there's no point in querying it more often than this...
    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    def _get_targets(self):
        """
        Wrapper around `arc.TargetGenerator.GetTargets()`.
        """
        tg = arc.TargetGenerator(self._usercfg, 1)
        return tg.FoundTargets()


    # ARC refreshes the InfoSys every 30 seconds by default;
    # there's no point in querying it more often than this...
    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    def _iterjobs(self):
        """
        Iterate over all jobs.
        """
        jobsuper = arc.JobSupervisor(self._usercfg, [])
        controllers = jobsuper.GetJobControllers()
        for c in controllers:
            c.GetJobInformation()
        return itertools.chain(* [c.GetJobs() for c in controllers])


    def _get_job(self, jobid):
        """Return the ARC `Job` object given its ID (connection URL)."""
        for job in self._iterjobs():
            if job.JobID.str() == jobid:
                return job
        raise IndexError("No job with ID '%s'" % jobid)


    def _get_job_and_controller(self, jobid):
        """
        Return a pair `(c, j)` where `j` is the `arc.Job` object
        corresponding to the given `jobid` and `c` is the
        corresponding `arc.JobController`.
        """
        jobsuper = arc.JobSupervisor(self._usercfg, [])
        controllers = jobsuper.GetJobControllers()
        # update job information
        for c in controllers:
            c.GetJobInformation()
        for c in controllers:
            for j in c.GetJobs():
                if j.JobID.str() == jobid:
                    # found, clean remote job sessiondir
                    return (c, j)
        raise KeyError("No job found with job ID '%s'" % jobid)


    @same_docstring_as(LRMS.submit_job)
    def submit_job(self, app):
        job = app.execution

        self.auths.get(self._resource.auth)

        # Initialize xrsl
        xrsl = app.xrsl(self._resource)
        log.debug("Application provided XRSL: %s" % xrsl)

        jd = arc.JobDescription()
        jd.Parse(xrsl)

        # perform brokering
        tg = arc.TargetGenerator(self._usercfg, 1)
        ld = arc.BrokerLoader()
        broker = ld.load("Random", self._usercfg)
        broker.PreFilterTargets(tg.GetExecutionTargets(), jd)
        
        submitted = False
        tried = 0
        j = arc.Job()
        while True:
            target = broker.GetBestTarget()
            if not target:
                break
            submitted = target.Submit(self._usercfg, jd, j)
            if not submitted:
                continue
            j.WriteJobsToFile(gc3libs.Default.ARC_JOBLIST_LOCATION, [j])
            job.lrms_jobid = j.JobID.str()
            return jd

        if not submitted and tried == 0:
            raise gc3libs.exceptions.LRMSSubmitError('No ARC targets found')


    # ARC refreshes the InfoSys every 30 seconds by default;
    # there's no point in querying it more often than this...
    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    def update_job_state(self, app):
        """
        Query the state of the ARC job associated with `app` and
        update `app.execution.state` accordingly.  Return the
        corresponding `Run.State`; see `Run.State` for more details.

        The mapping of ARC job statuses to `Run.State` is as follows: 

                    ==============  ===========
                    ARC job status  `Run.State`
                    ==============  ===========
                    ACCEPTED        SUBMITTED
                    SUBMITTING      SUBMITTED
                    PREPARING       SUBMITTED
                    QUEUING         SUBMITTED
                    RUNNING         RUNNING
                    FINISHING       RUNNING
                    FINISHED        TERMINATING
                    FAILED          TERMINATING
                    KILLED          TERMINATED
                    DELETED         TERMINATED
                    HOLD            STOPPED
                    OTHER           UNKNOWN
                    ==============  ===========

        Any other ARC job status is mapped to `Run.State.UNKNOWN`.
        """
        def map_arc_status_to_gc3job_status(status):
            if arc.JobState.ACCEPTED == status:
                return Run.State.SUBMITTED
            elif arc.JobState.PREPARING == status:
                return Run.State.SUBMITTED
            elif arc.JobState.SUBMITTING == status:
                return Run.State.SUBMITTED
            elif arc.JobState.QUEUING == status:
                return Run.State.SUBMITTED
            elif arc.JobState.RUNNING == status:
                return Run.State.RUNNING
            elif arc.JobState.FINISHING == status:
                return Run.State.RUNNING
            elif arc.JobState.FINISHED == status:
                return Run.State.TERMINATING
            elif arc.JobState.FAILED == status:
                return Run.State.TERMINATING
            elif arc.JobState.KILLED == status:
                return Run.State.TERMINATED
            elif arc.JobState.DELETED == status:
                return Run.State.TERMINATED
            elif arc.JobState.HOLD == status:
                return Run.State.STOPPED
            elif arc.JobState.OTHER == status:
                return Run.State.UNKNOWN
            elif arc.JobState.UNDEFINED == status:
                return Run.State.UNKNOWN
            else:
                raise gc3libs.exceptions.UnknownJobState(
                    "Unknown ARC job state '%s'" % status.GetGeneralState())

        self.auths.get(self._resource.auth)

        # try to intercept error conditions and translate them into
        # meaningful exceptions
        try:
            job = app.execution
            # arc_job = arclib.GetJobInfo(job.lrms_jobid)

            arc_job = self._get_job(job.lrms_jobid)
        except AttributeError, ex:
            # `job` has no `lrms_jobid`: object is invalid
            raise gc3libs.exceptions.InvalidArgument("Job object is invalid: %s"
                                                     % str(ex))
        except IndexError, ix:
            # no job found.
            # This could be caused by the InformationSystem not yet updated with the infortmation of the newly submitte job
            raise  gc3libs.exceptions.LRMSError(
                "No job found with ID: [%s]" % job.lrms_jobid)

        # update status
        state = map_arc_status_to_gc3job_status(arc_job.State)
        if arc_job.ExitCode != -1:
            job.returncode = arc_job.exitcode
        elif state in [Run.State.TERMINATING, Run.State.TERMINATING] and job.returncode is None:
            # XXX: it seems that ARC does not report the job exit code
            # (at least in some cases); let's make one up based on
            # some crude heuristics
            # if len(arc_job.Error) > 0:
            #     job.log("ARC reported error: %s" % str.join(arc_job.Error))
            #     job.returncode = (Run.Signals.RemoteError, -1)
            # # XXX: we should introduce a kind of "wrong requirements" error
            # elif (arc_job.RequestedTotalWallTime > -1 and arc_job.UsedTotalWallTime > -1
            #       and arc_job.UsedTotalWallTime > arc_job.RequestedTotalWallTime):
            #     job.log("Job exceeded requested wall-clock time (%d s),"
            #             " killed by remote batch system" 
            #             % arc_job.RequestedTotalWallTime)
            #     job.returncode = (Run.Signals.RemoteError, -1)
            # elif (arc_job.RequestedTotalCPUTime > -1 and arc_job.UsedTotalCPUTime > -1
            #       and arc_job.UsedTotalCPUTime > arc_job.RequestedTotalCPUTime):
            #     job.log("Job exceeded requested CPU time (%d s),"
            #             " killed by remote batch system" 
            #             % arc_job.RequestedTotalCPUTime)
            #     job.returncode = (Run.Signals.RemoteError, -1)
            # # note: arc_job.used_memory is in KiB (!), app.requested_memory is in GiB
            # elif (app.RequestedMainMemory > -1 and arc_job.UsedMainMemory > -1
            #       and (arc_job.UsedMainMemory / 1024) > (app.RequestedMainMemory * 1024)):
            #     job.log("Job used more memory (%d MB) than requested (%d MB),"
            #             " killed by remote batch system" 
            #             % (arc_job.UsedMainMemory / 1024, app.RequestedMainMemory * 1024))
            #     job.returncode = (Run.Signals.RemoteError, -1)
            # else:
            #     # presume everything went well...
            #     job.returncode = 0
            pass
        job.lrms_jobname = arc_job.Name

        job.stdout_filename = arc_job.StdOut
        job.stderr_filename = arc_job.StdErr

        #job.cores = arc_job.cpu_count
        job.original_exitcode = arc_job.ExitCode
        #job.used_walltime = arc_job.UsedTotalWallTime.GetPeriod() # exressed in sec.
        #job.used_cputime = arc_job.UsedTotalCPUTime.GetPeriod() # expressed in sec.
        job.used_memory = arc_job.UsedMainMemory # expressed in KiB

        # update ARC-specific info
        #job.arc_cluster = arc_job.cluster
        #job.arc_cpu_count = arc_job.cpu_count
        #job.arc_job_name = arc_job.Name
        #job.arc_queue = arc_job.Queue
        #job.arc_requested_cpu_time = arc_job.requested_cpu_time
        #job.arc_requested_wall_time = arc_job.requested_wall_time
        #job.arc_sstderr = arc_job.sstderr
        #job.arc_sstdin = arc_job.sstdin
        #job.arc_sstdout = arc_job.sstdout
        #job.arc_used_cpu_time = arc_job.used_cpu_time
        #job.arc_used_memory = arc_job.used_memory
        #job.arc_used_wall_time = arc_job.used_wall_time
        # FIXME: use Python's `datetime` types (RM)
        #if arc_job.submission_time.GetTime() > -1:
        #    job.arc_submission_time = str(arc_job.submission_time)
        #if arc_job.completion_time.GetTime() > -1:
        #    job.arc_completion_time = str(arc_job.completion_time)
        #else:
        #    job.arc_completion_time = ""

        job.state = state
        return state


    @same_docstring_as(LRMS.get_results)
    def get_results(self, app, download_dir, overwrite=False):
        # XXX: can raise encoding/decoding error if `download_dir`
        # is not ASCII, but the ARClib bindings don't accept
        # Python `unicode` strings.
        download_dir = str(download_dir)
        
        self.auths.get(self._resource.auth)
        
        job = app.execution
        c, j = self._get_job_and_controller(job.lrms_jobid)
        
        log.debug("Downloading job output into '%s' ...", download_dir)
        c.GetJob(j, download_dir, True, True)
        job.download_dir = download_dir


    @same_docstring_as(LRMS.free)
    def free(self, app):
        self.auths.get(self._resource.auth)
        controller, job = self._get_job_and_controller(app.execution.lrms_jobid)
        controller.Clean(job)


    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    def get_resource_status(self):
        # Get dynamic information out of the attached ARC subsystem 
        # (being it a single resource or a grid)
        # Fill self._resource object with dynamic information
        # return self._resource

        # dynamic information required (at least those):
        # total_queued
        # free_slots
        # user_running
        # user_queued

        self.auths.get(self._resource.auth)

        total_queued = 0
        free_slots = 0
        user_running = 0
        user_queued = 0

        targets = self._get_targets()

        def cap_to_0(n):
            if n < 0:
                return 0
            else:
                return n

        for t in targets:
            # total_queued
            total_queued += (
                cap_to_0(t.PreLRMSWaitingJobs)
                + cap_to_0(t.WaitingJobs)
                + cap_to_0(t.RunningJobs)
                + cap_to_0(t.StagingJobs)
                + cap_to_0(t.SuspendedJobs)
                )

            # free_slots
            free_slots += min(cap_to_0(t.FreeSlots),
                              cap_to_0(t.TotalSlots - t.UsedSlots))

        # user_running and user_queued
        for job in self._iterjobs():
            if job.State == arc.JobState.RUNNING:
                user_running = user_running + 1
            elif job.State == arc.JobState.QUEUING:
                user_queued = user_queued + 1

        self._resource.queued = total_queued
        self._resource.free_slots = free_slots
        self._resource.user_queued = user_queued
        self._resource.user_run = user_running
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


    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        job = app.execution

        assert job.has_key('lrms_jobid'), \
            "Missing attribute `lrms_jobid` on `Job` instance passed to `ArcLrms.peek`."

        self.auths.get(self._resource.auth)

        if size is None:
            size = sys.maxint

        # XXX: why on earth?
        # if int(offset) < 1024:
        #     offset = 0

        _remote_filename = job.lrms_jobid + '/' + remote_filename

        # get JobFTPControl handle
        jftpc = arclib.JobFTPControl()

        # download file
        log.debug("Downloading max %d bytes at offset %d of remote file '%s' into local file '%s' ..."
                  % (size, offset, remote_filename, local_file.name))

        # XXX: why this ? Because `local_file` could be a file name
        # (string) or a file-like object, as per function docstring.
        try:
           local_file_name = local_file.name
        except AttributeError:
           local_file_name = local_file

        arclib.JobFTPControl.Download(jftpc, 
                                      arclib.URL(_remote_filename), 
                                      int(offset), int(size), 
                                      local_file.name)

        log.debug("ArcLRMS.peek(): arclib.JobFTPControl.Download: completed")


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="arc",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
