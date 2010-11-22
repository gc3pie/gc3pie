#! /usr/bin/env python
#
"""
Job control on ARC0 resources.
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
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


import sys
import os
import time
import tempfile

import warnings
warnings.simplefilter("ignore")

import arclib

from gc3libs import log, Run
from gc3libs.backends import LRMS
import gc3libs.Exceptions as Exceptions
from gc3libs.utils import *
from gc3libs.Resource import Resource
import gc3libs.Default as Default


class ArcLrms(LRMS):
    """
    Manage jobs through the ARC middleware.
    """
    def __init__(self,resource, auths):
        # Normalize resource types
        if not resource.type == Default.ARC_LRMS:
            raise LRMSException("ArcLRMS.__init__(): Failed. Resource type execyted 'arc'. Received '%s'" % resource.type)

        self._resource = resource
        
        self._resource.ncores = int(self._resource.ncores)
        self._resource.max_memory_per_core = int(self._resource.max_memory_per_core) * 1000
        self._resource.max_walltime = int(self._resource.max_walltime)
        if self._resource.max_walltime > 0:
            # Convert from hours to minutes
            self._resource.max_walltime = self._resource.max_walltime * 60
            
        self._queues_cache_time = Default.CACHE_TIME # XXX: should it be configurable?

        self.isValid = 1

    def is_valid(self):
        return self.isValid


    @same_docstring_as(LRMS.cancel_job)
    def cancel_job(self, app):
        arclib.CancelJob(app.execution.lrms_jobid)


    def _get_queues(self):
        if (not hasattr(self, '_queues')) or (not hasattr(self, '_queues_last_accessed')) \
                or (time.time() - self._queues_last_updated > self._queues_cache_time):
            if self._resource.has_key('arc_ldap'):
                log.debug("Getting list of ARC resources from GIIS '%s' ...", 
                                   self._resource.arc_ldap)
                cls = arclib.GetClusterResources(arclib.URL(self._resource.arc_ldap),True,'',1)
            else:
                cls = arclib.GetClusterResources()
            log.debug('Got cluster list of length %d', len(cls))
            self._queues = arclib.GetQueueInfo(cls,arclib.MDS_FILTER_CLUSTERINFO, True, '', 5)
            log.debug('returned valid queue information for %d queues', len(self._queues))
            self._queues_last_updated = time.time()
        return self._queues

            
    @same_docstring_as(LRMS.submit_job)
    def submit_job(self, app):
        job = app.execution

        # Initialize xrsl
        xrsl = app.xrsl(self._resource)
        log.debug('Application provided XRSL: %s' % xrsl)
        try:
            # ARClib cannot handle unicode strings, so convert `xrsl` to ascii
            # XXX: should this be done in Application.xrsl() instead?
            xrsl = arclib.Xrsl(str(xrsl))
        except Exception, ex:
            raise LRMSSubmitError('Failed in getting `Xrsl` object from arclib: %s: %s'
                                  % (ex.__class__.__name__, str(ex)))

        queues = self._get_queues()
        if len(queues) == 0:
            raise LRMSSubmitError('No ARC queues found')

        targets = arclib.PerformStandardBrokering(arclib.ConstructTargets(queues, xrsl))
        if len(targets) == 0:
            raise LRMSSubmitError('No ARC targets found')

        try:
            lrms_jobid = arclib.SubmitJob(xrsl,targets)
        except arclib.JobSubmissionError:
            raise LRMSSubmitError('Got error from arclib.SubmitJob():')

        job.lrms_jobid = lrms_jobid
        return job


    @same_docstring_as(LRMS.update_job_state)
    def update_job_state(self, app):
        job = app.execution
        def map_arc_status_to_gc3job_status(status):
            try:
                return {
                    'ACCEPTED':  Run.State.SUBMITTED,
                    'SUBMITTING':Run.State.SUBMITTED,
                    'PREPARING': Run.State.SUBMITTED,
                    'INLRMS:Q':  Run.State.SUBMITTED,
                    'INLRMS:R':  Run.State.RUNNING,
                    'INLRMS:O':  Run.State.RUNNING,
                    'INLRMS:E':  Run.State.RUNNING,
                    'INLRMS:X':  Run.State.RUNNING,
                    'INLRMS:S':  Run.State.STOPPED,
                    'INLRMS:H':  Run.State.STOPPED,
                    'FINISHING': Run.State.RUNNING,
                    'EXECUTED':  Run.State.RUNNING,
                    'FINISHED':  Run.State.TERMINATED,
                    'CANCELING': Run.State.TERMINATED,
                    'FINISHED':  Run.State.TERMINATED,
                    'KILLED':    Run.State.TERMINATED,
                    'FAILED':    Run.State.TERMINATED,
                    'DELETED':   Run.State.TERMINATED,
                    }[status]
            except KeyError:
                raise UnknownJobState("Unknown ARC job state '%s'" % status)

        # Prototype from arclib
        arc_job = arclib.GetJobInfo(job.lrms_jobid)

        # update status
        state = map_arc_status_to_gc3job_status(arc_job.status)
        if arc_job.exitcode != -1:
            job.returncode = arc_job.exitcode
        job.lrms_jobname = arc_job.job_name # XXX: `lrms_jobname` is the name used in `sge.py`

        # FIXME: These should become part of mandatory Job information
        # as it's required for 'tail' function (SM)
        # -- They're already available from `Application` (RM)
        job.stdout_filename = arc_job.sstdout
        job.stderr_filename = arc_job.sstderr

        # update ARC-specific info
        job.arc_cluster = arc_job.cluster
        job.arc_cpu_count = arc_job.cpu_count
        job.arc_job_name = arc_job.job_name
        job.arc_queue = arc_job.queue
        job.arc_queue_rank = arc_job.queue_rank
        job.arc_requested_cpu_time = arc_job.requested_cpu_time
        job.arc_requested_wall_time = arc_job.requested_wall_time
        job.arc_sstderr = arc_job.sstderr
        job.arc_sstdin = arc_job.sstdin
        job.arc_sstdout = arc_job.sstdout
        job.arc_used_cpu_time = arc_job.used_cpu_time
        job.arc_used_memory = arc_job.used_memory
        job.arc_used_wall_time = arc_job.used_wall_time
        # FIXME: use Python's `datetime` types (RM)
        if arc_job.submission_time.GetTime() > -1:
            job.arc_submission_time = str(arc_job.submission_time)
        if arc_job.completion_time.GetTime() > -1:
            job.arc_completion_time = str(arc_job.completion_time)
        else:
            job.arc_completion_time = ""

        job.state = state
        return state


    @same_docstring_as(LRMS.get_results)
    def get_results(self, app, download_dir, overwrite=False):

        job = app.execution
        jftpc = arclib.JobFTPControl()

        log.debug("Downloading job output into '%s' ...", download_dir)
        try:
            arclib.JobFTPControl.DownloadDirectory(jftpc, job.lrms_jobid,download_dir)
            job.download_dir = download_dir
        except arclib.FTPControlError:
            # critical error. consider job remote data as lost
            raise DataStagingError("Failed downloading remote folder '%s'" 
                                   % job.lrms_jobid)

        return # XXX: return list of downloaded files?


    @same_docstring_as(LRMS.free)
    def free(self, app):

        job = app.execution
        jftpc = arclib.JobFTPControl()

        # Clean remote job sessiondir
        try:
            retval = arclib.JobFTPControl.Clean(jftpc,job.lrms_jobid)
        except arclib.FTPControlError:
            log.warning("Failed removing remote folder '%s'" % job.lrms_jobid)
            pass


    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        # Get dynamic information out of the attached ARC subsystem (being it a single resource or a grid)
        # Fill self._resource object with dynamic information
        # return self._resource

        # dynamic information required (at least those):
        # total_queued
        # free_slots
        # user_running
        # user_queued

        if self._resource.has_key('arc_ldap'):
            log.debug("Getting cluster list from %s ...", self._resource.arc_ldap)
            cls = arclib.GetClusterResources(arclib.URL(self._resource.arc_ldap),True,'',2)
        else:
            log.debug("Getting cluster list from ARC's default GIIS ...")
            cls = arclib.GetClusterResources()

        total_queued = 0
        free_slots = 0
        user_running = 0
        user_queued = 0

        def _normalize_value(val):
            # an ARC value may contains -1 when the subsystem cannot
            # get/resolve it we treat then these values as 0
            if val < 0:
                return 0
            else:
                return val

        for cluster in cls:
            queues =  arclib.GetQueueInfo(cluster,arclib.MDS_FILTER_CLUSTERINFO,True,"",1)
            if len(queues) == 0:
                log.error('No ARC queues found for resource %s' % str(cluster))
                continue
                # raise LRMSSubmitError('No ARC queues found')              

            list_of_jobs = arclib.GetAllJobs(cluster,True,"",1)

            for q in queues:
                q.grid_queued = _normalize_value(q.grid_queued)
                q.local_queued = _normalize_value(q.local_queued)
                q.prelrms_queued = _normalize_value(q.prelrms_queued)
                q.queued = _normalize_value(q.queued)

                q.cluster.used_cpus = _normalize_value(q.cluster.used_cpus)
                q.cluster.total_cpus = _normalize_value(q.cluster.total_cpus)

                # total_queued
                total_queued = total_queued +  q.grid_queued + q.local_queued + q.prelrms_queued + q.queued

                # free_slots
                # free_slots - free_slots + ( q.total_cpus - q.running )
                free_slots = free_slots + min((q.total_cpus - q.running),(q.cluster.total_cpus - q.cluster.used_cpus))

            # user_running and user_queued
            for job in list_of_jobs:
                if 'INLRMS:R' in job.status:
                    user_running = user_running + 1
                elif 'INLRMS:Q' in job.status:
                    user_queued = user_queued + 1


        # update self._resource with:
        # int queued
        # int running
        # int user_queued
        # int user_run
        # int used_quota = -1

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

        if size is None:
            size = sys.maxint

        # XXX: why on earth?
        if int(offset) < 1024:
            offset = 0

        _remote_filename = job.lrms_jobid + '/' + remote_filename

        # get JobFTPControl handle
        jftpc = arclib.JobFTPControl()

        # download file
        log.debug("Downloading %d bytes at offset %d of remote file '%s' into local file '%s' ..."
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

        log.info('status arclib.JobFTPControl.Download [ completed ]')


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="arc",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
