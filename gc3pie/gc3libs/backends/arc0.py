#! /usr/bin/env python
#
"""
Job control on ARC0 resources.
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
import os
import time
import tempfile

import warnings
warnings.simplefilter("ignore")

# NG's default packages install arclib into /opt/nordugrid/lib/pythonX.Y/site-packages;
# add this anyway in case users did not set their PYTHONPATH correctly
import sys
sys.path.append('/opt/nordugrid/lib/python%d.%d/site-packages' 
                % sys.version_info[:2])
# this is where arc0 libraries are installed from release 11.05
sys.path.append('/usr/lib/pymodules/python%d.%d/'
                % sys.version_info[:2])

import arclib
from gc3libs import log, Run
from gc3libs.backends import LRMS
import gc3libs.exceptions
from gc3libs.utils import *
from gc3libs.Resource import Resource


class ArcLrms(LRMS):
    """
    Manage jobs through the ARC middleware.
    """
    def __init__(self,resource, auths):
        # Normalize resource types
        assert resource.type == gc3libs.Default.ARC0_LRMS, \
            "ArcLRMS.__init__(): Failed. Resource type expected '%s'. Received '%s'" \
            % (gc3libs.Default.ARC0_LRMS, resource.type)

        self._resource = resource

        self.auths = auths

        self._resource.ncores = int(resource.ncores)
        self._resource.max_memory_per_core = int(resource.max_memory_per_core) * 1000
        self._resource.max_walltime = int(resource.max_walltime)
        if self._resource.max_walltime > 0:
            # Convert from hours to minutes
            self._resource.max_walltime = self._resource.max_walltime * 60
            
        if hasattr(resource, 'lost_job_timeout'):
            self._resource.lost_job_timeout = int(resource.lost_job_timeout)
        else:
            self._resource.lost_job_timeout = gc3libs.Default.ARC_LOST_JOB_TIMEOUT

        arcnotifier = arclib.Notify_getNotifier()
        arcnotifier.SetOutStream(arcnotifier.GetNullStream())
        # DEBUG: uncomment the following to print all ARC messages
        #arcnotifier.SetOutStream(arcnotifier.GetOutStream())
        #arcnotifier.SetNotifyLevel(arclib.VERBOSE)
        #arcnotifier.SetNotifyTimeStamp(True)

        self.isValid = 1

    def is_valid(self):
        return self.isValid


    @same_docstring_as(LRMS.cancel_job)
    def cancel_job(self, app):
        self.auths.get(self._resource.auth)
        try:
            arclib.CancelJob(app.execution.lrms_jobid)
        except Exception, ex:
            gc3libs.log.error('Failed while killing job. Error type %s, message %s' % (ex.__class__,str(ex)))
            raise gc3libs.exceptions.LRMSError('Failed while killing job. Error type %s, message %s' % (ex.__class__,str(ex)))


    # ARC refreshes the InfoSys every 30 seconds by default;
    # there's no point in querying it more often than this...
    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    def _get_clusters(self):
        """
        Wrapper around `arclib.GetClusterResources()`.  Query the ARC
        LDAP (at the address specified by the resource's ``arc_ldap``
        attribute, or the default GIIS) and return the corresponding
        `arclib.Cluster` object.
        """
        if self._resource.has_key('arc_ldap'):
            log.info("Updating ARC resource information from '%s'"
                      % self._resource.arc_ldap)
            return arclib.GetClusterResources(arclib.URL(self._resource.arc_ldap),
                                              True, '', 1)
        else:
            log.info("Updating ARC resource information from default GIIS")
            return arclib.GetClusterResources()


    # ARC refreshes the InfoSys every 30 seconds by default;
    # there's no point in querying it more often than this...
    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    def _get_jobs(self):
        """
        Wrapper around `arclib.GetAllJobs()`. Retrieve Jobs information from a
        given resource. Jobs are stored into a dictionary using 
        job.lrms_jobid as index.
        This is supposed to speedup the access to a given job object in the
        update_job_state() method.
        """
        jobs = {}
        clusters = self._get_clusters()
        log.debug('Arc0LRMS._get_clusters() returned %d cluster resources.' % len(clusters))
        job_list = arclib.GetAllJobs(clusters, True, '', 3)
        log.info("Updating list of jobs belonging to resource '%s': got %d jobs."
                 % (self._resource.name, len(job_list)))
        for job in job_list:
            jobs[job.id] = job
        return jobs

    # ARC refreshes the InfoSys every 30 seconds by default;
    # there's no point in querying it more often than this...
    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    def _get_queues(self):
        clusters = self._get_clusters()
        log.debug('_get_clusters returned [%d] cluster resources' % len(clusters))
        if not clusters:
            # empty list of clusters. Not following back to system GIIS configuration
            # returning empty list
            return clusters
        log.info("Updating resource Queues information")
        return arclib.GetQueueInfo(clusters, arclib.MDS_FILTER_CLUSTERINFO,
                                   True, '', 5)

            
    @same_docstring_as(LRMS.submit_job)
    def submit_job(self, app):
        job = app.execution

        self.auths.get(self._resource.auth)

        # Initialize xrsl
        xrsl = app.xrsl(self._resource)
        log.debug("Application provided XRSL: %s" % xrsl)
        try:
            xrsl = arclib.Xrsl(xrsl)
        except Exception, ex:
            raise gc3libs.exceptions.LRMSSubmitError('Failed in getting `Xrsl` object from arclib: %s: %s'
                                  % (ex.__class__.__name__, str(ex)))

        queues = self._get_queues()
        if len(queues) == 0:
            raise gc3libs.exceptions.LRMSSubmitError('No ARC queues found')

        targets = arclib.PerformStandardBrokering(arclib.ConstructTargets(queues, xrsl))
        if len(targets) == 0:
            raise gc3libs.exceptions.LRMSSubmitError('No ARC targets found')

        try:
            lrms_jobid = arclib.SubmitJob(xrsl,targets)
        except arclib.JobSubmissionError, ex:
            raise gc3libs.exceptions.LRMSSubmitError('Got error from arclib.SubmitJob(): %s' % str(ex))

        # save job ID for future reference
        job.lrms_jobid = lrms_jobid
        # state is known at this point, so mark this as a successful update
        job._arc0_state_last_checked = time.time()
        return job


    @staticmethod
    def _map_arc0_status_to_gc3pie_state(status):
        """
        Return the GC3Pie state corresponding to the given ARC status.

        See `update_job_state`:meth: for a complete table of the
        correspondence.

        :param str status: ARC0 job status.

        :raise gc3libs.exceptions.UnknownJobState: If there is no
        mapping of `status` to a GC3Pie state.
        """
        try:
            return {
                'ACCEPTED':  Run.State.SUBMITTED,
                'ACCEPTING': Run.State.SUBMITTED,
                'SUBMIT':    Run.State.SUBMITTED,
                'SUBMITTING':Run.State.SUBMITTED,
                'PREPARING': Run.State.SUBMITTED,
                'PREPARED':  Run.State.SUBMITTED,
                'INLRMS:Q':  Run.State.SUBMITTED,
                'INLRMS:R':  Run.State.RUNNING,
                'INLRMS:O':  Run.State.RUNNING,
                'INLRMS:E':  Run.State.RUNNING,
                'INLRMS:X':  Run.State.RUNNING,
                'INLRMS:S':  Run.State.STOPPED,
                'INLRMS:H':  Run.State.STOPPED,
                'FINISHING': Run.State.RUNNING,
                'EXECUTED':  Run.State.RUNNING,
                'FINISHED':  Run.State.TERMINATING,
                'CANCELING': Run.State.TERMINATING,
                'KILLING':   Run.State.TERMINATING,
                'FINISHED':  Run.State.TERMINATING,
                'FAILED':    Run.State.TERMINATING,
                'KILLED':    Run.State.TERMINATED,
                'DELETED':   Run.State.TERMINATED,
            }[status]
        except KeyError:
            raise gc3libs.exceptions.UnknownJobState("Unknown ARC0 job state '%s'" % status)


    # ARC refreshes the InfoSys every 30 seconds by default;
    # there's no point in querying it more often than this...
    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    def update_job_state(self, app):
        """
        Query the state of the ARC0 job associated with `app` and
        update `app.execution.state` accordingly.  Return the
        corresponding `Run.State`; see `Run.State` for more details.

        The mapping of ARC0 job statuses to `Run.State` is as follows: 

                    ==============  ===========
                    ARC job status  `Run.State`
                    ==============  ===========
                    ACCEPTED        SUBMITTED
                    ACCEPTING       SUBMITTED
                    SUBMITTING      SUBMITTED
                    PREPARING       SUBMITTED
                    PREPARED        SUBMITTED
                    INLRMS:Q        SUBMITTED
                    INLRMS:R        RUNNING
                    INLRMS:O        RUNNING
                    INLRMS:E        RUNNING
                    INLRMS:X        RUNNING
                    INLRMS:S        STOPPED
                    INLRMS:H        STOPPED
                    FINISHING       RUNNING
                    EXECUTED        RUNNING
                    FINISHED        TERMINATING
                    CANCELING       TERMINATING
                    FINISHED        TERMINATING
                    FAILED          TERMINATING
                    KILLED          TERMINATED
                    DELETED         TERMINATED
                    ==============  ===========

        Any other ARC job status is mapped to `Run.State.UNKNOWN`.  In
        particular, querying a job ID that is not found in the ARC
        information system will result in `UNKNOWN` state, as will
        querying a job that has just been submitted and has not yet
        found its way to the infosys.
        """
        job = app.execution
        self.auths.get(self._resource.auth)

        # try to intercept error conditions and translate them into
        # meaningful exceptions
        try:
            arc_jobs_info = self._get_jobs()
            arc_job = arc_jobs_info[job.lrms_jobid]
        except AttributeError, ex:
            # `job` has no `lrms_jobid`: object is invalid
            raise gc3libs.exceptions.InvalidArgument(
                "Job object is invalid: %s" % str(ex))
        except KeyError, ex:
            # No job found.  This could be caused by the
            # information system not yet updated with the information
            # of the newly submitted job.
            if not hasattr(job, 'state_last_changed'):
                # XXX: compatibility with running sessions, remove before release
                job.state_last_changed = time.time()
            if not hasattr(job, '_arc0_state_last_checked'):
                # XXX: compatibility with running sessions, remove before release
                job._arc0_state_last_checked = time.time()
            now = time.time()
            if (job.state == Run.State.SUBMITTED
                and now - job.state_last_changed < self._resource.lost_job_timeout):
                # assume the job was recently submitted, hence the
                # information system knows nothing about it; just
                # ignore the error and return the object unchanged
                return job.state
            elif (job.state in [ Run.State.SUBMITTED, Run.State.RUNNING ]
                  and now - job._arc0_state_last_checked < self._resource.lost_job_timeout):
                # assume transient information system failure;
                # ignore the error and return object unchanged
                return job.state
            else:
                raise  gc3libs.exceptions.UnknownJob(
                    "No job found corresponding to the ID '%s'" % job.lrms_jobid)
        job._arc0_state_last_checked = time.time()
        
        # update status
        state = self._map_arc0_status_to_gc3pie_state(arc_job.status)
        if arc_job.exitcode != -1:
            job.exitcode = arc_job.exitcode

        

        elif state in [Run.State.TERMINATING, Run.State.TERMINATED] and job.returncode is None:
            # XXX: it seems that ARC does not report the job exit code
            # (at least in some cases); let's make one up based on
            # some crude heuristics
            if arc_job.errors != '':
                job.log("ARC reported error: %s" % arc_job.errors)
                job.returncode = (Run.Signals.RemoteError, -1)
            # FIXME: we should introduce a kind of "wrong requirements" error
            elif (arc_job.requested_wall_time is not None
                  and arc_job.requested_wall_time != -1 
                  and arc_job.used_wall_time != -1 
                  and arc_job.used_wall_time > arc_job.requested_wall_time):
                job.log("Job exceeded requested wall-clock time (%d s),"
                        " killed by remote batch system" 
                        % arc_job.requested_wall_time)
                job.returncode = (Run.Signals.RemoteError, -1)
            elif (arc_job.requested_cpu_time is not None
                  and arc_job.requested_cpu_time != -1 
                  and arc_job.used_cpu_time != -1 
                  and arc_job.used_cpu_time > arc_job.requested_cpu_time):
                job.log("Job exceeded requested CPU time (%d s),"
                        " killed by remote batch system" 
                        % arc_job.requested_cpu_time)
                job.returncode = (Run.Signals.RemoteError, -1)
            # note: arc_job.used_memory is in KiB (!), app.requested_memory is in GiB
            elif (app.requested_memory is not None 
                  and app.requested_memory != -1 and arc_job.used_memory != -1 
                  and (arc_job.used_memory / 1024) > (app.requested_memory * 1024)):
                job.log("Job used more memory (%d MB) than requested (%d MB),"
                        " killed by remote batch system" 
                        % (arc_job.used_memory / 1024, app.requested_memory * 1024))
                job.returncode = (Run.Signals.RemoteError, -1)
            else:
                # presume everything went well...
                job.returncode = 0
        job.lrms_jobname = arc_job.job_name # XXX: `lrms_jobname` is the name used in `sge.py`

        # XXX: do we need these?  they're already in `Application.stdout` and `Application.stderr`
        job.stdout_filename = arc_job.sstdout
        job.stderr_filename = arc_job.sstderr

        # Common struture as described in Issue #78
        job.queue = arc_job.queue
        job.cores = arc_job.cpu_count
        job.original_exitcode = arc_job.exitcode
        job.used_walltime = arc_job.used_wall_time # exressed in sec.
        job.used_cputime = arc_job.used_cpu_time # expressed in sec.
        job.used_memory = arc_job.used_memory # expressed in KiB

        # if (arc_job.status == 'FAILED'):
        #     job.error_log = arc_job.errors

        # # update ARC-specific info
        # job.arc_cluster = arc_job.cluster
        # job.arc_cpu_count = arc_job.cpu_count
        # job.arc_job_name = arc_job.job_name
        # job.arc_queue = arc_job.queue
        # job.arc_queue_rank = arc_job.queue_rank
        # job.arc_requested_cpu_time = arc_job.requested_cpu_time
        # job.arc_requested_wall_time = arc_job.requested_wall_time
        # job.arc_sstderr = arc_job.sstderr
        # job.arc_sstdin = arc_job.sstdin
        # job.arc_sstdout = arc_job.sstdout
        # job.arc_used_cpu_time = arc_job.used_cpu_time
        # job.arc_used_memory = arc_job.used_memory
        # job.arc_used_wall_time = arc_job.used_wall_time
        # # FIXME: use Python's `datetime` types (RM)
        # if arc_job.submission_time.GetTime() != -1:
        #     job.arc_submission_time = str(arc_job.submission_time)
        # if arc_job.completion_time.GetTime() != -1:
        #     job.arc_completion_time = str(arc_job.completion_time)
        # else:
        #     job.arc_completion_time = ""

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
        jftpc = arclib.JobFTPControl()

        log.debug("Downloading %s output into '%s' ...", app, download_dir)
        try:
            jftpc.DownloadDirectory(job.lrms_jobid, download_dir)
            job.download_dir = download_dir
        except arclib.FTPControlError, ex:
            # FIXME: parsing error messages breaks if locale is not an
            # English-based one!
            if "Failed to allocate port for data transfer" in str(ex):
                raise gc3libs.exceptions.RecoverableDataStagingError(
                    "Recoverable Error: Failed downloading remote folder '%s': %s"
                    % (job.lrms_jobid, str(ex)))
            # critical error. consider job remote data as lost
            raise gc3libs.exceptions.UnrecoverableDataStagingError(
                "Unrecoverable Error: Failed downloading remote folder '%s': %s" 
                % (job.lrms_jobid, str(ex)))

        return 

    @same_docstring_as(LRMS.free)
    def free(self, app):

        self.auths.get(self._resource.auth)

        job = app.execution
        jftpc = arclib.JobFTPControl()

        # Clean remote job sessiondir
        try:
            retval = jftpc.Clean(job.lrms_jobid)
        except arclib.FTPControlError:
            log.warning("Failed removing remote folder '%s'" % job.lrms_jobid)
            pass


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

        def _normalize_value(val):
            # an ARC value may contains -1 when the subsystem cannot
            # get/resolve it we treat then these values as 0
            if val < 0:
                return 0
            else:
                return val

        queues = self._get_queues()
        if len(queues) == 0:
            raise gc3libs.exceptions.LRMSSubmitError('No ARC queues found')

        for q in queues:
            q.grid_queued = _normalize_value(q.grid_queued)
            q.local_queued = _normalize_value(q.local_queued)
            q.prelrms_queued = _normalize_value(q.prelrms_queued)
            q.queued = _normalize_value(q.queued)

            q.cluster.used_cpus = _normalize_value(q.cluster.used_cpus)
            q.cluster.total_cpus = _normalize_value(q.cluster.total_cpus)
            
            # total_queued
            total_queued = total_queued +  q.grid_queued + \
                           q.local_queued + q.prelrms_queued + q.queued

            # free_slots
            # free_slots - free_slots + ( q.total_cpus - q.running )
            free_slots = free_slots +\
                         min((q.total_cpus - q.running),\
                             (q.cluster.total_cpus - q.cluster.used_cpus))

        arc_jobs_info = self._get_jobs()
        # user_running and user_queued
        for job in arc_jobs_info.values():
            if 'INLRMS:R' in job.status:
                user_running = user_running + 1
            elif 'INLRMS:Q' in job.status:
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

        # `local_file` could be a file name (string) or a file-like
        # object, as per function docstring; ensure `local_file_name`
        # is the local path
        try:
           local_file_name = local_file.name
        except AttributeError:
           local_file_name = local_file

        # get JobFTPControl handle
        jftpc = arclib.JobFTPControl()
        remote_url = arclib.URL(job.lrms_jobid + '/' + remote_filename)

        # check remote file size
        remote_file_size = jftpc.Size(remote_url)
        if offset < 0:
            # consider this as 'starts from bottom'
            offset = remote_file_size + offset

        # download file
        log.debug("Downloading max %d bytes at offset %d of remote file '%s' into local file '%s' ..."
                  % (size, offset, remote_filename, local_file_name))
        jftpc.Download(remote_url, int(offset), int(size), local_file_name)
        log.debug("ArcLRMS.peek(): arclib.JobFTPControl.Download: completed")


    @same_docstring_as(LRMS.validate_data)
    def validate_data(self, data_file_list):
        """
        Supported protocols: file, gsiftp, srm, http, https
        """
        for url in data_file_list:
            log.debug("Resource %s: checking URL '%s' ..." % (self._resource.name, url))
            if not url.scheme in ['srm', 'lfc', 'file', 'http', 'gsiftp', 'https']:
                return False
        return True

    @same_docstring_as(LRMS.validate_data)
    def close(self):
        pass

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="arc",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
