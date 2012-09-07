#! /usr/bin/env python
#
"""
Job control on ARC0 resources.
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
__version__ = '2.0.0-a2 version (SVN $Revision$)'


import sys
import os
import shutil
import time
import tempfile

import warnings
warnings.simplefilter("ignore")

from gc3libs import log, Run
from gc3libs.backends import LRMS
import gc3libs.exceptions
from gc3libs.quantity import kB, GB, MB, hours, minutes, seconds
from gc3libs.utils import *

# this is where arc0 libraries are installed from release 11.05
sys.path.append('/usr/lib/pymodules/python%d.%d/'
                % sys.version_info[:2])
try:
    import arclib
    have_arclib_module = True
except ImportError:
    have_arclib_module = False


def _normalize_value(val):
    """
    ARC returns -1 when the subsystem cannot get/resolve a value; we
    treat then these values as 0 instead.
    """
    if val < 0:
        return 0
    else:
        return val


class ArcLrms(LRMS):
    """
    Manage jobs through the ARC middleware.

    In addition to attributes

      ===================  ============== =========
      Attribute name       Type           Required?
      ===================  ============== =========
      arc_ldap             string
      frontend             string         yes
      ===================  ============== =========


    """
    def __init__(self, name,
                 # this are inherited from the base LRMS class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime, auth,
                 # these are specific to the ARC0 backend
                 arc_ldap=None,
                 frontend=None,
                 lost_job_timeout=gc3libs.Default.ARC_LOST_JOB_TIMEOUT,
                 **extra_args):

        # check if arc module has been imported
        if not have_arclib_module:
            raise gc3libs.exceptions.LRMSError(
                "Could not import ARClib module, disable ARC0 resources.")

        # init base class
        LRMS.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth)

        # ARC0-specific setup
        self.lost_job_timeout = lost_job_timeout
        self.arc_ldap = arc_ldap
        if frontend is None:
            if self.arc_ldap is not None:
                # extract frontend information from arc_ldap entry
                try:
                    resource_url = gc3libs.url.Url(arc_ldap)
                    self.frontend = resource_url.hostname
                except Exception, err:
                    raise gc3libs.exceptions.ConfigurationError(
                        "Configuration error: resource '%s' has no valid 'arc_ldap' setting: %s: %s"
                        % (name, err.__class__.__name__, err.message))
            else:
                self.frontend = None

        # prevent ARClib logging to STDERR
        arcnotifier = arclib.Notify_getNotifier()
        arcnotifier.SetOutStream(arcnotifier.GetNullStream())
        # DEBUG: uncomment the following to print all ARC messages
        #arcnotifier.SetOutStream(arcnotifier.GetOutStream())
        #arcnotifier.SetNotifyLevel(arclib.VERBOSE)
        #arcnotifier.SetNotifyTimeStamp(True)

        self.targets_blacklist = []

    @same_docstring_as(LRMS.cancel_job)
    @LRMS.authenticated
    def cancel_job(self, app):
        try:
            arclib.CancelJob(app.execution.lrms_jobid)
        except Exception, ex:
            gc3libs.log.error('Failed while killing job. Error type %s, message %s' % (ex.__class__,str(ex)))
            raise gc3libs.exceptions.LRMSError('Failed while killing job. Error type %s, message %s' % (ex.__class__,str(ex)))


    # excluded_targets is the list of targets hostnames where the application
    # has been already running; thus to be excluded for the next submission
    # candidate_queues is the list of available queues
    # this method simply returns a
    def _filter_queues(self, candidate_queues, job):
        """
        Excludes from the list of candidate queuse those corresponding to hosts
        where a given job has been already running.
        If all queues have been already tried, clear execution_targets list and
        start again.
        """
        # use queue.cluster.hostname to match entries from job.execution_targets list
        queues = [ queue for queue in candidate_queues
                   if queue.cluster.hostname not in job.execution_targets ]
        if not queues:
            # assume all available targes have been tried. Clean list and start over again
            queues = candidate_queues
            job.execution_targets = [ ]

        return queues


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
        if self.arc_ldap is not None:
            log.info("Updating ARC resource information from '%s'"
                      % self.arc_ldap)
            return arclib.GetClusterResources(
                arclib.URL(self.arc_ldap), True, '', 1)
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
                 % (self.name, len(job_list)))
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
    @LRMS.authenticated
    def submit_job(self, app):
        job = app.execution

        # Initialize xrsl
        xrsl = app.xrsl(self)
        log.debug("Application provided XRSL: %s" % xrsl)
        try:
            xrsl = arclib.Xrsl(xrsl)
        except Exception, ex:
            raise gc3libs.exceptions.LRMSSubmitError('Failed in getting `Xrsl` object from arclib: %s: %s'
                                  % (ex.__class__.__name__, str(ex)))

        # queues = self._get_queues()
        queues = self._filter_queues(self._get_queues(), job)
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

        # extract target name from lrms_jobid
        # this will be attached to the job object.
        # see Issue 227
        url = arclib.URL(lrms_jobid)
        job.execution_targets.append(url.Host())

        # state is known at this point, so mark this as a successful update
        job._arc0_state_last_checked = time.time()
        return job


    @staticmethod
    def _map_arc0_status_to_gc3pie_state(status):
        """
        Return the GC3Pie state corresponding to the given ARC status.

        See `update_job_state`:meth: for a complete table of the
        correspondence.  ARC0 job states are documented in
        `<http://www.nordugrid.org/documents/arc_infosys.pdf>` on page
        39.

        :param str status: ARC0 job status.

        :raise gc3libs.exceptions.UnknownJobState: If there is no
        mapping of `status` to a GC3Pie state.
        """
        try:
            return {
                'ACCEPTING': Run.State.SUBMITTED,
                'ACCEPTED':  Run.State.SUBMITTED,
                'PREPARING': Run.State.SUBMITTED,
                'PREPARED':  Run.State.SUBMITTED,
                'SUBMITTING':Run.State.SUBMITTED,
                'SUBMIT':    Run.State.SUBMITTED,
                'INLRMS:Q':  Run.State.SUBMITTED,
                'INLRMS:R':  Run.State.RUNNING,
                'INLRMS:E':  Run.State.RUNNING,
                'INLRMS:O':  Run.State.RUNNING,
                'INLRMS:S':  Run.State.STOPPED,
                'INLRMS:H':  Run.State.STOPPED,
                # XXX: it seems that `INLRMS:X` is a notation used in
                # the manual to mean `INLRMS:*`, i.e., any of the
                # above, not an actual state ...
                'INLRMS:X':  Run.State.STOPPED,
                # the `-ING` states below are used by ARC to mean that
                # the GM has received a request for action but the job
                # has not yet terminated; in particular, the output is
                # not yet ready for retrieval, which is why we map
                # them to `RUNNING`.
                'FINISHING': Run.State.RUNNING,
                'KILLING':   Run.State.RUNNING, # ARC GM is sending signal
                'EXECUTED':  Run.State.RUNNING,
                'FINISHING': Run.State.RUNNING,
                'CANCELING': Run.State.RUNNING,
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
    @LRMS.authenticated
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
                    INLRMS:O        STOPPED
                    INLRMS:E        STOPPED
                    INLRMS:X        STOPPED
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

        # initialize the unknown counter
        if not hasattr(job, 'unknown_iteration'):
            job.unknown_iteration = 0

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
            now = time.time()
            if (now - job._arc0_state_last_checked) > gc3libs.Default.ARC_LOST_JOB_TIMEOUT:
                if not job.state == Run.State.UNKNOWN:
                    # set to UNKNOWN
                    job.state = Run.State.UNKNOWN
                    gc3libs.log.error(
                        "Failed updating status of task '%s' for [%d] sec."
                        " Setting to `UNKNOWN` state. ",
                        app, gc3libs.Default.ARC_LOST_JOB_TIMEOUT)
                # else:
                #     # just record failure updating job state
                #     gc3libs.log.warning("Failed updating job status. Assume transient information system failure. Return unchanged status.")
                # # return job.state
            elif (job.state == Run.State.SUBMITTED
                and now - job.state_last_changed < self.lost_job_timeout):
                gc3libs.log.warning(
                    "Failed updating state of task '%s'."
                    " Assuming it was recently submitted;"
                    " task state will not be changed.", app)
            elif (job.state in [ Run.State.SUBMITTED, Run.State.RUNNING ]
                  and now - job._arc0_state_last_checked < self.lost_job_timeout):
                gc3libs.log.warning(
                    "Failed updating state of task '%s'."
                    " Assuming transient information system failure;"
                    " task state will not be changed.", app)
            # # elif (job.state == Run.State.UNKNOWN
            # #       and job.unknown_iteration > gc3libs.Default.UNKNOWN_ITER_LIMIT):
            # #     # consider job as lost
            # #     raise gc3libs.exceptions.UnknownJob(
            # #         "No job found corresponding to the ID '%s'" % job.lrms_jobid)
            # else:
            #     gc3libs.log.error("Failed updating job status. Keeping status unchanged for [%d] times." % ((gc3libs.Default.UNKNOWN_ITER_LIMIT - job.unknown_iteration)))
            #     #raise  gc3libs.exceptions.UnknownJob(
            #     #    "No job found corresponding to the ID '%s'" % job.lrms_jobid)
            #     # job.state = Run.State.UNKNOWN
            #     job.unknown_iteration += 1
            #     app.changed = True

            # End of except. Return job state
            return job.state

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
                # XXX: how to deal with
                # 'Data staging failed (pre-processing); Failed in files upload (post-processing)'
                job.history("ARC reported error: %s" % arc_job.errors)
                if "Data staging failed" in arc_job.errors:
                    job.returncode = (Run.Signals.DataStagingFailure, -1)
                else:
                    job.returncode = (Run.Signals.RemoteError, -1)
            # FIXME: we should introduce a kind of "wrong requirements" error
            elif (arc_job.requested_wall_time is not None
                  and arc_job.requested_wall_time != -1
                  and arc_job.used_wall_time != -1
                  and arc_job.used_wall_time > arc_job.requested_wall_time):
                job.history("Job exceeded requested wall-clock time (%d s),"
                        " killed by remote batch system"
                        % arc_job.requested_wall_time)
                job.returncode = (Run.Signals.RemoteError, -1)
            elif (arc_job.requested_cpu_time is not None
                  and arc_job.requested_cpu_time != -1
                  and arc_job.used_cpu_time != -1
                  and arc_job.used_cpu_time > arc_job.requested_cpu_time):
                job.history("Job exceeded requested CPU time (%d s),"
                        " killed by remote batch system"
                        % arc_job.requested_cpu_time)
                job.returncode = (Run.Signals.RemoteError, -1)
            # note: arc_job.used_memory is in KiB (!)
            elif (app.requested_memory is not None
                  and arc_job.used_memory != -1
                  and arc_job.used_memory > app.requested_memory.amount(kB)):
                job.history("Job used more memory (%d MB) than requested (%s),"
                        " killed by remote batch system"
                        % (arc_job.used_memory / 1024, app.requested_memory.amount(MB)))
                job.returncode = (Run.Signals.RemoteError, -1)
            else:
                # presume everything went well...
                job.returncode = (0, 0)
        job.lrms_jobname = arc_job.job_name # see Issue #78

        # Common struture as described in Issue #78
        job.queue = arc_job.queue
        job.cores = arc_job.cpu_count
        job.original_exitcode = arc_job.exitcode
        job.used_walltime = arc_job.used_wall_time # exressed in sec.
        job.used_cputime = arc_job.used_cpu_time # expressed in sec.
        job.used_memory = arc_job.used_memory # expressed in KiB

        job.state = state
        return state


    @same_docstring_as(LRMS.get_results)
    @LRMS.authenticated
    def get_results(self, app, download_dir, overwrite=False):
        jobid = app.execution.lrms_jobid

        # XXX: can raise encoding/decoding error if `download_dir`
        # is not ASCII, but the ARClib bindings don't accept
        # Python `unicode` strings.
        download_dir = str(download_dir)

        # as ARC complains when downloading to an already-existing
        # directory, make a temporary directory for downloading files;
        # then move files to their final destination and delete the
        # temporary location.
        tmp_download_dir = tempfile.mkdtemp(suffix='.d', dir=download_dir)

        log.debug("Downloading %s output into temporary location '%s' ...", app, tmp_download_dir)
        try:
            jftpc = arclib.JobFTPControl()
            jftpc.DownloadDirectory(jobid, tmp_download_dir)
        except arclib.FTPControlError, ex:
            # remove temporary download location
            shutil.rmtree(tmp_download_dir, ignore_errors=True)
            # FIXME: parsing error messages breaks if locale is not an
            # English-based one!
            if "Failed to allocate port for data transfer" in str(ex):
                raise gc3libs.exceptions.RecoverableDataStagingError(
                    "Recoverable Error: Failed downloading remote folder '%s': %s"
                    % (jobid, str(ex)))
            # critical error. consider job remote data as lost
            raise gc3libs.exceptions.UnrecoverableDataStagingError(
                "Unrecoverable Error: Failed downloading remote folder '%s': %s"
                % (jobid, str(ex)))

        log.debug("Moving %s output into download location '%s' ...", app, download_dir)
        entries = os.listdir(tmp_download_dir)
        if not overwrite:
            # raise an early error before we start mixing files from
            # the old and new download directories
            for entry in entries:
                dst = os.path.join(download_dir, entry)
                if os.path.exists(dst):
                    # remove temporary download location
                    shutil.rmtree(tmp_download_dir, ignore_errors=True)
                    gc3libs.log.warning(
                        "Entry '%s' in download directory '%s' already exists,"
                        " and no overwriting was requested."
                        % (entry, download_dir))
        # move all entries to the final destination
        for entry in entries:
            src = os.path.join(tmp_download_dir, entry)
            dst = os.path.join(download_dir, entry)
            if os.path.isdir(dst):
                shutil.rmtree(dst)
            os.rename(src, dst)

        # remove temporary download location (XXX: is it correct to ignore errors here?)
        shutil.rmtree(tmp_download_dir, ignore_errors=True)

        app.execution.download_dir = download_dir
        return


    @same_docstring_as(LRMS.free)
    @LRMS.authenticated
    def free(self, app):
        job = app.execution
        jftpc = arclib.JobFTPControl()

        # Clean remote job sessiondir
        try:
            retval = jftpc.Clean(job.lrms_jobid)
        except arclib.FTPControlError:
            log.warning("Failed removing remote folder '%s'" % job.lrms_jobid)
            pass


    @cache_for(gc3libs.Default.ARC_CACHE_TIME)
    @LRMS.authenticated
    def get_resource_status(self):
        """
        Get dynamic information from the ARC infosystem and set
        attributes on the current object accordingly.

        The following attributes are set:

        * total_queued
        * free_slots
        * user_running
        * user_queued
        """
        total_queued = 0
        free_slots = 0
        user_running = 0
        user_queued = 0

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

        self.queued = total_queued
        self.free_slots = free_slots
        self.user_queued = user_queued
        self.user_run = user_running
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


    @same_docstring_as(LRMS.peek)
    @LRMS.authenticated
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        job = app.execution

        assert job.has_key('lrms_jobid'), \
            "Missing attribute `lrms_jobid` on `Job` instance passed to `ArcLrms.peek`."

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
            log.debug("Resource %s: checking URL '%s' ..." % (self.name, url))
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
