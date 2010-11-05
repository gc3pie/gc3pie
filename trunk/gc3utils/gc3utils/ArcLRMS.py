import sys
import os
import time
import tempfile

import warnings
warnings.simplefilter("ignore")

import gc3utils
from utils import *
from LRMS import LRMS
from Resource import Resource
import Job
import Application
import Default
import Exceptions

import arclib
 

class ArcLrms(LRMS):
    """
    Manage jobs through the ARC middleware.
    """

    def __init__(self,resource, auths):
        # Normalize resource types
        if resource.type is Default.ARC_LRMS:
            self._resource = resource
            self.isValid = 1

            self._resource.ncores = int(self._resource.ncores)
            self._resource.max_memory_per_core = int(self._resource.max_memory_per_core) * 1000
            self._resource.max_walltime = int(self._resource.max_walltime)
            if self._resource.max_walltime > 0:
                # Convert from hours to minutes
                self._resource.max_walltime = self._resource.max_walltime * 60

            self._queues_cache_time = Default.CACHE_TIME # XXX: should it be configurable?

    def is_valid(self):
        return self.isValid

    def _get_queues(self):
        if (not hasattr(self, '_queues')) or (not hasattr(self, '_queues_last_accessed')) \
                or (time.time() - self._queues_last_updated > self._queues_cache_time):
            if self._resource.has_key('arc_ldap'):
                gc3utils.log.debug("Getting list of ARC resources from GIIS '%s' ...", 
                                   self._resource.arc_ldap)
                cls = arclib.GetClusterResources(arclib.URL(self._resource.arc_ldap),True,'',1)
            else:
                cls = arclib.GetClusterResources()
            gc3utils.log.debug('Got cluster list of length %d', len(cls))
            # Temporarly disable this check
            #if len(cls) > 0:
            self._queues = arclib.GetQueueInfo(cls,arclib.MDS_FILTER_CLUSTERINFO, True, '', 5)
            gc3utils.log.debug('returned valid queue information for %d queues', len(self._queues))
            self._queues_last_updated = time.time()
            #else:
            ## return empty queues list
            # self._queues = [] 
        return self._queues
            
    def submit_job(self, application, job=None):

        # Initialize xrsl
        xrsl = application.xrsl(self._resource)
        gc3utils.log.debug('Application provided XRSL: %s' % xrsl)

        try:
            # ARClib cannot handle unicode strings, so convert `xrsl` to ascii
            # XXX: should this be done in Application.xrsl() instead?
            _xrsl = arclib.Xrsl(str(xrsl))
        except:
            #raise LRMSSubmitError('Failed in getting `Xrsl` object from arclib:', exc_info=True)
            raise LRMSSubmitError('Failed in getting `Xrsl` object from arclib:')

        queues = self._get_queues()
        if len(queues) == 0:
            raise LRMSSubmitError('No ARC queues found')

        targets = arclib.PerformStandardBrokering(arclib.ConstructTargets(queues, _xrsl))
        if len(targets) == 0:
            raise LRMSSubmitError('No ARC targets found')

        try:
            lrms_jobid = arclib.SubmitJob(_xrsl,targets)
        except arclib.JobSubmissionError:
            raise LRMSSubmitError('Got error from arclib.SubmitJob():')

        if job is None:
            job = Job.Job()
        job.lrms_jobid=lrms_jobid
        job.status=Job.JOB_STATE_SUBMITTED
        job.resource_name=self._resource.name

        # add submssion time reference
        job.submission_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # job.submission_time = time.localtime()

        return job

    def check_status(self, job):
        """
        Update `job.status` in-place to reflect the status of the
        corresponding ARC job.
        """

        def map_arc_status_to_gc3job_status(status):
            try:
                return {
                    'ACCEPTED':Job.JOB_STATE_SUBMITTED,
                    'SUBMITTING':Job.JOB_STATE_SUBMITTED,
                    'PREPARING':Job.JOB_STATE_SUBMITTED,
                    'INLRMS:Q':Job.JOB_STATE_SUBMITTED,
                    'INLRMS:R':Job.JOB_STATE_RUNNING,
                    'INLRMS:O':Job.JOB_STATE_RUNNING,
                    'INLRMS:S':Job.JOB_STATE_RUNNING,
                    'INLRMS:E':Job.JOB_STATE_RUNNING,
                    'INLRMS:X':Job.JOB_STATE_RUNNING,
                    'FINISHING':Job.JOB_STATE_RUNNING,
                    'CANCELING':Job.JOB_STATE_RUNNING,
                    'EXECUTED':Job.JOB_STATE_RUNNING,
                    'FINISHED':Job.JOB_STATE_FINISHED,
                    'KILLED':Job.JOB_STATE_FINISHED,
                    'FAILED':Job.JOB_STATE_FAILED,
                    'DELETED':Job.JOB_STATE_FAILED,
                    }[status]
            except KeyError:
                # any other status is mapped to 'UNKNOWN'
                return Job.JOB_STATE_UNKNOWN

        # Prototype from arclib
        arc_job = arclib.GetJobInfo(job.lrms_jobid)

        # update status
        # XXX: should we keep status intact in case the status is 'UNKNOWN' and retry later?
        job.status = map_arc_status_to_gc3job_status(arc_job.status)

        # set time stamps
        # XXX: use Python's `datetime` types
        if arc_job.submission_time.GetTime() > -1:
            # job.submission_time = self._date_normalize(str(arc_job.submission_time.GetTime()))
            job.submission_time = str(arc_job.submission_time)
        if arc_job.completion_time.GetTime() > -1:
            # job.completion_time = self._date_normalize(str(arc_job.completion_time.GetTime()))
            job.completion_time = str(arc_job.completion_time)
        else:
            job.completion_time = ""

        job.job_name = arc_job.job_name
        job.used_memory = arc_job.used_memory
        job.cpu_count = arc_job.cpu_count
        job.exit_code = arc_job.exitcode
        job.used_cpu_time = arc_job.used_cpu_time
        job.used_walltime = arc_job.used_wall_time
        job.requested_cpu_time = arc_job.requested_cpu_time
        job.requested_wall_time = arc_job.requested_wall_time
        job.queue = arc_job.queue

        # additional information. They should become part of mandatory Job information
        # they are required for 'tail' function
        job.stdout_filename = arc_job.sstdout
        job.stderr_filename = arc_job.sstderr

        # These should be removed. To check impact on additional services first
        # see Issue 27
        job.arc_cluster = arc_job.cluster
        job.arc_cpu_count = arc_job.cpu_count
        job.arc_exitcode = arc_job.exitcode
        job.arc_job_name = arc_job.job_name
        job.arc_queue = arc_job.queue
        job.arc_queue_rank = arc_job.queue_rank
        job.arc_requested_cpu_time = arc_job.requested_cpu_time
        job.arc_requested_wall_time = arc_job.requested_wall_time
        job.arc_sstderr = arc_job.sstderr
        job.arc_sstdout = arc_job.sstdout
        job.arc_sstdin = arc_job.sstdin
        job.arc_used_cpu_time = arc_job.used_cpu_time
        job.arc_used_wall_time = arc_job.used_wall_time
        job.arc_used_memory = arc_job.used_memory

        return job


    def get_results(self, job_obj):
        try:
            # get FTP control
            jftpc = arclib.JobFTPControl()
            if job_obj.has_key('job_local_dir'):
                _download_dir = job_obj.job_local_dir + '/' + job_obj.unique_token
            else:
                _download_dir = Default.JOB_FOLDER_LOCATION + '/' + job_obj.unique_token

            # Prepare/Clean download dir
            if gc3utils.Job.prepare_job_dir(_download_dir) is False:
                gc3utils.log.error('failed creating local folder %s' % _download_dir)
                raise IOError('Failed while creating local folder %s' % _download_dir)

            gc3utils.log.debug('downloading job into %s',_download_dir)
            try:
                arclib.JobFTPControl.DownloadDirectory(jftpc,job_obj.lrms_jobid,_download_dir)
            except arclib.FTPControlError:
                # critical error. consider job remote data as lost
                gc3utils.log.error('failed downloading remote folder %s' % job_obj.lrms_jobid)
                raise LRMSUnrecoverableError('failed downloading remote folder')

            # Clean remote job sessiondir
            try:
                retval = arclib.JobFTPControl.Clean(jftpc,job_obj.lrms_jobid)
            except arclib.FTPControlError:
                gc3utils.log.error('Failed wile removing remote folder %s' % job_obj.lrms_jobid)
                job_obj.warning_flag = 1

            # set job status to COMPLETED
            job_obj.download_dir = _download_dir
            job_obj.status = Job.JOB_STATE_COMPLETED
            
            return job_obj
        except arclib.JobFTPControlError:
            raise
        except arclib.FTPControlError:
            raise
        except:
            gc3utils.log.error('Failure in retrieving job results [%s]',sys.exc_info()[1])
            raise


    def get_resource_status(self):
        """
        Get the status of a single resource.
        Return a Resource object.
        """
        # Get dynamic information out of the attached ARC subsystem (being it a single resource or a grid)
        # Fill self._resource object with dynamic information
        # return self._resource

        # dynamic information required (at least those):
        # total_queued
        # free_slots
        # user_running
        # user_queued

        try:
            if self._resource.has_key('arc_ldap'):
                gc3utils.log.debug("Getting cluster list from %s ...", self._resource.arc_ldap)
                cls = arclib.GetClusterResources(arclib.URL(self._resource.arc_ldap),True,'',2)
            else:
                gc3utils.log.debug("Getting cluster list from ARC's default GIIS ...")
                cls = arclib.GetClusterResources()

            total_queued = 0
            free_slots = 0
            user_running = 0
            user_queued = 0
            
            def _normalize_value(val):
                # an ARC value may contains -1 when the subsystem cannot get/resolve it
                # we treat then these values as 0
                if val < 0:
                    return 0
                else:
                    return val

            for cluster in cls:
                queues =  arclib.GetQueueInfo(cluster,arclib.MDS_FILTER_CLUSTERINFO,True,"",1)
                if len(queues) == 0:
                    gc3utils.log.error('No ARC queues found for resource %s' % str(cluster))
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

            gc3utils.log.info("Updated resource '%s' status:"
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
            # TBCK: how to handle
            raise

    def cancel_job(self, job_obj):
        arclib.CancelJob(job_obj.lrms_jobid)
        return job_obj

    def tail(self, job_obj, filename, offset=0, buffer_size=None):
        """
        tail allows to get a snapshot of any valid file created by the job
        """
        try:

            # Sanitize offset
            if int(offset) < 1024:
                offset = 0

            # Sanitize buffer_size
            if  not buffer_size:
                buffer_size = sys.maxint

            _remote_filename = job_obj.lrms_jobid + '/' + filename
        
            # create temp file
            _tmp_filehandle = tempfile.NamedTemporaryFile(mode='w+b', suffix='.tmp', prefix='gc3_')

            # get JobFTPControl handle            
            jftpc = arclib.JobFTPControl()

            # download file
            gc3utils.log.debug('Downloading remote file %s into local tmp file %s' % (filename,_tmp_filehandle.name))
            # arclib.JobFTPControl.Download(jftpc,_remote_filename,_tmp_filehandle.name)
            #  arclib.JobFTPControl.Download(jftpc, arclib.URL, offset, buffer_size, local_file)

            gc3utils.log.debug('using %d %d ',offset,buffer_size)
            arclib.JobFTPControl.Download(jftpc, arclib.URL(_remote_filename), int(offset), int(buffer_size), _tmp_filehandle.name)

            gc3utils.log.debug('done')

            # pass content of filename as part of job object dictionary
            # assuming stdout/stderr are alqays limited in size
            # We read the entire content in one step
            # shall we foresee different strategies ?
            _tmp_filehandle.file.flush()
            _tmp_filehandle.file.seek(0)

            return _tmp_filehandle

            _file_content = ""

            for line in _tmp_filehandle.file:
                _file_content += str(line)

            #_file_content = _tmp_filehandle.file.read()

            # cleanup: close and remove tmp file
            _tmp_filehandle.close()

            return _file_content
        except:
            gc3utils.log.error('Failed while retrieving remote file %s', _remote_filename)
            raise

    def _date_normalize(self, date_string):
        return time.localtime(int(date_string))

