import sys
import os
import commands
import logging
import tempfile
import getpass
import re
import time
import ConfigParser
import shutil
import subprocess
import gc3utils
from utils import *
from LRMS import LRMS
from Resource import Resource
import Job
import Application
import Default
import Exceptions

import warnings
warnings.simplefilter("ignore")

import arclib
 
# -----------------------------------------------------
# ARC lrms
#

class ArcLrms(LRMS):

    isValid = 0
    _resource = None

    def __init__(self,resource, auths):
        gc3utils.log = logging.getLogger('gc3utils')
        
        # Normalize resource types
        if resource.type is Default.ARC_LRMS:
            self._resource = resource
            self.isValid = 1

            self._resource.ncores = int(self._resource.ncores)
            self._resource.max_memory_per_core = int(self._resource.max_memory_per_core) * 1000
            self._resource.walltime = int(self._resource.walltime)
            if self._resource.walltime > 0:
                # Convert from hours to minutes
                self._resource.walltime = self._resource.walltime * 60

            self._queues_cache_time = Default.CACHE_TIME # XXX: should it be configurable?

    def is_valid(self):
        return self.isValid

    def submit_job(self, application, job=None):
        return self._submit_job_arclib(application, job)

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
            self._queues = arclib.GetQueueInfo(cls,arclib.MDS_FILTER_CLUSTERINFO, True, '', 1)
            self._queues_last_updated = time.time()
        return self._queues
            
    def _submit_job_arclib(self, application, job=None):

        # Initialize xrsl
        xrsl = application.xrsl(self._resource)
        gc3utils.log.debug('Application provided XRSL: %s' % xrsl)

        # Aternative using arclib

        try:
            _xrsl = arclib.Xrsl(xrsl)
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
            raise LRMSSubmitError('Got error from arclib.SubmitJob():', exc_info=True)

        if job is None:
            job = Job.Job()
        job.lrms_jobid=lrms_jobid
        job.status=Job.JOB_STATE_SUBMITTED
        job.resource_name=self._resource.name

        # add submssion time reference
        job.submission_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        return job

                                
    def _submit_job_exec(self, application, job=None):
        """
        Submit job by calling the 'ngsub' command.
        """
        try:
            # Initialize xrsl from template
            GAMESS_XRSL_TEMPLATE = os.path.expandvars(Default.GAMESS_XRSL_TEMPLATE)
            
            if os.path.exists(GAMESS_XRSL_TEMPLATE):
                # GAMESS only needs 1 input file
                input_file_path = application.inputs[0]
                xrsl = from_template(GAMESS_XRSL_TEMPLATE,
                                     INPUT_FILE_NAME = os.path.splitext(os.path.basename(input_file_path))[0],
                                     INPUT_FILE_DIR = os.path.dirname(input_file_path))
                
                # append requirements to XRSL file
                if ( self._resource.walltime > 0 ):
                    gc3utils.log.debug('setting walltime...')
                    if int(application.requested_walltime) > 0:
                        requested_walltime = int(application.requested_walltime) * 60
                    else:
                        requested_walltime = self._resource.walltime
                    xrsl += '(cputime="%s")\n' % requested_walltime

                if ( self._resource.ncores > 0 ):
                    gc3utils.log.debug('setting cores...')
                    if int(application.requested_cores) > 0:
                        requested_cores = int(application.requested_cores)
                    else:
                        requested_cores = self._resource.ncores
                    xrsl += '(count="%s")\n' % requested_cores
                            
                if ( self._resource.memory_per_core > 0 ):
                    gc3utils.log.debug('setting memory')
                    if int(application.requested_memory) > 0:
                        requested_memory = int(application.requested_memory) * 1000
                    else:
                        requested_memory = int(self._resource.memory_per_core) * 1000
                        xrsl += '(memory="%s")\n' % requested_memory


                # Ready for real submission
                if ( self._resource.frontend == "" ):
                    # frontend not defined; use the entire arc-based infrastructure
                    _command = "ngsub -d2 -e '%s'" % xrsl
                else:
                    _command = "ngsub -d2 -c %s -e '%s'" % (self._resource.frontend, xrsl)
                gc3utils.log.debug('Running ARC command: %s',_command)
            
                (exitcode, output) = commands.getstatusoutput(_command)

                jobid_pattern = "Job submitted with jobid: "
                if exitcode != 0 or jobid_pattern not in output:
                    # Failed somehow
                    gc3utils.log.error("Command '%s' failed with exitcode %d and error: %s" 
                                       % (_command, exitcode,  output))
                    raise Exception('failed submitting to LRMS')

                # assuming submit successfull
                gc3utils.log.debug("ngsub command\t\t[ ok ]")

                # Extracting ARC jobid
                lrms_jobid = output.split(jobid_pattern)[1]
                gc3utils.log.debug('Job submitted with jobid: %s',lrms_jobid)

                if job is None:
                    job = Job.Job()
                job.lrms_jobid=lrms_jobid
                job.status=Job.JOB_STATE_SUBMITTED
                job.resource_name=self._resource.name
                job.log=output
                #                job.lrms_jobid = lrms_jobid
                #                job.status = Job.JOB_STATE_SUBMITTED
                #                job.resource_name = self._resource.name
                #                job.log = output
                
                return job

            else:
                gc3utils.log.critical('XRSL file not found %s', GAMESS_XRSL_TEMPLATE)
                raise Exception('template file for submission script not found')
        except:
            gc3utils.log.critical("Failure submitting job to resource '%s'"
                                  % self._resource.name)
            raise
        
    def check_status(self, job_obj):
        
        submitted_list = ['ACCEPTED','SUBMITTING','PREPARING','INLRMS:Q']
        running_list = ['INLRMS:R','INLRMS:O','INLRMS:S','INLRMS:E','INLRMS:X','FINISHING','CANCELING','EXECUTED']
        finished_list = ['FINISHED','KILLED']
        failed_list = ['FAILED','DELETED']

        try:
            
            # Prototype from arclib
            arc_job = arclib.GetJobInfo(job_obj.lrms_jobid)

            job_obj.cluster = arc_job.cluster
            job_obj.cpu_count = arc_job.cpu_count
            job_obj.exitcode = arc_job.exitcode
            job_obj.job_name = arc_job.job_name
            job_obj.queue = arc_job.queue
            job_obj.queue_rank = arc_job.queue_rank
            job_obj.requested_cpu_time = arc_job.requested_cpu_time
            job_obj.requested_wall_time = arc_job.requested_wall_time
            job_obj.sstderr = arc_job.sstderr
            job_obj.sstdout = arc_job.sstdout
            job_obj.sstdin = arc_job.sstdin
            job_obj.used_cpu_time = arc_job.used_cpu_time
            job_obj.used_wall_time = arc_job.used_wall_time
            job_obj.used_memory = arc_job.used_memory
            if arc_job.submission_time.GetTime() > -1:
                job_obj.submission_time = str(arc_job.submission_time)
            if arc_job.completion_time.GetTime() > -1:
                job_obj.completion_time = str(arc_job.completion_time)
            else:
                job_obj.completion_time = ""

            if arc_job.status in running_list:
                gc3utils.log.debug('job status: %s setting to RUNNING',arc_job.status)
                job_obj.status = Job.JOB_STATE_RUNNING
            elif arc_job.status in submitted_list:
                gc3utils.log.debug('job status: %s setting to SUBMITTED',arc_job.status)
                job_obj.status = Job.JOB_STATE_SUBMITTED
            elif arc_job.status in finished_list:
                gc3utils.log.debug('job status: %s setting to FINISHED',arc_job.status)
                job_obj.status = Job.JOB_STATE_FINISHED
            elif arc_job.status in failed_list:
                gc3utils.log.debug('job status: %s setting to FAILED',arc_job.status)
                job_obj.status = Job.JOB_STATE_FAILED
                
            return job_obj

        except:
            gc3utils.log.critical('Failure in checking status [%s]',sys.exc_info()[1])
            raise

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
            
            for cluster in cls:
                queues =  arclib.GetQueueInfo(cluster,arclib.MDS_FILTER_CLUSTERINFO,True,"",1)
                if len(queues) == 0:
                    gc3utils.log.error('No ARC queues found for resource %s' % str(cluster))
                    continue
                    # raise LRMSSubmitError('No ARC queues found')              

                list_of_jobs = arclib.GetAllJobs(cluster,True,"",1)
                
                for q in queues:
                    q.grid_queued = self._normalize_value(q.grid_queued)
                    q.local_queued = self._normalize_value(q.local_queued)
                    q.prelrms_queued = self._normalize_value(q.prelrms_queued)
                    q.queued = self._normalize_value(q.queued)

                    q.cluster.used_cpus = self._normalize_value(q.cluster.used_cpus)
                    q.cluster.total_cpus = self._normalize_value(q.cluster.total_cpus)

                    # total_queued
                    total_queued = total_queued +  q.grid_queued + q.local_queued + q.prelrms_queued + q.queued

                    # free_slots
                    # free_slots - free_slots + ( q.total_cpus - q.running )
                    free_slots = free_slots + min((q.total_cpus - q.running),(q.cluster.total_cpus - q.cluster.used_cpus))
                    # Obsolete this because free slots is a queue related concept and not a cluster one
                    # free_slots = free_slots + q.cluster.total_cpus - q.cluster.used_cpus

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

# ========== Internal methods =============

    def _normalize_value(self, val):
        # an ARC value may contains -1 when the subsystem cannot get/resolve it
        # we treat then these values as 0
        if val < 0:
            return 0
        else:
            return val

    def _get_xrsl(self, application):

        xrsl = application.xrsl()

        # append requirements to XRSL file
        if ( self._resource.walltime > 0 ):
            gc3utils.log.debug('setting walltime...')
            if int(application.requested_walltime) > 0:
                requested_walltime = int(application.requested_walltime) * 60
            else:
                requested_walltime = self._resource.walltime
            xrsl += '(cputime="%s")\n' % requested_walltime
            
        if ( self._resource.ncores > 0 ):
            gc3utils.log.debug('setting cores...')
            if int(application.requested_cores) > 0:
                requested_cores = int(application.requested_cores)
            else:
                requested_cores = self._resource.ncores
            xrsl += '(count="%s")\n' % requested_cores

        if ( self._resource.memory_per_core > 0 ):
            gc3utils.log.debug('setting memory')
            if int(application.requested_memory) > 0:
                requested_memory = int(application.requested_memory) * 1000
            else:
                requested_memory = int(self._resource.memory_per_core) * 1000
            xrsl += '(memory="%s")\n' % requested_memory

        try:
            _xrsl = arclib.Xrsl(xrsl)
            return _xrsl
        except:
            raise LRMSSubmitError('Failed while building Xrsl: %s' % sys.exc_info()[1])

    def GetResourceStatus(self):
        gc3utils.log.debug("Returning information of local resoruce")
        
        # SERGIO TBCK: check whether it works by Resource(self._resource) instead
        return Resource(resource_name=self._resource['resource_name'],
                        total_cores=self._resource['ncores'],
                        memory_per_core=self._resource['memory_per_core'])
