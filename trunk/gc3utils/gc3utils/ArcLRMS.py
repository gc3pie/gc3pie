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
sys.path.append('/opt/nordugrid/lib/python2.4/site-packages')
import warnings
warnings.simplefilter("ignore")
import arclib

# -----------------------------------------------------
# ARC lrms
#

class ArcLrms(LRMS):

    isValid = 0
#    GAMESS_XRSL_TEMPLATE = "$HOME/.gc3/gamess_template.xrsl"
    #    resource = []
    _resource = None

    def __init__(self,resource):
        gc3utils.log = logging.getLogger('gc3utils')
        
        if resource.type is Default.ARC_LRMS:
            self._resource = resource
            self.isValid = 1

            self._resource.ncores = int(self._resource.ncores)
            self._resource.max_memory_per_core = int(self._resource.max_memory_per_core) * 1000
            self._resource.walltime = int(self._resource.walltime)
            if self._resource.walltime > 0:
                # Convert from hours to minutes
                self._resource.walltime = self._resource.walltime * 60

                #            log.info('created valid instance of LRMS type %s with %d cores, %d walltime, %d memory',self._resource.name,self._resource.total_cores,self._resource.max_walltime,self._resource.max_memory_per_node)


    def submit_job(self, application):
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

                job = Job.Job()
                job.lrms_jobid = lrms_jobid
                job.status = Job.JOB_STATE_SUBMITTED
                job.resource_name = self._resource.name
                job.log = output

                return job

            else:
                gc3utils.log.critical('XRSL file not found %s', GAMESS_XRSL_TEMPLATE)
                raise Exception('template file for submission scritp not found')
        except:
            gc3utils.log.critical('Failure in submitting')
            raise

    def check_status(self, job_obj):
        
        submitted_list = ['ACCEPTED','SUBMITTING','PREPARING']
        running_list = ['INLRMS:R','INLRMS:Q','INLRMS:O','INLRMS:S','INLRMS:E','INLRMS:X','FINISHING','CANCELING','EXECUTED']
        finished_list = ['FINISHED']
        failed_list = ['FAILED','DELETED']

        try:
            
            # Prototype from arclib
            arc_job = arclib.GetJobInfo(job_obj.lrms_jobid)

            job_obj.cluster = arc_job.cluster
#            job_obj.completion_time = arc_job.completion_time
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

#===============================================================================
#            # Ready for real submission
#            _command = "ngstat "+job_obj.lrms_jobid
# 
#            gc3utils.log.debug('Running ARC command [ %s ]',_command)
# 
#            retval = commands.getstatusoutput(_command)
#            # jobstatusunknown_pattern = "This job was only very recently"
#            jobstatusunknown_pattern = "Job information not found"
#            jobstatusremoved_pattern = "Job information not found"
#            jobstatusok_pattern = "Status: "
#            jobexitcode_pattern = "Exit Code: "
#            if not retval[0] :
#                gc3utils.log.error("ngstat command\t\t[ failed ]")
#                gc3utils.log.debug(retval[1])
#                raise Exceptions.CheckStarusError('failed checking status to LRMS')
# 
#            if ( jobstatusunknown_pattern in retval[1] ):
#                gc3utils.log.debug('job status: RUNNING')
#                job_obj.insert('status',Job.RUNNING)
# 
#            elif ( jobstatusok_pattern in retval[1] ):
# 
#                # Extracting ARC job status
#                lrms_jobstatus = re.split(jobstatusok_pattern,retval[1])[1]
#                lrms_jobstatus = re.split("\n",lrms_jobstatus)[0]
# 
#                gc3utils.log.debug('lrms_jobstatus\t\t\t[ %s ]',lrms_jobstatus)
# 
#                if ( lrms_jobstatus in running_list ):
#                    jobstatus = "Status: RUNNING"
#                elif ( lrms_jobstatus in finished_list ):
#                    jobstatus = "Status: FINISHED"
# #                if ( lrms_jobstatus in submitted_list ):
# #                    jobstatus = "Status: SUBMITTED"
# #                elif ( lrms_jobstatus in running_list ):
# #                    jobstatus = "Status: RUNNING"
# #                elif ( ( lrms_jobstatus in finished_list ) | ( lrms_jobstatus in failed_list )):
# #                    lrms_exitcode = re.split(jobexitcode_pattern,retval[1])[1]
# #                    lrms_exitcode = re.split("\n",lrms_exitcode)[0]
# #                    jobstatus = "Status: FINISHED\nExit Code: "+lrms_exitcode
#                else:
#                    jobstatus = "Status: [ "+lrms_jobstatus+" ]"
# 
#            return [jobstatus,retval[1]]
#===============================================================================

        except:
            gc3utils.log.critical('Failure in checking status [%s]',sys.exc_info()[1])
            raise

    def get_results(self, job_obj):
        try:
            # get FTP control
            jftpc = arclib.JobFTPControl()
            if job_obj.has_key('job_folder'):
                _download_dir = job_obj.job_folder + '/' + job_obj.unique_token
            else:
                _download_dir = Default.JOB_FOLDER_LOCATION + '/' + job_obj.unique_token

            # Prepare/Clean download dir
            gc3utils.utils.prepare_job_dir(_download_dir)

            gc3utils.log.debug('downloading job into %s',_download_dir)
            arclib.JobFTPControl.DownloadDirectory(jftpc,job_obj.lrms_jobid,_download_dir)
            # Default.JOB_FOLDER_LOCATION+'/'+job_obj.unique_token)

            # Clean remote job sessiondir
            #            retval = arclib.JobFTPControl.Clean(jftpc,job_obj.lrms_jobid)

            # set job status to COMPLETED
            job_obj.download_dir = _download_dir
            job_obj.status = Job.JOB_STATE_COMPLETED
            
            return job_obj
        except JobFTPControlError:
            raise
        except FTPControlError:
            raise
        except:
            gc3utils.log.error('Failure in retrieving job results [%s]',sys.exc_info()[1])

#                  try:
#            result_location_pattern="Results stored at "
#            
#            _command = "ngget -keep -s FINISHED -d 2 -dir "+job_dir+" "+lrms_jobid

#            gc3utils.log.debug('Running ARC command [ %s ]',_command)

#            job_results_retrieved_pattern = "successfuly downloaded: 0"

#            retval = commands.getstatusoutput(_command)
#            if ( ( retval[0] != 0 ) ):
#                # Failed somehow
#                gc3utils.log.error("ngget command\t\t[ failed ]")
#                gc3utils.log.debug(retval[1])
#                raise Exception('failed getting results from LRMS')

#            if ( result_location_pattern in retval[1] ):
#                _result_location_folder = re.split(result_location_pattern,retval[1])[1]
#                _result_location_folder = re.split("\n",_result_location_folder)[0]
#                gc3utils.log.debug('Moving result data from [ %s ]',_result_location_folder)
#                if ( os.path.isdir(_result_location_folder) ):
#                    retval = commands.getstatusoutput("cp -ap "+_result_location_folder+"/* "+job_dir)
#                    if ( retval[0] != 0 ):
#                        gc3utils.log.error('Failed copying results data from [ %s ] to [ %s ]',_result_location_folder,job_dir)
#                    else:
#                        gc3utils.log.info('Copying results\t\t[ ok ]')
#                        gc3utils.log.debug('Removing [ %s ]',_result_location_folder)
#                        shutil.rmtree(_result_location_folder)
#                gc3utils.log.info('get_results\t\t\t[ ok ]')
#                return [True,retval[1]]
#            else:
#                return [False,retval[1]]
#        except:
#            gc3utils.log.critical('Failure in retrieving results')
#            raise

    def GetResourceStatus(self):
        gc3utils.log.debug("Returning information of local resoruce")
        return Resource(resource_name=self._resource['resource_name'],total_cores=self._resource['ncores'],memory_per_core=self._resource['memory_per_core'])
