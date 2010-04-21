import sys
import os
import commands
import logging
import tempfile
import getpass
import re
import md5
import time
import ConfigParser
import shutil
import subprocess
from utils import *
from LRMS import LRMS
from Resource import Resource
import Job
import Application

# -----------------------------------------------------
# ARC lrms
#

class ArcLrms(LRMS):

    isValid = 0
    GAMESS_XRSL_TEMPLATE = "$HOME/.gc3/gamess_template.xrsl"
    resource = []

    def __init__(self, resource):
        # check first that manadtory fields are defined
        # if resource['frontend'] == "" means access to the entire arc based infrastructure
        if (resource['type'] == "arc"):
            self._resource = resource
            self.isValid = 1

            self._resource['ncores'] = int(self._resource['ncores'])
            self._resource['memory_per_core'] = int(self._resource['memory_per_core']) * 1000
            self._resource['walltime'] = int(self._resource['walltime'])
            if (self._resource['walltime'] > 0 ):
                # convert from hours to minutes
                self._resource['walltime'] = self._resource['walltime'] * 60

            logging.debug('Init resource %s with %d cores, %d walltime, %d memory',self._resource['resource_name'],self._resource['ncores'],self._resource['walltime'],self._resource['memory_per_core'])



    def submit_job(self, unique_token, application, input_file):
        try:
            # Initialize xrsl from template
            self.GAMESS_XRSL_TEMPLATE = os.path.expandvars(self.GAMESS_XRSL_TEMPLATE)
                
            if ( os.path.exists(self.GAMESS_XRSL_TEMPLATE) & os.path.isfile(self.GAMESS_XRSL_TEMPLATE) ):

                _file_handle = tempfile.NamedTemporaryFile(suffix=".xrsl",prefix="gridgames_arc_")
                logging.debug('tmp file %s',_file_handle.name)

                # getting information from input_file
                _file_name = os.path.basename(input_file)
                _file_name_path = os.path.dirname(input_file)
                _file_name = _file_name.split(".inp")[0]
                logging.debug('Input file path %s dirpath %s from %s',_file_name,_file_name_path,input_file)

                # Modify xrsl template
                # step one: set inputfile references
                _command = "sed -e 's|INPUT_FILE_NAME|"+_file_name+"|g' -e 's|INPUT_FILE_PATH|"+_file_name_path+"|g' < "+self.GAMESS_XRSL_TEMPLATE+" > "+_file_handle.name

                # Cleaning up
                _file_handle.close()

                logging.debug('preparing SED command: %s',_command)
                retval = commands.getstatusoutput(_command)

                if ( retval[0] != 0 ):
                    # Failed somehow
                    logging.error("Create XRSL\t\t[ failed ]")
                    logging.debug(retval[1])
                    # Shall we dump anyway into lrms_log befor raising ?
                    raise Exception('failed creating submission file')

                _command = ""

                if ( self._resource['walltime'] > 0 ):
                    logging.debug('setting walltime...')
                    _command = "(cputime=\""+str(self._resource['walltime'])+"\")\n"

                if ( self._resource['ncores'] > 0 ):
                    logging.debug('setting cores...')
                    _command = _command+"(count=\""+str(self._resource['ncores'])+"\")\n"

                if ( self._resource['memory_per_core'] > 0 ):
                    logging.debug('setting memory')
                    _command = _command+"(memory=\""+str(self._resource['memory_per_core'])+"\")"

                if ( _command != "" ):
                    _command = "echo '"+_command+"' >> "+_file_handle.name
                    logging.debug('preparing echo command: %s',_command)
                    retval = commands.getstatusoutput(_command)
                    if ( retval[0] != 0 ):
                        logging.error("Create XRSL\t\t[ failed ]")
                        raise Exception('failed creating submission file')

                logging.debug('checking resource [ %s ]',self._resource['frontend'])
                # Ready for real submission
                if ( self._resource['frontend'] == "" ):
                    # frontend not defined; use the entire arc-based infrastructure
                    _command = "ngsub -d2 -f "+_file_handle.name
                else:
                    _command = "ngsub -d2 -c "+self._resource['frontend']+" -f "+_file_handle.name

                logging.debug('Running ARC command [ %s ]',_command)
            
                retval = commands.getstatusoutput(_command)

                jobid_pattern = "Job submitted with jobid: "

                if ( ( retval[0] != 0 ) | ( jobid_pattern not in retval[1] ) ):
                    # Failed somehow
                    logging.error("ngsub command\t\t[ failed ]")
                    logging.debug(retval[1])
                    raise Exception('failed submitting to LRMS')

                # assuming submit successfull
                logging.debug("ngsub command\t\t[ ok ]")
                logging.debug(retval[1])

                # Extracting ARC jobid
                lrms_jobid = re.split(jobid_pattern,retval[1])[1]
                logging.debug('Job submitted with jobid: %s',lrms_jobid)

                return [lrms_jobid,retval[1]]

            else:
                logging.critical('XRSL file not found %s',self.GAMESS_XRSL_TEMPLATE)
                raise Exception('template file for submission scritp not found')
        except:
            logging.critical('Failure in submitting')
            raise

    def check_status(self, lrms_jobid):
#        submitted_list = ['ACCEPTING','SUBMITTING','PREPARING']
        running_list = ['INLRMS:Q','INLRMS:R','EXECUTED', 'ACCEPTING','SUBMITTING','PREPARING']
        finished_list = ['FINISHED', 'FAILED']
#        failed_list = ['FAILED']
        try:
            # Ready for real submission
            _command = "ngstat "+lrms_jobid

            logging.debug('Running ARC command [ %s ]',_command)

            retval = commands.getstatusoutput(_command)
            # jobstatusunknown_pattern = "This job was only very recently"
            jobstatusunknown_pattern = "Job information not found"
            jobstatusremoved_pattern = "Job information not found"
            jobstatusok_pattern = "Status: "
            jobexitcode_pattern = "Exit Code: "
            if ( retval[0] != 0 ):
                # | ( jobstatus_pattern not in retval[1] ) ):
                # Failed somehow
                logging.error("ngstat command\t\t[ failed ]")
                logging.debug(retval[1])
                raise Exception('failed checking status to LRMS')

            if ( jobstatusunknown_pattern in retval[1] ):
                jobstatus = "Status: RUNNING"
#            elif ( jobstatusremoved_pattern in retval[1] ):
#                jobstatus = "Status: FINISHED"
            elif ( jobstatusok_pattern in retval[1] ):

                # Extracting ARC job status
                lrms_jobstatus = re.split(jobstatusok_pattern,retval[1])[1]
                lrms_jobstatus = re.split("\n",lrms_jobstatus)[0]

                logging.debug('lrms_jobstatus\t\t\t[ %s ]',lrms_jobstatus)

                if ( lrms_jobstatus in running_list ):
                    jobstatus = "Status: RUNNING"
                elif ( lrms_jobstatus in finished_list ):
                    jobstatus = "Status: FINISHED"
#                if ( lrms_jobstatus in submitted_list ):
#                    jobstatus = "Status: SUBMITTED"
#                elif ( lrms_jobstatus in running_list ):
#                    jobstatus = "Status: RUNNING"
#                elif ( ( lrms_jobstatus in finished_list ) | ( lrms_jobstatus in failed_list )):
#                    lrms_exitcode = re.split(jobexitcode_pattern,retval[1])[1]
#                    lrms_exitcode = re.split("\n",lrms_exitcode)[0]
#                    jobstatus = "Status: FINISHED\nExit Code: "+lrms_exitcode
                else:
                    jobstatus = "Status: [ "+lrms_jobstatus+" ]"

            return [jobstatus,retval[1]]

        except:
            logging.critical('Failure in checking status')
            raise

    def get_results(self,lrms_jobid,job_dir):
        try:
            result_location_pattern="Results stored at "
            
            _command = "ngget -keep -s FINISHED -d 2 -dir "+job_dir+" "+lrms_jobid

            logging.debug('Running ARC command [ %s ]',_command)

            job_results_retrieved_pattern = "successfuly downloaded: 0"

            retval = commands.getstatusoutput(_command)
            if ( ( retval[0] != 0 ) ):
                # Failed somehow
                logging.error("ngget command\t\t[ failed ]")
                logging.debug(retval[1])
                raise Exception('failed getting results from LRMS')

            if ( result_location_pattern in retval[1] ):
                _result_location_folder = re.split(result_location_pattern,retval[1])[1]
                _result_location_folder = re.split("\n",_result_location_folder)[0]
                logging.debug('Moving result data from [ %s ]',_result_location_folder)
                if ( os.path.isdir(_result_location_folder) ):
                    retval = commands.getstatusoutput("cp -ap "+_result_location_folder+"/* "+job_dir)
                    if ( retval[0] != 0 ):
                        logging.error('Failed copying results data from [ %s ] to [ %s ]',_result_location_folder,job_dir)
                    else:
                        logging.info('Copying results\t\t[ ok ]')
                        logging.debug('Removing [ %s ]',_result_location_folder)
                        shutil.rmtree(_result_location_folder)
                logging.info('get_results\t\t\t[ ok ]')
                return [True,retval[1]]
            else:
                return [False,retval[1]]
        except:
            logging.critical('Failure in retrieving results')
            raise

    def GetResourceStatus(self):
        logging.debug("Returning information of local resoruce")
        return Resource(resource_name=self._resource['resource_name'],total_cores=self._resource['ncores'],memory_per_core=self._resource['memory_per_core'])
