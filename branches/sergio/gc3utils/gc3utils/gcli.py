#!/usr/bin/env python

__author__="Sergio Maffioletti (sergio.maffioletti@gc3.uzh.ch)"
__date__="01 May 2010"
__copyright__="Copyright 2009 2011 Grid Computing Competence Center - UZH/GC3"
__version__="0.3"

from utils import *
import sys
import os
import logging
import ConfigParser
from optparse import OptionParser
from ArcLRMS import *
from SshLRMS import *
import Resource
import Default
import Scheduler
import Job
import Application

homedir = os.path.expandvars('$HOME')
rcdir = homedir + "/.gc3"
default_config_file_location = rcdir + "/config"
default_joblist_file = rcdir + "/.joblist"
default_joblist_lock = rcdir + "/.joblist_lock"
default_job_folder_location="$PWD"
default_wait_time = 3

ARC_LRMS = 1
SGE_LRMS = 2

class Gcli:

    SMSCG_AUTHENTICATION = 1
    SSH_AUTHENTICATION = 2

    def __init__(self, defaults, resource_list):
        try:
            if ( len(resource_list) == 0 ):
                raise Exception('could not read any valid resource configuration from config file')
            self._resources = resource_list
            self._defaults = defaults
        except:
            raise

#========== Start check_authentication ===========
    def check_authentication(self,authentication_type):
        if (authentication_type is Gcli.SMSCG_AUTHENTICATION):
            # Check grid access
            try:
                logging.debug('check_authentication for SMSCG')
                if ( (not utils.check_grid_authentication()) | (not utils.check_user_certificate()) ):
                    logging.error('grid credential expired')
                    return False
                return True
            except:
                return False

        if (authentication_type is Gcli.SSH_AUTHENTICATION):
            # Check ssh access
            try:
                logging.debug('check_authentication for SSH')
                if (not utils.check_ssh_authentication()):
                    logging.error('ssh-agent not active')
                    return False
                return True
            except:
                return False

        logging.error("Unknown requested authentication type [%d]",authentication_type)
        raise Exception('Unknown requested authentication type')

#========== Start enable_authentication ===========
    def enable_authentication(self,authentication_type):
        if (authentication_type is Gcli.SMSCG_AUTHENTICATION):
            # Getting AAI username
            #        _aaiUserName = None
            try:
                _aaiUserName = None
                
                self.AAI_CREDENTIAL_REPO = os.path.expandvars(self.AAI_CREDENTIAL_REPO)
                logging.debug('checking AAI credential file [ %s ]',self.AAI_CREDENTIAL_REPO)
                if ( os.path.exists(self.AAI_CREDENTIAL_REPO) & os.path.isfile(self.AAI_CREDENTIAL_REPO) ):
                    logging.debug('Opening AAI credential file in %s',self.AAI_CREDENTIAL_REPO)
                    _fileHandle = open(self.AAI_CREDENTIAL_REPO,'r')
                    _aaiUserName = _fileHandle.read()
                    _aaiUserName = _aaiUserName.rstrip("\n")
                    logging.debug('_aaiUserName: %s',_aaiUserName)
                utils.renew_grid_credential(_aaiUserName)
            except:
                logging.critical('Failed renewing grid credential [%s]',sys.exc_info()[1])
                return False
            return True
        if (authentication_type is Gcli.SSH_AUTHENTICATION):
            return True

        logging.error("Unknown requested authentication type [%d]",authentication_type)
        raise Exception('Unknown requested authentication type')


#========== Start gsub ===========
    def gsub(self, application_obj):
        # Obsolete: def gsub(self, application_to_run, input_file, selected_resource, job_local_dir, cores, memory, walltime):
        # returns an object of type Job
        # throw an exception if the method fails or if the Job object cannot be built successfully
        
        # gsub workflow:
        #    check input files from application
        #    create list of LRMSs
        #    create unique_token
        #    do Brokering
        #    submit job
        #    return job_obj
                
        # Parsing passed arguments
        # RFR: this should be application responsibility ?
        if (not check_inputfile(application_obj.input_file_name)):
            logging.critical('Input file argument\t\t\t[ failed ]'+application_obj.input_file_name)
            raise Exception('invalid input-file argument')
            
        logging.debug('checked inputfile')
        logging.debug('input_file: %s',application_obj.input_file)
        logging.debug('application tag: %s',application_obj.application_tag)
        logging.debug('application arguments: %s',application_obj.application_arguments)
        logging.debug('default_job_folder_location: %s',self._defaults.job_folder_location)
        logging.debug('requested cores: %s',str(application_obj.requested_cores))
        logging.debug('requested memory: %s GB',str(application_obj.requested_memory))
        logging.debug('requested walltime: %s hours',str(application_obj.requested_walltime))
        logging.info('Parsing arguments\t\t[ ok ]')

        # At this point self._resources contains either a list or a single LRMS reference 
        # Initialize LRMSs

        _lrms_list = []

        for _resource in self._resources:
            try:
                _lrms_list.append(self.__get_LRMS(_resource.name))
            except:
                logging.error('Exception creating LRMS instance %s',_resource.type)
                continue

        if ( len(_lrms_list) == 0 ):
            logging.critical('Could not initialize ANY lrms resource')
            raise Exception('no available LRMS found')

        logging.debug('Performing brokering')
        # decide which resource to use
        # (Job) = (Scheduler).PerformBrokering((Resource)[],(Application))
        try:
#            _selected_lrms = Scheduler.do_brokering(_lrms_list,application_obj)
            _selected_lrms_list = Scheduler.do_brokering(_lrms_list,application_obj)
            if len(_selected_lrms_list) > 0:
                logging.debug('Scheduler returned %d LRMS',len(_selected_lrms_list))
                logging.info('Select LRMS\t\t\t\t\t[ ok ]')
            else:
                raise BrokerException('Broker did not returned any valid LRMS')
        except:
            logging.critical('Failed in scheduling')
            raise

        # Stop here - debugging
        raise Exception('bye bye')

        # This method also takes care of crating the unique_token's folder
        try:
            unique_token = __create_job_unique_token(application_obj.inputfile,_selected_lrms.resource_name)
        except:
            logging.critical('Failed creating unique_token')
            raise

        # resource_name.submit_job(input, unique_token, application, lrms_log) -> returns [lrms_jobid,lrms_log]
        logging.debug('Submitting job with %s %s %s %s',unique_token, application_to_run, input_file, self.defaults['lrms_log'])

        # Scheduler.do_brokering should return a sorted list of valid lrms
        job = None
        
        for lrms in _selected_lrms_list:
            try:
                a = Auth(lrms.auth_type)
                job = lrms.submit_job(application_object)
                if job.is_valid():
                    job.insert('unique_token',unique_token)
                    logging.info('Submission process to LRMS backend\t\t\t[ ok ]')
            except AuthenticationFailed:
                continue
            except LRMSException:
                logging.critical('Failed Submitting job: %s',sys.exc_info()[1])
                continue

        if job is None:
            raise LRMSException('Failed submitting application to any LRMS')
#===============================================================================
# 
# 
# 
#        # resource_name.submit_job(input, unique_token, application, lrms_log) -> returns [lrms_jobid,lrms_log]
#        logging.debug('Submitting job with %s %s %s %s',unique_token, application_to_run, input_file, self.defaults['lrms_log'])
#        try:
#            job_obj = _selected_lrms.submit_job(application_obj)
#            job_obj.insert('unique_token',unique_token)
#            logging.info('Submission process to LRMS backend\t\t\t[ ok ]')
#        except:
#            logging.critical('Failed Submitting job: %s',sys.exc_info()[1])
#            raise
#===============================================================================

        if self.__log_job(job):
            logging.info('Dumping lrms log information\t\t\t[ ok ]')

        # return an object of type Job which contains also the unique_token
        return job

#======= Start gstat =========
# First variant gstat with no job obj passed
# list status of all jobs
# How to get the list of jobids ?
# We need an internal method for this
# This method returns a list of job objs 
    def gstat(self):
        job_return_list = []

        try:
            _list_of_runnign_jobs = __get_list_running_jobs()
        except:
            logging.debug('Failed obtaining list of running jobs %s',str(sys.exc_info()[1]))
            raise
            
        for _running_job in _list_of_runnign_jobs:
            try:
                job_return_list.append(gstat(_running_job))
            except:
                logging.debug('Exception when trying getting status of job %s: %s',_running_job.unique_token,str(sys.exc_info()[1]))
                continue                                

        return job_return_li    

    def gstat(self, job_obj):
        # returns an updated job object
        # create instance of LRMS depending on resource type associated to job
        
        _lrms = self.__get_LRMS(job_obj.resource_name)

        # check job status
        return _lrms.check_status(job_obj)


#====== Gget =======
    def gget(self, job_obj):
        # Create LRMS associated to job_obj
        # return LRMS.get_status(job_obj)
        
        # RFR: Do we really need something like this ?
        if job_obj.status is Job.FINISHED:
            return job_obj

        _lrms = self.__get_LRMS(job_obj.resource_name)
        return _lrms.get_results(job_obj)  


#=========     INTERNAL METHODS ============

    def __create_job_unique_token(self,input_file_name,resource_name):
        try:
            # create_unique_token
            unique_token = create_unique_token(input_file,resource_name)
            
            logging.debug('Generate Unique token: %s',unique_token)
            logging.info('Generate Unique token\t\t\t[ ok ]')
            
            # creating folder for job's session
            job_folder_location = os.path.expandvars(self._defaults.job_folder_location)
            
            logging.debug('creating folder for job session: %s/%s',self._defaults.job_folder_location,unique_token)
            os.mkdir(self._defaults.job_folder_location+'/'+unique_token)
            
            logging.info('Create job folder\t\t\t[ ok ]')
            return self._defaults.job_folder_location+'/'+unique_token
        except:
            logging.error('Failed creating job unique_token')
            raise


    def __log_job(self, job_obj):
        # dumping lrms_jobid
        # not catching the exception as this is supposed to be a fatal failure;
        # thus propagated to gsub's main try
        _fileHandle = open(job_obj.unique_token+'/'+self._defaults.job_file,'w')
        _fileHandle.write(job_obj.resource_name+'\t'+job_obj.lrms_jobid)
        _fileHandle.close()

        # if joblist_file & joblist_lock are not defined, use default
        #RFR: WHY DO I NEED THIS ?
        try:
            joblist_file
        except NameError:
            joblist_file = os.path.expandvars(self._defaults.joblist_file)

        try:
            joblist_lock
        except NameError:
            joblist_lock = os.path.expandvars(self._defaults.joblist_lock)



        # if joblist_file does not exist, create it
        if not os.path.exists(joblist_file):
            try:
                open(joblist_file, 'w').close()
                logging.debug(joblist_file + ' did not exist... created successfully.')
            except:
                logging.error('Failed opening joblist_file')
                return False

        logging.debug('appending jobid to joblist file as specified in defaults')
        try:
            # appending jobid to .jobs file as specified in defaults
            logging.debug('obtaining lock')
            if ( obtain_file_lock(joblist_file,joblist_lock) ):
                _fileHandle = open(joblist_file,'a')
                _fileHandle.write(job_obj.unique_token+'\n')
                _fileHandle.close()
            else:
                logging.error('Failed obtain lock')
                return False

        except:
            logging.error('Failed in appending current jobid to list of jobs in %s',self._defaults.joblist_file)
            logging.debug('Exception %s',sys.exc_info()[1])
            return False

        # release lock
        if ( (not release_file_lock(joblist_lock)) & (os.path.isfile(joblist_lock)) ):
            logging.error('Failed removing lock file')
            return False

        return True

    def __get_list_running_jobs(self):
        # This internal method is supposed to:
        # get a list of jobs still running
        # create instances of jobs
        # group them in a list
        # return such a list

        return __get_list_running_jobs_filesystem()
    
    def __get_list_running_jobs_filesystem(self):
        # This implementation is based on persistent information on the filesystem 
        # Read content of .joblist and return gstat for each of them
        
        try:
            # Read joblist_file get a list of unique_tokens and resource_names
            _joblist  = open(self._defaults.joblist_file,'r')
            _joblist.seek(0)
            _unique_tokens_list = re.split('\n',_joblist.read())
            _joblist.close()
        except:
            logging.debug('Failed reading joblist file in %s',self._defaults.joblist_file)
            raise
            
        # for each unique_token retrieve job information and create instance of Job obj
        _job_list = []

        for _unique_token in _unique_tokens_list:
            _job_list.append(utils.get_job_from_filesystem(unique_token,self._defaults.job_file))

        # Shall we check whether the list is empty or not ?
        return _job_list

#======= Static methods =======

    def __get_LRMS(self,resource_name):
        _lrms = None
        
        for _resource in self._resources:
            if _resource.name is resource_name:
                # there's a matching resource
                logging.debug('Creating instance of type %s for %s',_resource.type,_resource.frontend)
                try:
                    if _resource.type is self.ARC_LRMS:
                        _lrms = ArcLrms(_resource)
                    elif _resource.type is self.SGE_LRMS:
                        _lrms = SshLrms(_resource)
                    else:
                        logging.error('Unknown resource type %s',_resource.type)
                        raise Exception('Unknown resource type')
                except:
                    logging.error('Exception creating LRMS instance %s',_resource.type)
                    raise
        
        if _lrms is None:
            raise Exception('Failed finding resource associated to job')

        return _lrms
#====== End