#!/usr/bin/env python

__author__="Sergio Maffioletti (sergio.maffioletti@gc3.uzh.ch)"
__date__="01 May 2010"
__copyright__="Copyright 2009 2011 Grid Computing Competence Center - UZH/GC3"
__version__="0.3"

from utils import *
import sys
import os
from ArcLRMS import *
from SshLRMS import *
import Resource
import Default
import Scheduler
import Job
import Application
from Exceptions import *
import Authorization

class Gcli:

    def __init__(self, defaults, resource_list):
        try:
            if ( len(resource_list) == 0 ):
                raise Exception('0 lenght resource list')
            self._resources = resource_list
            self._defaults = defaults
        except:
            raise

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
            gc3utils.log.critical('Input file argument\t\t\t[ failed ]'+application_obj.input_file_name)
            raise Exception('invalid input-file argument')
            
        gc3utils.log.debug('checked inputfile')
        gc3utils.log.debug('input_file: %s',application_obj.input_file_name)
        gc3utils.log.debug('application tag: %s',application_obj.application_tag)
        gc3utils.log.debug('application arguments: %s',application_obj.application_arguments)
        gc3utils.log.debug('default_job_folder_location: %s',self._defaults.job_folder_location)
        gc3utils.log.debug('requested cores: %s',str(application_obj.requested_cores))
        gc3utils.log.debug('requested memory: %s GB',str(application_obj.requested_memory))
        gc3utils.log.debug('requested walltime: %s hours',str(application_obj.requested_walltime))
        gc3utils.log.info('Parsing arguments\t\t[ ok ]')

        gc3utils.log.debug('Performing brokering')
        # decide which resource to use
        # (Resource)[] = (Scheduler).PerformBrokering((Resource)[],(Application))
        try:
            _selected_resource_list = Scheduler.Scheduler.do_brokering(self._resources,application_obj)
            if len(_selected_resource_list) > 0:
                gc3utils.log.debug('Scheduler returned %d matched resources',len(_selected_resource_list))
                gc3utils.log.info('do_brokering\t\t\t\t\t[ ok ]')
            else:
                raise BrokerException('Broker did not returned any valid LRMS')
        except:
            gc3utils.log.critical('Failed in scheduling')
            raise

        # At this point self._resources contains either a list or a single LRMS reference
        # Initialize LRMSs

        _lrms_list = []
        
        for _resource in _selected_resource_list:
            try:
                _lrms_list.append(self.__get_LRMS(_resource.name))
            except:
                gc3utils.log.error('Failed creating LRMS %s',_resource.type)
                gc3utils.log.debug('%s',sys.exc_info()[1])
                continue
            
        if ( len(_lrms_list) == 0 ):
            gc3utils.log.critical('Could not initialize ANY lrms resource')
            raise Exception('no available LRMS found')

        # This method also takes care of crating the unique_token's folder
        try:
            unique_token = self.__create_job_unique_token(os.path.expandvars(application_obj.job_local_dir),application_obj.input_file_name,application_obj.application_tag)
        except:
            gc3utils.log.critical('Failed creating unique_token')
            raise

        # resource_name.submit_job(input, unique_token, application, lrms_log) -> returns [lrms_jobid,lrms_log]
#        gc3utils.log.debug('Submitting job with %s %s %s %s',unique_token, application_to_run, input_file, self.defaults['lrms_log'])

        # Scheduler.do_brokering should return a sorted list of valid lrms
        job = None
        
        for lrms in _lrms_list:
            try:
                a = Authorization.Auth()
                a.get(lrms._resource.type)
                job = lrms.submit_job(application_obj)
                if job.is_valid():
                    job.insert('unique_token',unique_token)
                    gc3utils.log.info('Submission process to LRMS backend\t\t\t[ ok ]')
                    # job submitted; leave loop
                    break
            except Exceptions.AuthenticationException:
                continue
            except LRMSException:
                gc3utils.log.critical('Failed Submitting job: %s',sys.exc_info()[1])
                continue

        if job is None:
            raise LRMSException('Failed submitting application to any LRMS')

        if self.__log_job(job):
            gc3utils.log.info('Dumping lrms log information\t\t\t[ ok ]')

        # return an object of type Job which contains also the unique_token
        return job

#======= Start gstat =========
# First variant gstat with no job obj passed
# list status of all jobs
# How to get the list of jobids ?
# We need an internal method for this
# This method returns a list of job objs 
    def gstat(self, job_obj):
        
        if job_obj is None:
            job_return_list = []

            try:
                _list_of_runnign_jobs = self.__get_list_running_jobs()
            except:
                gc3utils.log.debug('Failed obtaining list of running jobs %s',str(sys.exc_info()[1]))
                raise
        else:
            _list_of_runnign_jobs = job_obj

        for _running_job in _list_of_runnign_jobs:
            try:
                job_return_list.append(self.__gstat(_running_job))
            except:
                gc3utils.log.debug('Exception when trying getting status of job %s: %s',_running_job.unique_token,str(sys.exc_info()[1]))
                continue                                

        return job_return_li    

    def __gstat(self, job_obj):
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
    def glist(self, shortview):
        """List status of jobs."""
        global default_joblist_location

        try:

            # print the header
            if shortview == False:
                # long view
                print "%-100s %-20s %-10s" % ("[unique_token]","[name]","[status]")
            else:
                # short view
                print "%-20s %-10s" % ("[name]","[status]")

            # look in current directory for jobdirs
            jobdirs = []
            dirlist = os.listdir("./")
            for dir in dirlist:
                if os.path.isdir(dir) == True:
                    if os.path.exists(dir + "/.lrms_jobid") and os.path.exists(dir + "/.lrms_log"):
                        logging.debug(dir + "is a jobdir")
                        jobdirs.append(dir)

            # break down unique_token into vars
            for dir in jobdirs:
                unique_token = dir
                name =  '-'.join( unique_token.split('-')[0:-3])
                if os.path.exists(dir + "/.finished"):
                    status = "FINISHED"
                else:
                    retval,job_status_list = self.gstat(unique_token)
                    first = job_status_list[0]
                    status = first[1].split(' ')[1]

                if shortview == False:
                    # long view
                    print '%-100s %-20s %-10s' % (unique_token, name, status)
                else:
                    # short view
                    print '%-20s %-10s' % (name, status)

            logging.debug('Jobs listed.')

        except Exception, e:
            logging.critical('Failure in listing jobs')
            raise e

        return
    def gkill(self, unique_token):
        """Kill a job, and optionally remove the local job directory."""

        # todo : may be some redundancy to remove here
        
        global default_job_folder_location
        global default_joblist_lock
        global default_joblist_location
        
        if not check_jobdir(unique_token):
            raise Exception('invalid jobid')
        
        # check .finished file
        if check_inputfile(unique_token+'/'+self.defaults['lrms_finished']):
            logging.error('Job already finished.')
            return 

        # todo : may be some redundancy to remove here
            
        _fileHandle = open(unique_token+'/'+self.defaults['lrms_jobid'],'r')
        _raw_resource_info = _fileHandle.read()
        _fileHandle.close()

        _list_resource_info = re.split('\t',_raw_resource_info)

        logging.debug('lrms_jobid file returned %s elements',len(_list_resource_info))

        if ( len(_list_resource_info) != 2 ):
            raise Exception('failed to retrieve jobid')
        
        _resource = _list_resource_info[0]
        _lrms_jobid = _list_resource_info[1]
        
        logging.debug('frontend: [ %s ] jobid: [ %s ]',_resource,_lrms_jobid)
        logging.info('reading lrms_jobid info\t\t\t[ ok ]')

        if ( _resource in self.resource_list ):
            logging.debug('Found match for resource [ %s ]',_resource)
            logging.debug('Creating lrms instance')
            resource = self.resource_list[_resource]

        if ( resource['type'] == "arc" ):
            lrms = ArcLrms(resource)
        elif ( resource['type'] == "ssh"):
            lrms = SshLrms(resource)
        else:
            logging.error('Unknown resource type %s',resource['type'])
            
        (retval,lrms_log) = lrms.KillJob(_lrms_jobid)

#        except Exception, e:
#            logging.error('Failed to send qdel request to job: ' + unique_token)
#            raise e

        return

    def __create_job_unique_token(self,job_folder_location,input_file_name,resource_name):
        try:
            # create_unique_token
            unique_id = create_unique_token(input_file_name,resource_name)

            unique_token = job_folder_location+'/'+unique_id
            
            gc3utils.log.debug('Generate Unique token: %s',unique_token)
            gc3utils.log.info('Generate Unique token\t\t\t[ ok ]')
            
            # creating folder for job's session
#            self._defaults.job_folder_location = os.path.expandvars(self._defaults.job_folder_location)
            
            gc3utils.log.debug('creating folder for job session: %s',unique_token)
            os.makedirs(unique_token)

            gc3utils.log.info('Create job folder\t\t\t[ ok ]')
            return unique_token
        except:
            gc3utils.log.error('Failed creating job unique_token')
            raise


    def __log_job(self, job_obj):
        # dumping lrms_jobid
        # not catching the exception as this is supposed to be a fatal failure;
        # thus propagated to gsub's main try
        try:
            _fileHandle = open(job_obj.unique_token+'/'+Default.JOB_FILE,'w')
            _fileHandle.write(job_obj.resource_name+'\t'+job_obj.lrms_jobid)
            _fileHandle.close()
        except:
            gc3utils.log.error('failed updating job lrms_id')

        try:
            _fileHandle = open(job_obj.unique_token+'/'+Default.JOB_LOG,'w')
            _fileHandle.write(job_obj.log)
            _fileHandle.close()
        except:
            gc3utils.log.error('failed updating job log')
                        
        # if joblist_file & joblist_lock are not defined, use default
        #RFR: WHY DO I NEED THIS ?
        try:
            joblist_file
        except NameError:
            joblist_file = os.path.expandvars(Default.JOBLIST_FILE)

        try:
            joblist_lock
        except NameError:
            joblist_lock = os.path.expandvars(Default.JOBLIST_LOCK)

        # if joblist_file does not exist, create it
        if not os.path.exists(joblist_file):
            try:
                open(joblist_file, 'w').close()
                gc3utils.log.debug(joblist_file + ' did not exist... created successfully.')
            except:
                gc3utils.log.error('Failed opening joblist_file')
                return False

        gc3utils.log.debug('appending jobid to joblist file as specified in defaults')
        try:
            # appending jobid to .jobs file as specified in defaults
            gc3utils.log.debug('obtaining lock')
            if ( obtain_file_lock(joblist_file,joblist_lock) ):
                _fileHandle = open(joblist_file,'a')
                _fileHandle.write(job_obj.unique_token+'\n')
                _fileHandle.close()
            else:
                gc3utils.log.error('Failed obtain lock')
                return False

        except:
            gc3utils.log.error('Failed in appending current jobid to list of jobs in %s',Default.JOBLIST_FILE)
            gc3utils.log.debug('Exception %s',sys.exc_info()[1])
            return False

        # release lock
        if ( (not release_file_lock(joblist_lock)) & (os.path.isfile(joblist_lock)) ):
            gc3utils.log.error('Failed removing lock file')
            return False

        return True

    def __get_list_running_jobs(self):
        # This internal method is supposed to:
        # get a list of jobs still running
        # create instances of jobs
        # group them in a list
        # return such a list

        return self.__get_list_running_jobs_filesystem()
    
    def __get_list_running_jobs_filesystem(self):
        # This implementation is based on persistent information on the filesystem 
        # Read content of .joblist and return gstat for each of them
        
        try:
            # Read joblist_file get a list of unique_tokens and resource_names
            _joblist  = open(Default.JOBLIST_FILE,'r')
            _joblist.seek(0)
            _unique_tokens_list = re.split('\n',_joblist.read().strip())
            _joblist.close()
        except:
            gc3utils.log.debug('Failed reading joblist file in %s',Default.JOBLIST_FILE)
            raise
            
        # for each unique_token retrieve job information and create instance of Job obj
        _job_list = []

        for _unique_token in _unique_tokens_list:
            _job_list.append(utils.get_job(_unique_token))

        # Shall we check whether the list is empty or not ?
        return _job_list

#======= Static methods =======

    def __get_LRMS(self,resource_name):
        _lrms = None
        
        for _resource in self._resources:
            if _resource.name == resource_name:
                # there's a matching resource
                gc3utils.log.debug('Creating instance of type %s for %s',_resource.type,_resource.frontend)
                try:
                    if _resource.type is Default.ARC_LRMS:
                        _lrms = ArcLrms(_resource)
                    elif _resource.type is Default.SGE_LRMS:
                        _lrms = SshLrms(_resource)
                    else:
                        gc3utils.log.error('Unknown resource type %s',_resource.type)
                        raise Exception('Unknown resource type')
                except:
                    gc3utils.log.error('Exception creating LRMS instance %s',_resource.type)
                    raise
        
        if _lrms is None:
            raise Exception('Failed finding resource associated to job')

        return _lrms
#====== End
