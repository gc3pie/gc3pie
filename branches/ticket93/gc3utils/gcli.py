#!/usr/bin/env python

__author__="Sergio Maffioletti (sergio.maffioletti@gc3.uzh.ch)"
__date__="01 May 2010"
__copyright__="Copyright 2009, 2010 Grid Computing Competence Center - UZH/GC3"
__version__="0.3"

import utils
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
from fnmatch import fnmatch
import Authorization

class Gcli:

    def __init__(self, defaults, resource_list, auto_enable_auth):
        if ( len(resource_list) == 0 ):
            raise NoResources('Resource list has length 0')
        self._resources = resource_list
        self._defaults = defaults
        self.auto_enable_auth = auto_enable_auth

    def select_resource(self, match):
        """
        Alter the configured list of resources, and retain only those
        that satisfy predicate `match`.

        Argument `match` can be:

          - either a function (or a generic callable) that is passed
            each `Resource` object in turn, and should return a
            boolean indicating whether the resources should be kept
            (`True`) or not (`False`);

          - or it can be a string: only resources whose name matches
            (wildcards "*" and "?" are allowed) are retained.
        """
        try:
            self._resources = [ res for res in self._resources if match(res) ]
        except:
            # `match` is not callable, then assume it's a 
            # glob pattern and select resources whose name matches
            self._resources = [ res for res in self._resources
                                if fnmatch(res.name, match) ]

#========== Start gsub ===========
    def gsub(self, application_obj, **kw):
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        # gsub workflow:
        #    check input files from application
       #    create list of LRMSs
        #    create unique_token
        #    do Brokering
        #    submit job
        #    return job_obj
        
        # Parsing passed arguments
        gc3utils.log.debug('input_file(s): %s',application_obj.inputs)
        gc3utils.log.debug('application tag: %s',application_obj.application_tag)
        gc3utils.log.debug('application arguments: %s',application_obj.arguments)
        gc3utils.log.debug('default_job_folder_location: %s',self._defaults.job_folder_location)
        gc3utils.log.debug('requested cores: %s',str(application_obj.requested_cores))
        gc3utils.log.debug('requested memory: %s GB',str(application_obj.requested_memory))
        gc3utils.log.debug('requested walltime: %s hours',str(application_obj.requested_walltime))

        gc3utils.log.debug('Instantiating LRMSs')
        _lrms_list = []
        for _resource in self._resources:
            try:
                _lrms_list.append(self.__get_LRMS(_resource.name))
            except:
                # log exceptions but ignore them
                gc3utils.log.warning("Failed creating LRMS for resource '%s' of type '%s'",
                                     _resource.name, _resource.type)
                gc3utils.log.debug('gcli.py:gsub() got exception:', exc_info=True)
                continue
            
        if ( len(_lrms_list) == 0 ):
            raise NoResources("Could not initialize any computational resource - please check log and configuration file.")

        gc3utils.log.debug('Performing brokering')
        # decide which resource to use
        # (Resource)[] = (Scheduler).PerformBrokering((Resource)[],(Application))
        _selected_lrms_list = Scheduler.do_brokering(_lrms_list,application_obj)
        gc3utils.log.debug('Scheduler returned %d matching resources',
                           len(_selected_lrms_list))
        if 0 == len(_selected_lrms_list):
            raise NoResources("Could not select any compatible computational resource - please check log and configuration file.")

        # Scheduler.do_brokering should return a sorted list of valid lrms
        job = None
        for lrms in _selected_lrms_list:
            try:
                a = Authorization.Auth(auto_enable_auth)
                a.get(lrms._resource.type)
                job = lrms.submit_job(application_obj)
                if job.is_valid():
                    gc3utils.log.info('Successfully submitted process to LRMS backend')
                    # job submitted; leave loop
                    job.job_local_dir = application_obj.job_local_dir
                    break
            except AuthenticationException:
                # ignore authentication errors: e.g., we may fail some SSH connections but succeed in others
                gc3utils.log.debug("Authentication error in submitting to resource '%s'" 
                                   % lrms._resource.name)
                continue
            except LRMSException:
                gc3utils.log.error("Error in submitting job to resource '%s'", 
                                   lrms._resource.name, exc_info=True)
                continue
        if job is None or not job.is_valid():
            raise LRMSException('Failed submitting application to any LRMS')

        # return an object of type Job which contains also the unique_token
        return job

#======= Start gstat =========
# First variant gstat with no job obj passed
# list status of all jobs
# How to get the list of jobids ?
# We need an internal method for this
# This method returns a list of job objs 
    def gstat(self, job_obj, **kw):
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
       
        job_return_list = [] 
        if job_obj is None:
            try:
                _list_of_runnign_jobs = self.__get_list_running_jobs()
            except:
                gc3utils.log.debug('Failed obtaining list of running jobs %s',str(sys.exc_info()[1]))
                raise
        else:
            _list_of_runnign_jobs = [job_obj]

        for _running_job in _list_of_runnign_jobs:
            try:
                job_return_list.append(self.__gstat(_running_job, auto_enable_auth))
            except:
                gc3utils.log.debug('Exception when trying getting status of job %s: %s',_running_job.unique_token,str(sys.exc_info()[1]))
                continue                                

        return job_return_list

    def __gstat(self, job_obj, auto_enable_auth):
        # returns an updated job object
        # create instance of LRMS depending on resource type associated to job
        
        _lrms = self.__get_LRMS(job_obj.resource_name)

        # gc3utils.log.debug('current job status is %d' % job_obj.status)

        if not ( job_obj.status == gc3utils.Job.JOB_STATE_COMPLETED or job_obj.status == gc3utils.Job.JOB_STATE_FINISHED or job_obj.status == gc3utils.Job.JOB_STATE_FAILED or job_obj.status == gc3utils.Job.JOB_STATE_DELETED ):
            # check job status
            # gc3utils.log.debug('checking job status')
            a = Authorization.Auth(auto_enable_auth)
            a.get(_lrms._resource.type)                                
            job_obj = _lrms.check_status(job_obj)

        return job_obj

#====== Gget =======
    def gget(self, job_obj, **kw):
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        _lrms = self.__get_LRMS(job_obj.resource_name)

        a = Authorization.Auth(auto_enable_auth)
        a.get(_lrms._resource.type)
        #job_obj = _lrms.get_results(job_obj)

        try:
            return  _lrms.get_results(job_obj)
        except LRMSUnrecoverableError:
            job_obj.status = gc3utils.Job.JOB_STATE_FAILED
            return job_obj
        
#====== Glist =======
    def glist(self,resource_name, **kw):
        """ List status of a give resource."""
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

#        resource = self.__get_Resource(resource_name)
#        if resource is None:
#            raise Exceptions.ResourceNotFoundError('Resource not found')
        
        _lrms = self.__get_LRMS(resource_name)

        a = Authorization.Auth(auto_enable_auth)
        a.get(_lrms._resource.type)

        return  _lrms.get_resource_status()

#=========     INTERNAL METHODS ============
#    def __get_Resource(self,resource_name):
#        for index in range(0,len(self._resource)):
#            if self._resource[index].name == resource_name
#            return self._resource[index]
#        return None


    def _glist(self, shortview):
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
    def gkill(self, job_obj, **kw):
        """Kill a job, and optionally remove the local job directory."""

        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        _lrms = self.__get_LRMS(job_obj.resource_name)

        a = Authorization.Auth(auto_enable_auth)
        a.get(_lrms._resource.type)

        job_obj = _lrms.cancel_job(job_obj)
        gc3utils.log.debug('setting job status to DELETED')
        job_obj.status =  gc3utils.Job.JOB_STATE_DELETED
        return job_obj

        # todo : may be some redundancy to remove here
        
        #global default_job_folder_location
        #global default_joblist_lock
        #global default_joblist_location
        
        #if not check_jobdir(unique_token):
        #    raise Exception('invalid jobid')
        
        # check .finished file
        #if utils.check_inputfile(unique_token+'/'+self.defaults['lrms_finished']):
        #    logging.error('Job already finished.')
        #    return 

        # todo : may be some redundancy to remove here
            
        #_fileHandle = open(unique_token+'/'+self.defaults['lrms_jobid'],'r')
        #_raw_resource_info = _fileHandle.read()
        #_fileHandle.close()

        #_list_resource_info = re.split('\t',_raw_resource_info)

        #logging.debug('lrms_jobid file returned %s elements',len(_list_resource_info))

        #if ( len(_list_resource_info) != 2 ):
        #    raise Exception('failed to retrieve jobid')
        
        #_resource = _list_resource_info[0]
        #_lrms_jobid = _list_resource_info[1]
        
        #logging.debug('frontend: [ %s ] jobid: [ %s ]',_resource,_lrms_jobid)
        #logging.info('reading lrms_jobid info\t\t\t[ ok ]')

        #if ( _resource in self.resource_list ):
        #    logging.debug('Found match for resource [ %s ]',_resource)
        #    logging.debug('Creating lrms instance')
        #    resource = self.resource_list[_resource]

        #if ( resource['type'] == "arc" ):
        #    lrms = ArcLrms(resource)
        #elif ( resource['type'] == "ssh"):
        #    lrms = SshLrms(resource)
        #else:
        #    logging.error('Unknown resource type %s',resource['type'])
            
        #(retval,lrms_log) = lrms.KillJob(_lrms_jobid)

#        except Exception, e:
#            logging.error('Failed to send qdel request to job: ' + unique_token)
#            raise e

        #return

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
            if not os.path.isdir(Default.JOBS_DIR):
                # try to create it first
                gc3utils.log.error('JOBS_DIR %s Not found. creating it' % Default.JOBS_DIR)
                try:
                    os.makedirs(Default.JOBS_DIR)
                except:
                    gc3utils.log.critical('%s',sys.exc_info()[1])
                    raise RetrieveJobsFilesystemError('Failed accessing job dir %s' % Default.JOBS_DIR)

            _jobs_list = os.listdir(Default.JOBS_DIR)

            # for each unique_token retrieve job information and create instance of Job obj
            _job_list = []

            for _job in _jobs_list:
                try:
                    _job_list.append(utils.get_job(_job))
                except:
                    gc3utils.log.error('Failed retrieving job information for %s',_job)
                    gc3utils.log.debug('%s',sys.exc_info()[1])
                    continue

            return _job_list

        except:
            raise


#======= Static methods =======

    def __get_LRMS(self,resource_name):
        _lrms = None
        
        for _resource in self._resources:
            if _resource.name == resource_name:
                # there's a matching resource
                gc3utils.log.debug('Creating instance of type %s for %s', _resource.type, _resource.name)
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
