#!/usr/bin/env python
"""
Top-level interface to Grid functionality.
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
#
# Includes parts adapted from the ``bzr`` code, which is
# copyright (C) 2005, 2006, 2007, 2008, 2009 Canonical Ltd
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
__date__ = '$Date$'


from fnmatch import fnmatch
import os
import sys
import ConfigParser

import gc3libs
from gc3libs.application import Application
from gc3libs.backends.sge import SgeLrms
from gc3libs.backends.arc import ArcLrms
from gc3libs.authentication import Auth
import gc3libs.Default as Default
from gc3libs.Exceptions import *
import gc3libs.Job as Job
import gc3libs.Resource as Resource
import gc3libs.scheduler as scheduler
import gc3libs.utils as utils 


class Gcli:

    def __init__(self, defaults, resource_list, authorization, auto_enable_auth):
        if ( len(resource_list) == 0 ):
            raise NoResources('Resource list has length 0')
        self._resources = resource_list
        self._defaults = defaults
        self.authorization = authorization
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
    def gsub(self, application, job=None, **kw):
        """
        Submit a job running an instance of the given `application`.
        Return the `job` object, modified to refer to the submitted computational job,
        or a new instance of the `Job` class if `job` is `None` (default).
        """
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        # gsub workflow:
        #    check input files from application
        #    create list of LRMSs
        #    create unique_token
        #    do Brokering
        #    submit job
        #    return job_obj
        
        # Parsing passed arguments
        gc3libs.log.debug('input_file(s): %s',application.inputs)
        gc3libs.log.debug('application tag: %s',application.application_tag)
        gc3libs.log.debug('application arguments: %s',application.arguments)
        gc3libs.log.debug('default_job_folder_location: %s',self._defaults.job_folder_location)
        gc3libs.log.debug('requested cores: %s',str(application.requested_cores))
        gc3libs.log.debug('requested memory: %s GB',str(application.requested_memory))
        gc3libs.log.debug('requested walltime: %s hours',str(application.requested_walltime))

        gc3libs.log.debug('Instantiating LRMSs')
        _lrms_list = []
        for _resource in self._resources:
            try:
                _lrms_list.append(self.__get_LRMS(_resource.name))
            except:
                # log exceptions but ignore them
                gc3libs.log.warning("Failed creating LRMS for resource '%s' of type '%s'",
                                     _resource.name, _resource.type)
                gc3libs.log.debug('gcli.py:gsub() got exception:', exc_info=True)
                continue
            
        if ( len(_lrms_list) == 0 ):
            raise NoResources("Could not initialize any computational resource - please check log and configuration file.")

        gc3libs.log.debug('Performing brokering')
        # decide which resource to use
        # (Resource)[] = (Scheduler).PerformBrokering((Resource)[],(Application))
        _selected_lrms_list = scheduler.do_brokering(_lrms_list,application)
        gc3libs.log.debug('Scheduler returned %d matching resources',
                           len(_selected_lrms_list))
        if 0 == len(_selected_lrms_list):
            raise NoResources("Could not select any compatible computational resource - please check log and configuration file.")

        # Scheduler.do_brokering should return a sorted list of valid lrms
        for lrms in _selected_lrms_list:
            try:
                self.authorization.get(lrms._resource.authorization_type)
                job = lrms.submit_job(application, job)
                if job.is_valid():
                    gc3libs.log.info('Successfully submitted process to LRMS backend')
                    # job submitted; leave loop
                    if application.has_key('job_local_dir'):
                        job.job_local_dir = application.job_local_dir
                    else:
                        job.job_local_dir = os.getcwd()
                    break
            except AuthenticationException:
                # ignore authentication errors: e.g., we may fail some SSH connections but succeed in others
                gc3libs.log.debug("Authentication error in submitting to resource '%s'" 
                                   % lrms._resource.name)
                continue
            except LRMSException:
                gc3libs.log.error("Error in submitting job to resource '%s'", 
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
                gc3libs.log.debug('Failed obtaining list of running jobs %s',str(sys.exc_info()[1]))
                raise
        else:
            _list_of_runnign_jobs = [job_obj]

        for _running_job in _list_of_runnign_jobs:
            try:
                job_return_list.append(self.__gstat(_running_job, auto_enable_auth))
            except:
                gc3libs.log.debug('Exception when trying getting status of job %s: %s',_running_job.unique_token,str(sys.exc_info()[1]))
                continue                                

        return job_return_list

    def __gstat(self, job_obj, auto_enable_auth):
        # returns an updated job object
        # create instance of LRMS depending on resource type associated to job
        
        _lrms = self.__get_LRMS(job_obj.resource_name)

        # gc3libs.log.debug('current job status is %d' % job_obj.status)

        if not ( job_obj.status == gc3libs.Job.JOB_STATE_COMPLETED or job_obj.status == gc3libs.Job.JOB_STATE_FINISHED or job_obj.status == gc3libs.Job.JOB_STATE_FAILED or job_obj.status == gc3libs.Job.JOB_STATE_DELETED ):
            # check job status
            # gc3libs.log.debug('checking job status')
            #a = Auth(auto_enable_auth)
            self.authorization.get(_lrms._resource.authorization_type)
            job_obj = _lrms.check_status(job_obj)

        return job_obj

#====== Gget =======
    def gget(self, job, **kw):

        if job.status == gc3libs.Job.JOB_STATE_SUBMITTED or job.status == gc3libs.Job.JOB_STATE_UNKNOWN:
            raise OutputNotAvailableError('Output Not avilable')

        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
        _lrms = self.__get_LRMS(job.resource_name)
        self.authorization.get(_lrms._resource.authorization_type)

        try:
            return  _lrms.get_results(job)
        except LRMSUnrecoverableError:
            job.status = gc3libs.Job.JOB_STATE_COMPLETED
            return job
        
#====== Glist =======
    def glist(self,resource_name, **kw):
        """ List status of a give resource."""
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
        _lrms = self.__get_LRMS(resource_name)
        self.authorization.get(_lrms._resource.authorization_type)
        return  _lrms.get_resource_status()

#====== Gkill ========
    def gkill(self, job_obj, **kw):
        """Kill a job."""
        
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
        
        _lrms = self.__get_LRMS(job_obj.resource_name)
        
        self.authorization.get(_lrms._resource.authorization_type)
        
        job_obj = _lrms.cancel_job(job_obj)
        gc3libs.log.debug('setting job status to DELETED')
        job_obj.status =  gc3libs.Job.JOB_STATE_DELETED
        return job_obj

#====== Tail ========
    def tail(self, job, std='stdout', **kw):
        """
        Tail returns job object with .stdout or .stderr containing content of stdout or stderr respectively
        Note: For the time beind we allow only stdout or stderr as valid filenames
        """

        # Get authorization
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
        _lrms = self.__get_LRMS(job.resource_name)
        self.authorization.get(_lrms._resource.authorization_type)

        # Get offset and buffersize
        _remote_file_offset = kw.get('offset',0)
        _remote_file_buffer_size = kw.get('buffer_size',None)

        try:
            if std == 'stdout':
                filename = job.stdout_filename
            elif std == 'stderr':
                filename = job.stderr_filename
            else:
                raise Error('Invalid requested filename')

            file_handle = _lrms.tail(job,filename,_remote_file_offset,_remote_file_buffer_size)

            if file_handle:
                # return a file handle of the local copy
                return file_handle

        except AttributeError:
            gc3libs.log.critical('Missing attribute')
            raise
        except:
            raise

#=========     INTERNAL METHODS ============

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

    def __log_job(self, job_obj):
        # dumping lrms_jobid
        # not catching the exception as this is supposed to be a fatal failure;
        # thus propagated to gsub's main try
        try:
            _fileHandle = open(job_obj.unique_token+'/'+Default.JOB_FILE,'w')
            _fileHandle.write(job_obj.resource_name+'\t'+job_obj.lrms_jobid)
            _fileHandle.close()
        except:
            gc3libs.log.error('failed updating job lrms_id')

        try:
            _fileHandle = open(job_obj.unique_token+'/'+Default.JOB_LOG,'w')
            _fileHandle.write(job_obj.log)
            _fileHandle.close()
        except:
            gc3libs.log.error('failed updating job log')
                        
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
                gc3libs.log.debug(joblist_file + ' did not exist... created successfully.')
            except:
                gc3libs.log.error('Failed opening joblist_file')
                return False

        gc3libs.log.debug('appending jobid to joblist file as specified in defaults')
        try:
            # appending jobid to .jobs file as specified in defaults
            gc3libs.log.debug('obtaining lock')
            if ( obtain_file_lock(joblist_file,joblist_lock) ):
                _fileHandle = open(joblist_file,'a')
                _fileHandle.write(job_obj.unique_token+'\n')
                _fileHandle.close()
            else:
                gc3libs.log.error('Failed obtain lock')
                return False

        except:
            gc3libs.log.error('Failed in appending current jobid to list of jobs in %s',Default.JOBLIST_FILE)
            gc3libs.log.debug('Exception %s',sys.exc_info()[1])
            return False

        # release lock
        if ( (not release_file_lock(joblist_lock)) & (os.path.isfile(joblist_lock)) ):
            gc3libs.log.error('Failed removing lock file')
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
                gc3libs.log.error('JOBS_DIR %s Not found. creating it' % Default.JOBS_DIR)
                try:
                    os.makedirs(Default.JOBS_DIR)
                except:
                    gc3libs.log.critical('%s',sys.exc_info()[1])
                    raise RetrieveJobsFilesystemError('Failed accessing job dir %s' % Default.JOBS_DIR)

            _jobs_list = os.listdir(Default.JOBS_DIR)

            # for each unique_token retrieve job information and create instance of Job obj
            _job_list = []

            for _job in _jobs_list:
                try:
                    _job_list.append(Job.get_job(_job))
                except:
                    gc3libs.log.error('Failed retrieving job information for %s',_job)
                    gc3libs.log.debug('%s',sys.exc_info()[1])
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
                gc3libs.log.debug('Creating instance of type %s for %s', _resource.type, _resource.name)
                try:
                    if _resource.type is Default.ARC_LRMS:
                        _lrms = ArcLrms(_resource, self.authorization)
                    elif _resource.type is Default.SGE_LRMS:
                        _lrms = SgeLrms(_resource, self.authorization)
                    else:
                        gc3libs.log.error('Unknown resource type %s',_resource.type)
                        raise Exception('Unknown resource type')
                except:
                    gc3libs.log.error('Exception creating LRMS instance %s',_resource.type)
                    raise

        if _lrms is None:
            raise ResourceNotFoundError("Cannot find computational resource '%s'" % resource_name)

        return _lrms
#====== End


# === Configuration File

def import_config(config_file_locations, auto_enable_auth=True):
    (default_val, resources_vals, authorizations) = read_config(*config_file_locations)
    return (get_defaults(default_val),
            get_resources(resources_vals), 
            get_authorization(authorizations,auto_enable_auth), 
            auto_enable_auth)


def get_authorization(authorizations,auto_enable_auth):
    try:
        return Auth(authorizations, auto_enable_auth)
    except:
        gc3libs.log.critical('Failed initializing Authorization module')
        raise


def get_defaults(defaults):
    # Create an default object for the defaults
    # defaults is a list[] of values
    try:
        # Create default values
        return gc3libs.Default.Default(defaults)
    except:
        gc3libs.log.critical('Failed loading default values')
        raise
    

def get_resources(resources_list):
    # build Resource objects from the list returned from read_config
    #        and match with selectd_resource from comand line
    #        (optional) if not options.resource_name is None:
    resources = [ ]
    try:
        for key in resources_list.keys():
            resource = resources_list[key]
            gc3libs.log.debug('creating instance of Resource object... ')
            try:
                tmpres = gc3libs.Resource.Resource(resource)
            except Exception, x:
                gc3libs.log.error("rejecting resource '%s': %s: %s",
                                   key, x.__class__.__name__, str(x))
                continue
            gc3libs.log.debug('Checking resource type %s',resource['type'])
            if resource['type'] == 'arc':
                tmpres.type = gc3libs.Default.ARC_LRMS
            elif resource['type'] == 'ssh_sge':
                tmpres.type = gc3libs.Default.SGE_LRMS
            else:
                gc3libs.log.error('No valid resource type %s',resource['type'])
                continue
            gc3libs.log.debug('checking validity with %s',str(tmpres.is_valid()))
            resources.append(tmpres)
    except:
        gc3libs.log.critical('failed creating Resource list')
        raise
    return resources

                                
def read_config(*locations):
    """
    Read each of the configuration files listed in `locations`, and
    return a `(defaults, resources, authorizations)` triple that can
    be passed to the `Gcli` class constructor.
    """
    files_successfully_read = 0
    defaults = { }
    resources = gc3libs.utils.defaultdict(lambda: dict())
    authorizations = gc3libs.utils.defaultdict(lambda: dict())

    for location in locations:
        location = os.path.expandvars(location)
        if os.path.exists(location) and os.access(location, os.R_OK):
            gc3libs.log.debug("gcli.read_config(): reading file '%s' ..." % location)
        else:
            gc3libs.log.debug("gcli.read_config(): ignoring non-existent file '%s' ..." % location)
            continue # with next `location`

        # Config File exists; read it
        config = ConfigParser.ConfigParser()
        if location not in config.read(location):
            gc3libs.log.debug("Configuration file '%s' is unreadable or malformed: ignoring." 
                               % location)
            continue # with next `location`
        files_successfully_read += 1

        # update `defaults` with the contents of the `[DEFAULTS]` section
        defaults.update(config.defaults())

        for sectname in config.sections():
            if sectname.startswith('authorization/'):
                # handle authorization section
                gc3libs.log.debug("gcli.read_config(): adding authorization '%s' ", sectname)
                # extract authorization name and register authorization dictionary
                auth_name = sectname.split('/', 1)[1]
                authorizations[auth_name].update(dict(config.items(sectname)))

            elif  sectname.startswith('resource/'):
                # handle resource section
                resource_name = sectname.split('/', 1)[1]
                gc3libs.log.debug("gcli.read_config(): adding resource '%s' ", resource_name)
                config_items = dict(config.items(sectname))
                if config_items.has_key('enabled'):
                    config_items['enabled'] = utils.string_to_boolean(config_items['enabled'])
                    gc3libs.log.debug("Resource '%s': enabled=%s in file '%s'", 
                                       resource_name, config_items['enabled'], location)
                resources[resource_name].update(config_items)
                resources[resource_name]['name'] = resource_name

            else:
                # Unhandled sectname
                gc3libs.log.error("gcli.read_config(): unknown configuration section '%s' -- ignoring!", 
                                   sectname)

        gc3libs.log.debug("gcli.read_config(): read %d resources from configuration file '%s'",
                           len(resources), location)

        # remove disabled resources
        disabled_resources = [ ]
        for resource in resources.values():
            if resource.has_key('enabled') and not resource['enabled']:
                disabled_resources.append(resource['name'])
        for resource_name in disabled_resources:
            gc3libs.log.info("Ignoring computational resource '%s'"
                              " because of 'enabled=False' setting.",
                              resource_name)
            del resources[resource_name]

    if files_successfully_read == 0:
        raise NoConfigurationFile("Could not read any configuration file; tried locations '%s'."
                                  % str.join("', '", locations))

    return (defaults, resources, authorizations)
