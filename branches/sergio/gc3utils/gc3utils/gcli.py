#!/usr/bin/env python

__author__="Sergio Maffioletti (sergio.maffioletti@gc3.uzh.ch)"
__date__="01 february 2010"
__copyright__="Copyright 2009 2011 Grid Computing Competence Center - UZH/GC3"
__version__="0.2"

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


homedir = os.path.expandvars('$HOME')
rcdir = homedir + "/.gc3"
default_config_file_location = rcdir + "/config"
default_joblist_location = rcdir + "/.joblist"
default_joblist_lock = rcdir + "/.joblist_lock"
default_job_folder_location="$PWD"
default_wait_time = 3

class Gcli:

    SMSCG_AUTHENTICATION = 1
    SSH_AUTHENTICATION = 2

#    resource_list = {}
#    defaults = {}

    def __init__(self, defaults, resource_list):
        try:
            if ( len(resource_list) == 0 ):
                raise Exception('could not read any valid resource configuration from config file')
            self._resources = resource_list
            self._defaults = defaults
        except:
            raise

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

#===============================================================================
#    def checkGridCredential(self):
#        if (not checkGridAccess()):
#            if ( self.defaults['email_contact'] != "" ):
#                logging.debug('Sending notification email to [ %s ]',self.defaults['email_contact'])
#                send_email(self.defaults['email_contact'],"info@gc3.uzh.ch","GC3 Warning: Renew Grid credential","Please renew your credential")
#===============================================================================
                
#===============================================================================
#    def __init__(self, config_file_location):
#        try:
#            # read configuration file
#            _local_resource_list = {}
#            (self.defaults,_local_resource_list) = read_config(config_file_location)
# 
#            for _resource in _local_resource_list.values():
#                if ("ncores" in _resource) & ("memory_per_core" in _resource) & ("walltime" in _resource) & ("type" in _resource) & ("frontend" in _resource) & ("applications" in _resource):
#                    # Adding valid resources
#                    logging.debug('Adding valid resource description [ %s ]',_resource['resource_name'])
#                    self.resource_list[_resource['resource_name']] = _resource
# 
#            # Check if any resource configuration has been leaded
#            if ( len(self.resource_list) == 0 ):
#                raise Exception('could not read any valid resource configuration from config file')
# 
#            logging.info('Loading configuration file %s \t[ ok ]',config_file_location)
#        except:
#            logging.critical('Failed init gcli')
#            raise
#===============================================================================

    def glist(self, resource_name):
        # Returns an instance of object Resource containing a dictionary of Resource informations
        # Throw an Exception in case the method cannot be completed or the Resource object cannot be built
        try:
            if resource_name is None:
                # for time being we raise an exception with no implemented
                raise Exception('glist with no resource_name not yet implemented')

            if ( resource_name in self.resource_list ):
                logging.debug('Found match for user defined resource: %s',resource_name)
                resource_description = self.resource_list[resource_name]
            else:
                logging.critical('failed matching user defined resource: %s ',resource_name)
                raise Exception('failed matching user defined resource')
            
            logging.info('Check user defined resources\t\t\t[ ok ]')

            if ( resource_description['type'] == "arc" ):
                lrms = ArcLrms(resource_description)
            elif ( resource_description['type'] == "ssh"):
                lrms = SshLrms(resource_description)
            else:
                logging.error('Unknown resource type %s',resource_description['type'])
                raise Exception('Unknown resource type')

            return [0,lrms.GetResourceStatus()]
            
        except:
            logging.debug('glist failed due to exception')
            raise

    #===========================================================================
    # def checkGridCredential(self):
    #    if (not checkGridAccess()):
    #        if ( self.defaults['email_contact'] != "" ):
    #            logging.debug('Sending notification email to [ %s ]',self.defaults['email_contact'])
    #            send_email(self.defaults['email_contact'],"info@gc3.uzh.ch","GC3 Warning: Renew Grid \
    #            credential","Please renew your credential")
    #===========================================================================


    #===========================================================================
    # def checkGridAccess(self):
    #    # First check whehter it is necessary to check grid credential or not
    #    # if selected resource is type ARC or if there is at least 1 ARC resource in the resource list, then check Grid credential
    #    
    #    logging.debug('gcli: Checking Grid Credential')
    #    if ( (not utils.CheckGridAuthentication()) | (not utils.checkUserCertificate()) ):
    #        logging.error('Credential Expired')
    #        return False
    #    return True
    #===========================================================================

#===============================================================================
#    def renewGridCredential(self):
#        # Getting AAI username
# #        _aaiUserName = None
# 
#        try:
#            self.AAI_CREDENTIAL_REPO = os.path.expandvars(self.AAI_CREDENTIAL_REPO)
#            logging.debug('checking AAI credential file [ %s ]',self.AAI_CREDENTIAL_REPO)
#            if ( os.path.exists(self.AAI_CREDENTIAL_REPO) & os.path.isfile(self.AAI_CREDENTIAL_REPO) ):
#                logging.debug('Opening AAI credential file in %s',self.AAI_CREDENTIAL_REPO)
#                _fileHandle = open(self.AAI_CREDENTIAL_REPO,'r')
#                _aaiUserName = _fileHandle.read()
#                _aaiUserName = _aaiUserName.rstrip("\n")
#                logging.debug('_aaiUserName: %s',_aaiUserName)
#                RenewGridCredential(_aaiUserName)
#            else:
#                logging.critical('AAI_Credential information file not found')
#                raise Exception('AAI_Credential information file not found')
#        except:
#            logging.critical('Failed renewing grid credential [%s]',sys.exc_info()[1])
#            return False
#===============================================================================

    #===========================================================================
    # def __select_lrms(self,lrms_list,application):
    #    # start candidate_resource loop
    #    for lrms in lrms_list:
    #        
    #        if (application.cores > lrms.max_cores_per_job) | (application.memory > lrms.max_memory_per_core) | (application.walltime > lrms.max_walltime) :
    #            continue
    #        else:
    #            return lrms
    #    raise Exception('Failed finding lrms that could fullfill the application requirements')
    #===========================================================================


####### Start gusb method ########
    def gsub(self, application_obj):
        # Obsolete: def gsub(self, application_to_run, input_file, selected_resource, job_local_dir, cores, memory, walltime):
        # returns an object of type Job
        # throw an exception if the method fails or if the Job object cannot be built successfully
        
        global default_job_folder_location
        global default_joblist_location
        global default_joblist_lock
        
        # Parsing passed arguments
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

        for _single_resource in self._resources:
            logging.debug('Creating instance of type %s for %s',_single_resource.type,_single_resource.frontend)
            try:
                if _single_resource.type is self.ARC_LRMS:
                    _lrms_list.append(ArcLrms(_single_resource))
                elif _single_resource.type is self.SGE_LRMS:
                    _lrms_list.append(SshLrms(_single_resource))
                else:
                    logging.error('Unknown resource type %s',_single_resource.type)
                    continue
            except:
                logging.error('Exception creating LRMS instance %s',_single_resource.type)
                continue

        # Do we need this check ?
        if ( len(_lrms_list) == 0 ):
            logging.critical('Could not initialize ANY lrms resource')
            raise Exception('no available LRMS found')

        logging.debug('Performing brokering')
        # decide which resource to use
        # (Job) = (Scheduler).PerformBrokering((Resource)[],(Application))
        try:
            _selected_lrms = Scheduler.do_brokering(_lrms_list,application_obj)
            logging.debug('Selected LRMS: %s',_selected_lrms.resource.frontend)
            logging.info('Select LRMS\t\t\t\t\t[ ok ]')
        except:
            logging.critical('Failed in scheduling')
            raise

        # This method also takes care of crating the unique_token's folder
        try:
            unique_token = __create_job_unique_token(application_obj.inputfile,_selected_lrms.resource_name)
        except:
            logging.critical('Failed creating unique_token')
            raise

        # resource_name.submit_job(input, unique_token, application, lrms_log) -> returns [lrms_jobid,lrms_log]
        logging.debug('Submitting job with %s %s %s %s',unique_token, application_to_run, input_file, self.defaults['lrms_log'])
        try:
            job_obj = _selected_lrms.submit_job(application_obj)
            job_obj.update(unique_token=unique_token)
            logging.info('Submission process to LRMS backend\t\t\t[ ok ]')
        except:
            logging.critical('Failed Submitting job: %s',sys.exc_info()[1])
            raise

        if self.__log_job(job_obj):
            logging.info('Dumping lrms log information\t\t\t[ ok ]')

        # return an object of type Job which contains also the unique_token
        return job_obj

####### End gusb method ########
                                              
    def gget(self, unique_token):
        global default_job_folder_location
        global default_joblist_location
        global default_joblist_lock

        if ( (os.path.exists(unique_token) == False ) | (os.path.isdir(unique_token) == False) | ( not check_inputfile(unique_token+'/'+self.defaults['lrms_jobid']) ) ):
            logging.critical('Jobid Not valid')
            raise Exception('invalid jobid')

        logging.info('unique_token file check\t\t\t[ ok ]')

        # check .finished file
        if ( not check_inputfile(unique_token+'/'+self.defaults['lrms_finished']) ):
            _fileHandle = open(unique_token+'/'+self.defaults['lrms_jobid'],'r')
            _raw_resource_info = _fileHandle.read()
            _fileHandle.close()

            _list_resource_info = re.split('\t',_raw_resource_info)

            logging.debug('lrms_jobid file returned %s elements',len(_list_resource_info))

            if ( len(_list_resource_info) != 2 ):
                raise Exception('failed retieving jobid')

            logging.debug('frontend: [ %s ] jobid: [ %s ]',_list_resource_info[0],_list_resource_info[1])
            logging.info('reading lrms_jobid info\t\t\t[ ok ]')

            if ( _list_resource_info[0] in self.resource_list ):
                logging.debug('Found match for resource [ %s ]',_list_resource_info[0])
                logging.debug('Creating lrms instance')
                resource = self.resource_list[_list_resource_info[0]]
                if ( resource['type'] == "arc" ):
                    lrms = ArcLrms(resource)
                elif ( resource['type'] == "ssh"):
                    lrms = SshLrms(resource)
                else:
                    logging.error('Unknown resource type %s',resource['type'])
                    raise  Exception('unknown resource type')

                if ( (lrms.isValid != 1) | (lrms.check_authentication() == False) ):
                    logging.error('Failed validating lrms instance for resource %s',resource['resource_name'])
                    raise Exception('failed authenticating to LRMS')

                logging.info('Init LRMS\t\t\t[ ok ]')
                _lrms_jobid = _list_resource_info[1]
                logging.debug('_list_resource_info : ' + _list_resource_info[1])
                
                #_lrms_dirfolder = dirname(unique_token)
                (retval,lrms_log) = lrms.get_results(_lrms_jobid,unique_token)

                # dump lrms_log
                try:
                    logging.debug('Dumping lrms_log')
                    _fileHandle = open(unique_token+'/'+self.defaults['lrms_log'],'a')
                    _fileHandle.write('=== gget ===\n')
                    _fileHandle.write(lrms_log+'\n')
                    _fileHandle.close()
                except:
                    logging.error('Failed dumping lrms_log [ %s ]',sys.exc_info()[1])
                    
                if ( retval == False ):
                    logging.error('Failed getting results')
                    raise Exception('failed getting results from LRMS')
                
                logging.debug('check_status\t\t\t[ ok ]')

                # Job finished; results retrieved; writing .finished file
                try:
                    logging.debug('Creating finished file')
                    open(unique_token+"/"+self.defaults['lrms_finished'],'w').close()
                except:
                    logging.error('Failed creating finished file [ %s ]',sys.exc_info()[1])
                    # Should handle the exception differently ?      

                logging.debug('Removing jobid from joblist file')
                # Removing jobid from joblist file
                try:
                    default_joblist_location = os.path.expandvars(default_joblist_location)
                    default_joblist_lock = os.path.expandvars(default_joblist_lock)
                    
                    if ( obtain_file_lock(default_joblist_location,default_joblist_lock) ):
                        _newFileHandle = tempfile.NamedTemporaryFile(suffix=".xrsl",prefix="gridgames_arc_")
                        
                        _oldFileHandle  = open(default_joblist_location)
                        _oldFileHandle.seek(0)
                        for line in _oldFileHandle:
                            logging.debug('checking %s with %s',line,unique_token)
                            if ( not unique_token in line ):
                                logging.debug('writing line')
                                _newFileHandle.write(line)

                        _oldFileHandle.close()

                        os.remove(default_joblist_location)

                        _newFileHandle.seek(0)

                        logging.debug('replacing joblist file with %s',_newFileHandle.name)
                        os.system("cp "+_newFileHandle.name+" "+default_joblist_location)

                        _newFileHandle.close()

                    else:
                        raise Exception('Failed obtain lock')
                except:
                    logging.error('Failed updating joblist file in %s',default_joblist_location)
                    logging.debug('Exception %s',sys.exc_info()[1])

                # release lock
                if ( (not release_file_lock(default_joblist_lock)) & (os.path.isfile(default_joblist_lock)) ):
                    logging.error('Failed removing lock file')

            else:
                logging.critical('Failed finding matching resource name [ %s ]',_list_resource_info[0])
                raise
        return 0

    def gstat(self, unique_token):
        global default_joblist_location

        if ( unique_token != None):
            return [0,[self.__gstat(unique_token)]]
        else:
            # Read content of .joblist and return gstat for each of them
            default_joblist_location = os.path.expandvars(default_joblist_location)
            joblist  = open(default_joblist_location,'r')
            joblist.seek(0)
            lrmsjobid_single_string = joblist.read()
            joblist.close()
            lrmsjobid_list = re.split('\n',lrmsjobid_single_string)
            status_list = []
            if ( len(lrmsjobid_list) > 0 ):
                for _lrmsjobid in lrmsjobid_list:
                    if ( _lrmsjobid != "" ):
                        logging.debug('Checking status fo jobid [ %s ]',_lrmsjobid)
                        status_list.append(self.__gstat(_lrmsjobid))
            logging.debug('status_list contains [ %d ] elelemnts',len(status_list))
            return [0,status_list]

    def __gstat(self, unique_token):
        if ( (os.path.exists(unique_token) == False ) | (os.path.isdir(unique_token) == False) | ( not check_inputfile(unique_token+'/'+self.defaults['lrms_jobid']) ) ):
            logging.critical('Jobid Not valid')
            raise Exception('invalid jobid')

        logging.info('lrms_jobid file check\t\t\t[ ok ]')

        # check finished file
        if ( not check_inputfile(unique_token+'/'+self.defaults['lrms_finished']) ):
            _fileHandle = open(unique_token+'/'+self.defaults['lrms_jobid'],'r')
            _raw_resource_info = _fileHandle.read()
            _fileHandle.close()

            _list_resource_info = re.split('\t',_raw_resource_info)
            
            logging.debug('frontend: [ %s ] jobid: [ %s ]',_list_resource_info[0],_list_resource_info[1])
            logging.info('reading lrms_jobid info\t\t\t[ ok ]')
            
            if ( _list_resource_info[0] in self.resource_list ):
                logging.debug('Found match for resource [ %s ]',_list_resource_info[0])
                logging.debug('Creating lrms instance')
                resource = self.resource_list[_list_resource_info[0]]
                if ( resource['type'] == "arc" ):
                    lrms = ArcLrms(resource)
                elif ( resource['type'] == "ssh"):
                    lrms = SshLrms(resource)
                else:
                    logging.error('Unknown resource type %s',resource['type'])
                    raise Exception('unknown resource type')

                # check authentication
                if ( (lrms.isValid != 1) | (lrms.check_authentication() == False) ):
                    logging.error('Failed validating lrms instance for resource %s',resource['resource_name'])
                    raise Exception('failed authenticating to LRMS')

                logging.info('Init LRMS\t\t\t[ ok ]')
                _lrms_jobid = _list_resource_info[1]
                _lrms_dirfolder = dirname(unique_token)

                # check job status
                (retval,lrms_log) = lrms.check_status(_lrms_jobid)

                logging.info('check status\t\t\t[ ok ]')
            else:
                logging.critical('Failed finding matching resource name [ %s ]',_list_resource_info[0])
                raise Exception('failed finding matching resource')

        else:
            retval = "Status: FINISHED"

        logging.debug('Returning [ %s ] [ %s ]',unique_token,retval)

        return [unique_token,retval]


    # Internal functions
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
        # not catching the exception as this is suppoed to be a fatal failure;
        # thus propagated to gsub's main try
        _fileHandle = open(job_obj.unique_token+'/'+self._defaults.lrms_jobid,'w')
        _fileHandle.write(job_obj.resource_name+'\t'+job_obj.lrms_jobid)
        _fileHandle.close()

        # if joblist_location & joblist_lock are not defined, use default
        try:
            joblist_location
        except NameError:
            joblist_location = os.path.expandvars(self._defaults.joblist_location)

        try:
            joblist_lock
        except NameError:
            joblist_lock = os.path.expandvars(self._defaults.joblist_lock)

        # if joblist_location does not exist, create it
        if not os.path.exists(joblist_location):
            try:
                open(joblist_location, 'w').close()
                logging.debug(joblist_location + ' did not exist... created successfully.')
            except:
                logging.error('Failed opening joblist_location')
                return False

        logging.debug('appending jobid to joblist file as specified in defaults')
        try:
            # appending jobid to .jobs file as specified in defaults
            logging.debug('obtaining lock')
            if ( obtain_file_lock(joblist_location,joblist_lock) ):
                _fileHandle = open(joblist_location,'a')
                _fileHandle.write(job_obj.unique_token+'\n')
                _fileHandle.close()
            else:
                logging.error('Failed obtain lock')
                return False

        except:
            logging.error('Failed in appending current jobid to list of jobs in %s',self._defaults.joblist_location)
            logging.debug('Exception %s',sys.exc_info()[1])
            return False

        # release lock
        if ( (not release_file_lock(joblist_lock)) & (os.path.isfile(joblist_lock)) ):
            logging.error('Failed removing lock file')
            return False

        return True
                


def main():
    global default_job_folder_location
    global default_joblist_location
    global default_joblist_lock

    try:
        program_name = sys.argv[0]
        if ( os.path.basename(program_name) == "gsub" ):
            # Gsub
            # Parse command line arguments
            _usage = "%prog [options] application input-file"
            parser = OptionParser(usage=_usage)
            parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
            parser.add_option("-r", "--resource", action="store", dest="resource_name", metavar="STRING", default=None, help='Select resource destination')
            parser.add_option("-d", "--jobdir", action="store", dest="job_local_dir", metavar="STRING", default=None, help='Select job local folder location')
            parser.add_option("-c", "--cores", action="store", dest="ncores", metavar="INT", default=None, help='Set number of requested cores')
            parser.add_option("-m", "--memory", action="store", dest="memory_per_core", metavar="INT", default=None, help='Set memory per core request (GB)')
            parser.add_option("-w", "--walltime", action="store", dest="walltime", metavar="INT", default=None, help='Set requested walltime (hours)')

            (options, args) = parser.parse_args()

            # Configure logging service
            configure_logging(options.verbosity)

            if len(args) != 2:
                logging.critical('Command line argument parsing\t\t\t[ failed ]\n\tIncorrect number of arguments; expected 2 got %d ',len(args))
                #      parser.error('wrong number on arguments')
                #      parser.print_help()
                raise Exception('wrong number on arguments')

            # Checking whether it has been passed a valid application
            if ( args[0] != "gamess" ) & ( args[0] != "apbs" ):
                logging.critical('Application argument\t\t\t[ failed ]\n\tUnknown application: '+str(args[0]))
                raise Exception('invalid application argument')

            # check input file
            if ( not check_inputfile(args[1]) ):
                logging.critical('Input file argument\t\t\t[ failed ]'+args[1])
                raise Exception('invalid input-file argument')
                                        
        elif ( os.path.basename(program_name) == "grid-credential-renew" ):
            _usage = "Usage: %prog [options] aai_user_name"
            parser = OptionParser(usage=_usage)
            parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
            (options, args) = parser.parse_args()

            # Configure logging service
            configure_logging(options.verbosity)

            _aai_username = None
            if len(args) == 1:
                _aai_username = args[0]
                        
        elif ( os.path.basename(program_name) == "gstat" ):
            # Gstat
            # Parse command line arguments

            _usage = "Usage: %prog [options] jobid"
            parser = OptionParser(usage=_usage)
            parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
            (options, args) = parser.parse_args()
            
            # Configure logging service
            configure_logging(options.verbosity)

            logging.debug('Command lines argument length: [ %d ]',len(args))

            if len(args) > 1:
                logging.critical('Command line argument parsing\t\t\t[ failed ]\n\tIncorrect number of arguments; expected either 0 or 1 got %d ',len(args))
                parser.print_help()
                raise Exception('wrong number on arguments')

        elif ( os.path.basename(program_name) == "gget" ):
            # Gget
            # Parse command line arguments
            
            _usage = "Usage: %prog [options] jobid"
            parser = OptionParser(usage=_usage)
            parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
            (options, args) = parser.parse_args()
            
            # Configure logging service
            configure_logging(options.verbosity)

            if len(args) != 1:
                logging.critical('Command line argument parsing\t\t\t[ failed ]\n\tIncorrect number of arguments; expected 1 got %d ',len(args))
                parser.print_help()
                raise Exception('wrong number on arguments')

            logging.info('Parsing command line arguments\t\t[ ok ]')

            unique_token = args[0]

        elif ( os.path.basename(program_name) == "gkill" ):
            logging.info('gkill is not implemented yet')

        elif ( os.path.basename(program_name) == "glist" ):
            # Glist
            # Parse command line arguments
            
            _usage = "Usage: %prog [options] resource_name"
            parser = OptionParser(usage=_usage)
            parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
            (options, args) = parser.parse_args()
            
            # Configure logging service
            configure_logging(options.verbosity)
            
            logging.debug('Command lines argument length: [ %d ]',len(args))
            
            if len(args) != 1:
                logging.critical('Command line argument parsing\t\t\t[ failed ]\n\tIncorrect number of arguments; expected 1 got %d ',len(args))
                parser.print_help()
                raise Exception('wrong number on arguments')

            logging.info('Parsing command line arguments\t\t[ ok ]')
            
            resource_name = args[0]

        else:
            # Error
            print "Unknown command "+program_name
            return 1


        # End parsing command line arguments
        # Beging implementing methods

# ======================= Start Serving Requests =======================

        try:
            # Read configuration file to create Resource lists and default values
            # resources_list is a list of resource dictionaries
            (defaults,resources_list) = utils.read_config(default_config_file_location)
        except:
            logging.debug('Failed loading config file from %s',default_config_file_location)
            raise

        # build Resource objects from the list returned from read_config and match with selectd_resource from comand line (optional)
        #        if not options.resource_name is None:
        resources = []

        try:
            for resource in resources_list:
                if (not options.resource_name) & (not options.resource_name is resource['resource_name']):
                    logging.debug('Rejecting resource because of not matching with %s',options.resource_name)
                    continue
                logging.debug('creating instance of Resource object... ')
                tmpres = Resource()
                for items in resource:
                    tmpres.update(items=resource[items])
                if tmpres.isValid():
                    resources.append(tmpres)
                else:
                    logging.warning('Failed adding resource %s',resource.name)
        except:
            logging.critical('failed creating Resource list')
            raise

        # Create an default object for the defaults
        try:
            default = Default()
            for default_values in defaults:
                defaul.update(default_values=defaults[default_values])
            if not default.isValid():
                raise Exception('defaults not valids')
        except:
            logging.critical('Failed loading default values')
            raise

        #        gcli = Gcli(default_config_file_location)
        gcli = Gcli(default, resources)

        print "bye bye"
        return 0


#======================================= 

        # grid-credential-renew
        if ( os.path.basename(program_name) == "grid-credential-renew" ):
            logging.debug('Checking grid credential')
            if not gcli.check_authentication(Gcli.SMSCG_AUTHENTICATION):
                return gcli.enable_authentication(Gcli.SMSCG_AUTHENTICATION)
            else:
                return True

        # if there's at least 1 resource of type ARC, check grid access
        logging.debug('Checking grid credential')
        if not gcli.check_authentication(Gcli.SMSCG_AUTHENTICATION):
            gcli.enable_authentication(Gcli.SMSCG_AUTHENTICATION) 

        # gsub
        if ( os.path.basename(program_name) == "gsub" ):
            # gsub prototype: application_to_run, input_file, selected_resource, job_local_dir, cores, memory, walltime

            # Create Application obj
            application = Application(application_tag=application_tag,input_file=input_file)
            for option in options:
                application.update(option=options[option])
            if not application.isValid():
                raise Exception('Failed creating application object')

            job = gcli.gsub(application)
            
            if job.isValid():
                sys.stdout.write(job.jobid)
                sys.stdout.flush()
                return 0
            else:
                raise Exception('Job object not valid')
            
            #===================================================================
            # 
            #    
            # (exitcode,jobid) = gcli.gsub(args[0],os.path.abspath(args[1]),options.resource_name,options.job_local_dir,options.ncores,options.memory_per_core,options.walltime)
            # if (not exitcode):
            #    print jobid
            # else:
            #    raise Exception("submission terminated")
            #===================================================================

        # gstat    
        elif (os.path.basename(program_name) == "gstat" ):
            if ( len(args) > 0 ):
                (retval,job_status_list) = gcli.gstat(args[0])
            else:
                (retval,job_status_list) = gcli.gstat(None)
            if (not retval):
                logging.debug('Job_status_list')
                for _job_status_report in job_status_list:
                    sys.stdout.write('Job: '+_job_status_report[0]+'\n')
                    sys.stdout.write(_job_status_report[1]+'\n')
                    sys.stdout.flush()
            else:
                logging.debug('retval returned %d',retval)
                raise Exception("gstat terminated")
        # ggest
        elif (os.path.basename(program_name) == "gget"):
            retval = gcli.gget(unique_token)
            if (not retval):
                sys.stdout.write('Job results successfully retrieved in [ '+unique_token+' ]\n')
                sys.stdout.flush
            else:
                raise Exception("gget terminated")

        # glist
        elif (os.path.basename(program_name) == "glist"):
            (retval,resource_object) = gcli.glist(resource_name)
            if (not retval):
                if resource_object.__dict__.has_key("resource_name"):
                    sys.stdout.write('Resource Name: '+resource_object.__dict__["resource_name"]+'\n')
                if resource_object.__dict__.has_key("total_slots"):
                    sys.stdout.write('Total cores: '+str(resource_object.__dict__["total_slots"])+'\n')
                if resource_object.__dict__.has_key("total_runnings"):
                    sys.stdout.write('Total runnings: '+str(resource_object.__dict__["total_runnings"])+'\n')
                if resource_object.__dict__.has_key("total_queued"):
                    sys.stdout.write('Total queued: '+str(resource_object.__dict__["total_queued"])+'\n')
                if resource_object.__dict__.has_key("memory_per_core"):
                    sys.stdout.write('Memory per core: '+str(resource_object.__dict__["memory_per_core"])+'\n')
                sys.stdout.flush()
            else:
                raise Exception("glist terminated")
    except:
        logging.info('%s',sys.exc_info()[1])
        # think of a better error message
        # Should intercept the exception somehow and generate error message accordingly ?
        print os.path.basename(program_name)+" failed: "+str(sys.exc_info()[1])
        return 1
                
if __name__ == "__main__":
      sys.exit(main())
