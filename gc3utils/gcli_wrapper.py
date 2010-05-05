#!/usr/bin/env python

__author__="Sergio Maffioletti (sergio.maffioletti@gc3.uzh.ch)"
__date__="01 May 2010"
__copyright__="Copyright 2009 2011 Grid Computing Competence Center - UZH/GC3"
__version__="0.3"

import sys
import os
import logging
import ConfigParser
from optparse import OptionParser
from gc3utils import *
import gc3utils
import gc3utils.utils
import gc3utils.ArcLRMS
import gc3utils.SshLRMS
import gc3utils.Resource
import gc3utils.Default
import gc3utils.Job
import gc3utils.Application
import gc3utils.gcli
import gc3utils.Exceptions

def __get_defaults(defaults):
    # Create an default object for the defaults
    # defaults is a list[] of values
    try:
        # Create default values
        default = gc3utils.Default.Default(homedir=gc3utils.Default.HOMEDIR,config_file_location=gc3utils.Default.CONFIG_FILE_LOCATION,joblist_location=gc3utils.Default.JOBLIST_FILE,joblist_lock=gc3utils.Default.JOBLIST_LOCK,job_folder_location=gc3utils.Default.JOB_FOLDER_LOCATION)
        
        # Overwrite with what has been read from config 
        for default_values in defaults:
            default.insert(default_values,defaults[default_values])
            if not default.is_valid():
                raise Exception('defaults not valid')
    except:
        gc3utils.log.critical('Failed loading default values')
        raise

    return default


def __get_resources(options, resources_list):
    # build Resource objects from the list returned from read_config and match with selectd_resource from comand line (optional)
    #        if not options.resource_name is None:
    resources = []

    try:
        for resource in resources_list:
            # RFR: options.resource_name is NOT a key that is set by all command line commands
            if hasattr(options,'resource_name') and options.resource_name :
                if (not options.resource_name is resource['name']):
                    gc3utils.log.debug('Rejecting resource because of not matching with %s',options.resource_name)
                    continue
            gc3utils.log.debug('creating instance of Resource object... ')
            tmpres = gc3utils.Resource.Resource()

            for items in resource:
                gc3utils.log.debug('Updating with %s %s',items,resource[items])
                tmpres.insert(items,resource[items])

            gc3utils.log.debug('Checking resource type %s',resource['type'])
            if resource['type'] == 'arc':
                tmpres.insert("type",gc3utils.Default.ARC_LRMS)
            elif resource['type'] == 'ssh_sge':
                tmpres.insert("type",gc3utils.Default.SGE_LRMS)
            else:
                gc3utils.log.error('No valid resource type %s',resource['type'])
                continue

            gc3utils.log.debug('checking validity with %s',str(tmpres.is_valid()))
            
            if tmpres.is_valid():
                resources.append(tmpres)
            else:
                gc3utils.log.warning('Failed adding resource %s',resource['name'])
                    
    except:
        gc3utils.log.critical('failed creating Resource list')
        raise

    return resources


#====== Main ========
def main():
    homedir = os.path.expandvars('$HOME')
    rcdir = homedir + "/.gc3"
    default_config_file_location = rcdir + "/config"
    default_joblist_file = rcdir + "/.joblist"
    default_joblist_lock = rcdir + "/.joblist_lock"
    default_job_folder_location="$PWD"
    default_wait_time = 3

    logging.basicConfig(verbosity=10,format='%(asctime)s: %(levelname)s [%(name)s_%(module)s_%(funcName)s_%(lineno)d]:  %(message)s')
#    gc3utils.utils.configure_logger(options.verbosity,'gc3utils','/tmp/gc3utils.log')
            
#    global default_job_folder_location
#    global default_joblist_file
#    global default_joblist_lock

    try:
        program_name = sys.argv[0]
        if ( os.path.basename(program_name) == "gsub" ):
            # Gsub
            # Parse command line arguments
            _usage = "%prog [options] application input-file"
            parser = OptionParser(usage=_usage)
            parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
            parser.add_option("-r", "--resource", action="store", dest="resource_name", metavar="STRING", default=None, help='Select resource destination')
            parser.add_option("-d", "--jobdir", action="store", dest="job_local_dir", metavar="STRING", default=gc3utils.Default.JOB_FOLDER_LOCATION, help='Select job local folder location')
            parser.add_option("-c", "--cores", action="store", dest="ncores", metavar="INT", default=0, help='Set number of requested cores')
            parser.add_option("-m", "--memory", action="store", dest="memory_per_core", metavar="INT", default=0, help='Set memory per core request (GB)')
            parser.add_option("-w", "--walltime", action="store", dest="walltime", metavar="INT", default=0, help='Set requested walltime (hours)')
            parser.add_option("-a", "--args", action="store", dest="application_arguments", metavar="STRING", default=None, help='Application arguments')

            (options, args) = parser.parse_args()

            # Configure logging service
            gc3utils.utils.configure_logger(options.verbosity,'gc3utils','/tmp/gc3utils.log')
            
            if len(args) != 2:
                gc3utils.log.info('Command line argument parsing\t[ failed ]')
                gc3utils.log.critical('Incorrect number of arguments; expected 2 got %d ',len(args))
                raise Exception('wrong number on arguments')

#            # Checking whether it has been passed a valid application
#            if ( args[0] != "gamess" ) & ( args[0] != "apbs" ):
#                gc3utils.log.critical('Application argument\t\t\t[ failed ]\n\tUnknown application: '+str(args[0]))
#                raise Exception('invalid application argument')

            application_tag = args[0]

            # check input file
            if ( not gc3utils.utils.check_inputfile(args[1]) ):
                gc3utils.log.critical('Input file argument\t\t\t[ failed ]'+args[1])
                raise Exception('invalid input-file argument')

            input_file_name = args[1]
                                        
        elif ( os.path.basename(program_name) == "grid-credential-renew" ):
            _usage = "Usage: %prog [options] aai_user_name"
            parser = OptionParser(usage=_usage)
            parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
            (options, args) = parser.parse_args()

            # Configure logging service
            gc3utils.utils.configure_logger(options.verbosity,'gc3utils','/tmp/gc3utils.log')

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
            gc3utils.utils.configure_logger(options.verbosity,'gc3utils','/tmp/gc3utils.log')

            gc3utils.log.debug('Command lines argument length: [ %d ]',len(args))

            if len(args) > 1:
                gc3utils.log.critical('Command line argument parsing\t\t\t[ failed ]\n\tIncorrect number of arguments; expected either 0 or 1 got %d ',len(args))
                parser.print_help()
                raise Exception('wrong number on arguments')

            unique_token = args[0]
            if not os.path.isdir(unique_token):
                raise gc3utils.Exceptions.UniqueTokenError('unique_token not valid')
            
            gc3utils.log.debug('Using unique_token %s',unique_token)

        elif ( os.path.basename(program_name) == "gget" ):
            # Gget
            # Parse command line arguments
            
            _usage = "Usage: %prog [options] jobid"
            parser = OptionParser(usage=_usage)
            parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
            (options, args) = parser.parse_args()
            
            # Configure logging service
            # gc3utils.log.basicConfig(format='%(asctime)s: %(levelname)s [%(name)s_%(module)s_%(funcName)s]:  %(message)s')
            gc3utils.utils.configure_logger(options.verbosity,'gc3utils','/tmp/gc3utils.log')
            
                        

            if len(args) != 1:
                gc3utils.log.critical('Command line argument parsing\t\t\t[ failed ]\n\tIncorrect number of arguments; expected 1 got %d ',len(args))
                parser.print_help()
                raise Exception('wrong number on arguments')

            gc3utils.log.info('Parsing command line arguments\t\t[ ok ]')

            unique_token = args[0]

        elif ( os.path.basename(program_name) == "gkill" ):
            # Gkill
            shortview = True

            _usage = "%prog [options] unique_token"
            parser = OptionParser(usage=_usage)
            parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")

            (options, args) = parser.parse_args()

            # Configure logging service
            configure_logging(options.verbosity)

            logging.debug('Command lines argument length: [ %d ]',len(args))

            if len(args) != 1:
                logging.critical('Command line argument parsing\t\t\t[ failed ]\n\tIncorrect number of arguments; expected 1, got %d ',len(args))
                parser.print_help()
                print 'Usage: ' + _usage
                raise Exception('wrong number on arguments')

            unique_token = args[0]


        elif ( os.path.basename(program_name) == "glist" ):
            # Glist
            # Parse command line arguments
            
            _usage = "Usage: %prog [options] resource_name"
            parser = OptionParser(usage=_usage)
            parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
            parser.add_option("-s", "--short", action="store_true", dest="shortview", help="Short view.")
            parser.add_option("-l", "--long", action="store_false", dest="shortview", help="Long view.")
            (options, args) = parser.parse_args()
            
            # Configure logging service
            # gc3utils.log.basicConfig(format='%(asctime)s: %(levelname)s [%(name)s_%(module)s_%(funcName)s]:  %(message)s')
            gc3utils.utils.configure_logger(options.verbosity,'gc3utils','/tmp/gc3utils.log')
            
                        
            
            gc3utils.log.debug('Command lines argument length: [ %d ]',len(args))
            
            if len(args) != 1:
                gc3utils.log.critical('Command line argument parsing\t\t\t[ failed ]\n\tIncorrect number of arguments; expected 1 got %d ',len(args))
                parser.print_help()
                raise Exception('wrong number on arguments')

            gc3utils.log.info('Parsing command line arguments\t\t[ ok ]')
            
            resource_name = args[0]

        else:
            # Error
            print "Unknown command "+program_name
            return 1


        # End parsing command line arguments
        # Begin implementing methods

# ======================= Start Serving Requests =======================

        try:
            # Read configuration file to create Resource lists and default values
            # resources_list is a list of resource dictionaries
            (defaults,resources_list) = gc3utils.utils.read_config(default_config_file_location)
        except:
            gc3utils.log.debug('Failed loading config file from %s',default_config_file_location)
            raise

        resources = __get_resources(options, resources_list)
        default = __get_defaults(defaults)
        gc3utils.log.debug('Creating instance of Gcli')
        _gcli = gc3utils.gcli.Gcli(default, resources)

#======================================= 

        # grid-credential-renew
        if ( os.path.basename(program_name) == "grid-credential-renew" ):
            gc3utils.log.debug('Checking grid credential')
            if not _gcli.check_authentication(gc3utils.Default.SMSCG_AUTHENTICATION):
                return _gcli.enable_authentication(gc3utils.Default.SMSCG_AUTHENTICATION)
            else:
                return True

        gc3utils.log.debug('interpreting command %s',os.path.basename(program_name))
        # gsub
        if ( os.path.basename(program_name) == "gsub" ):
            # gsub prototype: application_to_run, input_file, selected_resource, job_local_dir, cores, memory, walltime

            # Create Application obj
            application = gc3utils.Application.Application(application_tag=application_tag,input_file_name=input_file_name,job_local_dir=options.job_local_dir,requested_memory=options.memory_per_core,requested_cores=options.ncores,requestd_resource=options.resource_name,requested_walltime=options.walltime,application_arguments=options.application_arguments)

            if not application.is_valid():
                raise Exception('Failed creating application object')

            job = _gcli.gsub(application)
            
            if job.is_valid():
                gc3utils.utils.display_job_status([job])
                return 0
            else:
                raise Exception('Job object not valid')

        # gstat    
        elif (os.path.basename(program_name) == "gstat" ):
            try:
                if (unique_token):
                    job_list = [_gcli.gstat(gc3utils.utils.get_job(unique_token))]
                else:
                    job_list = _gcli.gstat()
            except:
                gc3utils.log.critical('Failed retrieving job status')
                raise

            # Check validity of returned list
            for _job in job_list:
                if not _job.is_valid():
                    gc3utils.log.error('Returned job not valid. Removing from list')

            try:
                # Print result
                gc3utils.utils.display_job_status(job_list)
            except:
                gc3utils.log.error('Failed displaying job status results')
                raise
            #### SERGIO: STOPPED WORKING HERE
            
            return 0

        # gget
        elif (os.path.basename(program_name) == "gget"):
            retval = _gcli.gget(unique_token)
            if (not retval):
                sys.stdout.write('Job results successfully retrieved in [ '+unique_token+' ]\n')
                sys.stdout.flush
            else:
                raise Exception("gget terminated")

        # glist
        elif (os.path.basename(program_name) == "glist"):
            (retval,resource_object) = _gcli.glist(resource_name)
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
        elif (os.path.basename(program_name) == "gkill"):
            retval = gcli.gkill(unique_token)
            if (not retval):
                sys.stdout.write('Sent request to kill job ' + unique_token)
                sys.stdout.write('It may take a few moments for the job to finish.')
                sys.stdout.flush()
            else:
                raise Exception("gkill terminated")
            
    except SystemExit:
        return 0
    except:
        gc3utils.log.info('%s %s',sys.exc_info()[0], sys.exc_info()[1])
        #gc3utils.log.info('%s %s',sys.exc_info()[0], sys.exc_info()[1])
        # think of a better error message
        # Should intercept the exception somehow and generate error message accordingly ?
        print os.path.basename(program_name)+" failed: "+str(sys.exc_info()[1])
        return 1
                
if __name__ == "__main__":
      sys.exit(main())


# ======= Obsolete =============

#===============================================================================
#    def checkGridCredential(self):
#        if (not checkGridAccess()):
#            if ( self.defaults['email_contact'] != "" ):
#                gc3utils.log.debug('Sending notification email to [ %s ]',self.defaults['email_contact'])
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
#                    gc3utils.log.debug('Adding valid resource description [ %s ]',_resource['resource_name'])
#                    self.resource_list[_resource['resource_name']] = _resource
# 
#            # Check if any resource configuration has been leaded
#            if ( len(self.resource_list) == 0 ):
#                raise Exception('could not read any valid resource configuration from config file')
# 
#            gc3utils.log.info('Loading configuration file %s \t[ ok ]',config_file_location)
#        except:
#            gc3utils.log.critical('Failed init gcli')
#            raise
#===============================================================================

#===============================================================================
#    def glist(self, resource_name):
#        # Returns an instance of object Resource containing a dictionary of Resource informations
#        # Throw an Exception in case the method cannot be completed or the Resource object cannot be built
#        try:
#            if resource_name is None:
#                # for time being we raise an exception with no implemented
#                raise Exception('glist with no resource_name not yet implemented')
# 
#            if ( resource_name in self.resource_list ):
#                gc3utils.log.debug('Found match for user defined resource: %s',resource_name)
#                resource_description = self.resource_list[resource_name]
#            else:
#                gc3utils.log.critical('failed matching user defined resource: %s ',resource_name)
#                raise Exception('failed matching user defined resource')
#            
#            gc3utils.log.info('Check user defined resources\t\t\t[ ok ]')
# 
#            if ( resource_description['type'] == "arc" ):
#                lrms = ArcLrms(resource_description)
#            elif ( resource_description['type'] == "ssh"):
#                lrms = SshLrms(resource_description)
#            else:
#                gc3utils.log.error('Unknown resource type %s',resource_description['type'])
#                raise Exception('Unknown resource type')
# 
#            return [0,lrms.GetResourceStatus()]
#            
#        except:
#            gc3utils.log.debug('glist failed due to exception')
#            raise
#===============================================================================

    #===========================================================================
    # def checkGridCredential(self):
    #    if (not checkGridAccess()):
    #        if ( self.defaults['email_contact'] != "" ):
    #            gc3utils.log.debug('Sending notification email to [ %s ]',self.defaults['email_contact'])
    #            send_email(self.defaults['email_contact'],"info@gc3.uzh.ch","GC3 Warning: Renew Grid \
    #            credential","Please renew your credential")
    #===========================================================================

    #===========================================================================
    # def checkGridAccess(self):
    #    # First check whehter it is necessary to check grid credential or not
    #    # if selected resource is type ARC or if there is at least 1 ARC resource in the resource list, then check Grid credential
    #    
    #    gc3utils.log.debug('gcli: Checking Grid Credential')
    #    if ( (not utils.CheckGridAuthentication()) | (not utils.checkUserCertificate()) ):
    #        gc3utils.log.error('Credential Expired')
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
#            gc3utils.log.debug('checking AAI credential file [ %s ]',self.AAI_CREDENTIAL_REPO)
#            if ( os.path.exists(self.AAI_CREDENTIAL_REPO) & os.path.isfile(self.AAI_CREDENTIAL_REPO) ):
#                gc3utils.log.debug('Opening AAI credential file in %s',self.AAI_CREDENTIAL_REPO)
#                _fileHandle = open(self.AAI_CREDENTIAL_REPO,'r')
#                _aaiUserName = _fileHandle.read()
#                _aaiUserName = _aaiUserName.rstrip("\n")
#                gc3utils.log.debug('_aaiUserName: %s',_aaiUserName)
#                RenewGridCredential(_aaiUserName)
#            else:
#                gc3utils.log.critical('AAI_Credential information file not found')
#                raise Exception('AAI_Credential information file not found')
#        except:
#            gc3utils.log.critical('Failed renewing grid credential [%s]',sys.exc_info()[1])
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
                                              
#===============================================================================
#    def gget(self, unique_token):
#        global default_job_folder_location
#        global default_joblist_location
#        global default_joblist_lock
# 
#        if ( (os.path.exists(unique_token) == False ) | (os.path.isdir(unique_token) == False) | ( not check_inputfile(unique_token+'/'+self.defaults['lrms_jobid']) ) ):
#            gc3utils.log.critical('Jobid Not valid')
#            raise Exception('invalid jobid')
# 
#        gc3utils.log.info('unique_token file check\t\t\t[ ok ]')
# 
#        # check .finished file
#        if ( not check_inputfile(unique_token+'/'+self.defaults['lrms_finished']) ):
#            _fileHandle = open(unique_token+'/'+self.defaults['lrms_jobid'],'r')
#            _raw_resource_info = _fileHandle.read()
#            _fileHandle.close()
# 
#            _list_resource_info = re.split('\t',_raw_resource_info)
# 
#            gc3utils.log.debug('lrms_jobid file returned %s elements',len(_list_resource_info))
# 
#            if ( len(_list_resource_info) != 2 ):
#                raise Exception('failed retieving jobid')
# 
#            gc3utils.log.debug('frontend: [ %s ] jobid: [ %s ]',_list_resource_info[0],_list_resource_info[1])
#            gc3utils.log.info('reading lrms_jobid info\t\t\t[ ok ]')
# 
#            if ( _list_resource_info[0] in self.resource_list ):
#                gc3utils.log.debug('Found match for resource [ %s ]',_list_resource_info[0])
#                gc3utils.log.debug('Creating lrms instance')
#                resource = self.resource_list[_list_resource_info[0]]
#                if ( resource['type'] == "arc" ):
#                    lrms = ArcLrms(resource)
#                elif ( resource['type'] == "ssh"):
#                    lrms = SshLrms(resource)
#                else:
#                    gc3utils.log.error('Unknown resource type %s',resource['type'])
#                    raise  Exception('unknown resource type')
# 
#                if ( (lrms.isValid != 1) | (lrms.check_authentication() == False) ):
#                    gc3utils.log.error('Failed validating lrms instance for resource %s',resource['resource_name'])
#                    raise Exception('failed authenticating to LRMS')
# 
#                gc3utils.log.info('Init LRMS\t\t\t[ ok ]')
#                _lrms_jobid = _list_resource_info[1]
#                gc3utils.log.debug('_list_resource_info : ' + _list_resource_info[1])
#                
#                #_lrms_dirfolder = dirname(unique_token)
#                (retval,lrms_log) = lrms.get_results(_lrms_jobid,unique_token)
# 
#                # dump lrms_log
#                try:
#                    gc3utils.log.debug('Dumping lrms_log')
#                    _fileHandle = open(unique_token+'/'+self.defaults['lrms_log'],'a')
#                    _fileHandle.write('=== gget ===\n')
#                    _fileHandle.write(lrms_log+'\n')
#                    _fileHandle.close()
#                except:
#                    gc3utils.log.error('Failed dumping lrms_log [ %s ]',sys.exc_info()[1])
#                    
#                if ( retval == False ):
#                    gc3utils.log.error('Failed getting results')
#                    raise Exception('failed getting results from LRMS')
#                
#                gc3utils.log.debug('check_status\t\t\t[ ok ]')
# 
#                # Job finished; results retrieved; writing .finished file
#                try:
#                    gc3utils.log.debug('Creating finished file')
#                    open(unique_token+"/"+self.defaults['lrms_finished'],'w').close()
#                except:
#                    gc3utils.log.error('Failed creating finished file [ %s ]',sys.exc_info()[1])
#                    # Should handle the exception differently ?      
# 
#                gc3utils.log.debug('Removing jobid from joblist file')
#                # Removing jobid from joblist file
#                try:
#                    default_joblist_location = os.path.expandvars(default_joblist_location)
#                    default_joblist_lock = os.path.expandvars(default_joblist_lock)
#                    
#                    if ( obtain_file_lock(default_joblist_location,default_joblist_lock) ):
#                        _newFileHandle = tempfile.NamedTemporaryFile(suffix=".xrsl",prefix="gridgames_arc_")
#                        
#                        _oldFileHandle  = open(default_joblist_location)
#                        _oldFileHandle.seek(0)
#                        for line in _oldFileHandle:
#                            gc3utils.log.debug('checking %s with %s',line,unique_token)
#                            if ( not unique_token in line ):
#                                gc3utils.log.debug('writing line')
#                                _newFileHandle.write(line)
# 
#                        _oldFileHandle.close()
# 
#                        os.remove(default_joblist_location)
# 
#                        _newFileHandle.seek(0)
# 
#                        gc3utils.log.debug('replacing joblist file with %s',_newFileHandle.name)
#                        os.system("cp "+_newFileHandle.name+" "+default_joblist_location)
# 
#                        _newFileHandle.close()
# 
#                    else:
#                        raise Exception('Failed obtain lock')
#                except:
#                    gc3utils.log.error('Failed updating joblist file in %s',default_joblist_location)
#                    gc3utils.log.debug('Exception %s',sys.exc_info()[1])
# 
#                # release lock
#                if ( (not release_file_lock(default_joblist_lock)) & (os.path.isfile(default_joblist_lock)) ):
#                    gc3utils.log.error('Failed removing lock file')
# 
#            else:
#                gc3utils.log.critical('Failed finding matching resource name [ %s ]',_list_resource_info[0])
#                raise
#        return 0
# 
# 
# 
# 
#                
#        #=======================================================================
#        # if ( _list_resource_info[0] in self.resource_list ):
#        #        gc3utils.log.debug('Found match for resource [ %s ]',_list_resource_info[0])
#        #        gc3utils.log.debug('Creating lrms instance')
#        #        resource = self.resource_list[_list_resource_info[0]]
#        #        if ( resource['type'] == "arc" ):
#        #            lrms = ArcLrms(resource)
#        #        elif ( resource['type'] == "ssh"):
#        #            lrms = SshLrms(resource)
#        #        else:
#        #            gc3utils.log.error('Unknown resource type %s',resource['type'])
#        #            raise Exception('unknown resource type')
#        #=======================================================================
#===============================================================================
        
#===============================================================================
#    def gstat(self, unique_token):
#        global default_joblist_location
# 
#        if ( unique_token != None):
#            return [0,[self.__gstat(unique_token)]]
#        else:
#            # Read content of .joblist and return gstat for each of them
#            default_joblist_location = os.path.expandvars(default_joblist_location)
#            joblist  = open(default_joblist_location,'r')
#            joblist.seek(0)
#            lrmsjobid_single_string = joblist.read()
#            joblist.close()
#            lrmsjobid_list = re.split('\n',lrmsjobid_single_string)
#            status_list = []
#            if ( len(lrmsjobid_list) > 0 ):
#                for _lrmsjobid in lrmsjobid_list:
#                    if ( _lrmsjobid != "" ):
#                        gc3utils.log.debug('Checking status fo jobid [ %s ]',_lrmsjobid)
#                        status_list.append(self.__gstat(_lrmsjobid))
#            gc3utils.log.debug('status_list contains [ %d ] elelemnts',len(status_list))
#            return [0,status_list]
#===============================================================================

#===============================================================================
#    def __gstat(self, unique_token):
#        if ( (os.path.exists(unique_token) == False ) | (os.path.isdir(unique_token) == False) | ( not check_inputfile(unique_token+'/'+self.defaults['lrms_jobid']) ) ):
#            gc3utils.log.critical('Jobid Not valid')
#            raise Exception('invalid jobid')
# 
#        gc3utils.log.info('lrms_jobid file check\t\t\t[ ok ]')
# 
#        # check finished file
#        if ( not check_inputfile(unique_token+'/'+self.defaults['lrms_finished']) ):
#            _fileHandle = open(unique_token+'/'+self.defaults['lrms_jobid'],'r')
#            _raw_resource_info = _fileHandle.read()
#            _fileHandle.close()
# 
#            _list_resource_info = re.split('\t',_raw_resource_info)
#            
#            gc3utils.log.debug('frontend: [ %s ] jobid: [ %s ]',_list_resource_info[0],_list_resource_info[1])
#            gc3utils.log.info('reading lrms_jobid info\t\t\t[ ok ]')
#            
#            if ( _list_resource_info[0] in self.resource_list ):
#                gc3utils.log.debug('Found match for resource [ %s ]',_list_resource_info[0])
#                gc3utils.log.debug('Creating lrms instance')
#                resource = self.resource_list[_list_resource_info[0]]
#                if ( resource['type'] == "arc" ):
#                    lrms = ArcLrms(resource)
#                elif ( resource['type'] == "ssh"):
#                    lrms = SshLrms(resource)
#                else:
#                    gc3utils.log.error('Unknown resource type %s',resource['type'])
#                    raise Exception('unknown resource type')
# 
#                # check authentication
#                if ( (lrms.isValid != 1) | (lrms.check_authentication() == False) ):
#                    gc3utils.log.error('Failed validating lrms instance for resource %s',resource['resource_name'])
#                    raise Exception('failed authenticating to LRMS')
# 
#                gc3utils.log.info('Init LRMS\t\t\t[ ok ]')
#                _lrms_jobid = _list_resource_info[1]
#                _lrms_dirfolder = dirname(unique_token)
# 
#                # check job status
#                (retval,lrms_log) = lrms.check_status(_lrms_jobid)
# 
#                gc3utils.log.info('check status\t\t\t[ ok ]')
#            else:
#                gc3utils.log.critical('Failed finding matching resource name [ %s ]',_list_resource_info[0])
#                raise Exception('failed finding matching resource')
# 
#        else:
#            retval = "Status: FINISHED"
# 
#        gc3utils.log.debug('Returning [ %s ] [ %s ]',unique_token,retval)
# 
#        return [unique_token,retval]
# 
# 
#    # Internal functions
#===============================================================================
