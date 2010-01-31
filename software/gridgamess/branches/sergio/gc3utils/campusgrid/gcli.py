from utils import *
import sys
import os
import logging
import ConfigParser
from optparse import OptionParser
from ArcLRMS import *

default_config_file_location="$HOME/.gc3/config"
default_joblist_location="$HOME/.gc3/.joblist"
default_joblist_lock="$HOME/.gc3/.joblist_lock"
default_job_folder_location="$PWD"
default_wait_time = 3

class Gcli:

    resource_list = {}
    defaults = {}


    def __init__(self, config_file_location):
        try:
            # read configuration file
            (self.defaults,self.resource_list) = readConfig(config_file_location)

            # Check if any resource configuration has been leaded
            if ( len(self.resource_list) == 0 ):
                raise Exception('could not read any valid resource configuration from config file')

            logging.info('Loading configuration file %s \t[ ok ]',config_file_location)
        except:
            logging.critical('Failed init gcli')
            raise
                                                        

    def __select_lrms(self,lrms_list):
        return 0

    def gsub(self, application_to_run, input_file, selected_resource, job_local_dir, cores, memory, walltime):
        global default_job_folder_location
        global default_joblist_location
        global default_joblist_lock

        try:
            # Checking whether it has been passed a valid application
            if ( application_to_run != "gamess" ) & ( application_to_run != "apbs" ):
                logging.critical('Application argument\t\t\t[ failed ]\n\tUnknown application: '+application_to_run)
                raise Exception('invalid application argument')

            # Check input file
            if ( not check_inputfile(input_file) ):
                logging.critical('Input file argument\t\t\t[ failed ]'+input_file)
                raise Exception('invalid input-file argument')

            logging.info('Parsing arguments\t\t[ ok ]')


            # Initialize LRMSs
            _lrms_list = []

            if ( selected_resource != None ):
                if ( selected_resource in self.resource_list ):
                    logging.debug('Found match for user defined resource: %s',selected_resource)
                    candidate_resource = [self.resource_list[selected_resource]]
                else:
                    logging.critical('failed matching user defined resource: %s ',selected_resource)
                    raise Exception('failed matching user defined resource')

                logging.info('Check user defined resources\t\t\t[ ok ]')

            else:
                candidate_resource = self.resource_list.values()
                logging.debug('Creating list of lrms instances')
                
            # start candidate_resource loop
            for resource in candidate_resource:
                logging.debug('Creating instance of type %s for %s',resource['type'],resource['frontend'])
                if ( resource['type'] == "arc" ):
                    lrms = ArcLrms(resource)
                elif ( resource['type'] == "ssh"):
                    lrms = SshLrms(resource)
                else:
                    logging.error('Unknown resource type %s',resource['type'])
                    continue

                if ( (lrms.isValid == 1) & (lrms.check_authentication() == True) ):
                    _lrms_list.append(lrms)
                else:
                    logging.error('Failed validating lrms instance for resource %s',resource['resource_name'])

            # end of candidate_resource loop

            if ( len(_lrms_list) == 0 ):
                logging.critical('Could not initialize ANY lrms resource')
                raise Exception('no available LRMS found')

            logging.info('Init pool of LRMS resources \t\t\t[ ok ]')

            # check that qgms is a good version
            minimum_version = 0.1
            if ( not check_qgms_version(minimum_version) ):
                logging.warning('Application version mismatch')

            # decide which resource to use
            # select_lrms returns an index
            _selected_lrms = self.__select_lrms(_lrms_list)

            logging.debug('Selected LRMS: %s',_selected_lrms)

            # we trust select_lrms method to return a valid index
            # shall we cross check ?
            lrms = _lrms_list[_selected_lrms]

            logging.debug('LRMS selected %s %s',lrms.resource['frontend'],lrms.resource['resource_name'])
            logging.info('Select LRMS\t\t\t\t\t[ ok ]')

            # _dirname is basedir of inputfile
            # _inputname is the input name of te inputfile (e.g. exam01 from exam01.inp)
            # _inputfilename is the basename of the inputfile
            _dirname = dirname(input_file)
            _inputname = inputname(input_file)
            _inputfilename = inputfilename(input_file)

            # create_unique_token
            unique_token = create_unique_token(input_file,lrms.resource['resource_name'])

            logging.debug('Generate Unique token: %s',unique_token)
            logging.info('Generate Unique token\t\t\t[ ok ]')

            # creating folder for job's session
            default_job_folder_location = os.path.expandvars(default_job_folder_location)

            logging.debug('creating folder for job session: %s/%s',default_job_folder_location,unique_token)
            os.mkdir(default_job_folder_location+'/'+unique_token)

            logging.info('Create job folder\t\t\t[ ok ]')
                                                                                          
            lrms_log = None
            lrms_jobid = None

            # resource_name.submit_job(input, unique_token, application, lrms_log) -> returns [lrms_jobid,lrms_log]
            logging.debug('Submitting job with %s %s %s %s',unique_token, application_to_run, input_file, self.defaults['lrms_log'])
            (lrms_jobid,lrms_log) = lrms.submit_job(unique_token, application_to_run, input_file)

            logging.info('Submission process to LRMS backend\t\t\t[ ok ]')

            # dump lrms_log
            try:
                logging.debug('Dumping lrms_log and lrms_jobid')
                _fileHandle = open(default_job_folder_location+'/'+unique_token+'/'+self.defaults['lrms_log'],'a')
                _fileHandle.write(lrms_log+'\n')
                _fileHandle.close()
            except:
                logging.error('Failed dumping lrms_log [ %s ]',sys.exc_info()[1])

            if ( lrms_jobid == None ):
                logging.critical('Submit to LRMS\t\t\t[ failed ]')
                raise Exception('submission to LRMS failed')
            else:
                logging.info('Submit to LRMS\t\t\t\t[ ok ]')

            # dumping lrms_jobid
            # not catching the exception as this is suppoed to be a fatal failure;
            # thus propagated to gsub's main try
            _fileHandle = open(default_job_folder_location+'/'+unique_token+'/'+self.defaults['lrms_jobid'],'w')
            _fileHandle.write(lrms.resource['resource_name']+'\t'+lrms_jobid)
            _fileHandle.close()

            # if joblist_location & joblist_lock are not defined, use default
            try:
                joblist_location
            except NameError:
                joblist_location = os.path.expandvars(default_joblist_location)

            try:
                joblist_lock
            except NameError:
                joblist_lock = os.path.expandvars(default_joblist_lock)

            # if joblist_location does not exist, create it
            if not os.path.exists(joblist_location):
                open(joblist_location, 'w').close()
                logging.debug(joblist_location + ' did not exist.  created it.')

            logging.debug('appending jobid to .jobs file as specified in defaults')
            try:
                # appending jobid to .jobs file as specified in defaults
                logging.debug('obtaining lock')
                if ( obtain_file_lock(joblist_location,joblist_lock) ):
                    _fileHandle = open(joblist_location,'a')
                    _fileHandle.write(default_job_folder_location+'/'+unique_token+'\n')
                    _fileHandle.close()
                else:
                    raise Exception('Failed obtain lock')

            except:
                logging.error('Failed in appending current jobid to list of jobs in %s',joblist_location)
                logging.debug('Exception %s',sys.exc_info()[1])

            # release lock
            if ( (not release_file_lock(joblist_lock)) & (os.path.isfile(joblist_lock)) ):
                logging.error('Failed removing lock file')
                
            logging.info('Dumping lrms log information\t\t\t[ ok ]')

            return [0,default_job_folder_location+'/'+unique_token]

        except:
            raise
                                              
    def gget(self, jobid):
        pass

    def gstat(self, jobod):
        pass

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
            parser.add_option("-r", "--resource", action="store", dest="resource_name", metavar="STRING", default="", help='Select resource destination')
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
                                        
        elif ( os.path.basename(program_name) == "gstat" ):
            # Gstat
            print "Called gstat"
        elif ( os.path.basename(program_name) == "gget" ):
            print "Called gget"
            # Gget
        else:
            # Error
            print "Unknown command "+program_name
            return 1

        gcli = Gcli(default_config_file_location)

        if ( os.path.basename(program_name) == "gsub" ):
            # gsub prototype: application_to_run, input_file, selected_resource, job_local_dir, cores, memory, walltime
            (exitcode,jobid) = gcli.gsub(args[0],os.path.abspath(args[1]),None,None,None,None,None)
            if (not exitcode):
                print jobid
            else:
                raise Exception("submission terminated")
    except:
        logging.info('%s',sys.exc_info()[1])
        # think of a better error message
        # Should intercept the exception somehow and generate error message accordingly ?
        print "gsub failed: "+str(sys.exc_info()[1])
        return 1
                
if __name__ == "__main__":
      sys.exit(main())
      
