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

homedir = os.path.expandvars('$HOME')
rcdir = homedir + "/.gc3"
default_config_file_location = rcdir + "/config"
default_joblist_location = rcdir + "/.joblist"
default_joblist_lock = rcdir + "/.joblist_lock"
default_job_folder_location="$PWD"
default_wait_time = 3

class Hcli:

    resource_list = {}
    defaults = {}


    def __init__(self, config_file_location):
        try:
# probably don't need this stuff
            # read configuration file
            """
            _local_resource_list = {}
            (self.defaults,_local_resource_list) = readConfig(config_file_location)


            for _resource in _local_resource_list.values():
                if ("ncores" in _resource) & ("memory_per_core" in _resource) & \
                        ("walltime" in _resource) & ("type" in _resource) & ("frontend" in _resource) \
                        & ("applications" in _resource):
                    # Adding valid resources
                    logging.debug('Adding valid resource description [ %s ]',_resource['resource_name'])
                    self.resource_list[_resource['resource_name']] = _resource

            # Check if any resource configuration has been leaded
            if ( len(self.resource_list) == 0 ):
                raise Exception('could not read any valid resource configuration from config file')

            logging.info('Loading configuration file %s \t[ ok ]',config_file_location)
            """
        except:
            """
            logging.critical('Failed init gcli')
            """
            raise

    def Task(self,username,tasktitle,tasktype):
        """Create a task.""" 
        
        # Handle tasktype-specific things.
        if tasktype == 'hessian':
            print 'hessian'
        elif tasktype == 'singlejob':
            print 'singlejob'
        elif tasktype == 'continue':
            print 'continue'
        
        try:
            task = GridtaskModel().create(username, tasktitle)
        except:
            raise
    
        return task
    
 
    def ParseOptions(self,program_name,args):
        """Parse our general command line options."""
        
        _usage = "Usage: %prog [options] jobid"
        
        parser = OptionParser(usage=_usage)
        parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
        (options, args) = parser.parse_args()

        # todo : change these options to real ones

        # general options 
        parser.add_option("-e","--example", action="store", dest="mpi_path", default=None, help="example")
        parser.add_option("-d","--dummy", action="store", dest="mpi_path", default=None, help="dummy")

        # handle tasktype-specific options
        if tasktype == 'hessian':
            parser.add_option("-f","--foo", action="store", dest="mpi_path", default=None, help="example")
        elif tasktype == 'singlejob':
            parser.add_option("-g","--goo", action="store", dest="mpi_path", default=None, help="example")
        elif tasktype == 'continue':
            parser.add_option("-h","--hoo", action="store", dest="mpi_path", default=None, help="example")

      
        (options, args) = parser.parse_args()

        return options, args

            
        return options
        
        
            
        
    def ParseOptions(sefl,tasktype):
        """Parse our command line options."""


    
    def ConfigureLogging(self, verbosity):
        """Set up the logging system."""
    
        configure_logging(verbosity)
        
        return
    
    def SetupDatabase(self):
        """Initialize the database.  
         * If the database does not exist, create it.
         * If the tables do not exist, create them.
         
         Return a database object.
        """ 
        # todo : do we return a database object, or a database connection object, or ???
        
        dbhost = 'http://127.0.0.1'
        port = '5984'
        address = dbhost + ":" + port
        database = Mydb('gorg_site',address).createdatabase()
        database = Mydb('gorg_site',address).cdb()
        
        # todo : find out what this does
        GridjobModel.sync_views(database)
        GridrunModel.sync_views(database)
        GridtaskModel.sync_views(database)
        BaseroleModel.sync_views(database)
        
        return database
    
    def CheckInputFile(self,inputfile):
        """Perform various checks on the input file."""
        
        # todo : add some checks to see if it's a valid input file?
        try: 
            os.path.isfile(inputfile)
            return True
        except:
            raise 
            return False
        

      
def main():
    global default_job_folder_location
    global default_joblist_location
    global default_joblist_lock

    try:
        
        program_name = sys.argv[0]

        # todo : do we need this?  does the parser automatically know to look at the arguments?
        args = sys.argv
        ParseOptions(program_name,args)
        
        # Configure logging service
        ConfigureLogging(options.verbosity)

        # Check input file.  Can be made fancy later.
        CheckInputFile(inputfile)
        
        
    
        
        # Call a different method depending on the type of task we want to create.
        if tasktype == 'hessian':
            try:
                task = HessTask()
            except:
                raise     
        elif tasktype == 'singlejob':
            try:
                task = SinglejobTask()
            except:
                raise
        elif tasktype == 'continue':
            try:
                task = ContinueTask()
            except:
                raise
        else:
            logging.error('unknown task type')
            sys.exit(1)     

        hcli = Hcli(default_config_file_location)
        
        logging.error("your task id is" + task.id)
        

    except:
        raise
        sys.exit(1)
       
                
if __name__ == "__main__":
      sys.exit(main())
      
