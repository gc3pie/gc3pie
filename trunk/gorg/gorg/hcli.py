#!/usr/bin/env python

__author__="Sergio Maffioletti (sergio.maffioletti@gc3.uzh.ch)"
__date__="01 february 2010"
__copyright__="Copyright 2009 2011 Grid Computing Competence Center - UZH/GC3"
__version__="0.2"

from utils import *
import sys
import os
import logging
import logging.handlers
import ConfigParser
from optparse import OptionParser
from ArcLRMS import *
from SshLRMS import *
from ase-patched import *

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
        """
        Parse our general command line options.
    
         * Takes as input a program name and the command line arguments.
         * Returns: dictionary of options (options), list of remaining arguments (args)
     
        """
        
        _usage = "Usage: %prog [options] jobid"
        
        parser = OptionParser(usage=_usage)
        
        # todo : remove dummy options

        # common options 
        parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
        parser.add_option("-e","--example", action="store", dest="example", default=None, help="example")
        parser.add_option("-w","--woo", action="store", dest="woo", default=None, help="dummy")
        parser.add_option("-d", "--dir", dest="directory",default='~/tasks', help="Directory to save tasks in.")
        parser.add_option("-f", "--file", dest="file",default='exam01.inp', help="Gamess inp to restart from.")
        parser.add_option("-n", "--db_name", dest="db_name", default='gorg_site', help="Database name.")
        parser.add_option("-l", "--db_loc", dest="db_loc", default='http://127.0.0.1:5984', help="Database URI.")

        # handle tasktype-specific options
        if tasktype == 'hessian':
            parser.add_option("-x","--xoo", action="store", dest="xoo", default=None, help="example")
        elif tasktype == 'singlejob':
            parser.add_option("-y","--yoo", action="store", dest="yoo", default=None, help="example")
        elif tasktype == 'continue':
            parser.add_option("-z","--zoo", action="store", dest="zoo", default=None, help="example")
      
        (options, args) = parser.parse_args()

        return options, args
    
    
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
        
        
        db=Mydb(options.db_name,options.db_loc).cdb()

        
        # Call a different method depending on the type of task we want to create.
        if tasktype == 'hessian':
            try:
                # task = HessTask()
                gamess_calc = GamessGridCalc('mark', db)
                ghess = GHessian(db, gamess_calc, logging_level)
                ghess.run(atoms, params)
            except:
                raise     
        elif tasktype == 'single':
            try:
                #task = SinglejobTask()
                gamess_calc = GamessGridCalc('mark', db)
                gsingle = GSingle(db, gamess_calc, logging_level)
                gsingle.run(atoms, params)                
            except:
                raise
        elif tasktype == 'restart':
            try:
                #task = ContinueTask()
                gamess_calc = GamessGridCalc('mark', db)
                grestart = GRestart(db, gamess_calc, logging_level)
                grestart.run(atoms, params)
            except:
                raise
        else:
            logging.error('unknown task type')
            sys.exit(1)     

        hcli = Hcli(default_config_file_location)
        
        logger.error("your task id is" + task.id)
        

    except:
        raise
        sys.exit(1)
       
                
if __name__ == "__main__":
      sys.exit(main())
      
