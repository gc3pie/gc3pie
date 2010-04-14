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
#from ase-patched import *

homedir = os.path.expandvars('$HOME')
rcdir = homedir + "/.gc3"
default_config_file_location = rcdir + "/config"
default_joblist_location = rcdir + "/.joblist"
default_joblist_lock = rcdir + "/.joblist_lock"
default_job_folder_location="$PWD"
default_wait_time = 3

'''

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
'''
   
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

 
def CheckInputFile(inputfile):
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
        sysargs = sys.argv
        
        options, args = ParseOptions(program_name, sysargs)

        # Configure logging service
        logger = CreateBasicLogger(options.verbosity)
        
        if options.input == None:
            logging.info('No input file specified.  Exiting.')
            sys.exit(1)
                    
 
        # Check input file.  Can be made fancy later.
        try:
            CheckInputFile(options.input)
        except:
            raise
        
        #db=Mydb(options.db_name,options.db_loc).cdb()
        database = SetupDatabase()
        
        # Call a different method depending on the type of task we want to create.
        if options.tasktype == 'hessian':
            try:
                # task = HessTask()
                gamess_calc = GamessGridCalc(database)
                ghess = GHessian(database, gamess_calc, logging_level)
                ghess.run(atoms, params)
            except:
                raise     
        elif options.tasktype == 'single':
            try:
                #task = SinglejobTask()
                gamess_calc = GamessGridCalc(database)
                gsingle = GSingle(database, gamess_calc, logging_level)
                gsingle.run(atoms, params)                
            except:
                raise           
        elif options.tasktype == 'restart':
            try:
                #task = ContinueTask()
                gamess_calc = GamessGridCalc(database)
                grestart = GRestart(db, gamess_calc, logging_level)
                grestart.run(atoms, params)
            except:
                raise
        else:
            logging.error('unknown task type')
            sys.exit(1)     

        #hcli = Hcli(default_config_file_location)
        
        logger.error("your task id is" + task.id)
        

    except:
        raise
        sys.exit(1)
       
                
if __name__ == "__main__":
      sys.exit(main())
      
