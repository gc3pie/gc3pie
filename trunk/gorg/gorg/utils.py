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
from gorg_site.gorg_site.lib.mydb import Mydb
from gorg_site.gorg_site.model.gridjob import GridjobModel
from gorg_site.gorg_site.model.gridjob import GridrunModel
#from gorg_site.gorg_site.model.gridjob import GridtaskModel
from gorg_site.gorg_site.model.gridjob import BaseroleModel


from optparse import OptionParser
#from ase-patched import *

homedir = os.path.expandvars('$HOME')
rcdir = homedir + "/.gc3"
default_config_file_location = rcdir + "/config"
default_joblist_location = rcdir + "/.joblist"
default_joblist_lock = rcdir + "/.joblist_lock"
default_job_folder_location="$PWD"
default_wait_time = 3


 
def ParseOptions(program_name,args):
    """
    Parse our general command line options.

     * Takes as input a program name and the command line arguments.
     * Returns: dictionary of options (options), list of remaining arguments (args)
 
    """
    
    _usage = "Usage: %prog [options]"
    
    parser = OptionParser(usage=_usage)
    
    # todo : remove dummy options

    tasktype = program_name 
    
    # common options 
    #parser.add_option("-t","--task", action="store", dest="tasktype", default=None, help="Which type of task to perform.")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    parser.add_option("-e","--example", action="store", dest="example", default=None, help="example")
    parser.add_option("-w","--woo", action="store", dest="woo", default=None, help="dummy")
    parser.add_option("-d", "--dir", dest="directory", default='~/tasks', help="Directory to save tasks in.")
    parser.add_option("-i", "--input", dest="input", default=None, help="Input file.")
    parser.add_option("-n", "--db_name", dest="db_name", default='gorg_site', help="Database name.")
    parser.add_option("-l", "--db_loc", dest="db_loc", default='http://127.0.0.1:5984', help="Database URI.")

    
    # handle tasktype-specific options
    if tasktype == 'hessian':
        parser.add_option("-x","--xoo", action="store", dest="xoo", default=None, help="example")
    elif tasktype == 'single':
        parser.add_option("-y","--yoo", action="store", dest="yoo", default=None, help="example")
    elif tasktype == 'restart':
        parser.add_option("-z","--zoo", action="store", dest="zoo", default=None, help="example")
  
    
    parser.set_default('tasktype', tasktype)
    
    (options, args) = parser.parse_args()
  
    return options, args


def SetupDatabase():
    """Initialize the database.  
     * If the database does not exist, create it.
     * If the tables do not exist, create them.
     
     Return a database object.
    """ 
    # todo : do we return a database object, or a database connection object, or ???
    
    dbhost = 'http://127.0.0.1'
    port = '5984'
    address = dbhost + ":" + port
    
    setupdatabase = Mydb(couchdb_user='mpackard',couchdb_database='gorg_site',couchdb_url=address)
    setupdatabase.createdatabase()
    database = setupdatabase.cdb()
    #Correct
    GridjobModel.sync_views(database)
    
    # todo : find out what this does
    GridjobModel.sync_views(database)
    GridrunModel.sync_views(database)
#    GridtaskModel.sync_views(database)
    BaseroleModel.sync_views(database)
    
    return database


def CheckInputFile(inputfile):
    """Perform various checks on the input file."""
    
    # todo : add some checks to see if it's a valid input file?
    try: 
        os.path.isfile(inputfile)
        return True
    except:
        raise 
        return False
        


def CreateBasicLogger(verbosity):
    """
    Configure basic logger object.
    
     - Takes as input: verbosity variable
     - Returns: logger object
     
    """
    
    if ( verbosity > 5):
        logging_level = 10
    else:
        logging_level = (( 6 - verbosity) * 10)

    logger = logging.basicConfig(level=logging_level, format='%(asctime)s %(levelname)-8s %(message)s')

    return logger


#def CreateFileLogger(verbosity):
#    """
#    Configure file logger object.
#    
#     - Takes as input: verbosity variable
#     - Returns: logger object
#     
#    """
#    
#    pid = os.getpid()
#    
#    LOG_FILENAME = '/tmp/' + pid + '_python_scheduler_logger.out'
#
#    logger = logging.getLogger("restart_main")
#    logger.setLevel(self.logging_level)
#    file_handler = logging.handlers.RotatingFileHandler(
#              self.LOG_FILENAME, maxBytes=100000, backupCount=5)
#    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
#    file_handler.setFormatter(formatter)
#    logger.addHandler(file_handler)
#    
#
#    configure_logging(verbosity)
#    
#    return logger

