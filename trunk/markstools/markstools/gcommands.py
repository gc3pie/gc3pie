#!/usr/bin/env python

import sys
import os
import ConfigParser
from optparse import OptionParser

import markstools
from markstools.lib.exceptions import *
from markstools.lib.utils import configure_logger, read_config

from gorg.lib.utils import Mydb


# defaults - XXX: do they belong in ../gcli.py instead?
_homedir = os.path.expandvars('$HOME')
_rcdir = _homedir + "/.markstools"
_default_config_file_location = _rcdir + "/markstools.conf"
_default_job_folder_location = os.getcwd()


def _configure_system():
    config = read_config(_default_config_file_location)
    return config

#====== Main ========

def ghessian(*args, **kw):
    from markstools.io.gamess import ReadGamessInp, WriteGamessInp
    from markstools.calculators.gamess.calculator import GamessGridCalc
    from markstools.usertasks.ghessian import GHessian

    config = _configure_system()
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-f", "--file", dest="file",default='markstools/examples/water_UHF_gradient.inp', 
                      help="gamess inp to restart from.")
    parser.add_option("-v", "--verbose", action='count', dest="verbosity", default=config.verbosity, 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()

    configure_logger(options.verbosity) 

    # Connect to the database
    db = Mydb(config.database_user,config.database_name,config.database_url).cdb()

    try:
        myfile = open(options.file, 'rb')
        reader = ReadGamessInp(myfile)
    finally:
        myfile.close()

    params = reader.params
    atoms = reader.atoms
    
    ghessian = GHessian()
    gamess_calc = GamessGridCalc(db)
    ghessian.initialize(db, gamess_calc, atoms, params)
    ghessian.run()

def gtaskscheduler(*args, **kw):
    from markstools.usertasks.taskscheduler import TaskScheduler

    config = _configure_system()
    
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", action='count',dest="verbosity", default=config.verbosity, 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()

    configure_logger(options.verbosity) 

    task_scheduler = TaskScheduler(config.database_user,config.database_name,config.database_url)
    task_scheduler.run()

def gtestcron(*args, **kw):
    import time
    
    config = _configure_system()
    
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", action='count',dest="verbosity", default=config.verbosity, 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()

    configure_logger(options.verbosity) 
    markstools.log.debug('The gtestcron function was ran at %s.'%(time.asctime()))

def gorgsetup(*args, **kw):
    from gorg.model.gridjob import GridjobModel, GridrunModel
    from gorg.model.baserole import BaseroleModel
    from gorg.model.gridtask import GridtaskModel    
    
    config = _configure_system()
    # Connect to the database
    is_db_created = Mydb(config.database_user,config.database_name,config.database_url).createdatabase()
    db = Mydb(config.database_user,config.database_name,config.database_url).cdb()

    BaseroleModel.sync_views(db)
    GridjobModel.sync_views(db)
    GridrunModel.sync_views(db)
    GridtaskModel.sync_views(db)

if __name__ == '__main__':
    #This is needed because eric4 sends the following to the sys.argv variable
    #['gcommands.py', 'gcommands.py', u'gtestcron']
    #While the command line sends:
    #['gcommands.py', u'gtestcron']
    #Therefore we must  deal with both inputs

    for i in range(len(sys.argv)):
        if sys.argv[i].find('gcommands.py') == -1:
            break
    eval('%s(sys.argv[%d:])'%(sys.argv[i], i))
    sys.exit(0)
