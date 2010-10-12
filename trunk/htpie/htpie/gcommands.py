#!/usr/bin/env python

import sys
import os
#import getpass
import ConfigParser
from optparse import OptionParser

import htpie
from htpie.lib.exceptions import *
from htpie.lib.utils import configure_logger, read_config
from htpie.lib.flock import flock

# defaults - XXX: do they belong in ../gcli.py instead?
_homedir = os.path.expandvars('$HOME')
_rcdir = _homedir + "/.htpie"
_default_config_file_location = _rcdir + "/htpie.conf"
_default_log_file_location = _rcdir + "/gc3utils.log"
_default_job_folder_location = os.getcwd()


def _configure_system():
    config = read_config(_default_config_file_location)
    return config

#====== Main ========

def gstring(*args, **kw):
    from htpie.usertasks.gstring import GString
    from htpie.optimize import fire
    from htpie.optimize import lbfgs


    config = _configure_system()
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-s", "--start", dest="start", default='examples/opt_start2.inp', 
                      help="starting inp file for gstring method")
    parser.add_option("-e", "--end", dest="end", default='examples/opt_end2.inp', 
                      help="ending inp file for gstring method")
    parser.add_option("-a", "--application", dest="app_tag", default='gamess', 
                      help="add more v's to increase log output")
    parser.add_option("-v", "--verbose", action='count', dest="verbosity", default=config.verbosity, 
                      help="add more v's to increase log output")

    (options, args) = parser.parse_args()

    configure_logger(options.verbosity, _default_log_file_location) 

    if not os.path.isfile(options.start):
        sys.stdout.write('Can not locate file \'%s\'\n'%(options.start))
        return
    
    if not os.path.isfile(options.end):
        sys.stdout.write('Can not locate file \'%s\'\n'%(options.end))
        return
    
    #optimizer = fire.FIRE()
    optimizer = lbfgs.LBFGS()
    
    gstring = GString.create([options.start, options.end],  options.app_tag, optimizer)
    
    if gstring:
        sys.stdout.write('Successfully create GString %s\n'%(gstring.id))
    else:
        sys.stdout.write('Error occured while creating a GHessianTest\n')
    sys.stdout.flush()

def ghessiantest(*args, **kw):
    from htpie.usertasks.ghessiantest import GHessianTest

    config = _configure_system()
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-d", "--directory", dest="dir",default='examples/hessian', 
                      help="directory where the files to run the test are located")
    parser.add_option("-v", "--verbose", action='count', dest="verbosity", default=config.verbosity, 
                      help="add more v's to increase log output")
    parser.add_option("-a", "--application", dest="app_tag", default='gamess', 
                      help="add more v's to increase log output")
    (options, args) = parser.parse_args()

    configure_logger(options.verbosity, _default_log_file_location) 

    if not os.path.isdir(options.dir):
        sys.stdout.write('Can not locate directory \'%s\'\n'%(options.dir))
        return
 
    ghessiantest = GHessianTest.create(options.dir, options.app_tag)
    if ghessiantest:
        sys.stdout.write('Successfully create GHessianTest %s\n'%(ghessiantest.id))
    else:
        sys.stdout.write('Error occured while creating a GHessianTest\n')
    sys.stdout.flush()

def gsingle(*args, **kw):
    from htpie.usertasks.gsingle import GSingle

    config = _configure_system()
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-f", "--file", dest="file",default='examples/water_UHF_gradient.inp', 
                      help="gamess inp to restart from.")
    parser.add_option("-v", "--verbose", action='count', dest="verbosity", default=config.verbosity, 
                      help="add more v's to increase log output.")
    parser.add_option("-a", "--application", dest="app_tag", default='gamess', 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()

    configure_logger(options.verbosity, _default_log_file_location) 

    if not os.path.isfile(options.file):
        sys.stdout.write('Can not locate file \'%s\'\n'%(options.file))
        return
 
    task = GSingle.create([options.file], options.app_tag)
    if task:
        sys.stdout.write('Successfully create GSingle %s\n'%(task.id))
    else:
        sys.stdout.write('Error occured while creating a GSingle\n')
    sys.stdout.flush()

def ghessian(*args, **kw):
    from htpie.usertasks.ghessian import GHessian

    config = _configure_system()
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-f", "--file", dest="file",default='examples/water_UHF_gradient.inp', 
                      help="gamess inp to restart from.")
    parser.add_option("-v", "--verbose", action='count', dest="verbosity", default=config.verbosity, 
                      help="add more v's to increase log output.")
    parser.add_option("-a", "--application", dest="app_tag", default='gamess', 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()

    configure_logger(options.verbosity, _default_log_file_location) 

    if not os.path.isfile(options.file):
        sys.stdout.write('Can not locate file \'%s\'\n'%(options.file))
        return
 
    task = GHessian.create(options.file, options.app_tag)
    if task:
        sys.stdout.write('Successfully create GHessian %s\n'%(task.id))
    else:
        sys.stdout.write('Error occured while creating a GHessian\n')
    sys.stdout.flush()
    

def gtaskscheduler(*args, **kw):
    from htpie.usertasks.taskscheduler import TaskScheduler

    config = _configure_system()
    
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", action='count',dest="verbosity", default=config.verbosity, 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()

    configure_logger(options.verbosity, _default_log_file_location) 
    
    # Check to see if a gtaskscheduler is already running for my user.
    # If not, allow this one to run.
    lockfile = _rcdir + '/gtaskscheduler.lock'
    lock = flock(lockfile, True).acquire()
    if lock:
        task_scheduler = TaskScheduler()
        sys.stdout.write('Running\n')
        task_scheduler.run()
        sys.stdout.write('Done\n')
    else:
        htpie.log.debug('An instance of gtaskscheduler is already running.  Not starting another one.')
    sys.stdout.flush()

def gcontrol(*args, **kw):
    from htpie.usertasks.gcontrol import GControl
    
    config = _configure_system()
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-p", "--program_command", dest="program_command",  default='info', 
                      help="command to run against task")
    parser.add_option("-i", "--id", dest="id",  
                      help="task id to be acted upon")
    parser.add_option("-t", "--hours_ago",  type = "int", dest="hours_ago", default = None,  
                      help="limit show function to tasks last ran -t hours ago")
    parser.add_option("-l", "--long_format",  action="store_true", dest="long_format",  default=False, 
                      help="display more information")
    parser.add_option("-v", "--verbose", action='count',dest="verbosity", default=config.verbosity, 
                      help="add more v's to increase log output")
    
    (options, args) = parser.parse_args()
    
    def check():
        if options.id is None:
            print "A mandatory option is missing\n"
            parser.print_help()
            sys.exit(0)
    
    configure_logger(options.verbosity, _default_log_file_location) 
    
    
    if options.program_command == 'retry':
        check()
        GControl.retry(options.id)
    elif options.program_command == 'kill':
        check()
        GControl.kill(options.id)
    elif options.program_command == 'info':
        check()
        GControl.info(options.id, options.long_format)
    elif options.program_command == 'show':
        check()
        GControl.show(options.id, options.hours_ago)
#    elif options.program_command == 'files':
#        gcontrol.get_task_files()
    else:
        sys.stdout.write('Unknown program command %s'%s(options.program_command))

def gbig(*args, **kw):
    from htpie.usertasks import gbig

    config = _configure_system()
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-n", "--number", dest="num_little", type='int', default=10, 
                      help="number of glittle's this gbig should spawn")
    parser.add_option("-v", "--verbose", action='count', dest="verbosity", default=config.verbosity, 
                      help="add more v's to increase log output")

    (options, args) = parser.parse_args()

    configure_logger(options.verbosity, _default_log_file_location) 
    
    big = gbig.GBig.create(options.num_little)

    if gstring:
        sys.stdout.write('Successfully create GBig %s\n'%(big.id))
    else:
        sys.stdout.write('Error occured while creating a GBig\n')
    sys.stdout.flush()

if __name__ == '__main__':
    #This is needed because eric4 sends the following to the sys.argv variable
    #['gcommands.py', 'gcommands.py', u'gtestcron']
    #While the command line sends:
    #['gcommands.py', u'gtestcron']
    #Therefore we must  deal with both inputs
    import time
    for i in range(len(sys.argv)):
        if sys.argv[i].find('gcommands.py') == -1:
            break
    if sys.argv[i] !='gtaskscheduler':
        eval('%s(sys.argv[%d:])'%(sys.argv[i], i))
    else:
        while True:
            print 'running'
            eval('%s(sys.argv[%d:])'%(sys.argv[i], i))
            print 'sleeping'
            time.sleep(10)
    sys.exit(0)

