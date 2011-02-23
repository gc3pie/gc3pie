#!/usr/bin/env python
import htpie
from htpie.lib.exceptions import *
from htpie.lib import utils
from htpie.lib.flock import flock

import argparse
import sys
import os

# defaults - XXX: do they belong in ../gcli.py instead?
_homedir = os.path.expandvars('$HOME')
_rcdir = _homedir + "/.htpie"
_default_config_file_location = _rcdir + "/htpie.conf"
_default_log_file_location = _rcdir + "/gc3utils.log"
_default_job_folder_location = os.getcwd()

def read_config():
    return utils.read_config(_default_config_file_location)
    
def gbig(options):
    from htpie.usertasks.gbig import GBig
    
    task = GBig.create(options.num_little)

    if task:
        sys.stdout.write('Successfully create GBig %s\n'%(task.id))
    else:
        sys.stdout.write('Error occured while creating a GBig\n')
    sys.stdout.flush()

def grecursion(options):
    from htpie.usertasks.grecursion import GRecursion
    
    task = GRecursion.create(options.levels, options.num_children)

    if task:
        sys.stdout.write('Successfully create GRecursion %s\n'%(task.id))
    else:
        sys.stdout.write('Error occured while creating a GRecursion\n')
    sys.stdout.flush()

def gcontrol(options):
    from htpie.usertasks.gcontrol import GControl
    
    if options.program_command == 'retry':
        GControl.retry(options.id, options.long_format)
    elif options.program_command == 'kill':
        GControl.kill(options.id, options.long_format)
    elif options.program_command == 'info':
        GControl.info(options.id, options.long_format)
    elif options.program_command == 'query':
        GControl.query(options.id, options.time_ago, options.long_format)
    elif options.program_command == 'states':
        GControl.states(options.id, options.long_format)
    elif options.program_command == 'statediag':
        GControl.statediag(options.id, options.long_format)

def gsingle(options):
    from htpie.usertasks.gsingle import GSingle
 
    task = GSingle.create([options.file], options.app_tag)
    if task:
        sys.stdout.write('Successfully create GSingle %s\n'%(task.id))
    else:
        sys.stdout.write('Error occured while creating a GSingle\n')

def ghessian(options):
    from htpie.usertasks.ghessian import GHessian

    task = GHessian.create(options.file, options.app_tag)
    if task:
        sys.stdout.write('Successfully create GHessian %s\n'%(task.id))
    else:
        sys.stdout.write('Error occured while creating a GHessian\n')

def ghessiantest(options):
    from htpie.usertasks.ghessiantest import GHessianTest

    task = GHessianTest.create(options.dir, options.app_tag)
    if task:
        sys.stdout.write('Successfully create GHessianTest %s\n'%(task.id))
    else:
        sys.stdout.write('Error occured while creating a GHessianTest\n')

def gtaskscheduler(options):
    from htpie.usertasks.taskscheduler import TaskScheduler

    # Check to see if a gtaskscheduler is already running for my user.
    # If not, allow this one to run.
    lockfile = _rcdir + '/gtaskscheduler.lock'
    lock = flock(lockfile, True).acquire()
    if lock:
        task_scheduler = TaskScheduler()
        task_scheduler.run()
    else:
        htpie.log.critical('An instance of gtaskscheduler is already running.  Not starting another one.')

def gstring(options):
    from htpie.usertasks.gstring import GString
    from htpie.optimize import fire
    from htpie.optimize import lbfgs
    
    #optimizer = fire.FIRE()
    optimizer = lbfgs.LBFGS()
    
    task = GString.create([options.start, options.end],  options.app_tag, optimizer)
    
    if task:
        sys.stdout.write('Successfully create GString %s\n'%(task.id))
    else:
        sys.stdout.write('Error occured while creating a GHessianTest\n')

def main():
    # Read the system config file
    _config = read_config()
    
    parser = argparse.ArgumentParser(prog='PROG', add_help=True)
    parser.add_argument("-v", "--verbose", action='count', dest="verbosity", default=_config.verbosity, 
                      help="add more v's to increase log output")

    subcommand = parser.add_subparsers(title='subcommands',description='valid subcommands',help='additional help')
    
    # Task parser
    # Application  choices
    choices=('gamess', )
    parser_task = subcommand.add_parser('task', description='create a usertask')
    parser_task_command = parser_task.add_subparsers()
    ## GBig
    parser_task_subcommand = parser_task_command.add_parser('gbig', description='creates glittle tasks to test the system')
    parser_task_subcommand.add_argument('-n', dest="num_little", type=int, default=10, 
                                                                    help="number of glittle's this gbig should spawn")
    parser_task_subcommand.set_defaults(func=gbig)
    ## GRecursion
    parser_task_subcommand = parser_task_command.add_parser('grecursion', description='creates a recursive state machine with l levels')
    parser_task_subcommand.add_argument('-l', dest="levels", type=int, default=10, 
                                                                    help="number of recursive levels to create")
    parser_task_subcommand.add_argument('-n', dest="num_children", type=int, default=2, 
                                                                    help="number of children each level should have")
    parser_task_subcommand.set_defaults(func=grecursion)
    ## GSingle
    parser_task_subcommand = parser_task_command.add_parser('gsingle', description='run a single gamess-us batch job')
    parser_task_subcommand.add_argument("-f", "--file", dest="file",type=argparse.FileType('r'), default='examples/water_UHF_gradient.inp', 
                                                                    help="gamess input file")
    parser_task_subcommand.add_argument("-a", "--application", choices=choices, dest="app_tag", default='gamess', 
                                                                help="application to use")
    parser_task_subcommand.set_defaults(func=gsingle)
    ## GHessian
    parser_task_subcommand = parser_task_command.add_parser('ghessian', description='run a hessian gamess-us task')
    parser_task_subcommand.add_argument("-f", "--file", dest="file",type=argparse.FileType('r'), default='examples/water_UHF_gradient.inp', 
                                                                    help="gamess input file")
    parser_task_subcommand.add_argument("-a", "--application", choices=choices, dest="app_tag", default='gamess', 
                                                                help="application to use")
    parser_task_subcommand.set_defaults(func=ghessian)
    ## GHessianTest
    parser_task_subcommand = parser_task_command.add_parser('ghessiantest', description='run a hessiantest gamess-us task')
    parser_task_subcommand.add_argument("-d", "--directory", dest="dir",default='examples/ghessiantest', 
                                                                help="directory where the files to run the test are located")
    parser_task_subcommand.add_argument("-a", "--application", choices=choices, dest="app_tag", default='gamess', 
                                                                help="application to use")
    parser_task_subcommand.set_defaults(func=ghessiantest)
    ## GString
    parser_task_subcommand = parser_task_command.add_parser('gstring', description='run a string gamess-us task')
    parser_task_subcommand.add_argument("-s", "--start", dest="start", type=argparse.FileType('r'), default='examples/gstring_start.inp', 
                                         help="starting inp file for gstring method")
    parser_task_subcommand.add_argument("-e", "--end", dest="end", type=argparse.FileType('r'), default='examples/gstring_end.inp', 
                                        help="ending inp file for gstring method")
    parser_task_subcommand.add_argument("-a", "--application", choices=choices, dest="app_tag", default='gamess', 
                                        help="application to use")
    parser_task_subcommand.set_defaults(func=gstring)

    # GTaskscheduler
    parser_gtaskscheduler = subcommand.add_parser('gtaskscheduler', description='run the gtaskscheduler once')
    parser_gtaskscheduler.set_defaults(func=gtaskscheduler)
    
    # GControl parser
    parser_gcontrol = subcommand.add_parser('gcontrol', description='allows to query and control tasks')
    parser_gcontrol.add_argument("-l", "--long_format",  action='store_true', dest="long_format",  default=False, 
                      help="display more information")
    parser_gcontrol_subcommand = parser_gcontrol.add_subparsers(dest='program_command')
    parser_gcontrol_subcommand_sub = parser_gcontrol_subcommand.add_parser('info')
    parser_gcontrol_subcommand_sub.add_argument("-i", "--id", dest="id",help="task id to be acted upon", required=True)
    parser_gcontrol_subcommand_sub = parser_gcontrol_subcommand.add_parser('kill')
    parser_gcontrol_subcommand_sub.add_argument("-i", "--id", dest="id",help="task id to be acted upon", required=True)
    parser_gcontrol_subcommand_sub = parser_gcontrol_subcommand.add_parser('retry')
    parser_gcontrol_subcommand_sub.add_argument("-i", "--id", dest="id",help="task id to be acted upon", required=True)
    parser_gcontrol_subcommand_sub = parser_gcontrol_subcommand.add_parser('query')
    parser_gcontrol_subcommand_sub.add_argument("-i", "--id", dest="id",help="task type to be acted upon", required=True)
    parser_gcontrol_subcommand_sub.add_argument("-time", "--time-ago", type=float, dest="time_ago",help="number of hours ago since executed last")
    parser_gcontrol_subcommand_sub = parser_gcontrol_subcommand.add_parser('statediag')
    parser_gcontrol_subcommand_sub.add_argument("-i", "--id", dest="id",help="task type to be acted upon", required=True)
    parser_gcontrol.set_defaults(func=gcontrol)
    
    sys.stdout.flush()
    
    if __name__ == '__main__':
        # Needed when using eric4 because eric4 puts two extra values at the beginning of the sys.argv list
        options = parser.parse_args(sys.argv[2:])
    else:
        options = parser.parse_args()
    
    utils.configure_logger(options.verbosity, _default_log_file_location) 
    options.func(options)

if __name__ == '__main__':
    main()
