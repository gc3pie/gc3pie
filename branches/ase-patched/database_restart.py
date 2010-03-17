'''
Created on Dec 28, 2009

@author: mmonroe
'''

try:
    import stackless
    stackless_pressent = True
except:
    stackless_pressent = False
    print 'Not using stackless'

from optparse import OptionParser
import cPickle
import os

DO_NOT_USE_SLACKLESS=False

def my_main(options, myChannel=None):
    import shutil
    import glob
    import sys
    import logging
    import logging.handlers
    import cStringIO
    import re
    from time import sleep
    import numpy as np
    import copy
    import cStringIO as StringIO
    from ase.io.gamess import ReadGamessInp,WriteGamessInp
    from ase.calculators.gamess import GamessGridCalc
    
    from gc3utils.gcli import Gcli
    
    sys.path.append('/home/mmonroe/apps/gorg')
    from gorg_site.gorg_site.model.gridjob import GridjobModel
    from gorg_site.gorg_site.model.gridtask import GridtaskModel
    from gorg_site.gorg_site.lib.mydb import Mydb
    
    LOG_FILENAME = '/tmp/python_scheduler_logger.out'
    TASK_FILE_PREFIX = 'task_'
    LOGGING_LEVELS = (logging.CRITICAL, logging.ERROR, 
                  logging.WARNING, logging.INFO, logging.DEBUG)
    EXT_RE = '.restart_%d'
    RE_RESTART_NUM =  r""".restart_([0-9])*"""
    RE_INP = r""".inp"""
    EXT_INP = '.inp'
    SLEEP_TIME=.1#Seconds to sleep between job status checks when not using tasklets
    GRID_RESOURCE='schrodinger'
   
    def create_logger(options):
        logger = logging.getLogger("restart_main")
        #Setup logger   
        level = len(LOGGING_LEVELS) if len(options.verbose) > len(LOGGING_LEVELS) else len(options.verbose)
        logger.setLevel(level)    
        file_handler = logging.handlers.RotatingFileHandler(
                  LOG_FILENAME, maxBytes=100000, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    def run_scheduler():
        from gorg.gridjobscheduler import GridjobScheduler
        job_scheduler = GridjobScheduler()
        job_scheduler.handle_ready_jobs()
        job_scheduler.handle_waiting_jobs()
        job_scheduler.handle_running_jobs()
        job_scheduler.handle_finished_jobs()
        print 'Done running gridjobscheduler.py'
    
    logger=create_logger(options)
    
    #Parse all the parametersto keep track of the file names
    (filepath, filename) = os.path.split(options.file)
    if not filepath:
        filepath =  os.getcwd()
    
    # Create the job
    myfile = open(options.file, 'rb')
    new_reader = ReadGamessInp(myfile)
    myfile.close()
    params = copy.deepcopy(new_reader.params)
    atoms = copy.deepcopy(new_reader.atoms)
    params.j_user_params['restart_number'] = 0
    gamess_calc = GamessGridCalc(j_db_name='gorg_site',j_db_url='http://127.0.0.1:5984')
    # Now we make a task
    a_task=GridtaskModel()
    a_task.author='mark'
    a_task.title = 'Marks wonderful task'

    #Now we loop until we have meet our finish condition
    done = False
    while not done:
        # Start the calculation
        a_result = gamess_calc.calculate(atoms,  params, a_task)
        logger.info('Task id %s.'%(a_task.id))
        print a_task.id
        logger.info('Submited job %s to batch system.'%(a_result.j_job.id))
        job_done = False
        while not job_done:
            run_scheduler()
            if myChannel:
                logger.info('Restart tasklet wiating for job %s.'%(a_result.j_job.id))
                del logger
                myChannel.receive()
                logger=create_logger(options)
            else:
                logger.info('Restart %s is going to sleep.'%(a_result.j_job.id))
                sleep(SLEEP_TIME)
            job_done = a_result.wait(timeout=10)
            assert a_result.j_job.status != 'ERROR'
        a_result.read()
        if a_result.is_exit_successful():
            if not a_result.is_geom_located():                
                params.r_orbitals = a_result.get_orbitals(raw=True)
                params.r_hessian = a_result.get_hessian(raw=True)
                params.j_user_params['restart_number'] += 1
                # Make sure that the orbitals an hessian will be read in the inp file
                params.set_group_param('$GUESS', 'GUESS', 'MOREAD')
                params.set_group_param('$STATPT', 'HESS', 'READ')
                atoms.set_positions(a_result.get_coords())
                logger.info('Saved job %s to database.'%(a_result.j_job.id))
            else:
                done = True
                logger.info('Restart sequence using %s has finished successfully.'%(filename))
        else:
            logger.critical('GAMESS returned an error while running file %s.'%(a_result.j_job.id))

def save_state(func, options):
    myChannel = stackless.channel()
    t1 = stackless.tasklet(func)(options, myChannel)
    t1.run()
    output = open('%s/%s'%(options.directory,'task_data.pkl'), 'wb')
    cPickle.dump(myChannel, output)
    output.close() 
    t1.kill()
    exit()

if __name__ == '__main__':
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-d", "--dir", dest="directory",default='~/tasks', 
                      help="directory to save tasks in.")
    parser.add_option("-f", "--file", dest="file",default='exam01.inp', 
                      help="gamess inp to restart from.")
    parser.add_option("-v", "--verbose", dest="verbose", default='', 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()
    if not options.directory:
        parser.error("must specify a task directory with -d")
    options.directory=os.path.expanduser(options.directory.rstrip('/'))
    if stackless_pressent and not DO_NOT_USE_SLACKLESS:
        save_state(my_main, options)
    else:
        my_main(options)

