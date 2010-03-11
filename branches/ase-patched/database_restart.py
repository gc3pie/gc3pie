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
    import tempfile
    from ase.io.gamess import ReadGamessInp,WriteGamessInp
    from ase.calculators.gamess import Gamess
    
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
    SLEEP_TIME=2#Seconds to sleep between job status checks when not using tasklets
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

    db=Mydb('gorg_site','http://127.0.0.1:5984').cdb()
    
    #Parse all the parametersto keep track of the file names
    (filepath, filename) = os.path.split(options.file)
    if not filepath:
        filepath =  os.getcwd()
    
    # Create the job
    a_job = GridjobModel()
    a_job.author = 'mark'
    a_job.defined_type = 'GAMESS'
    a_job.title = os.path.basename(filename)
    a_job.user_params['restart_number'] = 0
    myfile =  open(options.file, 'rb')
    a_job.input_file=os.path.basename(os.path.splitext(myfile.name)[-1].lstrip('.'))
    a_job=a_job.put_attachment(db, myfile, os.path.splitext(myfile.name)[-1].lstrip('.'))
    logger.info('Saved job %s to database.'%(a_job.id))
    myfile.close()
    
    # Now lets add it to a task
    a_task=GridtaskModel()
    a_task.author='mark'
    a_task.title = os.path.basename(myfile.name)
    a_task.add_job(a_job)
    a_task.store(db)
    logger.info('Restart sequence saved in task id: %s'%(a_task.id))
    job_list=a_task.get_jobs(db)
    
    #Now we loop until we have meet our finish condition
    done = False
    while not done:
        logger.info('Submitting file %s to batch system.'%(a_job.title))
        while not a_job.status == 'DONE':
            if myChannel:
                logger.info('Restart tasklet %s is going to sleep.'%(a_job.title))
                del logger
                myChannel.receive()
                logger=create_logger(options)
            else:
                logger.info('Restart %s is going to sleep.'%(a_job.title))
                sleep(SLEEP_TIME)
            # Check the database to see if the job has completed
            a_job=a_job.load(db, a_job.id)
            run_scheduler()
            assert a_job.status != 'ERROR'
        """Parse the GAMESS file and generate a new file to submit if
            the calculation is not done yet."""
        new_reader = ReadGamessInp()
        f_attachments= a_job.attachments_to_files(db)
        new_reader.read_file(f_attachments['inp'])
        a_molecule = new_reader.get_molecule()
        params = new_reader.get_params()
        a_molecule.set_calculator(Gamess(gamess_params=copy.deepcopy(params), **f_attachments))
        #We get the last printed coords in string format, and need to convert them
        parsed_out=a_molecule.get_calculator().parsed_out
        #We tell the GAMESS calc to parse a the results here
        a_molecule.get_potential_energy()
        if parsed_out.status_exit_successful():
            if not parsed_out.status_geom_located():
                # Delete the id so we will create a new job when we store it
                a_job = a_job.copy()
                a_job.user_params['restart_number'] += 1
                new_molecule = a_molecule.copy() 
                params.set_group_param('$GUESS', 'GUESS', 'MOREAD')
                params.set_group_param('$STATPT', 'HESS', 'READ')
                params.set_hessian(a_molecule.get_calculator().get_hessian(a_molecule, raw_format=True))
                params.set_orbitals(a_molecule.get_calculator().get_orbitals(a_molecule, raw_format=True))
                new_molecule.set_calculator(Gamess(gamess_params=params))
                new_molecule.set_positions(a_molecule.get_calculator().get_coords_result(a_molecule))
                new_writer = WriteGamessInp()
                temp_new_input = tempfile.NamedTemporaryFile(suffix=EXT_INP)
                new_writer.write(temp_new_input.name,  new_molecule)                               
                a_job=a_job.put_attachment(db, temp_new_input, a_job.input_file)
                logger.info('Saved job %s to database.'%(a_job.id))
                a_task.add_job(a_job)
                a_task.store(db)
                myfile.close()
                temp_new_input.close()
            else:
                done = True
                logger.info('Restart sequence using %s has finished successfully.'%(filename))
        else:
            logger.critical('GAMESS returned an error while running file %s.'%(a_job.title))

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

