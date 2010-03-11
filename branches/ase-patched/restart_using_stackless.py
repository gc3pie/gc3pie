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
    import re
    from time import sleep
    import numpy as np
    import copy

    from ase.io.gamess import ReadGamessInp,WriteGamessInp
    from ase.calculators.gamess import Gamess
    from gc3utils.gcli import Gcli
    
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
    
    def batch_job_finished(filepath, jobID, logger):
        myGcli=Gcli('/home/mmonroe/.gc3/config')
        job_status=myGcli.gstat(jobID)
        logger.info('Job status is %s'%(job_status))
        if job_status[1][0][1].find('FINISHED')  != -1:        
            myGcli.gget(jobID)
            shutil.copy( glob.glob('%s/*.dat'%jobID)[0], filepath)
            shutil.copy( glob.glob('%s/*.stdout'%jobID)[0], filepath)
            #shutil.copy( glob.glob('%s/*.stderr'%jobID)[0], filepath)
            shutil.copy( glob.glob('%s/*.po'%jobID)[0], filepath)
            return True
        else:
            return False
        
    def submit_batch_job(filepath, filename, logger):
        myGcli=Gcli('/home/mmonroe/.gc3/config')
        logger.info('Submitting job %s.'%(filename))
        logger.debug('job_local_dir is %s'%os.path.dirname(filename))
        job_info=myGcli.gsub(application_to_run='gamess', input_file=filename,
                    selected_resource=GRID_RESOURCE, job_local_dir=filepath, 
                    cores=256, memory=2, walltime=19.9)
        return job_info[1] #return jobID
    
    def create_logger(options):
        logger = logging.getLogger("restart_main")
        #Setup logger   
        level = len(LOGGING_LEVELS) if len(options.verbose) > len(LOGGING_LEVELS) else len(options.verbose)
        logger.setLevel(level)    
        handler = logging.handlers.RotatingFileHandler(
                  LOG_FILENAME, maxBytes=100000, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    logger=create_logger(options)
    
    #Parse all the parametersto keep track of the file names
    (filepath, filename) = os.path.split(options.file)
    if not filepath:
        filepath =  os.getcwd()
    (shortname, extension) = os.path.splitext(filename)
    file_main_title = re.split(RE_RESTART_NUM + '|' + RE_INP, filename)[0]
    restart_id = re.search(RE_RESTART_NUM, filename)
    if restart_id:
        restart_id=int(restart_id.group(1))
    else:
        restart_id = 0
        new_restart_file=file_main_title + EXT_RE%(restart_id) 
        shutil.copy(file_main_title + EXT_INP,  new_restart_file + EXT_INP)
    #Now we loop until we have meet our finish condition
    done = False
    while not done:
        logger.info('Submitting file %s to batch system.'%(new_restart_file + EXT_INP))
        job_id = submit_batch_job(filepath, '%s/%s'%(filepath, new_restart_file + EXT_INP), logger)
        job_is_done = False
        while not job_is_done:
            if myChannel:
                logger.info('Restart tasklet %s is going to sleep.'%(new_restart_file + EXT_INP))
                del logger
                myChannel.receive()
                logger=create_logger(options)
            else:
                logger.info('Restart %s is going to sleep.'%(new_restart_file + EXT_INP))
                sleep(SLEEP_TIME)
            job_is_done = batch_job_finished(filepath, job_id, logger)
        """Parse the GAMESS file and generate a new file to submit if
            the calculation is not done yet."""
        new_reader = ReadGamessInp()
        new_reader.read_file(filepath+'/'+new_restart_file+EXT_INP)
        a_molecule = new_reader.get_molecule()
        params = new_reader.get_params()
        a_molecule.set_calculator(Gamess(gamess_params=copy.deepcopy(params), result_file=filepath+'/'+new_restart_file))
        #We get the last printed coords in string format, and need to convert them
        parsed_out=a_molecule.get_calculator().parsed_out
        #We tell the GAMESS calc to parse a the results here
        a_molecule.get_potential_energy()
        if parsed_out.status_exit_successful():
            if not parsed_out.status_geom_located():
                restart_id +=1
                new_restart_file = file_main_title + EXT_RE%(restart_id) 
                logger.info('Creating restart file %s/'%(new_restart_file+EXT_INP))
                new_molecule = a_molecule.copy() 
                params.set_group_param('$GUESS', 'GUESS', 'MOREAD')
                params.set_group_param('$STATPT', 'HESS', 'READ')
                params.set_hessian(a_molecule.get_calculator().get_hessian(a_molecule, raw_format=True))
                params.set_orbitals(a_molecule.get_calculator().get_orbitals(a_molecule, raw_format=True))
                new_molecule.set_calculator(Gamess(gamess_params=params))
                new_molecule.set_positions(a_molecule.get_calculator().get_coords_result(a_molecule))
                new_writer = WriteGamessInp()            
                new_writer.write(filepath+'/'+new_restart_file+EXT_INP,  new_molecule)
            else:
                done = True
                logger.info('Restart sequence using %s has start finished successfully.'%(filename))
        else:
            logger.critical('GAMESS returned an error while running file %s.'%(new_restart_file+EXT_INP))

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
