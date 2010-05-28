'''
Created on Dec 28, 2009

@author: mmonroe
'''
from optparse import OptionParser
import markstools
import os
import copy
import sys
import numpy as np

from markstools.io.gamess import ReadGamessInp, WriteGamessInp
from markstools.calculators.gamess.calculator import GamessGridCalc
from markstools.lib import utils
from gorg.model.gridtask import TaskInterface

class State(object):
    WAIT = 'WAIT'
    PROCESS = 'PROCESS'
    POSTPROCESS = 'POSTPROCESS'
    ERROR = 'ERROR'
    COMPLETED = 'COMPLETED'
    
    all = [WAIT, PROCESS, ERROR, COMPLETED, POSTPROCESS]
    pause = [WAIT]
    terminal = [ERROR,  COMPLETED]

class GSingle(object):
 
    def __init__(self):
        self.status = State.ERROR
        self.status_mapping = {State.WAIT: self.handle_wait_state, 
                                             State.PROCESS: self.handle_process_state, 
                                             State.POSTPROCESS: self.handle_postprocess_state, 
                                             State.ERROR: self.handle_terminal_state, 
                                             State.COMPLETED: self.handle_terminal_state}
        
        self.a_task = None
        self.calculator = None

    def initialize(self, db, calculator, atoms, params, application_to_run='gamess', selected_resource='gc3',  cores=8, memory=2, walltime=-1):
        self.calculator = calculator
        self.a_task = TaskInterface(db).create(self.__class__.__name__)
        
        params.title = 'single job'
        a_job = self.calculator.generate(atoms, params, self.a_task, application_to_run, selected_resource, cores, memory, walltime)
        self.calculator.calculate(self.a_task)
        markstools.log.info('Submitted task %s for execution.'%(self.a_task.id))
        self.status = State.WAIT
        
    def load(self, db,  a_task):
        self.a_task = a_task
        self.status = self.a_task.status
        str_calc = self.a_task.user_data_dict['calculator']
        self.calculator = eval(str_calc + '(db)')
    
    def save(self):
        self.a_task.status = self.status
        self.a_task.user_data_dict['calculator'] = self.calculator.__class__.__name__
    
    def handle_wait_state(self):
        from gorg.gridjobscheduler import GridjobScheduler
        job_scheduler = GridjobScheduler('mark','gorg_site','http://130.60.144.211:5984')
        job_list = self.a_task.children
        new_status = State.PROCESS
        for a_job in job_list:
            job_done = False
            job_scheduler.run()
            job_done = a_job.wait(timeout=0)
            if not job_done:
                new_status=State.WAIT
                markstools.log.info('Restart waiting for job %s.'%(a_job.id))
                break
        self.status = new_status
    
    def handle_process_state(self):
        job_list = self.a_task.children
        for a_job in job_list:
            a_result = self.calculator.parse(a_job)
            if not a_result.exit_successful():
                msg = 'GAMESS returned an error while running job %s.'%(a_job.id)
                markstools.log.critical(msg)
                raise Exception, msg
        self.status = State.POSTPROCESS

    def handle_postprocess_state(self):
        self.status = State.COMPLETED

    def handle_terminal_state(self):
        print 'I do nothing!!'

    def handle_missing_state(self):
        print 'Do something when the state is not in our map.'
   
    def step(self):
        try:
            self.status_mapping.get(self.status, self.handle_missing_state)()
        except:
            self.status=State.ERROR
            markstools.log.critical('GHessian Errored while processing task %s \n%s'%(self.a_task.id, utils.format_exception_info()))
        self.save()
    
    def run(self):
        if self.status not in State.terminal:
            self.step()
        else:
            assert false,  'You are trying to step a terminated status.'
        while self.status not in State.pause and self.status not in State.terminal:
            self.step()

def main(options):
    # Connect to the database
    db = utils.Mydb('mark',options.db_name,options.db_url).cdb()

    myfile = open(options.file, 'rb')
    reader = ReadGamessInp(myfile)
    myfile.close()
    params = reader.params
    atoms = reader.atoms
    
    gsingle = GSingle()
    gamess_calc = GamessGridCalc(db)
    gsingle.initialize(db, gamess_calc, atoms, params)
    
    gsingle.run()
    import time
    while gsingle.status not in State.terminal:
        time.sleep(10)
        gsingle.run()

    print 'gsingle done. Create task %s'%(ghessian.a_task.id)

if __name__ == '__main__':
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-f", "--file", dest="file",default='markstools/examples/water_UHF_gradient.inp', 
                      help="gamess inp to restart from.")
    parser.add_option("-v", "--verbose", action='count', dest="verbose", default=0, 
                      help="add more v's to increase log output.")
    parser.add_option("-n", "--db_name", dest="db_name", default='gorg_site', 
                      help="add more v's to increase log output.")
    parser.add_option("-l", "--db_url", dest="db_url", default='http://130.60.144.211:5984', 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()
    
    import logging
    from markstools.lib.utils import configure_logger
    logging.basicConfig(
        level=logging.ERROR, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
        
    configure_logger(options.verbose)
    
    main(options)

    sys.exit()
