import markstools

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
from gorg.lib.utils import Mydb
from gorg.lib import state
from gorg.gridjobscheduler import STATE_COMPLETED as RUN_COMPLETED
from gorg.gridjobscheduler import STATE_ERROR as RUN_ERROR

class State(object):
    WAIT = 'WAIT'
    STEP='STEP'
    EXECUTE = 'EXECUTE'
    ERROR='ERROR'
    DONE='DONE'
    pause = [WAIT]
    terminal = [ERROR,  DONE]

class GOptimize_lbfgs(object):

    def __init__(self):
        self.state = State.ERROR
        self.state_mapping = {State.WAIT: self.handle_wait_state, 
                                             State.STEP: self.handle_step_state, 
                                             State.EXECUTE: self.handle_execute_state, 
                                             State.ERROR: self.handle_terminal_state, 
                                             State.DONE: self.handle_terminal_state}
        
        self.a_task = None
        self.calculator = None

    def initialize(self, db, calculator, atoms, params, application_to_run='gamess', selected_resource='ocikbpra',  cores=2, memory=1, walltime=-1):
        self.calculator = calculator
        self.a_task = TaskInterface(db).create(self.__class__.__name__)
        self.a_task.user_data_dict['total_jobs'] = 0
        params.title = 'job_number_%d'%a_task.user_data_dict['total_jobs']
        a_job = self.calculator.generate(atoms, params, self.a_task, application_to_run, selected_resource, cores, memory, walltime)
        self.calculator.calculate(self.a_task)
        self.state = STATES.WAITING

    def load(self, db,  a_task):
        self.a_task = a_task
        self.state = self.a_task.state
        str_calc = self.a_task.user_data_dict['calculator']
        self.calculator = eval(str_calc + '(db)')
    
    def save(self):
        self.a_task.state = self.state
        self.a_task.user_data_dict['calculator'] = self.calculator.__class__.__name__
    
    def handle_wait_state(self):
        job_scheduler = GridjobScheduler()
        job_list = [self.a_task.children[-1]]
        new_state=State.STEP
        for a_job in job_list:
            job_done = False
            job_scheduler.run()            
            job_done = a_job.wait(timeout=0)
            if not job_done:
                log.info('Restart waiting for job %s.'%(a_job.id))
                new_state=State.WAITING
                break
        self.state = new_state
    
    def handle_step_state(self):
        a_job = self.a_task.children[-1]
        a_result = self.calculator.parse(a_job)
        myfile = a_job.get_attachment('.inp')
        reader = ReadGamessInp(myfile)
        myfile.close()
        params = reader.params
        atoms = reader.atoms
        opt = LBFGS(atoms)
        opt.initialize()
        # If we have a file, load it, otherwise this is the first iteration step
        if os.path.isfile(a_task.id):
            opt.load(a_task.id)
        #restart if we need to
        new_positions = opt.step(result.get_positions(), result.get_forces())
        opt.dump(a_task.id)
        atoms.set_positions(new_positions)
        new_job = self.cargo.calculator.generate(atoms, params, self.a_task, **a_job.run_params)
        self.state = State.EXECUTE
    
    def handle_missing_state(self):
        raise UnhandledStateError('State %s is not implemented.'%(self.state))
    
    def handle_terminal_state(self):
        pass
    
    def step(self):
        try:
            self.status_mapping.get(self.status, self.handle_missing_state)()
        except:
            self.status = STATES.ERROR
            markstools.log.critical('Errored while processing task %s \n%s'%(self.a_task.id, utils.format_exception_info()))
        self.save()
    
    def run(self):
        while not self.status.terminal:
            self.step()
            if self.status.pause:
                break

def main(options):
    # Connect to the database
    db=Mydb('mark',options.db_name,options.db_url).cdb()
    
    # Parse the gamess inp file
    myfile = open(options.file, 'rb')
    reader = ReadGamessInp(myfile)
    myfile.close()
    params = reader.params
    atoms = reader.atoms
    
    fsm = GOptimize()
    gamess_calc = GamessGridCalc(db)
    fsm.create(db, gamess_calc, atoms, params)
    
    while fsm.state not in TerminalState:
        fsm.run()

    print 'GOptimize_lbfgs is done'


if __name__ == '__main__':
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-f", "--file", dest="file",default='markstools/examples/exam01.inp', 
                      help="gamess inp to restart from.")
    parser.add_option("-v", "--verbose", dest="verbose", default='', 
                      help="add more v's to increase log output.")
    parser.add_option("-n", "--db_name", dest="db_name", default='gorg_site', 
                      help="add more v's to increase log output.")
    parser.add_option("-l", "--db_url", dest="db_url", default='http://127.0.0.1:5984', 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()
    
    #Setup logger
    LOGGING_LEVELS = (logging.CRITICAL, logging.ERROR, 
                                    logging.WARNING, logging.INFO, logging.DEBUG)
    options.logging_level = len(LOGGING_LEVELS) if len(options.verbose) > len(LOGGING_LEVELS) else len(options.verbose)
    create_file_logger(options.logging_level)
    main(options)

    sys.exit()


