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

STATE_WAIT = state.State.create('WAIT', 'WAIT desc', True)
STATE_PROCESS = state.State.create('PROCESS', 'PROCESS desc')
STATE_POSTPROCESS = state.State.create('POSTPROCESS', 'POSTPROCESS desc')
STATE_ERROR = state.State.create('ERROR', 'ERROR desc', terminal = True)
STATE_COMPLETED = state.State.create('COMPLETED', 'COMPLETED desc', terminal = True)

STATES = state.StateContainer([STATE_WAIT, STATE_PROCESS, STATE_POSTPROCESS, 
                            STATE_ERROR, STATE_COMPLETED])


class GRestart(object):

    def __init__(self):
        self.status = STATES.ERROR
        self.status_mapping = {STATES.WAIT: self.handle_wait_state, 
                                             STATES.PROCESS: self.handle_process_state, 
                                             STATES.POSTPROCESS: self.handle_postprocess_state, 
                                             STATES.ERROR: self.handle_terminal_state, 
                                             STATES.COMPLETED: self.handle_terminal_state}
        self.a_task = None
        self.calculator = None
    
    def initialize(self, db, calculator, atoms, params, application_to_run='gamess', selected_resource='ocikbpra',  cores=2, memory=1, walltime=-1):
        self.a_task = TaskInterface(db).create(self.__class__.__name__)
        self.calculator = calculator
        self.a_task.user_data_dict['restart_number'] = 0
        self.a_task.store()
        params.title = 'restart_number_%d'%self.a_task.user_data_dict['restart_number']
        a_job = self.calculator.generate(atoms, params, self.a_task, application_to_run, selected_resource, cores, memory, walltime)
        self.calculator.calculate(a_job)
        self.status = STATES.WAIT

    def load(self, db,  task_id):
        self.a_task = TaskInterface(db).load(task_id)
        str_calc = a_task.user_data_dict['calculator']
        self.calculator = eval(str_calc + '(db)')
    
    def save(self):
        self.a_task.status = self.status
        self.a_task.user_data_dict['calculator'] = self.calculator.__class__.__name__
        self.a_task.store()
    
    def handle_wait_state(self):
        from gorg.gridjobscheduler import GridjobScheduler
        job_scheduler = GridjobScheduler('mark','gorg_site','http://130.60.144.211:5984')
        job_list = self.a_task.children
        new_status = STATES.PROCESS
        job_scheduler.run()
        for a_job in job_list:
            job_done = False
            job_done = a_job.wait(timeout=0)
            if not job_done:
                new_status=STATES.WAIT
                markstools.log.info('Waiting for job %s.'%(a_job.id))
                break
        self.status = new_status
    
    def handle_process_state(self):
        a_job = self.a_task.children[-1]
        a_result = self.calculator.parse(a_job)
        params = copy.deepcopy(a_result.params)
        atoms = a_result.atoms.copy()
        
        if a_result.exit_successful():
            a_job.status = RUN_COMPLETED
            a_job.store()
        else:
            a_job.status = RUN_ERROR
            a_job.store()
            markstools.log.critical('GAMESS returned an error while running job %s.'%(a_job.id))
            new_state = STATES.ERROR
            return

        if not a_result.geom_located():                
            params.r_orbitals = a_result.get_orbitals(raw=True)
            params.r_hessian = a_result.get_hessian(raw=True)
            self.a_task.user_data_dict['restart_number'] += 1
            params.title = 'restart_number_%d'%self.a_task.user_data_dict['restart_number']
            # Make sure that the orbitals an hessian will be read in the inp file
            params.set_group_param('$GUESS', 'GUESS', 'MOREAD')
            params.set_group_param('$STATPT', 'HESS', 'READ')
            atoms.set_positions(a_result.get_positions())
            a_new_job = self.calculator.generate(atoms, params, self.a_task, 
                                                 a_job.run.application.application_tag, a_job.run.application.requested_resource,  
                                                 a_job.run.application.requested_cores, a_job.run.application.requested_memory, a_job.run.application.requested_walltime)
            a_new_job.add_parent(a_job)
            self.calculator.calculate(a_new_job)
            self.status = STATES.WAIT
        else:
            self.status = STATES.POSTPROCESS
            markstools.log.info('Restart sequence task id %s has finished successfully.'%(self.a_task.id))
    
    def handle_postprocess_state(self):
        self.status = STATES.COMPLETED
        pass
    
    def handle_terminal_state(self):
        print 'I do nothing!!'
    
    def handle_missing_state(self):
        print 'Do something when the state is not in our map.'
    
    def step(self):
        try:
            self.status_mapping.get(self.status, self.handle_missing_state)()
        except:
            self.status = STATES.ERROR
            markstools.log.critical('GRestart Errored while processing task %s \n%s'%(self.a_task.id, utils.format_exception_info()))
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
  
    grestart = GRestart()
    gamess_calc = GamessGridCalc(db)
    grestart.initialize(db, gamess_calc, atoms, params)
    
    grestart.run()
    import time
    while not grestart.status.terminal:
        time.sleep(10)
        grestart.run()
    

if __name__ == '__main__':
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-f", "--file", dest="file",default='markstools/examples/exam01.inp', 
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
        
    #configure_logger(options.verbose)
    configure_logger(10)
    import gorg.lib.utils
    gorg.lib.utils.configure_logger(10)
    
    main(options)

    sys.exit()
