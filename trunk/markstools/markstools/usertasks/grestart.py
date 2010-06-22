from optparse import OptionParser
import markstools
import os
import copy
import sys
import numpy as np

from markstools.io.gamess import ReadGamessInp, WriteGamessInp
from markstools.calculators.gamess.calculator import GamessGridCalc
from markstools.lib import utils
from markstools.lib import usertask

from gorg.lib import state
from gorg.model.gridtask import TaskInterface
from gorg.lib.utils import Mydb
from gorg.gridjobscheduler import STATES as JOB_SCHEDULER_STATES

STATE_WAIT = state.State.create('WAIT', 'WAIT desc')
STATE_PROCESS = state.State.create('PROCESS', 'PROCESS desc')
STATE_POSTPROCESS = state.State.create('POSTPROCESS', 'POSTPROCESS desc')

class GRestart(usertask.UserTask):

    STATES = state.StateContainer([STATE_WAIT, STATE_PROCESS, STATE_POSTPROCESS, 
                            usertask.STATE_ERROR, usertask.STATE_COMPLETED])
    
    def __init__(self):
        self.status = self.STATES.ERROR
        self.status_mapping = {self.STATES.WAIT: self.handle_wait_state, 
                                             self.STATES.PROCESS: self.handle_process_state, 
                                             self.STATES.POSTPROCESS: self.handle_postprocess_state, 
                                             self.STATES.ERROR: self.handle_terminal_state, 
                                             self.STATES.COMPLETED: self.handle_terminal_state}
        self.a_task = None
        self.calculator = None
    
    def initialize(self, db, calculator, atoms, params, application_to_run='gamess', selected_resource='ocikbpra',  cores=2, memory=1, walltime=1):
        self.a_task = TaskInterface(db).create(self.__class__.__name__)
        self.calculator = calculator
        self.a_task.user_data_dict['restart_number'] = 0
        self.a_task.store()
        params.title = 'restart_number_%d'%self.a_task.user_data_dict['restart_number']
        a_job = self.calculator.generate(atoms, params, self.a_task, application_to_run, selected_resource, cores, memory, walltime)
        self.calculator.calculate(a_job)
        self.status = self.STATES.WAIT
        self.save()


    def handle_wait_state(self):
        from gorg.gridjobscheduler import GridjobScheduler
        job_scheduler = GridjobScheduler('mark','gorg_site','http://localhost:5984')
        job_list = self.a_task.children
        new_status = self.STATES.PROCESS
        job_scheduler.run()
        for a_job in job_list:
            job_done = False
            job_done = a_job.wait(timeout=0)
            if not job_done:
                new_status=self.STATES.WAIT
                markstools.log.info('Waiting for job %s.'%(a_job.id))
                break
        self.status = new_status
    
    def handle_process_state(self):
        a_job = self.a_task.children[-1]
        a_result = self.calculator.parse(a_job)
        params = copy.deepcopy(a_result.params)
        atoms = a_result.atoms.copy()
        
        if a_result.exit_successful():
            a_job.status = JOB_SCHEDULER_STATES.COMPLETED
            a_job.store()
        else:
            a_job.status = JOB_SCHEDULER_STATES.ERROR
            a_job.store()
            markstools.log.critical('GAMESS returned an error while running job %s.'%(a_job.id))
            new_state = self.STATES.ERROR
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
            self.status = self.STATES.WAIT
        else:
            self.status = self.STATES.POSTPROCESS
            markstools.log.info('Restart sequence task id %s has finished successfully.'%(self.a_task.id))
    
    def handle_postprocess_state(self):
        self.status = self.STATES.COMPLETED
        pass
    
    def handle_terminal_state(self):
        print 'I do nothing!!'
    

def logging(options):    
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

def parse_options():
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-f", "--file", dest="file",default='markstools/examples/exam01.inp', 
                      help="gamess inp to restart from.")
    parser.add_option("-v", "--verbose", action='count', dest="verbose", default=0, 
                      help="add more v's to increase log output.")
    parser.add_option("-n", "--db_name", dest="db_name", default='gorg_site', 
                      help="add more v's to increase log output.")
    parser.add_option("-l", "--db_url", dest="db_url", default='http://localhost:5984', 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()
    return options

def main():
    options = parse_options()
    logging(options)
    
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
    main()
    sys.exit(0)
