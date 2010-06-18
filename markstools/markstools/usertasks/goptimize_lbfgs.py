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
from markstools.lib import usertask
from markstools.optimize import lbfgs

from gorg.model.gridtask import TaskInterface
from gorg.lib.utils import Mydb
from gorg.lib import state
from gorg.gridjobscheduler import STATES as JOB_SCHEDULER_STATES

STATE_WAIT = state.State.create('WAIT', 'WAIT desc')
STATE_STEP = state.State.create('STEP', 'STEP desc')
STATE_POSTPROCESS = state.State.create('POSTPROCESS', 'POSTPROCESS desc')

class GOptimize_lbfgs(usertask.UserTask):
    STATES = state.StateContainer([STATE_WAIT, STATE_STEP, STATE_POSTPROCESS, 
                            usertask.STATE_ERROR, usertask.STATE_COMPLETED])
                            
    def __init__(self):
        self.status = self.STATES.ERROR
        self.status_mapping = {self.STATES.WAIT: self.handle_wait_state, 
                                             self.STATES.STEP: self.handle_step_state, 
                                             self.STATES.ERROR: self.handle_terminal_state, 
                                             self.STATES.COMPLETED: self.handle_terminal_state}
        
        self.a_task = None
        self.calculator = None

    def initialize(self, db, calculator, atoms, params, application_to_run='gamess', selected_resource='gc3',  cores=8, memory=2, walltime=3):
        self.calculator = calculator
        self.a_task = TaskInterface(db).create(self.__class__.__name__)
        self.a_task.user_data_dict['total_jobs'] = 0
        params.title = 'job_number_%d'%self.a_task.user_data_dict['total_jobs']
        a_job = self.calculator.generate(atoms, params, self.a_task, application_to_run, selected_resource, cores, memory, walltime)
        self.calculator.calculate(self.a_task)
        self.status = self.STATES.WAIT
    
    def handle_wait_state(self):
        from gorg.gridjobscheduler import GridjobScheduler
        job_scheduler = GridjobScheduler('mark','gorg_site','http://130.60.144.211:5984')
        job_list = [self.a_task.children[-1]]
        new_status = self.STATES.STEP
        job_scheduler.run()
        for a_job in job_list:
            job_done = False
            job_done = a_job.wait(timeout=0)
            if not job_done:
                new_status=self.STATES.WAIT
                markstools.log.info('Waiting for job %s.'%(a_job.id))
                break
        self.status = new_status
    
    def handle_step_state(self):
        a_job = self.a_task.children[-1]
        a_result = self.calculator.parse(a_job)
        myfile = a_job.get_attachment('.inp')
        reader = ReadGamessInp(myfile)
        myfile.close()
        params = reader.params
        params.title ='a_title'
        atoms = reader.atoms
        opt = lbfgs.LBFGS(atoms)
        opt.initialize()
        myfile = self.a_task.get_attachment('lbfsg_matrix')
        if myfile is not None:
            opt.load(myfile)
            myfile.close()
        #restart if we need to
        new_positions = opt.step(a_result.atoms.get_positions(), a_result.get_forces())
        myfile = opt.dump()
        self.a_task.put_attachment(myfile, 'lbfsg_matrix')
        myfile.close()
        atoms.set_positions(new_positions)
        new_job = self.calculator.generate(atoms, params, self.a_task, 
                                                                a_job.run.application.application_tag, a_job.run.application.requested_resource,  
                                                                a_job.run.application.requested_cores, a_job.run.application.requested_memory, 
                                                                a_job.run.application.requested_walltime)
        self.calculator.calculate(new_job)
        self.status = self.STATES.WAIT
    
    def handle_terminal_state(self):
        pass

def main(options):
    # Connect to the database
    db = Mydb('mark',options.db_name,options.db_url).cdb()

    myfile = open(options.file, 'rb')
    reader = ReadGamessInp(myfile)
    myfile.close()
    params = reader.params
    atoms = reader.atoms
    
    goptimize = GOptimize_lbfgs()
    gamess_calc = GamessGridCalc(db)
    goptimize.initialize(db, gamess_calc, atoms, params)
    
    goptimize.run()

    print 'goptimize done. Create task %s'%(goptimize.a_task.id)

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
        
    #configure_logger(options.verbose)
    configure_logger(10)
    import gorg.lib.utils
    gorg.lib.utils.configure_logger(10)
    
    main(options)

    sys.exit(0)


