from markstools.lib.statemachine import *

from optparse import OptionParser
import logging
import os
import copy
import sys

from markstools.io.gamess import ReadGamessInp
from markstools.calculators.gamess.calculator import GamessGridCalc

from gorg.model.gridtask import TaskInterface
from gorg.lib.utils import Mydb
from gorg.gridjobscheduler import GridjobScheduler

_log = logging.getLogger('markstools')

class Cargo(object):
    def __init__(self, a_task, calculator):
        self.a_task = a_task
        self.calculator = calculator

class GRestart(StateMachine):
    WAIT = State()
    EXECUTE = State()
    PROCESS = State()
    POSTPROCESS = State()
    
    def __init__(self):
        super(GRestart, self).__init__()
        self.cargo = None
    
    def start(self, db, calculator, atoms, params, application_to_run='gamess', selected_resource='ocikbpra',  cores=2, memory=1, walltime=-1):
        super(GRestart, self).start(self.EXECUTE )
        a_task = TaskInterface(db).create(self.__class__.__name__)
        a_task.user_data_dict['restart_number'] = 0
        params.title = 'restart_number_%d'%a_task.user_data_dict['restart_number']
        a_job = calculator.generate(atoms, params, a_task, application_to_run, selected_resource, cores, memory, walltime)
        self.cargo = Cargo(a_task, calculator)

    def restart(self, db,  a_task):
        super(GRestart, self).start(eval('self.%s'%(a_task.state)))
        str_calc = a_task.user_data_dict['calculator']
        self.cargo = Cargo(a_task, eval(str_calc + '(db)'))
    
    def save_state(self):
        self.cargo.a_task.state = super(GRestart, self).save_state()
        self.cargo.a_task.user_data_dict['calculator'] = self.cargo.calculator.__class__.__name__
        return self.cargo.a_task

    @on_main(EXECUTE)
    def execute(self):
        job_list = self.cargo.calculator.calculate(self.cargo.a_task.children[-1])
        for a_job in job_list:
            _log.info('Submited job %s to batch system.'%(a_job.id))
        return self.WAIT
    
    @on_main(WAIT)
    def wait(self):
        job_scheduler = GridjobScheduler(db_url='http://130.60.144.211:5984')
        job_list = [self.cargo.a_task.children[-1]]
        new_state=self.PROCESS
        for a_job in job_list:
            job_done = False
            job_scheduler.run()
            job_done = a_job.wait(timeout=0)
            if not job_done:
                _log.info('Restart waiting for job %s.'%(a_job.id))
                new_state=self.WAIT
                break
        return new_state
    
    @on_main(PROCESS)
    def process(self):
        a_job = self.cargo.a_task.children[-1]
        a_result = self.cargo.calculator.parse(a_job)
        params = copy.deepcopy(a_result.params)
        atoms = a_result.atoms.copy()
        if a_result.exit_successful():
            if not a_result.geom_located():                
                params.r_orbitals = a_result.get_orbitals(raw=True)
                params.r_hessian = a_result.get_hessian(raw=True)
                self.cargo.a_task.user_data_dict['restart_number'] += 1
                params.title = 'restart_number_%d'%self.cargo.a_task.user_data_dict['restart_number']
                # Make sure that the orbitals an hessian will be read in the inp file
                params.set_group_param('$GUESS', 'GUESS', 'MOREAD')
                params.set_group_param('$STATPT', 'HESS', 'READ')
                atoms.set_positions(a_result.get_coords())
                a_new_job = self.cargo.calculator.generate(atoms, params, self.cargo.a_task, **a_job.run_params)
                a_new_job.add_parent(a_job)
                new_state = self.EXECUTE
            else:
                new_state = self.POSTPROCESS
                _log.info('Restart sequence task id %s has finished successfully.'%(self.cargo.a_task.id))
        else:
            msg = 'GAMESS returned an error while running job %s.'%(a_job.id)
            _log.critical(msg)
            new_state = self.ERROR
        return new_state
    
    @on_main(POSTPROCESS)
    def postprocess(self):
        return self.DONE

def main(options):
    # Connect to the database
    db=Mydb('mark',options.db_name,options.db_url).cdb()
    
    # Parse the gamess inp file
    myfile = open(options.file, 'rb')
    reader = ReadGamessInp(myfile)
    myfile.close()
    params = reader.params
    atoms = reader.atoms
    
    fsm = GRestart()
    gamess_calc = GamessGridCalc(db)
    fsm.start(db, gamess_calc, atoms, params)
    for i in range(10):
        fsm.run()
    a_task = fsm.save_state()
    print a_task.id
    

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
