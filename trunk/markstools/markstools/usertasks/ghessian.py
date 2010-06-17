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
from markstools.lib import usertask

from gorg.model.gridtask import TaskInterface
from gorg.lib.utils import Mydb
from gorg.lib import state
from gorg.gridjobscheduler import STATES as JOB_SCHEDULER_STATES

STATE_WAIT = state.State.create('WAIT', 'WAIT desc')
STATE_PROCESS = state.State.create('PROCESS', 'PROCESS desc')
STATE_POSTPROCESS = state.State.create('POSTPROCESS', 'POSTPROCESS desc')

class GHessian(usertask.UserTask):
    H_TO_PERTURB = 0.0052918
    GRADIENT_CONVERSION=1.8897161646320724
    
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

    def initialize(self, db, calculator, atoms, params, application_to_run='gamess', selected_resource='gc3',  cores=8, memory=2, walltime=None):
        self.calculator = calculator
        self.a_task = TaskInterface(db).create(self.__class__.__name__)
        self.a_task.user_data_dict['total_jobs'] = 0
        self.a_task.store()
        perturbed_postions = self.repackage(atoms.get_positions())
        params.title = 'job_number_%d'%self.a_task.user_data_dict['total_jobs']
        first_job = self.calculator.generate(atoms, params, self.a_task, application_to_run, selected_resource, cores, memory, walltime)
        for a_position in perturbed_postions[1:]:
            self.a_task.user_data_dict['total_jobs'] += 1
            params.title = 'job_number_%d'%self.a_task.user_data_dict['total_jobs']
            atoms.set_positions(a_position)
            sec_job = calculator.generate(atoms, params, self.a_task, application_to_run, selected_resource, cores, memory, walltime)
        self.calculator.calculate(self.a_task)
        markstools.log.info('Submitted task %s for execution.'%(self.a_task.id))
        self.status = self.STATES.WAIT
    
    def handle_wait_state(self):
        from gorg.gridjobscheduler import GridjobScheduler
        job_scheduler = GridjobScheduler('mark','gorg_site','http://130.60.144.211:5984')
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
        job_list = self.a_task.children
        for a_job in job_list:
            a_result = self.calculator.parse(a_job)
            if not a_result.exit_successful():
                a_job.status = JOB_SCHEDULER_STATES.ERROR
                msg = 'GAMESS returned an error while running job %s.'%(a_job.id)
                markstools.log.critical(msg)
                raise Exception, msg
            a_job.status = JOB_SCHEDULER_STATES.COMPLETED
            a_job.store()
        self.status = self.STATES.POSTPROCESS

    def handle_postprocess_state(self):
        result_list  = list()
        for a_job in self.a_task.children:
            result_list.append(self.calculator.parse(a_job))
        num_atoms = len(result_list[-1].atoms.get_positions())
        gradMat = np.zeros((num_atoms*len(result_list), 3), dtype=np.longfloat)
        count = 0
        for a_result in result_list:
            grad = a_result.get_forces()
            for j in range(0, len(grad)):
                gradMat[count]=grad[j]
                count +=1
        mat = self.calculateNumericalHessian(num_atoms, gradMat)
        postprocess_result = mat/self.GRADIENT_CONVERSION
        
        f_hess = open('%s_ghessian.mjm'%(self.a_task.id), 'w')
        print f_hess
        WriteGamessInp.build_gamess_matrix(postprocess_result, f_hess)
        f_hess.close()
        self.status = self.STATES.COMPLETED

    def handle_terminal_state(self):
        print 'I do nothing!!'

    def perturb(self, npCoords):
        stCoords= np.reshape(np.squeeze(npCoords), len(npCoords)*3, 'C')
        E =  np.vstack([np.zeros((1, len(stCoords))), np.eye((len(stCoords)),(len(stCoords)))])
        return self.H_TO_PERTURB*E+stCoords

    def repackage(self, org_coords):
        stCoords = self.perturb(org_coords)
        newCoords = list()
        for i in stCoords:
            i=i.reshape((len(i)/3, 3))
            newCoords.append(i)
        return newCoords
        
    def calculateNumericalHessian(self, sz, gradient):
        gradient= np.reshape(gradient,(len(gradient)/sz,sz*3),'C').T    
        hessian = np.zeros((3*sz, 3*sz), dtype=np.longfloat)
        for i in range(0, 3*sz):
            for j in range(0, 3*sz):
                hessian[i, j] = (1.0/(2.0*self.H_TO_PERTURB))*((gradient[i, j+1]-gradient[i, 0])+(gradient[j, i+1]-gradient[j, 0]))
        return hessian

def main(options):
    # Connect to the database
    db = Mydb('mark',options.db_name,options.db_url).cdb()

    myfile = open(options.file, 'rb')
    reader = ReadGamessInp(myfile)
    myfile.close()
    params = reader.params
    atoms = reader.atoms
    
    ghessian = GHessian()
    gamess_calc = GamessGridCalc(db)
    ghessian.initialize(db, gamess_calc, atoms, params)
    
    ghessian.run()

    print 'ghessian done. Create task %s'%(ghessian.a_task.id)

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
