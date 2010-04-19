'''
Created on Dec 28, 2009

@author: mmonroe
'''
from statemachine import *

from optparse import OptionParser
import logging
import os
import copy
import sys
import numpy as np

from markstools.io.gamess import ReadGamessInp, WriteGamessInp
from markstools.calculators.gamess import GamessGridCalc
from gorg.lib.mydb import Mydb

class Cargo(object):
    def __init__(self, a_task, calculator):
        self.a_task = a_task
        self.calculator = calculator
        
class GHessian(StateMachine):
    H_TO_PERTURB = 0.0052918
    GRADIENT_CONVERSION=1.8897161646320724
    WAIT = State()
    PROCESS = State()
    
    def __init__(self, logging_level=1):
        super(GHessian, self).__init__(logging_level)
        self.cargo = None

    def start(self, db, calculator, atoms, params, application_to_run='gamess', selected_resource='ocikbpra',  cores=2, memory=1, walltime=-1):
        super(GHessian, self).start(self.WAIT )
        from gorg_site.gorg_site.model.gridtask import TaskInterface
        a_task = TaskInterface(db).create(self.__class__.__name__)
        a_task.user_data_dict['total_jobs'] = 0
        
        perturbed_postions = self.repackage(atoms.get_positions())
        params.title = 'job_number_%d'%a_task.user_data_dict['total_jobs']
        first_job = calculator.generate(atoms, params, a_task, application_to_run, selected_resource, cores, memory, walltime)
        for a_position in perturbed_postions[1:]:
            a_task.user_data_dict['total_jobs'] += 1
            params.title = 'job_number_%d'%a_task.user_data_dict['total_jobs']
            atoms.set_positions(a_position)
            sec_job = calculator.generate(atoms, params, a_task, application_to_run, selected_resource, cores, memory, walltime)
        calculator.calculate(a_task)
        self.logger.info('Submitted task %s for execution.'%(a_task.id))
        self.cargo = Cargo(a_task, calculator)

    def restart(self, db,  a_task):
        super(GHessian, self).start(eval('self.%s'%(a_task.state)))
        str_calc = a_task.user_data_dict['calculator']
        self.cargo = Cargo(a_task, eval(str_calc + '(db)'))
    
    def save_state(self):
        self.cargo.a_task.state = super(GHessian, self).save_state()
        self.cargo.a_task.user_data_dict['calculator'] = self.cargo.calculator.__class__.__name__
        return self.cargo.a_task
    
    @on_main(WAIT)
    def wait(self):
        sys.path.append('/home/mmonroe/apps/gorg')
        from gorg.gridjobscheduler import GridjobScheduler
        job_scheduler = GridjobScheduler()
        job_list = self.cargo.a_task.children
        new_state=self.PROCESS
        for a_job in job_list:
            job_done = False
            job_scheduler.run()
            self.logger.info('Restart waiting for job %s.'%(a_job.id))
            job_done = a_job.wait(timeout=0)
            if not job_done:
                new_state=self.WAIT
                break
        return new_state
    
    @on_main(PROCESS)
    def process_loop(self):
        done = False
        job_list = self.cargo.a_task.children
        for a_job in job_list:
            a_result = self.cargo.calculator.parse(a_job)
            if not a_result.exit_successful():
                msg = 'GAMESS returned an error while running job %s.'%(a_job.id)
                self.logger.critical(msg)
                raise Exception, msg
        done = True
        return self.DONE

#TODO: Wouldn't if be nice if on_enter on_main and on_leave could share variables with each other?
    @on_leave(PROCESS)
    def postprocess(self):
        result_list  = list()
        for a_job in self.cargo.a_task.children:
            result_list.append(self.cargo.calculator.parse(a_job))
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
        
        f_hess = open('%s_ghessian.mjm'%(self.cargo.a_task.id), 'w')
        WriteGamessInp.build_gamess_matrix(postprocess_result, f_hess)
        f_hess.close()

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
    db=Mydb('mark',options.db_name,options.db_loc).cdb()
    
    # Parse the gamess inp file
    myfile = open(options.file, 'rb')
    reader = ReadGamessInp(myfile)
    myfile.close()
    params = reader.params
    atoms = reader.atoms
    
    fsm = GHessian(options.logging_level)
    gamess_calc = GamessGridCalc(db)
    fsm.start(db, gamess_calc, atoms, params)
    fsm.run()
    a_task = fsm.save_state()
    print a_task.id
    

if __name__ == '__main__':
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-f", "--file", dest="file",default='hess.test2.gamess.inp', 
                      help="gamess inp to restart from.")
    parser.add_option("-v", "--verbose", dest="verbose", default='', 
                      help="add more v's to increase log output.")
    parser.add_option("-n", "--db_name", dest="db_name", default='gorg_site', 
                      help="add more v's to increase log output.")
    parser.add_option("-l", "--db_loc", dest="db_loc", default='http://127.0.0.1:5984', 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()
    
    #Setup logger
    LOGGING_LEVELS = (logging.CRITICAL, logging.ERROR, 
                                    logging.WARNING, logging.INFO, logging.DEBUG)
    options.logging_level = len(LOGGING_LEVELS) if len(options.verbose) > len(LOGGING_LEVELS) else len(options.verbose)

    main(options)

    sys.exit()
