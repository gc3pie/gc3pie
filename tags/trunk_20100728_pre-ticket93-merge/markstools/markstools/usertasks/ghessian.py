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

from gorg.model.gridtask import GridtaskModel
from gorg.lib.utils import Mydb
from gorg.lib import state
from gorg.gridscheduler import STATES as JOB_SCHEDULER_STATES

STATE_PROCESS = state.State.create('PROCESS', 'PROCESS desc')
STATE_POSTPROCESS = state.State.create('POSTPROCESS', 'POSTPROCESS desc')

class GHessian(usertask.UserTask):
    H_TO_PERTURB = 0.0052918
    GRADIENT_CONVERSION=1.8897161646320724
    
    STATES = state.StateContainer([usertask.STATE_WAIT, usertask.STATE_RETRY, STATE_PROCESS, STATE_POSTPROCESS, 
                            usertask.STATE_ERROR, usertask.STATE_COMPLETED, usertask.STATE_KILL, usertask.STATE_KILLED])

    def __init__(self):
        self.status = self.STATES.ERROR
        self.status_mapping = {self.STATES.WAIT: self.handle_wait_state, 
                                             self.STATES.PROCESS: self.handle_process_state, 
                                             self.STATES.POSTPROCESS: self.handle_postprocess_state,
                                             self.STATES.RETRY: self.handle_retry_state, 
                                             self.STATES.KILL: self.handle_kill_state, 
                                             self.STATES.KILLED: self.handle_terminal_state, 
                                             self.STATES.ERROR: self.handle_terminal_state, 
                                             self.STATES.COMPLETED: self.handle_terminal_state}
        self.a_task = None
        self.calculator = None

    def initialize(self, db, calculator, atoms, params, application_to_run='gamess', selected_resource='pra',  cores=8, memory=2, walltime=1):
        self.calculator = calculator
        self.a_task = GridtaskModel(db).create(self.__class__.__name__)
        self.a_task.user_data_dict['total_jobs'] = 0
        perturbed_postions = self.repackage(atoms.get_positions())
        params.title = 'job_number_%d'%self.a_task.user_data_dict['total_jobs']
        first_job = self.calculator.generate(atoms, params, self.a_task, application_to_run, selected_resource, cores, memory, walltime)
        for a_position in perturbed_postions[1:]:
            self.a_task.user_data_dict['total_jobs'] += 1
            params.title = 'job_number_%d'%self.a_task.user_data_dict['total_jobs']
            atoms.set_positions(a_position)
            sec_job = calculator.generate(atoms, params, self.a_task, application_to_run, selected_resource, cores, memory, walltime)
            first_job.add_child(sec_job)
        first_job.store()
        self.calculator.calculate(self.a_task)
        markstools.log.info('Submitted task %s for execution.'%(self.a_task.id))
        self.status = self.STATES.WAIT
        self.save()
    
    def handle_wait_state(self):
        job_list = self.a_task.children
        new_status = self.STATES.PROCESS
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
