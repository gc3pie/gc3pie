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
import glob

from markstools.io.gamess import ReadGamessInp, WriteGamessInp
from markstools.calculators.gamess.calculator import GamessGridCalc
from markstools.lib import utils
from markstools.lib import usertask

from gorg.model.gridtask import GridtaskModel
from gorg.lib.utils import Mydb
from gorg.lib import state
from gorg.gridscheduler import STATES as JOB_SCHEDULER_STATES

STATE_PROCESS =  state.State.create('PROCESS', 'PROCESS desc')
STATE_EXECUTE =  state.State.create('EXECUTE', 'EXECUTE desc')

STATE_POSTPROCESS =  state.State.create('POSTPROCESS', 'POSTPROCESS desc')

class GDirRun(usertask.UserTask):
 
    STATES = state.StateContainer([usertask.STATE_WAIT, usertask.STATE_RETRY, STATE_EXECUTE, STATE_PROCESS, STATE_POSTPROCESS, 
                            usertask.STATE_ERROR, usertask.STATE_COMPLETED, usertask.STATE_KILL, usertask.STATE_KILLED])
                            
    def __init__(self):
        self.status = self.STATES.ERROR
        self.status_mapping = {self.STATES.WAIT: self.handle_wait_state, 
                                             self.STATES.PROCESS: self.handle_process_state, 
                                             self.STATES.EXECUTE: self.handle_execute_state, 
                                             self.STATES.POSTPROCESS: self.handle_postprocess_state,
                                             self.STATES.KILL: self.handle_kill_state, 
                                             self.STATES.KILLED: self.handle_terminal_state, 
                                             self.STATES.ERROR: self.handle_terminal_state, 
                                             self.STATES.COMPLETED: self.handle_terminal_state}
        self.a_task = None
        self.calculator = None

    def initialize(self, db, calculator, dir, max_running=5, application_to_run='gamess', selected_resource='pra',  cores=2, memory=2, walltime=-1):
        self.calculator = calculator
        self.a_task = GridtaskModel(db).create(self.myname)
        self.a_task.user_data_dict['max_running'] = max_running
        self.a_task.user_data_dict['currently_running'] = 0
        for a_file in glob.glob('%s/*.inp'%(dir)):            
            try:
                myfile = open(a_file, 'rb')
                reader = ReadGamessInp(myfile)        
            finally:
                myfile.close()

            params = reader.params
            atoms = reader.atoms
            params.title = os.path.basename(myfile.name)
            a_job = self.calculator.generate(atoms, params, self.a_task, application_to_run, selected_resource, cores, memory, walltime)
        markstools.log.info('Submitted task %s for execution.'%(self.a_task.id))
        self.status = self.STATES.EXECUTE
        self.save()
    
    def handle_execute_state(self):
        job_list = self.a_task.children
        while self.a_task.user_data_dict['currently_running'] < self.a_task.user_data_dict['max_running']  \
                    and len(job_list) !=  self.a_task.user_data_dict['currently_running']:
            for a_job in job_list:
                if a_job.status == JOB_SCHEDULER_STATES.HOLD:
                    self.calculator.calculate(a_job)
                    self.a_task.user_data_dict['currently_running'] += 1
                    break
        self.status = self.STATES.WAIT
    
    def handle_wait_state(self):
        job_list = self.a_task.children
        new_status = self.STATES.PROCESS
        for a_job in job_list:
            if a_job.status != JOB_SCHEDULER_STATES.HOLD:
                job_done = False
                job_done = a_job.wait(timeout=0)
                if not job_done:
                    new_status=self.STATES.WAIT
                    markstools.log.info('%s waiting for job %s.'%(self.myname, a_job.id))
                    break
                else:
                    if self.a_task.user_data_dict['currently_running'] != 0:
                        self.a_task.user_data_dict['currently_running'] -= 1
                        new_status = self.STATES.EXECUTE
        self.status = new_status
    
    def handle_process_state(self):
        job_list = self.a_task.children
        for a_job in job_list:
            a_result = self.calculator.parse(a_job)
            if not a_result.exit_successful():
                a_job.status = JOB_SCHEDULER_STATES.ERROR
                raise CalculatorError('%s returned an error while running job %s.'%(self.calcualtor, a_job.id))
            a_job.status = JOB_SCHEDULER_STATES.COMPLETED
            a_job.store()
        self.status = self.STATES.POSTPROCESS

    def handle_postprocess_state(self):
        self.status = self.STATES.COMPLETED

    def handle_terminal_state(self):
        print 'I do nothing!!'
