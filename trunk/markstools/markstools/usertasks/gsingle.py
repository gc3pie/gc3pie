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

STATE_PROCESS =  state.State.create('PROCESS', 'PROCESS desc')
STATE_POSTPROCESS =  state.State.create('POSTPROCESS', 'POSTPROCESS desc')

class GSingle(usertask.UserTask):
 
    STATES = state.StateContainer([usertask.STATE_WAIT, STATE_PROCESS, STATE_POSTPROCESS, 
                            usertask.STATE_ERROR, usertask.STATE_COMPLETED,  usertask.STATE_KILL, usertask.STATE_KILLED])
                            
    def __init__(self):
        self.status = self.STATES.ERROR
        self.status_mapping = {self.STATES.WAIT: self.handle_wait_state, 
                                             self.STATES.PROCESS: self.handle_process_state, 
                                             self.STATES.POSTPROCESS: self.handle_postprocess_state,
                                             self.STATES.KILL: self.handle_kill_state, 
                                             self.STATES.KILLED: self.handle_terminal_state, 
                                             self.STATES.ERROR: self.handle_terminal_state, 
                                             self.STATES.COMPLETED: self.handle_terminal_state}
        self.a_task = None
        self.calculator = None

    def initialize(self, db, calculator, atoms, params, application_to_run='gamess', selected_resource='pra',  cores=2, memory=2, walltime=-1):
        self.calculator = calculator
        self.a_task = GridtaskModel(db).create(self.__class__.__name__)
        
        params.title = 'singlejob'
        a_job = self.calculator.generate(atoms, params, self.a_task, application_to_run, selected_resource, cores, memory, walltime)
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
                markstools.log.info('%s waiting for job %s.'%(self.myname, a_job.id))
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
        self.status = self.STATES.COMPLETED

    def handle_terminal_state(self):
        print 'I do nothing!!'
