'''
Created on Dec 28, 2009

@author: mmonroe
'''
import time

import markstools
from markstools.lib import utils
from markstools.lib.exceptions import *
from gorg.lib import state
from gorg.gridscheduler import STATES as JOB_SCHEDULER_STATES
from gorg.model.gridtask import GridtaskModel
from markstools.calculators.gamess.calculator import GamessGridCalc
from couchdb import http

STATE_ERROR = state.State.create('ERROR', 'ERROR desc', terminal = True)
STATE_COMPLETED = state.State.create('COMPLETED', 'COMPLETED desc', terminal = True)
STATE_KILL = state.State.create('KILL', 'KILL desc')
STATE_RETRY = state.State.create('RETRY', 'RETRY desc')
STATE_KILLED = state.State.create('KILLED', 'KILLED desc', terminal = True)
STATE_WAIT = state.State.create('WAIT', 'WAIT desc')

class UserTask(object):
        
    def load(self, db,  task_id):
        self.a_task = GridtaskModel(db).load(id=task_id)
        self.status = self.a_task.status
        str_calc = self.a_task.user_data_dict['calculator']
        self.calculator = eval(str_calc + '(db)')
    
    def save(self):
        self.a_task.status = self.status
        self.a_task.user_data_dict['calculator'] = self.calculator.__class__.__name__
        try:
            self.a_task.store()
        except http.ResourceConflict:
            #The document in the database does not match the one we are trying to save.
            # Lets just ignore the error and let the state machine run
            markstools.log.critical('Could not save task %s due to a document revision conflict'%(self.a_task.id))
        
    def handle_missing_state(self):
        raise UnhandledStateError('State %s is not implemented.'%(self.state))
    
    def handle_kill_state(self):
        job_list = self.a_task.children
        for a_job in job_list:
            counter = 0
            sleep_amount = 5
            max_sleep_amount = 30
            while a_job.status.locked and counter < max_sleep_amount:
                time.sleep(sleep_amount)
                counter += sleep_amount
                markstools.log.info('Waiting for Job %s to go into killable state.'%(a_job.id))
                markstools.log.debug('Job is in state %s'%(a_job.status))
            if a_job.status.locked:
                raise DocumentError('Job %s can not be killed, it is in locked state %s'%(a_job.id, a_job.status))
            a_job.status = JOB_SCHEDULER_STATES.KILL
            a_job.store()
        self.status = self.STATES.KILLED
   
    def handle_retry_state(self):
        job_list = self.a_task.children
        for a_job in job_list:
            if a_job.status == JOB_SCHEDULER_STATES.ERROR:
                markstools.log.info('Job %s is in error, retrying.'%(a_job.id))
                a_job.status = JOB_SCHEDULER_STATES.READY
            a_job.store()
        self.status = self.STATES.WAIT
    
    def step(self):
        try:
            self.status_mapping.get(self.status, self.handle_missing_state)()
        except:
            self.status = self.STATES.ERROR
            markstools.log.critical('%s errored while processing task %s \n%s'%(self.__class__.__name__, self.a_task.id, utils.format_exception_info()))
        self.save()
    
    def run(self):
        while not self.status.terminal:
            time.sleep(10)
            self.load(self.a_task.db, self.a_task.id)
            self.step()
    
    @property
    def  myname(self):
        return self.__class__.__name__
