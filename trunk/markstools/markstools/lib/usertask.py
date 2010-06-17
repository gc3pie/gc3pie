'''
Created on Dec 28, 2009

@author: mmonroe
'''
import time

import markstools
from markstools.lib import utils
from markstools.lib.exceptions import *
from gorg.lib import state

STATE_ERROR = state.State.create('ERROR', 'ERROR desc', terminal = True)
STATE_COMPLETED = state.State.create('COMPLETED', 'COMPLETED desc', terminal = True)

class UserTask(object):
    
    def load(self, db,  task_id):
        self.a_task = TaskInterface(db).load(task_id)
        self.status = self.a_task.status
        str_calc = self.a_task.user_data_dict['calculator']
        self.calculator = eval(str_calc + '(db)')
    
    def save(self):
        self.a_task.status = self.status
        self.a_task.user_data_dict['calculator'] = self.calculator.__class__.__name__
        self.a_task.store()
        
    def handle_missing_state(self):
        raise UnhandledStateError('State %s is not implemented.'%(self.state))
   
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
            self.step()
