import htpie

from htpie.enginemodel import Task
from htpie.lib.exceptions import *
from htpie.lib import utils
from htpie.states import *

from pymongo.objectid import ObjectId

import time
import datetime

def configure_logger():
    import htpie
    import logging
    import os
    import multiprocessing
    if not htpie.log.handlers:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fname = multiprocessing.current_process().name
        fname = os.path.expanduser('~/.htpie/gc3utils_%s.log'%(fname))
        logging_level = 10 * max(1, 5-100)
        htpie.log.setLevel(logging_level)
        file_handler = logging.handlers.RotatingFileHandler(fname, maxBytes=2000000, backupCount=5)
        file_handler.setFormatter(formatter)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        htpie.log.addHandler(file_handler)
        htpie.log.addHandler(stream_handler)

#    from multiprocessing import get_logger    
#    log = get_logger()
#    log.setLevel(logging.DEBUG)
#    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#    file_handler = logging.handlers.RotatingFileHandler(os.path.expanduser('~/.htpie/gc3utils.log'), maxBytes=2000000, backupCount=5)
#    file_handler.setFormatter(formatter)
#
#    stream_handler = logging.StreamHandler()
#    stream_handler.setFormatter(formatter)
#    
#    log.addHandler(file_handler)
#    log.addHandler(stream_handler)

def _thread_exec_fsm(fsm_class, id):
    from htpie import statemachine
    import multiprocessing

    statemachine.configure_logger()
    fsm = fsm_class()
    
    time.sleep(.25)
    #htpie.log.debug('Third Thread name: %s'%(multiprocessing.current_process().name))
    fsm.load(id)
    fsm.step()

class StateMachine(object):    
    _cls_task = Task
    
    def __init__(self):
        self.task = None
        self.state_mapping = { States.KILL: self.handle_kill_state, 
                                              States.COMPLETE: self.handle_terminal_state,
                                            }
    
    def step(self):
        htpie.log.debug('%s is processing %s %s'%(self.name, self.task.id, self.task.state))
        try:
            self.task.acquire()
        except AuthorizationException,  e:
            #Do nothing but log the error and return
            pass
        else:
            try:
                self.transition = Transitions.RUNNING
                done = self.state_mapping.get(self.state, self.handle_missing_state)()
                if done:
                    self.transition = Transitions.COMPLETE
                else:
                    self.transition = Transitions.PAUSED
            except:
                self.transition = Transitions.ERROR
                htpie.log.critical('%s errored while processing %s \n%s'%(self.__class__.__name__, self.task.id, utils.format_exception_info()))
            finally:
                self.task.last_exec_d = datetime.datetime.now()
                self.task.release()
    
    def run(self):
        while self.transition == Transitions.PAUSED:
            self.load(self.task.id)
            self.step()
            time.sleep(10)
    
    def save(self):
        self.task.save()
    
    def load(self, id):
        self.task = self._cls_task.objects.with_id(id)
    
    def transition():
        def fget(self):
            return self.task.transition
        def fset(self, transition):
            self.task.transition = transition
            self.save()
        return locals()
    transition = property(**transition())
    
    def state():
        def fget(self):
            return self.task.state
        def fset(self, state):
            self.task.state = state
            self.save()
        return locals()
    state = property(**state())    
    
    @property
    def  name(self):
        return self.__class__.__name__
        
    def handle_missing_state(self):
        raise UnhandledStateException('State %s is not implemented in %s.'%(self.state, self.name))
    
    def handle_terminal_state(self):
        pass
    
    def handle_kill_state(self):
        pass
