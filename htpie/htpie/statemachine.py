import time
import datetime

from htpie import model
from htpie.lib.exceptions import *
from htpie.lib import utils
from pymongo.objectid import ObjectId

class States(object):
    COMPLETE = u'STATE_COMPLETED'
    KILL = u'STATE_KILL'

class Transitions(object):
    ERROR = u'ACTION_ERROR'
    COMPLETE = u'ACTION_COMPLETE'
    RUNNING = u'ACTION_RUNNING'
    PAUSED = u'ACTION_PAUSED'
    HOLD = u'ACTION_HOLD'

class StateMachine(object):    
    _cls_task = model.Task
    
    def __init__(self):
        self.task = None
        self.state_mapping = { States.KILL: self.handle_kill_state, 
                                              States.COMPLETE: self.handle_terminal_state,
                                            }
    
    def step(self):
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
            self.load(self.task.__class__, self.task.id)
            self.step()
            time.sleep(10)
    
    def save(self):
        self.task.save()
    
    def load(self, id):
        self.task = self._load(id)
    
    @classmethod
    def _load(cls, id):
        return cls._cls_task.load(id)
    
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
