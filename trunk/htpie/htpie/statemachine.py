import htpie

from htpie.enginemodel import Task
from htpie.lib.exceptions import *
from htpie.lib import utils
from htpie import status
from htpie import states

from pymongo.objectid import ObjectId

import time
import datetime

__all__ = ['StateTypes', 'state', 'fromto', 'StateMachine']

def _default_state_handler(self):
    return True

def _thread_exec_fsm(fsm_class, id, log_verbosity):
    import htpie
    utils.configure_logger(log_verbosity)
    fsm = fsm_class()
    fsm.load(id)
    fsm.step()

#State Types
class StateTypes(object):
    MULTI = 'MULTI'
    ONCE = 'ONCE'

def state(state_name,  type=StateTypes.MULTI):
    def decorator(f):
        f.state = (state_name,  type)
        return f
    return decorator

def fromto(start_state, end_state):
    def decorator(f):
        f.transition = (start_state, end_state)
        return f
    return decorator

class RegisteringType(type):
    def __init__(cls, name, bases, attrs):
        cls.states = states.States()
        for key, val in attrs.iteritems():
            properties = getattr(val, 'state', None)
            if properties is not None:
                (state,  type) = properties
                cls.states.addstate(state, val, type)
        
        for key, val in attrs.iteritems():
            properties = getattr(val, 'transition', None)
            if properties is not None:
                (start_state, end_state) = properties
                cls.states.addtran(start_state, end_state, val)
        
        #Put a default loop state at the end of each transition.
        for state, type in cls.states.types.iteritems():
            if type == StateTypes.MULTI:
                cls.states.addtran(state, state, _default_state_handler)
            elif type == StateTypes.ONCE:
                pass
            else:
                assert False,  'Unkown Statetype'

class StateMachine(object):

    __metaclass__ = RegisteringType
    
    def __init__(self):
        self.task = None
    
    def step(self):
        htpie.log.debug('%s is processing %s %s'%(self.name, self.task.id, self.task.state))
        try:
            self.task.acquire()
        except AuthorizationException,  e:
            #Do nothing but log the error and return
            pass
        else:
            try:
                self.task.status = status.Status.RUNNING
                self.states.getstate_fun(self.task.state, self.handle_missing_state)(self)
                #Now go through the transitions
                _found = False
                transitions = self.states.gettran(self.task.state, [])
                for transition in transitions:
                    if transition[1](self):
                        self.task.setstate(*transition)
                        _found = True
                        break
                if not _found:
                    self.task.status = status.Status.ONCE
            except:
                self.task.status = status.Status.ERROR
                htpie.log.critical('%s errored while processing %s \n%s'%(self.__class__.__name__, self.task.id, utils.format_exception_info()))
            finally:
                self.task.last_exec_d = datetime.datetime.now()
                self.task.release()

    def run(self):
        while not self.done():
            if self.task.status == status.Staus.ERROR:
                break
            self.load(self.task.id)
            self.step()
            time.sleep(10)
    
    def save(self):
        self.task.save()
    
    def load(self, id):
        self.task = self.cls_task().objects.with_id(id)
    
    @property
    def  name(self):
        return self.__class__.__name__
    
    def handle_missing_state(self):
        raise UnhandledStateError('%s %s is in unhandled state %s'%(self.name, self.task.id, self.state))
