import htpie

from htpie.enginemodel import Task
from htpie.lib.exceptions import *
from htpie.lib import utils
from htpie import status
from htpie import states

from pymongo.objectid import ObjectId

import time
import datetime

__all__ = ['StateTypes', 'StatePrint','state', 'transtate','fromto', 'StateMachine']

def TRUE(self):
    return True

def _default_tran_state(self):
    pass

def _thread_exec_fsm(fsm_class, id, log_verbosity):
    import htpie
    utils.configure_logger(log_verbosity)
    fsm = fsm_class()
    fsm.load(id)
    fsm.step()

class StateTypes(object):
    MULTI = 'MULTI' #Define a transition to itself
    ONCE = 'ONCE'    #Run the State once
    
class StatePrint(object):
    '''Define the colors and positions to use when diagramming the 
    state machine'''
    START = {'color':'red', 'shape':'box', 'group':'START'}
    NORMAL = {'color':'black', 'group':'NEXT'}
    COMPLETE = {'color':'black'}

def state(state_name,  type=StateTypes.MULTI, color=StatePrint.NORMAL):
    def decorator(f):
        f.state = (state_name,  type, color)
        return f
    return decorator

def transtate(start_state,  end_state, type=StateTypes.MULTI, color=StatePrint.NORMAL):
    '''This defines the state and transition at the same time. The state
    just 'passes' and the function this is decorating becomes the transtion.'''
    def decorator(f):
        f.transtate = (start_state, end_state, type,  color)
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
                (state,  type, color) = properties
                cls.states.addstate(state, val, type, color)

        for key, val in attrs.iteritems():
            properties = getattr(val, 'transtate', None)
            if properties is not None:
                (start_state, end_state, type, color) = properties
                cls.states.addstate(start_state, _default_tran_state, type, color)
                cls.states.addtran(start_state, end_state, val)

        for key, val in attrs.iteritems():
            properties = getattr(val, 'transition', None)
            if properties is not None:
                (start_state, end_state) = properties
                cls.states.addtran(start_state, end_state, val)
        
        #Put a default loop state at the end of each transition.
        for _state, _type in cls.states.types.iteritems():
            if _type == StateTypes.MULTI:
                cls.states.addtran(_state, _state, TRUE)
            elif _type == StateTypes.ONCE:
                pass
            elif _type:
                assert False,  'Unkown Statetype: %s'%(_type)

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
