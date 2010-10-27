import htpie
from htpie.lib import utils
from htpie import enginemodel as model
from htpie.statemachine import *
from htpie.application import gamess
from htpie.lib.exceptions import *
 
import sys

_STATEMACHINE_CLASS = 'GLittleStateMachine'
_TASK_CLASS = 'GLittle'

class States(object):
    READY = u'STATE_READY'
    WAITING = u'STATE_WAIT'
    POSTPROCESS = u'STATE_POSTPROCESS'
    KILL = 'STATE_KILL'

class GLittle(model.Task):
    count = model.IntField(default = 0)
    obj = model.PickleField()
    
    def display(self, long_format=False):
        output = '%s %s %s %s\n'%(self.cls_name, self.id, self.state, self.status)
        output += 'Task submitted: %s\n'%(self.create_d)
        output += 'Task last ran: %s\n'%(self.last_exec_d)
        output += 'Delta: %s\n'%(self.last_exec_d - self.create_d)      
        output += 'WAIT State count: %d\n'%(self.count)
        obj_size = sys.getsizeof(self.obj.pickle)
        obj_size = obj_size / 1048576.0
        output += 'Fake obj size in MB: %f\n'%(obj_size)
        if long_format:
            pass
        return output
    
    def kill(self):
        try:
            self.acquire(120)
        except:
            raise
        else:
            self.setstate(States.KILL, 'kill')
            self.release()

    def successful(self):
        if self.state == States.POSTPROCESS:
            return True
    
    @classmethod
    def create(cls, app_tag='gamess', requested_cores=2, requested_memory=2, requested_walltime=2):
        task = super(GLittle, cls,).create()
        
        task.obj = model.PickleProxy()
        task.obj.pickle = ''
        task.setstate(States.READY, 'init')
        task.save()      
        return task
    
    @staticmethod
    def cls_fsm():
        return eval(_STATEMACHINE_CLASS)

class GLittleStateMachine(StateMachine):

    @state(States.READY)
    def handle_ready_state(self):
        pass
    
    @fromto(States.READY, States.WAITING)
    def handle_tran_ready(self):
        return True
    
    @state(States.WAITING)
    def handle_waiting_state(self):
        self.task.count += 1
        obj = self.task.obj.pickle
        # We will add 1 megabyte to obj each time we
        # run this state
        obj += 'T'*1048576
        self.task.obj.pickle = obj
    
    @fromto(States.WAITING, States.POSTPROCESS)
    def handle_tran_waiting(self):
        if self.task.count > 10:
            return True
    
    @state(States.POSTPROCESS, StateTypes.ONCE)
    def handle_postprocess_state(self):
        return True
    
    @state(States.KILL, StateTypes.ONCE)
    def handle_kill_state(self):
        return True
    
    @staticmethod
    def cls_task():
        return eval(_TASK_CLASS)
