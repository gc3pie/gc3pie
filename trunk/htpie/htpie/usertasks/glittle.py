import htpie
from htpie.lib import utils
from htpie import enginemodel as model
from htpie import statemachine
from htpie.application import gamess
from htpie.lib.exceptions import *
 
class States(statemachine.States):
    READY = u'STATE_READY'
    WAITING = u'STATE_WAIT'
    POSTPROCESS = u'STATE_POSTPROCESS'

class Transitions(statemachine.Transitions):
    pass

class GLittle(model.Task):
    count = model.IntField(default = 0)
    obj = model.PickleField()

    def display(self, long_format=False):
        output = '%s %s %s %s\n'%(self.cls_name, self.id, self.state, self.transition)
        output += 'Task submitted: %s\n'%(self.create_d)
        output += 'Task last ran: %s\n'%(self.last_exec_d)
        output += 'Delta: %s\n'%(self.last_exec_d - self.create_d)      
        output += 'WAIT State count: %d\n'%(self.count)
        
        if long_format:
            pass
        return output
    
    def retry(self):
        if self.transition == Transitions.ERROR:
            try:
                self.acquire(120)
            except:
                raise
            else:
                self.transition = Transitions.PAUSED
                self.release()
    
    def kill(self):
        try:
            self.acquire(120)
        except:
            raise
        else:
            self.state = States.KILL
            self.transition = Transitions.PAUSED
            self.release()
            #htpie.log.debug('GLittle %s will be killed'%(self.id))
    
    @classmethod
    def create(cls, app_tag='gamess', requested_cores=2, requested_memory=2, requested_walltime=2):
        task = super(GLittle, cls,).create()
        
        task.obj = model.PickleProxy()
        task.obj.pickle = ''
        task.state = States.READY
        task.transition = Transitions.PAUSED
        task.save()      
        return task

class GLittleStateMachine(statemachine.StateMachine):
    _cls_task = GLittle
    
    def __init__(self):
        super(GLittleStateMachine, self).__init__()
        self.state_mapping.update({States.READY: self.handle_ready_state, 
                                                    States.WAITING: self.handle_waiting_state, 
                                                    States.POSTPROCESS: self.handle_postprocess_state, 
                                                    States.KILL: self.handle_kill_state, 
                                                    })
    
    def handle_ready_state(self):
        self.state = States.WAITING
    
    def handle_waiting_state(self):
        self.task.count += 1
        obj = self.task.pickle
        # We will add 1 megabyte to obj each time we
        # run this state
        obj += 'T'*1048576
        self.task.pickle = obj
        if self.task.count > 10:
            self.state = States.POSTPROCESS
    
    def handle_postprocess_state(self):
        self.state = States.COMPLETE
        return True
    
    def handle_kill_state(self):
        return True
    
    def handle_missing_state(self, a_run):
        raise UnhandledStateError('GLittle %s is in unhandled state %s'%(self.task.id, self.state))
