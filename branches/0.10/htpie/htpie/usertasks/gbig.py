import htpie
from htpie.lib import utils
from htpie import enginemodel as model
from htpie import statemachine
from htpie.application import gamess
from htpie.lib.exceptions import *

from htpie.usertasks import glittle
 
class States(statemachine.States):
    READY = u'STATE_READY'
    WAITING = u'STATE_WAIT'
    POSTPROCESS = u'STATE_POSTPROCESS'

class Transitions(statemachine.Transitions):
    pass

class GBig(model.Task):
    num_children = model.IntField()
    children = model.ListField(model.ReferenceField(glittle.GLittle))
    
    def display(self, long_format=False):
        output = '%s %s %s %s\n'%(self.cls_name, self.id, self.state, self.transition)
        output += 'Task submitted: %s\n'%(self.create_d)
        output += 'Task last ran: %s\n'%(self.last_exec_d)
        output += 'Delta: %s\n'%(self.last_exec_d - self.create_d)      
        
        output += 'Number of glittle\'s: %d\n'%(self.num_children)
        
        if long_format:
            for child in self.children:
                output += '%s %s %s %s\n'%(child.cls_name, child.id, child.state, child.transition)
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
                for child in self.children:
                    try:
                        child.retry()
                    except:
                        pass
    
    def kill(self):
        try:
            self.acquire(120)
        except:
            raise
        else:
            self.state = States.KILL
            self.transition = Transitions.PAUSED
            self.release()
            htpie.log.debug('GBig %s will be killed'%(self.id))
            for child in self.children:
                try:
                    child.kill()
                except:
                    pass
    
    @classmethod
    def create(cls, num_little,  app_tag='gamess', requested_cores=2, requested_memory=2, requested_walltime=2):
        task = super(GBig, cls,).create()
        
        for i in xrange(num_little):
            task.children.append(glittle.GLittle.create())
        task.num_children = num_little
        task.state = States.READY
        task.transition = Transitions.PAUSED
        task.save()        
        return task

class GBigStateMachine(statemachine.StateMachine):
    _cls_task = GBig
    
    def __init__(self):
        super(GBigStateMachine, self).__init__()
        self.state_mapping.update({States.READY: self.handle_ready_state, 
                                                    States.WAITING: self.handle_waiting_state, 
                                                    States.POSTPROCESS: self.handle_postprocess_state, 
                                                    States.KILL: self.handle_kill_state, 
                                                    })
    
    def handle_ready_state(self):
        self.state = States.WAITING
    
    def handle_waiting_state(self):
        if self._wait_util_done():
            self.state = States.POSTPROCESS
    
    def handle_postprocess_state(self):
        self.state = States.COMPLETE
        return True
    
    def handle_kill_state(self):
        return True
    
    def handle_missing_state(self, a_run):
        raise UnhandledStateError('GBig %s is in unhandled state %s'%(self.task.id, self.state))

    def _wait_util_done(self):
        children = self.task.children
        count = 0
        for child in children:
            if child.done():
                if child.successful():
                    count += 1
                else:
                    raise ChildNodeException('Child task %s has been unsuccessful'%(child.id))
        
        if count == len(children):
            return True
        else:
            return False


        
            
    
    
