import htpie
from htpie.lib import utils
from htpie import enginemodel as model
from htpie.statemachine import *
from htpie.application import gamess
from htpie.lib.exceptions import *

from htpie.usertasks import glittle
 
_TASK_CLASS = 'GBig'
_STATEMACHINE_CLASS = 'GBigStateMachine'
 
class GBig(model.Task):
    num_children = model.IntField()
    count = model.IntField()
    children = model.ListField(model.ReferenceField(glittle.GLittle))

    def display(self, long_format=False):
        output = '%s %s %s %s\n'%(self.cls_name, self.id, self.state, self.status)
        output += 'Task submitted: %s\n'%(self.create_d)
        output += 'Task last ran: %s\n'%(self.last_exec_d)
        output += 'Delta: %s\n'%(self.last_exec_d - self.create_d)      
        
        output += 'Number of glittle\'s: %d\n'%(self.num_children)
        
        if long_format:
            for child in self.children:
                output += '%s %s %s %s\n'%(child.cls_name, child.id, child.state, child.status)
        return output
    
    def retry(self):
        super(GBig, self).retry()
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
            self.setstate(States.KILL, 'kill')
            self.release()
            htpie.log.debug('GBig %s will be killed'%(self.id))
            for child in self.children:
                try:
                    child.kill()
                except:
                    pass
    
    def successful(self):
        if self.state == States.POSTPROCESS:
            return True
    
    @classmethod
    def create(cls, num_little,  app_tag='gamess', requested_cores=2, requested_memory=2, requested_walltime=2):
        task = super(GBig, cls).create()
        
        for i in xrange(num_little):
            task.children.append(glittle.GLittle.create())
        task.num_children = num_little
        task.setstate(States.READY, 'init')
        task.save()        
        return task
    
    @staticmethod
    def cls_fsm():
        return eval(_STATEMACHINE_CLASS)

class States(object):
    READY = u'STATE_READY'
    WAITING = u'STATE_WAIT'
    POSTPROCESS = u'STATE_POSTPROCESS'
    KILL = 'STATE_KILL'

class GBigStateMachine(StateMachine):
    
    @state(States.READY)
    def handle_ready_state(self):
        pass
    
    @fromto(States.READY, States.WAITING)
    def handle_tran_ready(self):
        return True
    
    @state(States.WAITING)
    def handle_waiting_state(self):
        pass
    
    @fromto(States.WAITING, States.POSTPROCESS)
    def handle_tran_waiting(self):
        if self._wait_util_done():
            return True
    
    @state(States.POSTPROCESS, StateTypes.ONCE)
    def handle_postprocess_state(self):
        pass
    
    @state(States.KILL, StateTypes.ONCE)
    def handle_kill_state(self):
        return True

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
    
    @staticmethod
    def cls_task():
        return eval(_TASK_CLASS)
