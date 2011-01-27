import htpie
from htpie.lib import utils
from htpie import enginemodel as model
from htpie.statemachine import *
from htpie.application import gamess
from htpie.lib.exceptions import *

from htpie.usertasks import glittle
 
_TASK_CLASS = 'GRecursion'
_STATEMACHINE_CLASS = 'GRecursionStateMachine'
 
class GRecursion(model.Task):
    num_children = model.IntField(default=0)
    level = model.IntField()
    children = model.ListField(model.ReferenceField('GRecursion'))

    def display(self, long_format=False):
        output = '%s %s %s %s\n'%(self.cls_name, self.id, self.state, self.status)
        output += 'Task submitted: %s\n'%(self.create_d)
        output += 'Task last ran: %s\n'%(self.last_exec_d)
        output += 'Delta: %s\n'%(self.last_exec_d - self.create_d)      
        output += 'Level: %d\n'%(self.level) 
        output += 'Number of children: %d\n'%(self.num_children)
        
        if long_format:
            output += '-'*80+'\n'
            for child in self.children:
                output += '%s %s %s %s %d\n'%(child.cls_name, child.id, child.state, child.status, child.level)
        return output
    
    def retry(self):
        super(GRecursion, self).retry()
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
            htpie.log.debug('GRecursion %s will be killed'%(self.id))
            for child in self.children:
                try:
                    child.kill()
                except:
                    pass
    
    def successful(self):
        if self.state == States.POSTPROCESS:
            return True
    
    @classmethod
    def create(cls, levels, num_children):
        task = super(GRecursion, cls).create()
        task.level = levels
        levels -= 1
        if levels >= 0: 
            for i in xrange(num_children):
                task.children.append(GRecursion.create(levels, num_children))
            task.num_children = num_children
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

class GRecursionStateMachine(StateMachine):
    
    @transtate(States.READY,  States.WAITING, color=StatePrint.START)
    def handle_tran_ready_waiting(self):
        if self._wait_util_done():
            return True    
    
    @transtate(States.WAITING, States.POSTPROCESS, type=StateTypes.ONCE)
    def handle_tran_waiting_postprocess(self):
        return True

    @state(States.POSTPROCESS, StateTypes.ONCE)
    def handle_postprocess_state(self):
        pass
    
    @state(States.KILL, StateTypes.ONCE, color=StatePrint.START)
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
