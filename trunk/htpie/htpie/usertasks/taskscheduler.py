import htpie
from htpie import enginemodel as model
from htpie import statemachine
from htpie.lib import utils
from htpie.usertasks.usertasks import *

class TaskScheduler(object):
    
    def handle_waiting_tasks(self):
        counter = 0
        model.Task.implicit_release()
        for node_name, the_classes in fsm_classes.items():
            node_class = the_classes[0]
            fsm_class = the_classes[1]
            fsm = fsm_class()
            task_name = fsm.name.replace('StateMachine', '')
            #to_process = node_class.doc().find({'transition':statemachine.Transitions.PAUSED, '_type': task_name})
            #If the task is not in a terminal state, then process it. We do this because a thread could crash before
            #setting the transition to PAUSE.
            avoid = statemachine.Transitions.terminal()
            avoid.append(statemachine.Transitions.HOLD)
            #We sort because we want to avoid gcontrol running into the scheduler and getting stuck behind it. By
            #reversing the sort, they should pass each other because they are moving through the tasks in different directions
            #q = model.Q(transition__nin = avoid, _lock='')
            #to_process = node_class.objects.filter(q).order_by('-__id')
            to_process = node_class.objects(transition__nin = avoid, _lock='').order_by('-__id').only('id')
            #to_process = node_class.objects._collection.find({'transition':{'$nin':avoid} , '_lock': u''}).sort('_id', pymongo.DESCENDING)
            htpie.log.info('%d %s task(s) are going to be processed'%(to_process.count(),  task_name))
            for a_node in to_process:
                counter += 1
                fsm.load(a_node.id)
                fsm.step()
        htpie.log.info('TaskScheduler has processed %s task(s)\n%s'%(counter, '-'*80))

    
    def run(self):
        try:
            self.handle_waiting_tasks()
        except:
            htpie.log.critical('%s errored\n%s'%(self.__class__.__name__, utils.format_exception_info()))
