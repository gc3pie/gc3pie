import sys
import htpie
from htpie import model
from htpie import statemachine
from htpie.lib import utils
import time

import pymongo

module_names = {'GSingle':'htpie.usertasks.gsingle',
                              'GHessian':'htpie.usertasks.ghessian',
                              'GHessianTest':'htpie.usertasks.ghessiantest', 
                              'GString':'htpie.usertasks.gstring', 
                            }

fsm_classes = dict()
for node_name, node_class in module_names.items():
    __import__(node_class)
    fsm_classes[node_name] = (eval('sys.modules[node_class].%s'%(node_name)), 
                                                eval('sys.modules[node_class].%sStateMachine'%(node_name)))   

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
            to_process = node_class.doc().find({'transition':{'$nin':avoid} , '_type': task_name, '_lock': u''}).sort('_id', pymongo.DESCENDING)
            htpie.log.info('%d %s task(s) are going to be processed'%(to_process.count(),  task_name))
            for a_node in to_process:
                counter += 1
                htpie.log.debug('TaskScheduler is processing task %s'%(a_node.id))
                htpie.log.debug('%s is in state %s'%(a_node['_type'], a_node.state))
                fsm.load(a_node.id)
                fsm.step()
        htpie.log.info('TaskScheduler has processed %s task(s)\n%s'%(counter, '-'*80))

    
    def run(self):
        try:
            self.handle_waiting_tasks()
        except:
            htpie.log.critical('%s errored\n%s'%(self.__class__.__name__, utils.format_exception_info()))
