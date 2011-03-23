import htpie

from htpie import enginemodel as model
from htpie import statemachine
from htpie import status

from htpie.lib import utils
from htpie.usertasks.usertasks import *

import multiprocessing

# To turn on multi threading uncomment all -uc, and comment all -c

#-uc _taskscheduler_pool = multiprocessing.Pool(processes=5)

class TaskScheduler(object):
    
    def handle_waiting_tasks(self):
        counter = 0
        model.Task.implicit_release()
        for task_name, the_classes in fsm_classes.items():
            task_class, fsm_class = the_classes
            #If the task is not in a terminal state, then process it. We do this because a thread could crash before
            #setting the transition to PAUSE.
            avoid = status.Status.terminal()
            #We sort because we want to avoid gcontrol running into the scheduler and getting stuck behind it. By
            #reversing the sort, they should pass each other because they are moving through the tasks in different directions
            to_process = task_class.objects(_status__nin = avoid, _lock='').order_by('-__id').only('id')
            htpie.log.info('%d %s task(s) are going to be processed'%(to_process.count(),  task_class().cls_name))
            for a_task in to_process:
                counter += 1
                # -c below line
                statemachine._thread_exec_fsm(fsm_class, str(a_task.id), htpie.log.level)
                #-uc _taskscheduler_pool.apply_async(statemachine._thread_exec_fsm, (fsm_class, str(a_task.id), htpie.log.level))
        #-uc _taskscheduler_pool.close()
        #-uc _taskscheduler_pool.join()
        htpie.log.info('TaskScheduler has processed %s task(s)\n%s'%(counter, '-'*80))

    def run(self):
        try:
            self.handle_waiting_tasks()
        except:
            htpie.log.critical('%s errored\n%s'%(self.__class__.__name__, utils.format_exception_info()))
