import markstools

from optparse import OptionParser
import sys
from gorg.model.gridtask import GridtaskModel, TaskInterface
from gorg.lib.utils import Mydb
from gc3utils.gcli import Gcli

# The key is the name of the class where the usetask is programmed, and the value is the module location
module_names = {'GHessian':'markstools.usertasks.ghessian', 
                              'GSingle':'markstools.usertasks.gsingle', 
                              'GRestart':'markstools.usertasks.grestart'}

usertask_classes = dict()
for usertask_name, usertask_module in module_names.items():
    __import__(usertask_module)
    usertask_classes[usertask_name] = eval('sys.modules[usertask_module].%s'%(usertask_name))

class TaskScheduler(object):
    
    def __init__(self, db_username, db_name, db_url):
        self.db=Mydb(db_username, db_name,db_url).cdb()
        
    def handle_waiting_tasks(self):
        task_counter = 0
        for usertask_name, usertask_class in usertask_classes.items():
            for a_state in usertask_class.STATES.values():
                if a_state not in usertask_class.STATES.terminal:
                    task_list = GridtaskModel.view_status(self.db, key=[usertask_name, a_state])
                    markstools.log.debug('%d %s task(s) in state %s are going to be processed'%(len(task_list), usertask_name, a_state))
                    for raw_task in task_list:
                        task_counter += 1
                        markstools.log.debug('TaskScheduler is processing task %s'%(raw_task.id))
                        usertask = usertask_class()
                        usertask.load(self.db, raw_task.id)
                        usertask.step()
        markstools.log.info('TaskScheduler has processed %s task(s)'%(task_counter))

    def run(self):
        self.handle_waiting_tasks()
