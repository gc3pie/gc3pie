from markstools.lib.statemachine import StateMachine
from grestart import GRestart
from ghessian import GHessian
import markstools

from optparse import OptionParser
import sys
from gorg.model.gridtask import GridtaskModel, TaskInterface
from gorg.lib.utils import Mydb
from gc3utils.gcli import Gcli


class GridtaskScheduler(object):
    
    def __init__(self, db_username, db_name, db_url):
        self.db=Mydb(db_username, db_name,db_url).cdb()
        self.view_state_tasks = GridtaskModel.view_state(self.db)
        
    def handle_waiting_tasks(self):
        task_list = self.view_state_tasks[StateMachine.stop_state()]
        markstools.log.debug('%d tasks are going to be processed'%(len(task_list)))
        for raw_task in task_list:
            a_task = TaskInterface(self.db)
            a_task.task = raw_task
            fsm = eval(a_task.title + '()')
            fsm.restart(self.db, a_task)
            state = fsm.run()
            a_task = fsm.save_state()
            markstools.log.debug('Task %s has been processed and is now in state %s'%(a_task.id, state))
    
    def run(self):
        self.handle_waiting_tasks()

def main(options):
    task_scheduler = GridtaskScheduler('mark',options.db_name,options.db_loc)
    task_scheduler.run()
    print 'Done running gridtaskscheduler.py'
    
if __name__ == '__main__':
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", action='count',dest="verbose", default=0, 
                      help="add more v's to increase log output.")
    parser.add_option("-n", "--db_name", dest="db_name", default='gorg_site', 
                      help="add more v's to increase log output.")
    parser.add_option("-l", "--db_loc", dest="db_loc", default='http://130.60.144.211:5984', 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()
    
    import logging
    from markstools.lib.utils import configure_logger
    logging.basicConfig(
        level=logging.ERROR, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
        
    configure_logger(options.verbose)
    
    main(options)
    sys.exit()
