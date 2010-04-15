from statemachine import StateMachine
from grestart import GRestart
from ghessian import GHessian

from optparse import OptionParser
import logging
import sys
sys.path.append('/home/mmonroe/apps/gorg')
from gorg_site.gorg_site.model.gridtask import GridtaskModel, TaskInterface
from gorg_site.gorg_site.lib.mydb import Mydb
from gc3utils.gcli import Gcli

class GridtaskScheduler(object):
    LOG_FILENAME = '/tmp/python_scheduler_logger.out'
    
    def __init__(self, db_username, db_name, db_url, logging_level):
        self.db=Mydb(db_username, db_name,db_url).cdb()
        self.view_state_tasks = GridtaskModel.view_by_state(self.db)
        self.logging_level = logging_level
        self.logger = self._create_logger(self.logging_level)
        
    def handle_waiting_tasks(self):
        task_list = self.view_state_tasks[StateMachine.stop_state()]
        self.logger.debug('%d tasks are going to be processed'%(len(task_list)))
        for raw_task in task_list:
            a_task = TaskInterface(self.db)
            a_task.task = raw_task
            fsm = eval(a_task.title + '(self.logging_level)')
            fsm.restart(self.db, a_task)
            state = fsm.run()
            a_task = fsm.save_state()
            self.logger.debug('Task %s has been processed and is now in state %s'%(a_task.id, state))
    
    def run(self):
        self.handle_waiting_tasks()
    
    def _create_logger(self, logging_level):
        import logging
        import logging.handlers
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging_level)
        file_handler = logging.handlers.RotatingFileHandler(
                  self.LOG_FILENAME, maxBytes=100000, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

def main(options):
    task_scheduler = GridtaskScheduler('mark',options.db_name,options.db_loc, options.logging_level)
    task_scheduler.run()
    print 'Done running gridtaskscheduler.py'
    
if __name__ == '__main__':
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", dest="verbose", default='', 
                      help="add more v's to increase log output.")
    parser.add_option("-n", "--db_name", dest="db_name", default='gorg_site', 
                      help="add more v's to increase log output.")
    parser.add_option("-l", "--db_loc", dest="db_loc", default='http://127.0.0.1:5984', 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()
    
    #Setup logger
    LOGGING_LEVELS = (logging.CRITICAL, logging.ERROR, 
                                    logging.WARNING, logging.INFO, logging.DEBUG)
    options.logging_level = len(LOGGING_LEVELS) if len(options.verbose) > len(LOGGING_LEVELS) else len(options.verbose)
    main(options)
    sys.exit()
