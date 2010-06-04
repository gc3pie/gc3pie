import markstools

from optparse import OptionParser
import sys
from gorg.model.gridtask import GridtaskModel, TaskInterface
from gorg.lib.utils import Mydb
from gc3utils.gcli import Gcli

# The key is the name of the class where the usetask is programmed, and the value is the module location
module_names = {'GHessian':'markstools.usertasks.ghessian', 'GSingle':'markstools.usertasks.gsingle'}
usertask_modules = dict()
for usertask_name, usertask_module in module_names.items():
    __import__(usertask_module)
    usertask_modules[usertask_name] = sys.modules[usertask_module]

class TaskScheduler(object):
    
    def __init__(self, db_username, db_name, db_url):
        self.db=Mydb(db_username, db_name,db_url).cdb()
        
    def handle_waiting_tasks(self):
        for usertask_name, usertask_module in usertask_modules:
            task_list = GridtaskModel.view_status(self.db, keys = [usertask_name,  map(str, usertask_module.STATES.pause)])
            markstools.log.debug('%d %s task(s) are going to be processed'%(len(task_list), usertask_name))
            for raw_task in task_list:
                a_task = TaskInterface(self.db)
                a_task.task = raw_task
                exec('usertask = usertask_module.%s()'%(usertask_name))
                usertask.load(self.db, a_task)
                usertask.step()
                markstools.log.debug('Task %s has been processed and is now in state %s'%(a_task.id, state))
        
    def run(self):
        self.handle_waiting_tasks()

def main(options):
    task_scheduler = TaskScheduler('mark',options.db_name,options.db_loc)
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
