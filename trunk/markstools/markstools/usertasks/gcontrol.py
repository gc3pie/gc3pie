'''
Created on Dec 28, 2009

@author: mmonroe
'''
from optparse import OptionParser
import markstools
import os
import sys
import shutil
import time

from gorg.model.gridtask import TaskInterface
from gorg.lib.utils import Mydb
import gorg.lib.exceptions
from gorg.gridjobscheduler import STATES as JOB_SCHEDULER_STATES

# The key is the name of the class where the usetask is programmed, and the value is the module location
module_names = {'GHessian':'markstools.usertasks.ghessian', 'GSingle':'markstools.usertasks.gsingle'}
usertask_classes = dict()
for usertask_name, usertask_module in module_names.items():
    __import__(usertask_module)
    usertask_classes[usertask_name] = eval('sys.modules[usertask_module].%s'%(usertask_name))


class GControl(object):
 
    def __init__(self):
        self.a_task = None
        self.calculator = None

    def initialize(self):
        pass
        
    def load(self, db,  task_id):
        self.a_task = TaskInterface(db).load(task_id)
        str_calc = self.a_task.user_data_dict['calculator']
        self.calculator = eval(str_calc + '(db)')
    
    def save(self):
        pass
       
    def kill_task(self):
        if not self.a_task.terminal:
            job_list = self.a_task.children
            for a_job in job_list:
                counter = 0
                sleep_amount = 5
                max_sleep_amount = 30
                while a_job.status.locked and counter < max_sleep_amount:
                    time.sleep(sleep_amount)
                    counter += sleep_amount
                    markstools.log.info('Waiting for Job %s to go into killable state.'%(a_job.id))
                    markstools.log.debug('In state %s'%(a_job.status))
                if a_job.status.locked:
                    raise DocumentError('Job %s can not be killed, it is in locked state %s'%(a_job.id, a_job.status))
                a_job.status = JOB_SCHEDULER_STATES.KILL
                a_job.store()
            self.a_task.terminal = usertask_modules[self.a_task.title].KILL
            self.a_task.store()
    
    def retry_task(self):
        if self.a_task.terminal:
            job_list = self.a_task.children
            for a_job in job_list:
                if a_job.status == JOB_SCHEDULER_STATES.ERROR:
                    markstools.log.info('Retrying Job %s.'%(a_job.id))
                    markstools.log.debug('Was in state %s now in state %s'%(a_job.status,  JOB_SCHEDULER_STATES.READY))
                    a_job.status = JOB_SCHEDULER_STATES.READY
                    a_job.store()
            self.a_task.terminal = usertask_modules[self.a_task.title].READY
            self.a_task.store()

def main(options):
    # Connect to the database
    db = Mydb('mark',options.db_name,options.db_url).cdb()

    gcontrol = GControl()
    gamess_calc = GamessGridCalc(db)
    gcontrol.load(db, options.task_id)
    gcontrol.run()

    print 'ginfo is done'


if __name__ == '__main__':
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-t", "--task_id", dest="task_id",  
                      help="Task ID to be queued.")
    parser.add_option("-v", "--verbose", action='count', dest="verbose", default=0, 
                      help="add more v's to increase log output.")
    parser.add_option("-n", "--db_name", dest="db_name", default='gorg_site', 
                      help="add more v's to increase log output.")
    parser.add_option("-l", "--db_url", dest="db_url", default='http://130.60.144.211:5984', 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()
    
    if options.task_id is None:
        print "A mandatory option is missing\n"
        parser.print_help()
        sys.exit(0)

    import logging
    from markstools.lib.utils import configure_logger
    logging.basicConfig(
        level=logging.ERROR, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
    
    configure_logger(10)
    #configure_logger(options.verbose)
    
    main(options)

    sys.exit(0)
