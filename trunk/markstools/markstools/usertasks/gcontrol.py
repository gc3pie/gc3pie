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

from markstools.io.gamess import ReadGamessInp, WriteGamessInp
from markstools.calculators.gamess.calculator import GamessGridCalc
from markstools.lib import utils
from markstools.lib.status import State,  Status

from gorg.model.gridtask import TaskInterface
from gorg.lib.utils import Mydb
from gorg.gridjobscheduler import STATE_KILL as JOB_KILL
from gorg.gridjobscheduler import STATE_READY as JOB_READY
from gorg.gridjobscheduler import STATE_ERROR as JOB_ERROR



STATE_KILL = State('KILL', 'KILL desc')
STATE_RETRY = State('RETRY', 'RETRY desc')
STATE_ERROR = State('ERROR', 'ERROR desc', terminal = True)
STATE_COMPLETED = State('COMPLETED', 'CCOMPLETED desc', terminal = True)

STATES = Status([STATE_RETRY, STATE_KILL, STATE_ERROR, STATE_COMPLETED])

class GControl(object):
 
    def __init__(self, init_status = STATES.RETRY):
        self.status = init_status
        self.status_mapping = {STATES.KILL: self.handle_kill_state, 
                                              STATES.RETRY: self.handle_retry_state, 
                                              STATES.ERROR: self.handle_terminal_state, 
                                              STATES.COMPLETED: self.handle_terminal_state}
        
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
       
    def handle_kill_state(self):
        job_list = self.a_task.children
        for a_job in job_list:
            if not a_job.status.terminal:
                while not a_job.status.pause:
                    time.sleep(5)
                    markstools.log.info('Waiting for Job %s to go into killable state.'%(a_job.id))
                    markstools.log.debug('In state %s'%(a_job.status))
                a_job.status = JOB_KILL
                a_job.store()
        self.status = STATES.COMPLETED
    
    def handle_retry_state(self):
        job_list = self.a_task.children
        for a_job in job_list:
            if a_job.status.terminal == JOB_ERROR:
                markstools.log.info('Retrying Job %s.'%(a_job.id))
                markstools.log.debug('Was in state %s now in state %s'%(a_job.status, JOB_READY))
                a_job.status = JOB_READY
                a_job.store()
        self.status = STATES.COMPLETED
    
    def handle_terminal_state(self):
        print 'I do nothing!!'

    def handle_missing_state(self):
        print 'Do something when the state is not in our map.'
   
    def step(self):
        try:
            self.status_mapping.get(self.status, self.handle_missing_state)()
        except:
            self.status = STATES.ERROR
            markstools.log.critical('Errored while processing task %s \n%s'%(self.a_task.id, utils.format_exception_info()))
        self.save()
    
    def run(self):
        while not self.status.terminal:
            self.step()
            if self.status.pause:
                break


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
