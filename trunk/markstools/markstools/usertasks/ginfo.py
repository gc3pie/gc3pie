'''
Created on Dec 28, 2009

@author: mmonroe
'''
from optparse import OptionParser
import markstools
import os
import sys
import numpy as np

from markstools.io.gamess import ReadGamessInp, WriteGamessInp
from markstools.calculators.gamess.calculator import GamessGridCalc
from markstools.lib import utils
from markstools.lib.status import State,  Status

from gorg.model.gridtask import TaskInterface
from gorg.lib.utils import Mydb


STATE_INFO = State('INFO', 'INFO desc')
STATE_GET_FILES = State('GET_FILES', 'GET_FILES desc')
STATE_ERROR = State('ERROR', 'ERROR desc', terminal = True)
STATE_COMPLETED = State('COMPLETED', 'CCOMPLETED desc', terminal = True)

STATES = Status([STATE_INFO, STATE_GET_FILES, STATE_ERROR, STATE_COMPLETED])

class GInfo(object):
 
    def __init__(self, init_status = STATES.INFO):
        self.status = init_status
        self.status_mapping = {STATES.INFO: self.handle_info_state, 
                                              STATES.GET_FILES: self.handle_get_files_state, 
                                              STATES.ERROR: self.handle_terminal_state, 
                                              STATES.COMPLETED: self.handle_terminal_state}
        
        self.a_task = None
        self.calculator = None

    def initialize(self):
        pass
        
    def load(self, db,  a_task):
        self.a_task = a_task
        str_calc = self.a_task.user_data_dict['calculator']
        self.calculator = eval(str_calc + '(db)')
    
    def save(self):
        pass
       
    def handle_info_state(self):
        sys.stdout.write('Info on Task %s\n'%(self.a_task.id))
        sys.stdout.write('---------------\n')
        job_list = self.a_task.children
        sys.stdout.write('Total number of jobs    %d\n'%(len(job_list)))
        sys.stdout.write('Overall Task status %s\n'%(self.a_task.status_overall))
        for a_job in job_list:
            job_done = False
            sys.stdout.write('---------------\n')
            sys.stdout.write('Job %s status %s\n'%(a_job.id, a_job.status))
            sys.stdout.write('Run %s\n'%(a_job.run_id))
            sys.stdout.write('gc3utils application obj\n')
            sys.stdout.write('%s\n'%(a_job.run.application))
            sys.stdout.write('gc3utils job obj\n')
            sys.stdout.write('%s\n'%(a_job.run.job))
            
            job_done = a_job.wait(timeout=0)
            if job_done:
                a_result = self.calculator.parse(a_job)
                sys.stdout.write('Exit status %s\n'%(a_result.exit_successful()))
        sys.stdout.flush()
        self.status = STATES.COMPLETED
    
    def handle_get_files_state(self):
        job_list = self.a_task.children
        for a_job in job_list:
            f_list = a_job.attachments
            map(file.close, f_list)
            sys.stdout('Job %s\n'%(a_job.id))
            for a_file in f_list:
                sys.stdout('Files %s\n'%(a_file.name ))
        self.status = STATES.COMPLETED
        
    def handle_terminal_state(self):
        print 'I do nothing!!'

    def handle_missing_state(self):
        print 'Do something when the state is not in our map.'
   
    def step(self):
        try:
            self.status_mapping.get(self.status, self.handle_missing_state)()
        except:
            self.status=STATES.ERROR
            markstools.log.critical('GHessian Errored while processing task %s \n%s'%(self.a_task.id, utils.format_exception_info()))
        self.save()
    
    def run(self):
        if not self.status.terminal:
            self.step()
        else:
            assert false,  'You are trying to step a terminated status.'
        while not self.status.pause and not self.status.terminal:
            self.step()


def main(options):
    # Connect to the database
    db = Mydb('mark',options.db_name,options.db_url).cdb()

    ginfo = GInfo()
    gamess_calc = GamessGridCalc(db)
    a_task = TaskInterface(db).load(options.task_id)
    ginfo.load(db, a_task)
    ginfo.run()

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
    
    import logging
    from markstools.lib.utils import configure_logger
    logging.basicConfig(
        level=logging.ERROR, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
        
    configure_logger(options.verbose)
    
    main(options)

    sys.exit()
