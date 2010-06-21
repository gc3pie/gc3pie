'''
Created on Dec 28, 2009

@author: mmonroe
'''
from optparse import OptionParser
import markstools
import os
import sys
import shutil

from markstools.io.gamess import ReadGamessInp, WriteGamessInp
from markstools.calculators.gamess.calculator import GamessGridCalc
from markstools.lib import utils

from gorg.model.gridtask import TaskInterface
from gorg.lib.utils import Mydb

class GInfo(object):
 
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

    def get_info(self):
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
    
    def get_files(self):
        job_list = self.a_task.children
        root_dir = 'tmp'
        task_dir = '/%s/%s'%(root_dir, self.a_task.id)
        os.mkdir(task_dir)
        for a_job in job_list:
            f_list = a_job.attachments
            job_dir = '%s/%s'%(task_dir, a_job.id)
            os.mkdir(job_dir)
            sys.stdout.write('Job %s\n'%(a_job.id))
            for a_file in f_list.values():
                a_file.close()
                shutil.copy2(a_file.name, job_dir)
        sys.stdout.write('Files in directory %s\n'%(task_dir))

def main():
    options = parse_options()
    logging(options)
    # Connect to the database
    db = Mydb('mark',options.db_name,options.db_url).cdb()

    ginfo = GInfo()
    gamess_calc = GamessGridCalc(db)
    ginfo.load(db, options.task_id)
    ginfo.get_files()
    print 'ginfo is done'

def logging(options):    
    import logging
    from markstools.lib.utils import configure_logger
    logging.basicConfig(
        level=logging.ERROR, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
        
    #configure_logger(options.verbose)
    configure_logger(10)
    import gorg.lib.utils
    gorg.lib.utils.configure_logger(10)

def parse_options():
    #Set up command line options
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-t", "--task_id", dest="task_id",  
                      help="Task ID to be queued.")
    parser.add_option("-v", "--verbose", action='count', dest="verbose", default=0, 
                      help="add more v's to increase log output.")
    parser.add_option("-n", "--db_name", dest="db_name", default='gorg_site', 
                      help="add more v's to increase log output.")
    parser.add_option("-l", "--db_url", dest="db_url", default='http://localhost:5984', 
                      help="add more v's to increase log output.")
    (options, args) = parser.parse_args()
    if options.task_id is None:
        print "A mandatory option is missing\n"
        parser.print_help()
        sys.exit(0)
    return options

if __name__ == '__main__':

    
    main()
    sys.exit(0)
