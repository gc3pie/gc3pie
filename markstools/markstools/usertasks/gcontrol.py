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
from markstools.calculators.gamess.calculator import GamessGridCalc

# The key is the name of the class where the usetask is programmed, and the value is the module location
module_names = {'GHessian':'markstools.usertasks.ghessian', 
                              'GSingle':'markstools.usertasks.gsingle', 
                              'GRestart':'markstools.usertasks.grestart'}

usertask_classes = dict()
for usertask_name, usertask_module in module_names.items():
    __import__(usertask_module)
    usertask_classes[usertask_name] = eval('sys.modules[usertask_module].%s'%(usertask_name))

class GControl(object):
 
    def __init__(self, db_username, db_name, db_url,  task_id):
        db=Mydb(db_username, db_name,db_url).cdb()
        self.a_task = TaskInterface(db).load(task_id)
        self.cls_task = usertask_classes[self.a_task.title]
        str_calc = self.a_task.user_data_dict['calculator']
        self.calculator = eval(str_calc + '(db)')

    def kill_task(self):
        if not self.a_task.status.terminal:
            self.a_task.status = self.cls_task.STATES.KILL
            self.a_task.store()
    
    def retry_task(self):
        if self.a_task.status.terminal:
            self.a_task.status = self.cls_task.STATES.RETRY
            self.a_task.store()
    
    def get_task_info(self):
        sys.stdout.write('Info on Task %s\n'%(self.a_task.id))
        sys.stdout.write('---------------\n')
        job_list = self.a_task.children
        sys.stdout.write('Total number of jobs    %d\n'%(len(job_list)))
        sys.stdout.write('Overall Task status %s\n'%(self.a_task.status_counts))
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
                if a_result.exit_successful():
                    sys.stdout.write('Job exited successfully with energy %s\n'%(a_result.get_potential_energy()))
                else:
                    sys.stdout.write('Job did not exit successfully\n')

    
    def get_task_files(self):
        job_list = self.a_task.children
        root_dir = 'tmp'
        task_dir = '/%s/%s'%(root_dir, self.a_task.id)
        if not os.path.isdir(task_dir):
            os.mkdir(task_dir)
        for a_job in job_list:
            try:
                f_list = a_job.attachments
                sys.stdout.write('Job %s\n'%(a_job.id))
                for a_file in f_list.values():
                    shutil.copy2(a_file.name, '%s/%s_%s'%(task_dir, a_job.id, os.path.basename(a_file.name)))
            finally:
                map(file.close, f_list.values())
        sys.stdout.write('Files in directory %s\n'%(task_dir))
