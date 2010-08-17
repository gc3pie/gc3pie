import htpie

from htpie.lib import utils
from htpie.lib.exceptions import *
from htpie import model
from htpie import statemachine

import sys

module_names = {'GSingle':'htpie.usertasks.gsingle',
                              'GHessian':'htpie.usertasks.ghessian',
                            }

fsm_classes = dict()
for node_name, node_class in module_names.items():
    __import__(node_class)
    fsm_classes[node_name] = (eval('sys.modules[node_class].%s'%(node_name)), 
                                                eval('sys.modules[node_class].%sStateMachine'%(node_name)))

class GControl(object):
    
    @staticmethod
    def kill(id):
        doc = model.Task.load(id)
        doc.kill()
        htpie.log.info('Task %s will be killed'%(id))
    
    @staticmethod
    def retry(id):
        doc = model.Task.load(id)
        doc.retry()
        htpie.log.info('Task %s will be retried'%(id))
    
#    def get_task_info(self):
#        sys.stdout.write('Info on Task %s\n'%(self.a_task.id))
#        sys.stdout.write('---------------\n')
#        job_list = self.a_task.children
#        sys.stdout.write('Total number of jobs    %d\n'%(len(job_list)))
#        sys.stdout.write('Overall Task status %s\n'%(self.a_task.status_counts))
#        for a_job in job_list:
#            job_done = False
#            sys.stdout.write('---------------\n')
#            sys.stdout.write('Job %s status %s\n'%(a_job.id, a_job.status))
#            sys.stdout.write('Run %s\n'%(a_job.run_id))
#            sys.stdout.write('gc3utils application obj\n')
#            sys.stdout.write('%s\n'%(a_job.run.application))
#            sys.stdout.write('gc3utils job obj\n')
#            sys.stdout.write('%s\n'%(a_job.run.job))
#            
#            job_done = a_job.wait(timeout=0)
#            if job_done:
#                a_result = self.calculator.parse(a_job)
#                if a_result.exit_successful():
#                    sys.stdout.write('Job exited successfully with energy %s\n'%(a_result.get_potential_energy()))
#                else:
#                    sys.stdout.write('Job did not exit successfully\n')
#
#    def get_task_files(self):
#        job_list = self.a_task.children
#        root_dir = 'tmp'
#        task_dir = '/%s/%s'%(root_dir, self.a_task.id)
#        if not os.path.isdir(task_dir):
#            os.mkdir(task_dir)
#        for a_job in job_list:
#            try:
#                f_list = a_job.attachments
#                sys.stdout.write('Job %s\n'%(a_job.id))
#                for a_file in f_list.values():
#                    shutil.copy2(a_file.name, '%s/%s_%s'%(task_dir, a_job.id, os.path.basename(a_file.name)))
#            finally:
#                map(file.close, f_list.values())
#        sys.stdout.write('Files in directory %s\n'%(task_dir))
