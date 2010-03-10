import os
import sys
import tempfile
import glob
sys.path.append('/home/mmonroe/apps/gorg')
from gorg_site.gorg_site.model.gridjob import GridjobModel
from gorg_site.gorg_site.model.gridtask import GridtaskModel
from gorg_site.gorg_site.lib.mydb import Mydb
from gc3utils.gcli import Gcli

class GridjobScheduler(object):
    INPUT_FILE_ATTACHMENT_NAME = 'input_file'
    INPUT_FILE_EXT = '.inp'
   
    def __init__(self, db_name='gorg_site', db_url='http://127.0.0.1:5984', 
                 glic_location='/home/mmonroe/.gc3/config'):
        self.db=Mydb(db_name,db_url).cdb()
        self.gcli = Gcli(glic_location)
    
    def handle_ready_jobs(self):
        #Handle jobs that are ready to be submitted to the grid
        job_view_by_status=GridjobModel.view(self.db, 'by_status', key='READY')
        for job in job_view_by_status:
            try:
                # Pass the run_params dictionary as a keyword list to the function            
                myfile = job.get_attachment(self.db, self.INPUT_FILE_ATTACHMENT_NAME)
                temp_input = tempfile.NamedTemporaryFile(suffix=self.INPUT_FILE_EXT)
                temp_input.write(myfile)
                temp_input.flush()
                run_params = job.run_params
                result = self.gcli.gsub(job_local_dir=tempfile.gettempdir(), input_file=temp_input.name, **run_params)
                job.gsub_unique_token = result[1]
                # TODO: Get the real status, the job may be waiting in the queue, not running
                job.status='RUNNING'
            except:
                job.gsub_message=formatExceptionInfo()
                job.status='ERROR'
            job.store(self.db)

    def handle_waiting_jobs(self):
        job_view_by_status=GridjobModel.view(self.db, 'by_status', key='WAITING')
        for job in job_view_by_status:
            try:
                result = self.gcli.gstat(job.gsub_unique_token)
                job.status = result[1][0][1].split()[-1]
            except:
                job.gsub_message=formatExceptionInfo()
                job.status='ERROR'
            job.store(self.db)

    def handle_running_jobs(self):
        job_view_by_status=GridjobModel.view(self.db, 'by_status', key='RUNNING')
        for job in job_view_by_status:
            try:
                result = self.gcli.gstat(job.gsub_unique_token)
                job.status = result[1][0][1].split()[-1]
            except:
                job.gsub_message=formatExceptionInfo()
                job.status='ERROR'
            job.store(self.db)

    def handle_finished_jobs(self):
        job_view_by_status=GridjobModel.view(self.db, 'by_status', key='FINISHED')
        for job in job_view_by_status:
            try:
                # TODO: gget returns 0 when it works, what do I do when it doesn't work? Cann't it return DONE or something?
                token = job.gsub_unique_token
                result = self.gcli.gget(token)
                #job.status = result[1][0][1].split()[-1]
                output_files=glob.glob('%s/*.*'%job.gsub_unique_token)
                for file_path in output_files:
                    myfile = open(file_path, 'rb')
                    job = job.put_attachment(self.db, myfile, os.path.splitext(myfile.name)[-1].lstrip('.'))
                    job.status='DONE'
            except:
                job.gsub_message=formatExceptionInfo()
                job.status='ERROR'
            job.store(self.db)
    
def formatExceptionInfo(maxTBlevel=5):
    '''Make the exception output pretty'''
    import traceback
    import sys

    cla, exc, trbk = sys.exc_info()
    excName = cla.__name__
    try:
        excArgs = exc.__dict__["args"]
    except KeyError:
        excArgs = "<no args>"
    excTb = traceback.format_tb(trbk, maxTBlevel)
    return (excName, excArgs, excTb)
        
def main():
    job_scheduler = GridjobScheduler()
    job_scheduler.handle_ready_jobs()
    job_scheduler.handle_waiting_jobs()
    job_scheduler.handle_running_jobs()
    job_scheduler.handle_finished_jobs()
    print 'Done running gridjobscheduler.py'


if __name__ == "__main__":
    main()
    sys.exit()
