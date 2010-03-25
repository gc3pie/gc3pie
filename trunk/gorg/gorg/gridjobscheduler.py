import os
import sys
import tempfile
import glob
sys.path.append('/home/mmonroe/apps/gorg')
from gorg_site.gorg_site.model.gridrun import GridrunModel
from gorg_site.gorg_site.lib.mydb import Mydb
from gc3utils.gcli import Gcli

class GridjobScheduler(object):
    def __init__(self, db_name='gorg_site', db_url='http://127.0.0.1:5984', 
                 glic_location='/home/mmonroe/.gc3/config'):
        self.db=Mydb(db_name,db_url).cdb()
        self.gcli = Gcli(glic_location)
        self.view_status_runs = GridrunModel.view_by_status(self.db)
    
    def handle_ready_jobs(self):
        #Handle jobs that are ready to be submitted to the grid
        view_runs = self.view_status_runs[GridrunModel.POSSIBLE_STATUS['READY']]
        for a_run in view_runs:
            try:
                # Get the files that we need to generatea grid run
                f_list = a_run.attachments_to_files(self.db, a_run.files_to_run.keys())
#TODO: We handle multiple input files here, but gcli can not handle them
                assert len(f_list)==1,  'gcli.gsub does not handle multiple input files.'
                run_params = a_run.run_params
                f_dir=os.path.dirname( f_list.values()[0].name)
                result = self.gcli.gsub(job_local_dir=f_dir, input_file=f_list.values()[0].name, **run_params)
                a_run.gsub_unique_token = result[1]
                # TODO: Get the real status, the run may be waiting in the queue, not running
                a_run.status=GridrunModel.POSSIBLE_STATUS['RUNNING']
            except:
                a_run.gsub_message=formatExceptionInfo()
                a_run.status=GridrunModel.POSSIBLE_STATUS['ERROR']
            f_list.values()[0].close()
            a_run.commit(self.db)

    def handle_waiting_jobs(self):
        view_runs = self.view_status_runs[GridrunModel.POSSIBLE_STATUS['WAITING']]
        for a_run in view_runs:
            try:
                result = self.gcli.gstat(a_run.gsub_unique_token)
                a_run.status = result[1][0][1].split()[-1]
            except:
                a_run.gsub_message=formatExceptionInfo()
                a_run.status=GridrunModel.POSSIBLE_STATUS['ERROR']
            a_run.commit(self.db)

    def handle_running_jobs(self):
        view_runs = self.view_status_runs[GridrunModel.POSSIBLE_STATUS['RUNNING']]
        for a_run in view_runs:
            try:
                result = self.gcli.gstat(a_run.gsub_unique_token)
                a_run.status = result[1][0][1].split()[-1]
            except:
                a_run.gsub_message=formatExceptionInfo()
                a_run.status=GridrunModel.POSSIBLE_STATUS['ERROR']
            a_run.commit(self.db)

    def handle_finished_jobs(self):
        view_runs = self.view_status_runs[GridrunModel.POSSIBLE_STATUS['FINISHED']]
        for a_run in view_runs:
            try:
                # TODO: gget returns 0 when it works, what do I do when it doesn't work? Cann't it return DONE or something?
                token = a_run.gsub_unique_token
                result = self.gcli.gget(token)
                #job.status = result[1][0][1].split()[-1]
                output_files=glob.glob('%s/*.*'%a_run.gsub_unique_token)
                for f_name in output_files:
                    a_file = open(f_name, 'rb')
                    a_run = a_run.put_attachment(self.db, a_file, os.path.basename(a_file.name)) #os.path.splitext(a_file.name)[-1].lstrip('.')
                    a_run.status=GridrunModel.POSSIBLE_STATUS['DONE']
            except:
                a_run.gsub_message=formatExceptionInfo()
                a_run.status=GridrunModel.POSSIBLE_STATUS['ERROR']
            a_run.commit(self.db)
        
    def run(self):
        self.handle_ready_jobs()
        self.handle_waiting_jobs()
        self.handle_running_jobs()
        self.handle_finished_jobs()
    
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
    job_scheduler.run()
    print 'Done running gridjobscheduler.py'


if __name__ == "__main__":
    main()
    sys.exit()
