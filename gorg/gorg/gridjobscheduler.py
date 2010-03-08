import os
import tempfile
import glob

from gorg_site.gorg_site.model.gridjob import GridjobModel
from gorg_site.gorg_site.model.gridtask import GridtaskModel
from gorg_site.gorg_site.lib.mydb import Mydb
from ..gc3utils.gc3utils.gcli import Gcli

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
    db=Mydb('gorg_site','http://127.0.0.1:5984').cdb()
    gcli = Gcli()
    #Handle jobs that are ready to be submitted to the grid
    job_view_by_status=GridjobModel.view(db, 'by_status', key='READY')
    for job in job_view_by_status:
        try:
            # Pass the run_params dictionary as a keyword list to the function
            myfile = job.get_attachment(db, 'input')
            temp_input = tempfile.NamedTemporaryFile(delete=False)
            temp_input.write(myfile.read())
            temp_input.close()
            job.gsub_unique_token = gcli.gsub(**job.run_params, job_local_dir=temp_input.gettempdir(), input_file=temp_input.name)
            # TODO: Get the real status, the job may be waiting in the queue, not running
            job.status='RUNNING'
        except:
            job.gsub_message=formatExceptionInfo()
        job.store(db)

    job_view_by_status=GridjobModel.view(db, 'by_status', key='WAITING')
    for job in job_view_by_status:
        try:
            job.status = gcli.gstat(job.gsub_unique_token)            
        except:
            job.gsub_message=formatExceptionInfo()
        job.store(db)
            
    job_view_by_status=GridjobModel.view(db, 'by_status', key='RUNNING')
    for job in job_view_by_status:
        try:
            job.status = gcli.gstat(job.gsub_unique_token)           
        except:
            job.gsub_message=formatExceptionInfo()
        job.store(db)

    job_view_by_status=GridjobModel.view(db, 'by_status', key='FINISHED')
    for job in job_view_by_status:
        try:
            job.status = gcli.gget(job.gsub_unique_token)
            output_files=glob.glob('%s/*.*'%job.gsub_unique_token)[0]
            for file_path in output_files:
                myfile = open(file_path)
                job.put_attachment(db, myfile, myfile.name)
        except:
            job.gsub_message=formatExceptionInfo()
        job.store(db)

if __name__ == "__main__":
    main()
