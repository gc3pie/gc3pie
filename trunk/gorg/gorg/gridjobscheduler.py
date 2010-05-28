import os
import sys
import tempfile
import glob
import gorg

from gorg.model.gridjob import GridrunModel, States
from gorg.lib.exceptions import *
from gorg.lib.utils import Mydb, configure_logger, formatExceptionInfo

from gc3utils import Job, Application, gcommands, utils, Exceptions
import gc3utils.Exceptions

TerminalStates = [Job.JOB_STATE_HOLD, Job.JOB_STATE_ERROR, Job.JOB_STATE_COMPLETED]

class GridjobScheduler(object):
    def __init__(self, couchdb_user = 'mark',  couchdb_database='gorg_site', couchdb_url='http://127.0.0.1:5984'):
        self.db=Mydb(couchdb_user, couchdb_database, couchdb_url).cdb()
        self.view_status_runs = GridrunModel.view_status(self.db)
        self._gcli = gcommands._get_gcli()
    
    def handle_ready_run(self, a_run):
        try:
            f_list = a_run.attachments_to_files(self.db, a_run.files_to_run.keys())
    #TODO: We handle multiple input files here, but gcli can not handle them
            if len(f_list)!=1:
                raise ValueError('gcli.gsub does not handle multiple input files.')
            for a_file in f_list.values():
                a_run.application['inputs'].append(a_file.name)
            a_run.job = self._gcli.gsub(a_run.application)
            utils.persist_job_filesystem(a_run.job)
            
            a_run.status = States.WAITING
        finally:
            map(file.close, f_list.values())
        return a_run

    def handle_waiting_run(self, a_run):
        a_run.job = self._gcli.gstat(a_run.job)[0]
        
        if a_run.job.status == Job.JOB_STATE_WAIT:
            pass
        elif a_run.job.status == Job.JOB_STATE_RUNNING:
            pass
        elif a_run.job.status == Job.JOB_STATE_FINISHED:            
            a_run.status = States.RETRIEVING
        elif a_run.job.status == Job.JOB_STATE_FAILED:
            a_run.status = States.ERROR
        
        utils.persist_job_filesystem(a_run.job)
        
        return a_run
 
    def handle_retrieving_run(self, a_run):        
        a_run.job = self._gcli.gget(a_run.job)
        utils.persist_job_filesystem(a_run.job)
        #TODO: fix me a bug
        #output_files = glob.glob('%s/%s/*.*'%(a_run.application.job_local_dir, a_run.job.unique_token))
        output_files = glob.glob('/home/mmonroe/%s/*.*'%(a_run.job.unique_token))
        for f_name in output_files:
            try:
                a_file = open(f_name, 'rb')
                a_run = a_run.put_attachment(self.db, a_file, os.path.basename(a_file.name)) #os.path.splitext(a_file.name)[-1].lstrip('.')
            finally:
                a_file.close()
        a_run.status = States.COMPLETED
        return a_run

    def handle_unreachable_run(self, a_run):
        #TODO: Notify the user that they need to log into the clusters again, maybe using email?
        a_run.status = States.NOTIFIED
        return a_run
    
    def handle_notified_run(self, a_run):
        try:
            a_run.job = self._gcli.gstat(a_run.job)[0]
            utils.persist_job_filesystem(a_run.job)
            a_run.status = States.WAITING
            
        except gc3utils.Exceptions.AuthenticationException:
            a_run.status = States.NOTIFIED
        return a_run
    
    def run(self):
        for a_state in States.all:
            if a_state not in States.terminal:
                view_runs = self.view_status_runs[a_state]
                for a_run in view_runs:
                    try:
                        if States.READY == a_state:
                            a_run = self.handle_ready_run(a_run)
                        elif States.WAITING == a_state:
                            a_run = self.handle_waiting_run(a_run)
                        elif States.RETRIEVING == a_state:
                            a_run = self.handle_retrieving_run(a_run)
                        elif States.UNREACHABLE == a_state:
                            a_run = self.handle_unreachable_run(a_run)
                        elif States.NOTIFIED == a_state:
                            a_run = self.handle_notified_run(a_run)
                        else:
                            raise UnhandledStateError('Run id %s is in unhandled state %s'%(a_run.id, a_run.status))
                    except gc3utils.Exceptions.AuthenticationException:
                        a_run.status = States.UNREACHABLE
                    except:
                        a_run.gsub_message=formatExceptionInfo()
                        a_run.status=States.ERROR
                        gorg.log.critical('GridjobScheduler Errored while processing run id %s \n%s'%(a_run.id, a_run.gsub_message))
                    a_run.commit(self.db)

def main():
    import logging
    logging.basicConfig(
        level=logging.ERROR, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
        
    configure_logger(10)
    job_scheduler = GridjobScheduler('mark','gorg_site','http://130.60.144.211:5984')
    job_scheduler.run()
    print 'Done running gridjobscheduler.py'

if __name__ == "__main__":
    main()
    sys.exit()
