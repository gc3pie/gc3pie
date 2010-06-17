import os
import sys
import tempfile
import glob
import gorg

from gorg.lib.exceptions import *
from gorg.lib.utils import Mydb, configure_logger, formatExceptionInfo
from gorg.lib import *
from gc3utils import Job, Application, gcommands, utils, Exceptions
import gc3utils.Exceptions
from gorg.lib import state
from couchdb import http


STATE_WAITING = state.State.create('WAITING', 'WAITING desc', pause = True)
STATE_RETRIEVING = state.State.create('RETRIEVING', 'RETRIEVING desc')
STATE_UNREACHABLE = state.State.create('UNREACHABLE', 'UNREACHABLE desc')
STATE_NOTIFIED = state.State.create('NOTIFIED', 'NOTIFIED desc')
STATE_TOPARSE = state.State.create('TOPARSE', 'TOPARSE desc', terminal = True)

STATES = state.StateContainer( [state.DEFAULT_HOLD, state.DEFAULT_READY, 
                                                    STATE_WAITING, STATE_RETRIEVING,  
                                                    STATE_UNREACHABLE, STATE_NOTIFIED, 
                                                    state.DEFAULT_ERROR, state.DEFAULT_COMPLETED, 
                                                    STATE_TOPARSE, state.DEFAULT_KILL])

class GridjobScheduler(object):
    def __init__(self, couchdb_user = 'mark',  couchdb_database='gorg_site', couchdb_url='http://127.0.0.1:5984'):
        from gorg.model.gridjob import GridrunModel
        self.db=Mydb(couchdb_user, couchdb_database, couchdb_url).cdb()
        self.view_status_runs = GridrunModel.view_status(self.db)
        self._gcli = gcommands._get_gcli()
        self.status_mapping =   {STATES.READY: self.handle_ready_state, 
                                                STATES.WAITING: self.handle_waiting_state, 
                                                STATES.RETRIEVING: self.handle_retrieving_state, 
                                                STATES.UNREACHABLE: self.handle_unreachable_state, 
                                                STATES.NOTIFIED: self.handle_notified_state, 
                                                STATES.KILL: self.handle_kill_state}
    
    def handle_ready_state(self, a_run):
#TODO: If we get an error storing the run, we will have submitted it to the grid
# but that info will not have been stored in the database
        for a_file in a_run.files_to_run:
            try:
                f_open = a_run.get_attachment(a_file)
                a_run.application['inputs'].append(f_open.name)
            finally:
                f_open.close()
        a_run.job = self._gcli.gsub(a_run.application)
        utils.persist_job_filesystem(a_run.job)
        gorg.log.info('Submitted run %s to the grid'%(a_run.id))
        a_run.status = STATES.WAITING
        return a_run

    def handle_waiting_state(self, a_run):
        a_run.job = self._gcli.gstat(a_run.job)[0]
        
        if a_run.job.status == Job.JOB_STATE_SUBMITTED:
            a_run.status = STATES.WAITING
        elif a_run.job.status == Job.JOB_STATE_RUNNING:
            a_run.status = STATES.WAITING
        elif a_run.job.status == Job.JOB_STATE_FINISHED:            
            a_run.status = STATES.RETRIEVING
        elif a_run.job.status == Job.JOB_STATE_FAILED or \
              a_run.job.status == Job.JOB_STATE_DELETED or \
              a_run.job.status == Job.JOB_STATE_UNKNOWN:
            a_run.status = STATES.ERROR
        
        utils.persist_job_filesystem(a_run.job)
        gorg.log.debug('Run %s status is now %s'%(a_run.id, a_run.status))
        return a_run
 
    def handle_retrieving_state(self, a_run):        
        a_run.job = self._gcli.gget(a_run.job)
        utils.persist_job_filesystem(a_run.job)
        output_files = glob.glob('%s/%s/*.*'%(a_run.application.job_local_dir, a_run.job.unique_token))
        for f_name in output_files:
            try:
                a_file = open(f_name, 'rb')
                a_run.put_attachment(a_file, os.path.basename(a_file.name)) #os.path.splitext(a_file.name)[-1].lstrip('.')
            finally:
                a_file.close()
        a_run.status = STATES.TOPARSE
        return a_run

    def handle_unreachable_state(self, a_run):
        #TODO: Notify the user that they need to log into the clusters again, maybe using email?
        a_run.status = STATES.NOTIFIED
        return a_run
    
    def handle_notified_state(self, a_run):
        try:
            a_run.job = self._gcli.gstat(a_run.job)[0]
            utils.persist_job_filesystem(a_run.job)
            a_run.status = STATES.WAITING
        except gc3utils.Exceptions.AuthenticationException:
            a_run.status = STATES.NOTIFIED
        return a_run
    
    def handle_kill_state(self, a_run):
        a_run.job = self._gcli.gkill(a_run.job)
        utils.persist_job_filesystem(a_run.job)
        a_run.status = STATES.KILLED
    
    def handle_missing_state(self, a_run):
        raise UnhandledStateError('Run id %s is in unhandled state %s'%(a_run.id, a_run.status))
    
    def step(self, a_run):
        try:
            a_run = self.status_mapping.get(a_run.status, self.handle_missing_state)(a_run)
        except gc3utils.Exceptions.AuthenticationException:
            a_run.status = STATES.UNREACHABLE
        except http.ResourceConflict:
            #The document in the database does not match the one we are trying to save.
            # Lets just ignore the error and let the state machine run
            gorg.log.critical('GridjobScheduler could not save run %s due to a document revision conflict'%(a_run.id))
        except:
            a_run.gsub_message=formatExceptionInfo()
            a_run.status=STATES.ERROR
            gorg.log.critical('GridjobScheduler Errored while processing run id %s \n%s'%(a_run.id, a_run.gsub_message))
        a_run.store()
        return a_run
    
    def run(self):
        from gorg.model.gridjob import RunInterface
        for a_state in STATES.all:
            if not a_state.terminal:
                view_runs = self.view_status_runs[a_state.view_key]
                for raw_run in view_runs:
                    a_run = RunInterface(self.db).load(raw_run.id)
                    while not a_run.status.terminal:
                        a_run = self.step(a_run)
                        if a_run.status.pause:
                            break

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
