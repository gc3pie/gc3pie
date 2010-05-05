import os
import sys
import tempfile
import glob
import gorg

from gorg.model.gridjob import GridrunModel, PossibleStates, TerminalStates
from gorg.lib.exceptions import *
from gorg.lib.utils import Mydb, create_filelogger, formatExceptionInfo
from gc3utils.gcli import Gcli

class GridjobScheduler(object):
    def __init__(self, db_name='gorg_site', db_url='http://127.0.0.1:5984', 
                 gcli_location='/home/mmonroe/.gc3/config'):
        self.db=Mydb('mark', db_name,db_url).cdb()
        self.view_status_runs = GridrunModel.view_status(self.db)
        self.gcli = Gcli(gcli_location)
    
    def handle_ready_jobs(self, a_run):
        #Handle jobs that are ready to be submitted to the grid
        # Get the files that we need to generatea grid run
        try:
            f_list = a_run.attachments_to_files(self.db, a_run.files_to_run.keys())
    #TODO: We handle multiple input files here, but gcli can not handle them
            if len(f_list)!=1:
                raise ValueError('gcli.gsub does not handle multiple input files.')
            run_params = a_run.run_params
            f_dir=os.path.dirname( f_list.values()[0].name)
            result = self.gcli.gsub(job_local_dir=f_dir, input_file=f_list.values()[0].name, **run_params)
            a_run.gsub_unique_token = result[1]
            # TODO: Get the real status, the run may be waiting in the queue, not running
            a_run.status=PossibleStates['WAITING']
        finally:
            map(file.close, f_list.values())
        return a_run

    def handle_waiting_jobs(self, a_run):
        result = self.gcli.gstat(a_run.gsub_unique_token)
        a_run.status = result[1][0][1].split()[-1]
        return a_run
 
    def handle_running_jobs(self, a_run):
        result = self.gcli.gstat(a_run.gsub_unique_token)
        a_run.status = result[1][0][1].split()[-1]
        return a_run

    def handle_finished_jobs(self, a_run):
        # TODO: gget returns 0 when it works, what do I do when it doesn't work? Cann't it return DONE or something?
        token = a_run.gsub_unique_token
        a_run.status=PossibleStates['RETRIEVING']
        a_run.commit(self.db)
        result = self.gcli.gget(token)
        output_files=glob.glob('%s/*.*'%a_run.gsub_unique_token)
        for f_name in output_files:
            try:
                a_file = open(f_name, 'rb')
                a_run = a_run.put_attachment(self.db, a_file, os.path.basename(a_file.name)) #os.path.splitext(a_file.name)[-1].lstrip('.')
            finally:
                a_file.close()
        a_run.status=PossibleStates['DONE']
        return a_run

    def handle_unreachable_jobs(self, a_run):
        #TODO: Notify the user that they need to log into the clusters again, maybe using email?
        a_run.status = PossibleStates['NOTIFIED']
        return a_run
    
    def handle_notified_jobs(self, a_run):
        try:
            result = self.gcli.gstat(a_run.gsub_unique_token)
            a_run.status = result[1][0][1].split()[-1]
        except AuthenticationError:
            a_run.status = PossibleStates['NOTIFIED']
        return a_run
    
    def run(self):
        view_runs = self.view_status_runs
        for a_state in PossibleStates:
            if a_state not in TerminalStates:
                view_runs = self.view_status_runs[a_state]
                for a_run in view_runs:
                    try:
                        if PossibleStates['READY'] == a_state:
                            a_run = self.handle_ready_jobs(a_run)
                        elif PossibleStates['WAITING'] == a_state:
                            a_run = self.handle_waiting_jobs(a_run)
                        elif PossibleStates['RUNNING'] == a_state:
                            a_run = self.handle_running_jobs(a_run)
                        elif PossibleStates['FINISHED'] == a_state:
                            a_run = self.handle_finished_jobs(a_run)
                        elif PossibleStates['UNREACHABLE'] == a_state:
                            a_run = self.handle_unreachable_jobs(a_run)
                        elif PossibleStates['NOTIFIED'] == a_state:
                            a_run = self.handle_notified_jobs(a_run)
                        else:
                            raise UnhandledStateError('Run id %s is in unhandled state %s'%(a_run.id, a_run.status))
                    except AuthenticationError:
                        a_run.status = PossibleStates['UNREACHABLE']
                    except:
                        a_run.gsub_message=formatExceptionInfo()
                        a_run.status=PossibleStates['ERROR']
                        log.critical('GridjobScheduler Errored while processing run id %s \n%s'%(a_run.id, a_run.gsub_message))
                    a_run.commit(self.db)

def main():
    job_scheduler = GridjobScheduler()
    job_scheduler.run()
    print 'Done running gridjobscheduler.py'


if __name__ == "__main__":
    create_filelogger(1)
    main()
    sys.exit()
