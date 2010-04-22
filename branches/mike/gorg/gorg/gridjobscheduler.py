import os
import sys
import tempfile
import glob
import logging

from gorg.model.gridrun import GridrunModel, PossibleStates, TerminalStates
from gorg.lib.utils import Mydb, create_file_logger, formatExceptionInfo
from gc3utils.gcli import Gcli

class GridjobScheduler(object):
    def __init__(self, db_name='gorg_site', db_url='http://127.0.0.1:5984', 
                 gcli_location='/home/mmonroe/.gc3/config'):
        self.db=Mydb('mark', db_name,db_url).cdb()
        self.view_status_runs = GridrunModel.view_by_status(self.db)
        self.logger  = logging.getLogger(self.__class__.__name__)
        self.gcli = Gcli(gcli_location)
    
    def handle_ready_jobs(self):
        #Handle jobs that are ready to be submitted to the grid
        # Get the files that we need to generatea grid run
        try:
            f_list = a_run.attachments_to_files(self.db, a_run.files_to_run.keys())
    #TODO: We handle multiple input files here, but gcli can not handle them
            assert len(f_list)==1,  'gcli.gsub does not handle multiple input files.'
            run_params = a_run.run_params
            f_dir=os.path.dirname( f_list.values()[0].name)
            result = self.gcli.gsub(job_local_dir=f_dir, input_file=f_list.values()[0].name, **run_params)
            a_run.gsub_unique_token = result[1]
            # TODO: Get the real status, the run may be waiting in the queue, not running
            a_run.status=PossibleStates['RUNNING']
        finally:
            f_list.values()[0].close()
        return a_run

    def handle_waiting_jobs(self):
        result = self.gcli.gstat(a_run.gsub_unique_token)
        a_run.status = result[1][0][1].split()[-1]
 
    def handle_running_jobs(self):
        result = self.gcli.gstat(a_run.gsub_unique_token)
        a_run.status = result[1][0][1].split()[-1]

    def handle_finished_jobs(self):
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
            except IOError:
                a_file.close()
                raise
        a_run.status=PossibleStates['DONE']

    def handle_retrieving_jobs(self):
        pass
    def handle_done_jobs(self):
        pass
    def handle_error_jobs(self):
        pass

    def handle_unreachable_jobs(self):
        #TODO: Notify the user that they need to log into the clusters again, maybe using email?
        a_run.status = PossibleStates['NOTIFIED']
    
    def handle_notified_jobs(self):
        try:
            result = self.gcli.gstat(a_run.gsub_unique_token)
            a_run.status = result[1][0][1].split()[-1]
        except AuthenticationError:
            a_run.status = PossibleStates['NOTIFIED']
    
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
                            raise NotImplementedError('Run id %s is in unknown state %s'%(a_run.id, a_run.status))
                    except AuthenticationError:
                        a_run.status = PossibleStates['UNREACHABLE']
                    except:
                        a_run.gsub_message=formatExceptionInfo()
                        a_run.status=PossibleStates['ERROR']
                        self.logger.critical('GridjobScheduler Errored while processing run id %s \n%s'%(a_run.id, a_run.gsub_message))
                    a_run.commit(self.db)

def main():
    job_scheduler = GridjobScheduler()
    job_scheduler.run()
    print 'Done running gridjobscheduler.py'


if __name__ == "__main__":
    create_file_logger(1)
    main()
    sys.exit()
