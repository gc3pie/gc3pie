import htpie
from htpie.lib import utils
from htpie import model
from htpie import statemachine
from htpie.application import gamess
from htpie.lib.exceptions import *

import gc3utils.Application
import gc3utils.Default
import gc3utils.gcli
import gc3utils.Job
import gc3utils.Exceptions

import glob
from datetime import datetime
import time

_app_tag_mapping = dict()
_app_tag_mapping['gamess']=gamess.GamessApplication


def _print_job_info(job_obj):
    output = ''
    for key in job_obj.keys():
        if not key == 'log' and not ( str(job_obj[key]) == '-1' or  str(job_obj[key]) == '' ):
            if key == 'status':
                output +="%-20s  %-10s \n" % (key, gc3utils.Job.job_status_to_string(job_obj[key]))
            else:
                output += "%-20s  %-10s \n" % (key,job_obj[key])
    return output

class Nested(object):
    
    @staticmethod
    def _replace_period(data):
        match = '.'
        convert_to = '`'
        if isinstance(data, str) or \
            isinstance(data, unicode):
            data = data.replace(match, convert_to)
        return data
    
    @staticmethod
    def _place_period(data):
        match = '`'
        convert_to = '.'
        if isinstance(data, str) or \
            isinstance(data, unicode):
            data = data.replace(match, convert_to)
        return data
    
    @staticmethod
    def _recur_map(fun, data): 
        if hasattr(data, "__iter__"): 
            if isinstance(data, dict):
                dict2list = data.items()
                mod = [Nested._recur_map(fun, elem) for elem in dict2list]
                list2dict = dict()
                for key, value in mod:
                    list2dict[key] = value
                return  list2dict
            elif isinstance(data, list) or isinstance(data, tuple):
                return [Nested._recur_map(fun, elem) for elem in data] 
        else: 
            return fun(data) 
    
    @staticmethod
    def unicode_to_str(data):
        def str_fun(data):
            if isinstance(data, unicode):
                return str(data)
            else:
                return data
        return Nested._recur_map(str_fun, data)
    
    @staticmethod
    def replace_period(data):
        return Nested._recur_map(Nested._replace_period, data)
    
    @staticmethod
    def place_period(data):
        return Nested._recur_map(Nested._place_period, data)
    

class States(statemachine.States):
    READY = u'STATE_READY'
    WAITING = u'STATE_WAIT'
    RETRIEVING = u'STATE_RETRIEVING'
    UNREACHABLE = u'STATE_UNREACHABLE'
    NOTIFIED = u'STATE_NOTIFIED'
    POSTPROCESS = u'STATE_POSTPROCESS'

class Transitions(statemachine.Transitions):
    pass

class GSingle(model.Task):
    
    structure = {
        'gc3_application' : dict, 
        'gc3_job' : dict, 
        'gc3_temp': dict,
        'result': gamess.GamessResult, 
        'coord':model.MongoMatrix, 
    }
    
    default_values = {
        'state': States.READY,
        '_type':u'GSingle',  
        'transition': Transitions.HOLD
    }
    
    def display(self, long_format=False):
        job = self.job
        application = self.application
        tm_format = '%Y-%m-%d %H:%M:%S'
        output = '%s %s %s %s\n'%(self._type, self.id, self.state, self.transition)
        
        if job:
            submission_time = datetime.strptime(job.submission_time, tm_format)
            output += 'Job submitted: %s\n'%(submission_time)
            if 'completion_time' in job:
                if job.completion_time:
                    completion_time = datetime.strptime(job.completion_time, tm_format)
                    output += 'Job completed: %s\n'%(completion_time)
                    delta = completion_time - submission_time
                    output += 'Delta: %s\n'%(delta)
        
        output += 'Input file(s):\n'
        f_list = self.open('input')
        for f in f_list:
            output += '%s\n'%(f.name)
        [f.close() for f in f_list]
        output += 'Ouput file(s):\n'
        f_list = self.open('output')
        for f in f_list:
            output += '%s\n'%(f.name)
        
        if long_format:
            output += 'GC3 job info:\n'
            output += _print_job_info(job)
            #output += '\nGC3 Application:\n%s\n'%(application)
        else:
            if job:
                output += 'GC3 job number: %s\n'%(job.unique_token)
        
        return output

    def application():        
        def fget(self):
            application = self.gc3_application
            if self.gc3_application:
                self.gc3_application = Nested.unicode_to_str(self.gc3_application)
                application = Nested.place_period(self.gc3_application)
                application = _app_tag_mapping[self.app_tag].gc3_pass_through(**application)
            return application
        def fset(self, application):
            self.gc3_application = Nested.replace_period(application)
        return locals()
    application = property(**application())
    
    def job():        
        def fget(self):
            job = self.gc3_job
            if self.gc3_job:                
                self.gc3_job = Nested.unicode_to_str(self.gc3_job)
                job = Nested.place_period(self.gc3_job)
                job = gc3utils.Job.Job(job)
            return job
        def fset(self, job):
            self.gc3_job = Nested.replace_period(job)
        return locals()
    job = property(**job())
    
    def retry(self):
        if self.transition == Transitions.ERROR:
            try:
                self.acquire()
            except:
                raise
            else:
                self.transition = Transitions.PAUSED
                self.release()
                self.save()
    
    def kill(self):
        try:
            self.acquire()
        except:
            raise
        else:
            self.state = States.KILL
            self.release()
            htpie.log.debug('GSingle %s will be killed'%(self.id))
    
    @classmethod
    def create(cls, f_list,  app_tag='gamess', requested_cores=2, requested_memory=2, requested_walltime=2):
        task = super(GSingle, cls,).create()
        task.app_tag = u'%s'%(app_tag)
        for f_name in f_list:
            task.attach_file(f_name, 'input')
        
        atoms, params = _app_tag_mapping[task.app_tag].parse_input(f_list[0])
        task.coord = model.MongoMatrix.create()
        task.coord.matrix = atoms.get_positions()
        
        task.gc3_temp = _app_tag_mapping[task.app_tag].temp_application(requested_cores, 
                                                                                                                      requested_memory, 
                                                                                                                      requested_walltime 
                                                                                                                        ) 
        
        task.transition = Transitions.PAUSED
        task.save()        
        return task

model.con.register([GSingle])

class GSingleStateMachine(statemachine.StateMachine):
    _cls_task = GSingle
    
    def __init__(self):
        super(GSingleStateMachine, self).__init__()
        self.state_mapping.update({States.READY: self.handle_ready_state, 
                                                    States.WAITING: self.handle_waiting_state, 
                                                    States.RETRIEVING: self.handle_retrieving_state, 
                                                    States.UNREACHABLE: self.handle_unreachable_state, 
                                                    States.NOTIFIED: self.handle_notified_state,
                                                    States.POSTPROCESS: self.handle_postprocess_state, 
                                                    States.KILL: self.handle_kill_state, 
                                                    })
        config_file = gc3utils.Default.CONFIG_FILE_LOCATION
        self._gcli = gc3utils.gcli.Gcli(*gc3utils.gcli.import_config(config_file))
    
    def handle_ready_state(self):
        #Need to sleep to give the arc info system time to update itself
        #with any jobs just submitted by me
        time.sleep(90)
        f_to_run = self.task.mk_local_copy('input')
        map(file.close, f_to_run)
        f_name = f_to_run[0].name
        
        #self.task.application = _app_tag_mapping[self.task.appstate_mapping_tag].gc3_application(a_file.name, **Nested.unicode_to_str(self.task.gc3_temp))
        l_application = _app_tag_mapping[self.task.app_tag].gc3_application(f_name, **Nested.unicode_to_str(self.task.gc3_temp))
        self.task.job = self._gcli.gsub(l_application)
        # When I dump the application obj into mongodb it will change the dict because of mongokit limitation, so be careful
        self.task.application = l_application
        gc3utils.Job.persist_job(self.task.job)
        htpie.log.debug('Submitted gsingle %s to the grid'%(self.task.id))
        self.state = States.WAITING
    
    def handle_waiting_state(self):
        temp = self._gcli.gstat(self.task.job)
        self.task.job = temp[0]
        gc3utils.Job.persist_job(self.task.job)
        
        if self.task.job.status == gc3utils.Job.JOB_STATE_SUBMITTED:
            self.state = States.WAITING
        elif self.task.job.status== gc3utils.Job.JOB_STATE_RUNNING:
            self.state = States.WAITING
        elif self.task.job.status == gc3utils.Job.JOB_STATE_FINISHED:            
            self.state = States.RETRIEVING
        elif self.task.job.status == gc3utils.Job.JOB_STATE_FAILED or \
            self.state == gc3utils.Job.JOB_STATE_DELETED or \
            self.state == gc3utils.Job.JOB_STATE_UNKNOWN:
            raise GC3Exception('GC3 job errored: \n %s'%(self.task.job))
    
    def handle_retrieving_state(self):        
        self.task.job = self._gcli.gget(self.task.job)
        gc3utils.Job.persist_job(self.task.job)
        output_files = glob.glob('%s/*.*'%(self.task.job.download_dir))
        for f_name in output_files:
            self.task.attach_file(f_name, 'output')
        htpie.log.debug('Retrieved gsingle %s data'%(self.task.id))
        self.state = States.POSTPROCESS
    
    def handle_postprocess_state(self):
        app = _app_tag_mapping[self.task.app_tag]
        f_list = self.task.mk_local_copy('output')
        self.task.result = app.parse_result(f_list)
        self.state = States.COMPLETE
        return True
    
    def handle_unreachable_state(self):
        #TODO: Notify the user that they need to log into the clusters again, maybe using email?
        self.state = States.NOTIFIED
    
    def handle_notified_state(self):
        try:
            self.task.job = self._gcli.gget(self.task.job)
            gc3utils.Job.persist_job(self.task.job)
            self.state = States.WAITING
        except gc3utils.Exceptions.AuthenticationException:
            self.state = States.NOTIFIED
    
    def handle_kill_state(self):
        if self.task.job:
            self.task.job = self._gcli.gkill(self.task.job)
            gc3utils.Job.persist_job(self.task.job)
            if self.task.job.status == gc3utils.Job.JOB_STATE_COMPLETED or \
                    self.task.job.status == gc3utils.Job.JOB_STATE_FAILED or \
                    self.task.job.status == gc3utils.Job.JOB_STATE_DELETED:
                print 'This is the job that will be cleaned next'
                print self.task.job
                #gc3utils.Job.clean_job(self.task.job)
        return True
    
    def handle_missing_state(self, a_run):
        raise UnhandledStateError('GSingle %s is in unhandled state %s'%(self.task.id, self.state))

if __name__ == '__main__':
    import sys
    utils.configure_logger(10)
    a_run = GSingle.create(['/home/mmonroe/gtests/exam01.inp'])
    fsm = GSingleStateMachine()
    fsm.load(GSingle, a_run.id)
    fsm.run()
    print a_run.id
    sys.exit(1)



        
            
    
    
