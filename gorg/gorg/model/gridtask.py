from couchdb.mapping import *
from baserole import *
from gridjob import *
from couchdb import client as client
import time
import gorg

from gorg.lib import state

#reduce_func_author_status ='''
#def reducefun(keys, values, rereduce):
#    status_list = list()
#    for a_job in values['doc']:
#        status_list = a_job['status']
#    return status_list
#    '''    

STATE_HOLD = state.State.create('HOLD', 'HOLD desc')

oldddd = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'GridjobModel':
            if doc['sub_type'] == 'TASK':
                if doc['owned_by_task']:                        
                    yield (doc['_id'], doc['owned_by_task']), doc
                else:
                    yield (doc['_id'], doc['_id']), docgridtask.py
            if doc['sub_type'] == 'JOB':
                yield (doc['owned_by_task'], doc['_id']) , doc
    '''


class GridtaskModel(BaseroleModel):
    
    SUB_TYPE = 'GridtaskModel'
    VIEW_PREFIX = 'GridtaskModel'
    sub_type = TextField(default=SUB_TYPE)
    status = DictField(default=STATE_HOLD)
    
    def __init__(self, *args):
        super(GridtaskModel, self).__init__(*args)
    
    
    @ViewField.define('GridtaskModel')
    def view_author(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridtaskModel':
                    yield doc['author'],doc
    
    @ViewField.define('GridtaskModel')
    def view_all(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridtaskModel':
                    yield doc['_id'],doc
    
    #TODO: FIX ME, issue here if we store tasks and jobs in the same list, we can not wrap them here
#    @ViewField.define('GridtaskModel', wrapper=GridjobModel)
#    def view_children(doc):
#        if 'base_type' in doc:
#            if doc['base_type'] == 'BaseroleModel':
#                if doc['sub_type'] == 'GridtaskModel':
#                    for job_id in doc['children_ids']:
#                        yield job_id, {'_id':job_id}
    
    @ViewField.define('GridtaskModel')
    def view_children(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridtaskModel':
                    for job_id in doc['children_ids']:
                        yield job_id, doc
    
    @ViewField.define('GridtaskModel')
    def view_status(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridtaskModel':
                    yield [doc['title'],  doc['status']], doc

    @classmethod
    def sync_views(cls, db,  only_names=False):
        from couchdb.design import ViewDefinition
        definition_list = list()
        for key, value in cls.__dict__.items():
            if isinstance(value, ViewField):
                definition_list.append(eval('cls.%s'%(key)))
        ViewDefinition.sync_many( db,  definition_list)
    
class TaskInterface(BaseGraphInterface):
    
    def create(self, title):
        self.wrap(GridtaskModel().create(self.db.username, title))
        self.store()
        gorg.log.debug('Task %s has been created'%(self.id))
        return self
    
    def load(self, id=None):
        if not id:
            id = self.id
        self.wrap(GridtaskModel.load(self.db, id))
        return self
    
    def status():
        def fget(self):
            return state.State(**self._obj.status)
        def fset(self, status):
            if isinstance(status, tuple):
                value = status[0]
                key = status[1]
            else:
                value = status
                key = 'I have no key'            

            if self.status.locked is not None:
                if self.status.locked == key:
                    self._obj.status = value
                else:
                    raise DocumentError('Task %s is locked, and you provided the wrong key.'%(self.id))
            else:
                self._obj.status = value
        return locals()
    status = property(**status())

    def _status_children(self):
        status_list = list()
        self.load()
        view = GridrunModel.view_job_status(self.db, keys = self._obj.children_ids)
        for a_row in view:
            status_list.append(a_row)
        return tuple(status_list)
    
    def status_counts():
        def fget(self):
            status_list = self._status_children()
            status_dict = dict()
            for a_status in status_list:
                if a_status.description not in status_dict:
                    status_dict[a_status.description] = 0
                status_dict[a_status.description] += 1
            return status_dict
        return locals()
    status_counts = property(**status_counts())
    
    def jobs():
        def fget(self):
            child_list = self.children
            ret = list()
            for a_child in child_list:
                if isinstance(a_child, JobInterface):
                    ret.append(a_child)
            return tuple(ret)
        return locals()
    jobs = property(**jobs())
    
    def tasks():
        def fget(self):
            child_list = self.children
            ret = list()
            for a_child in child_list:
                if isinstance(a_child, TaskInterface):
                    ret.append(a_child)
            return tuple(ret)
        return locals()
    tasks = property(**tasks())

#    def status_overall():
#        def fget(self):
#            status_dict = self.status_counts
#            job_count = sum(status_dict.values())
#            if job_count == 0:
#                return {}
#            for a_status in status_dict:
#                if status_dict[a_status] == job_count:
#                    return a_status
#            if status_dict[STATES.ERROR] != 0:
#                return STATES.ERROR
#            else:
#                return STATES.WAITING
#        return locals()
#    status_overall = property(**status_overall())
#
#    def status_percent_done():
#        def fget(self):
#            status_dict = self.status_counts()
#            # We treat a no status just like any other status value
#            return (status_dict[STATES.COMPLETED.description] / len(status_dict)) * 100
#        return locals()
#    status_percent_done = property(**status_percent_done())
    
#    def wait(self, timeout=60):
#        from time import sleep
#        check_freq=10
#        if timeout == 'INFINITE':
#            timeout = sys.maxint
#        if check_freq > timeout:
#            check_freq = timeout
#        starting_time = time.time()
#        while True:
#            my_status = self.status_overall
#            if starting_time + timeout < time.time() or my_status.terminal:
#                break
#            else:
#                time.sleep(check_freq)
#        if my_status.terminal:
#            # We did not timeout 
#            return True
#        else:
#            # Timed out
#            return False
