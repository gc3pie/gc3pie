from couchdb.mapping import *
from baserole import BaseroleModel, BaseroleInterface
from gridjob import *
from couchdb import client as client
import time
import gorg

#reduce_func_author_status ='''
#def reducefun(keys, values, rereduce):
#    status_list = list()
#    for a_job in values['doc']:
#        status_list = a_job['status']
#    return status_list
#    '''    


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
    state = TextField()
    
    def __init__(self, *args):
        super(GridtaskModel, self).__init__(*args)

    def commit(self, db):
        self.store(db)
    
    def refresh(self, db):
        self = GridtaskModel.load(db, self.id)
        return self
        
    def delete(self, db):
        pass
    
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
    
    #TODO: FIX
    @ViewField.define('GridtaskModel', wrapper=GridjobModel)
    def view_children(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridtaskModel':
                    for job_id in doc['children']:
                        yield job_id, {'_id':job_id}
    
    @ViewField.define('GridtaskModel')
    def view_state(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridtaskModel':
                    yield doc['state'], doc

    @classmethod
    def sync_views(cls, db,  only_names=False):
        from couchdb.design import ViewDefinition
        definition_list = list()
        for key, value in cls.__dict__.items():
            if isinstance(value, ViewField):
                definition_list.append(eval('cls.%s'%(key)))
        ViewDefinition.sync_many( db,  definition_list)
    
class TaskInterface(BaseroleInterface):
    
    def create(self, title):
        self.controlled = GridtaskModel().create(self.db.username, title)
        self.controlled.commit(self.db)
        log.debug('Task %s has been created'%(self.id))
        return self

    def load(self, id):
        self.controlled=GridtaskModel.load(self.db, id)
        return self

    def _status(self):
        status_list = list()
        self.controlled.refresh(self.db)
        view = GridrunModel.view_job_status(self.db, keys = self.controlled.children)
        for a_row in view.rows:
            status_list.append(a_row.value)
        return tuple(status_list)
    
    def status():
        def fget(self):
            status_list = self._status()
            status_dict = dict()
            for a_status in PossibleStates:
                status_dict[a_status]  = 0
            for a_status in status_list:
                status_dict[a_status] += 1
            return status_dict
        return locals()
    status = property(**status())

    def status_overall():
        def fget(self):
            status_dict = self.status
            job_count = sum(status_dict.values())
            if job_count == 0:
                return PossibleStates['HOLD']
            for a_status in status_dict:
                if status_dict[a_status] == job_count:
                    return a_status
            if status_dict[PossibleStates['ERROR']] != 0:
                return PossibleStates['ERROR']
            else:
                return PossibleStates['RUNNING']
        return locals()
    status_overall = property(**status_overall())

    def status_percent_done():
        def fget(self):
            status_dict = self._status()
            # We treat a no status just like any other status value
            return (status_dict['DONE'] / len(status_dict)) * 100
        return locals()
    status_percent_done = property(**status_percent_done())
    
    def wait(self, target_status=PossibleStates['DONE'], timeout=60, check_freq=10):
        from time import sleep
        if timeout == 'INFINITE':
            timeout = sys.maxint
        if check_freq > timeout:
            check_freq = timeout
        starting_time = time.time()
        while True:
            my_status = self.controlled.status_overall
            assert my_status != PossibleStates['ERROR'], 'Task %s returned an error.'%self.id
            if starting_time + timeout < time.time() or my_status == target_status:
                break
            else:
                time.sleep(check_freq)
        if my_status == target_status:
            # We did not timeout 
            return True
        else:
            # Timed or errored out
            return False
    
    def task():
        def fget(self):
            return self.controlled
        def fset(self, a_task):
            self.controlled = a_task
        return locals()
    task = property(**task())
    
    def state():
        def fget(self):
            return self.controlled.state
        def fset(self, state):
            self.controlled.state = state
            self.controlled.commit(self.db)
        return locals()
    state = property(**state())
