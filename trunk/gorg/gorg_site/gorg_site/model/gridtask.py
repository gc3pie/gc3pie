from couchdb import schema as sch
from couchdb.schema import  Schema
from baserole import BaseroleModel, BaseroleInterface
from gridjob import GridjobModel, JobInterface
from gridrun import GridrunModel
from couchdb import client as client
import time

map_func_task = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'BaseroleModel':
            if doc['sub_type'] == 'GridtaskModel':
                yield doc['_id'],doc
    '''

map_func_author = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'BaseroleModel':
            if doc['sub_type'] == 'GridtaskModel':
                yield doc['author'],doc
    '''

#reduce_func_author_status ='''
#def reducefun(keys, values, rereduce):
#    status_list = list()
#    for a_job in values['doc']:
#        status_list = a_job['status']
#    return status_list
#    '''    

map_func_children = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'BaseroleModel':
            if doc['sub_type'] == 'GridtaskModel':
                for job_id in doc['children']:
                    yield job_id, doc
    '''

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
    sub_type = sch.TextField(default=SUB_TYPE)
    
    def __init__(self, *args):
        super(GridtaskModel, self).__init__(*args)

    def commit(self, db):
        self.store(db)
    
    def refresh(self, db):
        self = GridtaskModel.load(db, self.id)
        
    def delete(self, db):
        pass
    
    @staticmethod
    def view_by_author(db, **options):
        return GridtaskModel.my_view(db,  'by_author', **options)

    @staticmethod
    def view_by_children(db, **options):
        return GridtaskModel.my_view(db, 'by_children', **options)

    @classmethod
    def my_view(cls, db, viewname, **options):
        from couchdb.design import ViewDefinition
        viewnames = cls.sync_views(db, only_names=True)
        assert viewname in viewnames, 'View not in view name list.'
        a_view = super(cls, cls).view(db, '%s/%s'%(cls.VIEW_PREFIX, viewname), **options)
        #a_view=.view(db, 'all/%s'%viewname, **options)
        return a_view
    
    @classmethod
    def sync_views(cls, db,  only_names=False):
        from couchdb.design import ViewDefinition
        if only_names:
            viewnames=('by_task','by_author', 'by_children')
            return viewnames
        else:
            by_task = ViewDefinition(cls.VIEW_PREFIX, 'by_task', map_func_task, wrapper=cls, language='python')
            by_author = ViewDefinition(cls.VIEW_PREFIX, 'by_author', map_func_author, wrapper=cls, language='python')
            by_children = ViewDefinition(cls.VIEW_PREFIX, 'by_children', map_func_children, wrapper=cls, language='python')
            views=[by_task, by_author, by_children]
            ViewDefinition.sync_many( db,  views)
        return views
    
class TaskInterface(BaseroleInterface):
    
    def create(self, author, title):
        self.controlled = GridtaskModel().create(author, title)
        self.controlled.commit(self.db)
        return self

    def load(self, id):
        self.controlled=GridtaskModel.load(self.db, id)
        return self

    def _status(self):
        status_list = list()
        self.controlled.refresh(self.db)
        view = GridrunModel.view_by_job_status(self.db, keys = self.controlled.children)
        for a_row in view.rows:
            status_list.append(a_row.value)
        return tuple(status_list)
    
    def status():
        def fget(self):
            status_list = self._status()
            status_dict = dict()
            for a_status in GridrunModel.POSSIBLE_STATUS:
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
            for a_status in status_dict:
                if status_dict[a_status] == job_count:
                    return a_status
            if status_dict[GridrunModel.POSSIBLE_STATUS['ERROR']] != 0:
                return GridrunModel.POSSIBLE_STATUS['ERROR']
            else:
                return GridrunModel.POSSIBLE_STATUS['RUNNING']
        return locals()
    status_overall = property(**status_overall())

    def status_percent_done():
        def fget(self):
            status_dict = self._status()
            # We treat a no status just like any other status value
            return (tatus_dict['DONE'] / len(status_dict)) * 100
        return locals()
    status_percent_done = property(**status_percent_done())
    
    def task():
        def fget(self):
            return self.controlled
        def fset(self, a_task):
            self.controlled = a_task
        return locals()
    task = property(**task())
