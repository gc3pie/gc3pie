from couchdb import schema as sch
from couchdb.schema import  Schema
from baserole import BaseroleModel
from gridjob import GridjobModel, JobInterface
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
    
class TaskInterface(object):
    def __init__(self, db):
        self.db = db
        self.a_task = None
    
    def create(self, author, title):
        self.a_task = GridtaskModel().create(author, title)
        self.a_task.commit(self.db)
        return self
    
    def load(self, id):
        self.a_task=GridtaskModel.load(self.db, id)
    
    def children():
        def fget(self):
            self.a_task.refresh(self.db)
            job_list=list()
            for job_id in self.a_task.children:
                a_job = JobInterface(self.db).load(job_id)
                job_list.append(a_job)
            return tuple(job_list)
        return locals()
    children = property(**children())

    def add_child(self, child):
        child_job = child.a_job
        assert isinstance(child_job, GridjobModel),  'Only jobs can be chilren.'
        if child_job.id not in self.a_task.children:
            self.a_task.children.append(child_job.id)
        self.a_task.commit(self.db)

    def status():
        def fget(self):
            self.a_task.refresh(self.db)
            for job_id in self.a_task.children:
                a_job = GridjobModel.load(self.db, job_id)
                status_list.append(a_job.get_status())
            return tuple(status_list)
        return locals()
    status = property(**status())
    
    def status_percent_done():
        def fget(self):
            status_list = self.status
            num_done = 0
            for a_status in status_list:
                if a_status == 'DONE':
                    num_done +=1
            # We treat a no status just like any other status value
            return (num_done / len(status_list)) * 100
        return locals()
    status_percent_done = property(**status_percent_done())
    
    def user_data_dict():        
        def fget(self):
            self.a_task.refresh(self.db)
            return self.a_task.user_data_dict
        def fset(self, user_dict):
            self.a_task.user_data_dict = user_dict
            self.a_task.commit(self.db)
        return locals()
    user_data_dict = property(**user_data_dict())
    
    def id():        
        def fget(self):
            return self.a_task.id
        return locals()
    id = property(**id())
    
