from couchdb import schema as sch
from couchdb.schema import  Schema
from baserole import BaseroleModel
from gridjob import GridjobModel
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
                    yield job_id, doc['_id']
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
        self._job_list = list()
    
    def commit_all(self, db):
        self.commit(db)
        for a_job in self._job_list:
            a_job.commit(db)
        
    def commit(self, db):
        self.store(db)
    
    def add_child(self, child):
        assert isinstance(child, GridjobModel),  'Tasks can not be chilren.'
        if not child.id in self.children:
                self.children.append(child.id)
        self._job_list.append(child)
    
    def get_jobs(self):
        return tuple(self._job_list)
    
    def refresh(self, db):
        self = GridtaskModel.load(db, self.id)
    
    @staticmethod
    def load_task(db, task_id):
        a_task =GridtaskModel.load(db, task_id)
        for job_id in a_task.children:
            a_job=GridjobModel.load_job(db, job_id)
            a_task._job_list.append(a_job)
        return a_task

    def get_status(self, db):
        """Returns the overall status of this task."""
        self.get_jobs(db)
        for a_job in _job_list:            
            status_list.append(a_job.get_status(db))
        return tuple(status_list)
    
    def get_percent_done(self, db):
        status_list=self.get_status(db)
        num_done = 0
        for a_status in status_list:
            if a_status == 'DONE':
                num_done +=1
        # We treat a no status just like any other status value
        return (num_done / len(status_list)) * 100
    
    def delete(self, db):
        pass
    
    @staticmethod
    def view_by_author(db, author):
        return GridtaskModel.my_view(db, 'by_author', key=author)
    
    @staticmethod
    def view_by_children(db):
        return GridtaskModel.my_view(db, 'by_children')
    
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
