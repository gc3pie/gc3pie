from couchdb import schema as sch
from couchdb.schema import  Schema
from gridrun import GridrunModel
from baserole import BaseroleModel
from couchdb import client as client
import time

map_func_job = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'BaseroleModel':
            if doc['sub_type'] == 'GridjobModel':
                yield doc['_id'],doc
    '''

map_func_author = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'BaseroleModel':
            if doc['sub_type'] == 'GridjobModel':
                yield doc['author'],doc
    '''

map_func_children = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'BaseroleModel':
            if doc['sub_type'] == 'GridjobModel':
                for job_id in doc['children']:
                    yield job_id, doc
    '''

class GridjobModel(BaseroleModel):
    SUB_TYPE = 'GridjobModel'
    VIEW_PREFIX = 'GridjobModel'
    sub_type = sch.TextField(default=SUB_TYPE)    
    
    def __init__(self, *args):
        super(GridjobModel, self).__init__(*args)
        self._run_id = None
    
    def commit(self, db):
        self.store(db)
    
    def refresh(self, db):
        self = GridjobModel.load(db, self.id)
    
    @staticmethod
    def load_job(db, job_id):
        a_job = GridjobModel.load(db, job_id)
        view = GridrunModel.view_by_job(db, key=job_id)
        a_job._run_id = view.view.wrapper(view.rows[0]).id
        return a_job
    
    @staticmethod
    def view_by_job(db, **options):
        return GridjobModel.my_view(db, 'by_job', **options)
    
    @staticmethod
    def view_by_children(db, **options):
        return GridjobModel.my_view(db, 'by_children', **options)

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
            viewnames=('by_job', 'by_author', 'by_children')
            return viewnames
        else:
            by_job = ViewDefinition(cls.VIEW_PREFIX, 'by_job', map_func_job, wrapper=cls, language='python')
            by_author = ViewDefinition(cls.VIEW_PREFIX, 'by_author', map_func_author, wrapper=cls, language='python')
            by_children = ViewDefinition(cls.VIEW_PREFIX, 'by_children', map_func_children, wrapper=None, language='python')
            views=[by_job, by_author, by_children]
            ViewDefinition.sync_many( db,  views)
        return views
    
class JobInterface(object):
    def __init__(self, db):
        self.db = db
        self.a_job = None
    
    def create(self, author, title,  files_to_run, application_to_run='gamess', 
                        selected_resource='ocikbpra',  cores=2, memory=1, walltime=-1):
        self.a_job = GridjobModel().create(author, title)
        a_run = GridrunModel()
        a_run = a_run.create( self.db, files_to_run, self.a_job, application_to_run, 
                        selected_resource,  cores, memory, walltime)
        self.a_job._run_id = a_run.id
        self.a_job.commit(self.db)
        return self
    
    def load(self, id):
        self.a_job=GridjobModel.load_job(self.db, id)
        return self

    def add_child(self, child):
        child_job = child.a_job
        assert isinstance(child_job, GridjobModel),  'Only jobs can be chilren.'
        self.a_job.refresh(self.db)
        if child_job.id not in self.a_job.children:
            self.a_job.children.append(child_job.id)
        self.a_job.commit(self.db)
    
    def add_parent(self, parent):
        parent.add_child(self)
    
    def task():
        def fget(self):
            from gridtask import GridtaskModel
            self.a_job.refresh(self.db)
            view = GridtaskModel.view_by_children(self.db)
            a_task=view[self.a_job.id]
            return tuple(a_task)
        return locals()
    task = property(**task())

    def children():            
        def fget(self):
            self.a_job.refresh(self.db)
            job_list=list()
            for job_id in self.a_job.children:
                a_job = GridjobModel.load(self.db, job_id)
                job_list.append(a_job)
            return tuple(job_list)
        return locals()
    children = property(**children())

    def parents():            
        def fget(self):
            job_list = list()
            view = GridjobModel.view_by_children(self.db)
            for a_parent in view[self.a_job.id]:
                job_list.append(a_parent)
            return tuple(job_list)
        return locals()
    parents = property(**parents())
    
    def run():
        def fget(self):
            return GridrunModel.load(self.db, self.a_job._run_id)
        return locals()
    run = property(**run())

    def status():
        def fget(self):
            a_run = self.run
            return a_run.status
        def fset(self, status):
            a_run = self.run
            a_run.status = status
            a_run.commit(self.db)
        return locals()
    status = property(**status())
    
    def get_attachment(self, ext):
        import os
        for key in self.attachments:
            if os.path.splitext(key)[-1] == '.'+ext:
                return self.attachments[key]

    def attachments():
        def fget(self):
            return self.run.attachments_to_files(self.db)
        return locals()
    attachments = property(**attachments())
    
    def user_data_dict():        
        def fget(self):
            self.a_job.refresh(self.db)
            return self.a_job.user_data_dict
        def fset(self, user_dict):
            self.a_job.user_data_dict = user_dict
            self.a_job.commit(self.db)
        return locals()
    user_data_dict = property(**user_data_dict())
        
    def result_data_dict():        
        def fget(self):
            self.a_job.refresh(self.db)
            return self.a_job.result_data_dict
        def fset(self, result_dict):
            self.a_job.result_data_dict = result_dict
            self.a_job.commit(self.db)
        return locals()
    result_data_dict = property(**result_data_dict())
    
    def id():        
        def fget(self):
            return self.a_job.id
        return locals()
    id = property(**id())
    
    def run_id():        
        def fget(self):
            return self.a_job._run_id
        return locals()
    run_id = property(**run_id())
