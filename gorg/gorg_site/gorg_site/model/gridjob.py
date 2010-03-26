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
        self._run = None
    
    def create(self, author, title,  files_to_run, application_to_run='gamess', 
                   selected_resource='ocikbpra',  cores=2, memory=1, walltime=-1):
        self = super(GridjobModel, self).create(author, title)
        self._run = GridrunModel()
        self._run = self._run.create( author, files_to_run, self, application_to_run, 
                            selected_resource,  cores, memory, walltime)
        return self

    def commit(self, db):        
        from gridtask import GridtaskModel
        view = GridtaskModel.view_by_children(db)
        assert len(view[self.id]) == 1, 'Commit the task associated with this job first.'
        assert self._run is not None,  'No run associated with job %s'%self.id
        self._run.commit(db, self)
        self.store(db)
    
    def add_child(self, child):
        assert isinstance(child, GridjobModel),  'Tasks can not be chilren.'
        if not child.id in self.children:
                self.children.append(child.id)
    
    def refresh(self, db):
        self = GridjobModel.load(db, self.id)
    
    @staticmethod
    def load_job(db, job_id):
        a_job = GridjobModel.load(db, job_id)
        a_job._run = GridrunModel.view_by_job(db, job_id)
        return a_job

    def add_parent(self, parent):
        parent.add_child(self)
    
    def get_task(self, db):
        from gridtask import GridtaskModel

        view = GridtaskModel.view_by_children(db)
        a_task=view[self.id]
        return a_task
    
    @staticmethod
    def view_by_job(db):
        return GridjobModel.my_view(db, 'by_job')
    
    def get_children(self, db):
        job_list = list()
        for job_id in self.children:
            job_list.append(GridjobModel.load(db, job_id))
        return tuple(job_list)

    def get_parents(self, db):
        job_list = list()
        view = GridjobModel.view_by_chilren(db)
        for a_parent in view[self.id]:
            job_list.append(a_parent)
        return tuple(job_list)
    
    def get_run(self):
        assert self._run is not None,  'No run is associated with this job'
        return self._run
        
    def get_status(self, db):
        a_run = self.get_run(db)
        return a_run.status

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
