from couchdgridjob.pyb import schema as sch
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

class GridjobModel(BaseroleModel):
    SUB_TYPE = 'GridjobModel'
    VIEW_PREFIX = 'GridjobModel'
    sub_type = sch.TextField(default=self.SUB_TYPE)    
    
    _run_before_commit = None
    
    def create(self, db, author, title,  files_to_run, application_to_run='gamess', 
                   selected_resource='ocikbpra',  cores=2, memory=1, walltime=-1):
#TODO: There is a bug, GridrunModel() is taking on the _hold_file_pointers as if it is global. Once set, it stays set
        self = super(GridjobModel, self).create(db, author, title)
        self._run_before_commit = GridrunModel()
        self._run_before_commit._hold_file_pointers=list()
        self._run_before_commit = self._run_before_commit.create( db, files_to_run, self, application_to_run, 
                            selected_resource,  cores, memory, walltime)
        return self

    def commit(self, db):        
        view = GridrunModel.view_by_job(db, self.id)
        # Make sure that a run in the database is associated with this job or
        # that we are going to commit a run to the db in here
        assert len(view) == 1 or self._run_before_commit is not None,  'No run associated with job %s'%self.id
        if self._run_before_commit is not None:
            self._run_before_commit.commit(db, self)
            self._run_before_commit = None
        self.store(db)
  
    def add_parent(self, parent):
        parent.add_child(self)
    
    def get_task(self, db):
        view = GridjobModel.view_by_children(db)
        task_id=view[self.id]
        return GridtaskModel.load(db, task_id)
    
    @staticmethod
    def view_by_job(db):
        return GridjobModel.my_view(db, 'by_job')
    
    def get_children(self, db):
        job_list = list()
        for job_id in children:
            job_list.append(GridjobModel.load(db, job_id))
        return tuple(job_list)

    def get_run(self, db):
        if self._run_before_commit is None:
            a_view = GridrunModel.view_by_job(db, self.id)
            assert len(a_view) == 1, 'No run found. Does job %s have a run associated with it?'%job_id
            return a_view.view.wrapper(a_view.rows[0])
        else:
            return self._run_before_commit
        
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
            viewnames=('by_job', 'by_author')
            return viewnames
        else:
            by_job = ViewDefinition(cls.VIEW_PREFIX, 'by_job', map_func_job, wrapper=cls, language='python')
            by_author = ViewDefinition(cls.VIEW_PREFIX, 'by_author', map_func_author, wrapper=cls, language='python')
            views=[by_job, by_author]
            ViewDefinition.sync_many( db,  views)
        return views
