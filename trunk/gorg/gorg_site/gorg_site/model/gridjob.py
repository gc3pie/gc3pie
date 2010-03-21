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
map_func_by_task = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'BaseroleModel':
            if doc['sub_type'] == 'GridjobModel':
                yield doc['owned_by_task'], doc
    '''

class GridjobModel(BaseroleModel):
    SUB_TYPE = 'GridjobModel'
    VIEW_PREFIX = 'GridjobModel'

    owned_by_task = sch.TextField()
    owned_by_parent = sch.ListField(sch.TextField())
    
    _run_before_commit = None
    
    def create_run(self, db, files_to_run, application_to_run='gamess', 
                   selected_resource='ocikbpra',  cores=2, memory=1, walltime=-1):
        self._run_before_commit = GridrunModel().create( db, files_to_run, self, application_to_run, 
                            selected_resource,  cores, memory, walltime)
        return self._run_before_commit

    def create(self, author, title, a_task):
        self.id = GridjobModel.generate_new_docid()
        self.sub_type = self.SUB_TYPE
        self.author = author
        self.title = title
        self.owned_by_task = a_task.id
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
    
    def add_parents(self, my_parents):
        # If the user only passes in a single parent, we should handle it well
        if not isinstance(my_parents,tuple) and not isinstance(my_parents,list):
            my_parents = [my_parents]
        for a_parent in my_parents:
            if not a_parent.id in self.owned_by_parent:
                self.owned_by_parent.append(a_parent.id)
    
#    def get_parents(self, db):
#        my_parents=list()
#        for a_parent_id in self.owned_by_parent:
#            a_parent = GridjobModel.load(db, a_parent_id)
#            my_parents.append(a_parent)
#        return tuple(my_parents)
    
    @staticmethod
    def view_by_task(db, task_id):
        return GridjobModel.my_view(db, 'by_task', key=task_id)
    
    def get_parents(self, db):
        a_view = GridjobModel.my_view(db, 'by_job', keys=self.owned_by_parent)
        parents = list()
        for a_parent in a_view:
            parents.append(a_parent)
        return tuple(parents)

    def get_task(self, db):
        return GridtaskModel.load(db, self.owned_by_task)

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
            viewnames=('by_job', 'by_task')
            return viewnames
        else:
            by_job = ViewDefinition(cls.VIEW_PREFIX, 'by_job', map_func_job, wrapper=cls, language='python')
            by_task = ViewDefinition(cls.VIEW_PREFIX, 'by_task', map_func_by_task, wrapper=cls, language='python')
            views=[by_job, by_task]
            ViewDefinition.sync_many( db,  views)
        return views
