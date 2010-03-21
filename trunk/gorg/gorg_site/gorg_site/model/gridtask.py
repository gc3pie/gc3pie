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

oldddd = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'GridjobModel':
            if doc['sub_type'] == 'TASK':
                if doc['owned_by_task']:                        
                    yield (doc['_id'], doc['owned_by_task']), doc
                else:
                    yield (doc['_id'], doc['_id']), doc
            if doc['sub_type'] == 'JOB':
                yield (doc['owned_by_task'], doc['_id']) , doc
    '''

class GridtaskModel(BaseroleModel):
    SUB_TYPE = 'GridtaskModel'
    VIEW_PREFIX = 'GridtaskModel'
    def create(self, author, title):
        self.id = GridtaskModel.generate_new_docid()
        self.sub_type = self.SUB_TYPE
        self.author = author
        self.title = title
        return self
    
    def commit(self, db):
        # Make sure that a job in the database is associated with this task
        view = GridjobModel.view_by_task(db, self.id)
        assert len(view) > 0,  'No job in database associated with task %s'%self.id
        self.store(db)
        
    def get_jobs(self, db):
        job_view = GridjobModel.view_by_task(db, self.id)
        job_list = list()
        for a_job in job_view:
            job_list.append(a_job)
        return tuple(job_list)

    def get_status(self, db):
        """Returns the overall status of this task."""
        job_view = GridjobModel.view_by_task(db, self.id)
        status_list = list()
        for a_job in job_view:            
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
    def stat_():
        pass
    
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
            viewnames=('by_task','by_author')
            return viewnames
        else:
            by_task = ViewDefinition(cls.VIEW_PREFIX, 'by_task', map_func_task, wrapper=cls, language='python')
            by_author = ViewDefinition(cls.VIEW_PREFIX, 'by_author', map_func_author, wrapper=cls, language='python')
            views=[by_task, by_author]
            ViewDefinition.sync_many( db,  views)
        return views

def commit_all(db, a_task, job_list):
    for a_job in job_list:
        a_job.commit(db)
    a_task.commit(db)
