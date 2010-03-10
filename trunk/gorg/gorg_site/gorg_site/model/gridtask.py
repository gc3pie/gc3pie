from couchdb import schema as sch
from couchdb import client as client
from gridjob import GridjobModel

import time

map_func_all = '''
    def mapfun(doc):
        if 'type' in doc:
            if doc['type'] == 'GridtaskModel':
                yield None, doc
    '''
map_func_status = '''
    def mapfun(doc):
        if 'type' and 'status' in doc:
            if doc['type'] == 'GridtaskModel':
                yield doc['status'], doc
    '''
map_func_author = '''
    def mapfun(doc):
         if 'type' and 'author' in doc:
            if doc['type'] == 'GridtaskModel':
                yield doc['author'], doc
    '''
map_func_title = '''
    def mapfun(doc):
        if 'type' and 'title' in doc:
            if doc['type'] == 'GridtaskModel':
                yield doc['title'], doc
    '''
# You can not use a view to retrieve all the jobs associated with a task.
# The key must be the task key, so that you can filter on it. But you
# can only match a job using the job key. Therefore you need to associate the 
# task key with each job key, and then use the job key to get the job. If the
# job had a reference to the task, it would work, but we do not want that.
map_func_task_owns = '''
    def mapfun(doc):
        if 'type' and 'title' in doc:
            if doc['type'] == 'GridtaskModel':
                yield doc['_id'], doc['job_relations']
            if doc['type'] == 'GridjobModel':
                yield doc['_id'], doc
    '''
reduce_func_task_owns ='''
    def reducefun(keys, values, rereduce=False):
        return keys
    '''
''' task_id = None
        loop_it=values.__iter__()
        for value in loop_it:
            if value['type'] == 'GridtaskModel':
                task_id=values['_id']
                return [task_id, loop_it.next()]'''
class GridtaskModel(sch.Document):       
    VIEW_PREFIX = 'gridtask'
    POSSIBLE_STATUS = ('READY', 'WAITING','RUNNING','RETRIEVING','FINISHED', 'DONE','ERROR')
    author = sch.TextField()
    title = sch.TextField()
    dat = sch.DateTimeField(default=time.gmtime())
    type = sch.TextField(default='GridtaskModel')
    # Type defined by the user
    defined_type = sch.TextField(default='GridtaskModel')
    # Each new entry is added as a parent.
    # A parent may or may not have childern
    # A parent without childern has an empty list
    job_relations = sch.DictField()
    status = sch.TextField(default = 'READY')

    def add_job(self, a_job, my_parent=tuple()):
        assert a_job.id,  'Job must first be saved to the database before being added to a task.'
        if not a_job.id in self.job_relations:
            self.job_relations[a_job.id]=list()
        # If the user only passes in a single parent, we should handle it well
        if not hasattr(my_parent,'__iter__'):
            my_parent = tuple(my_parent)
        for a_parent in my_parent:
            self.job_relations[a_parent].append(a_job.id)
    
    def get_jobs(self, db):
        job_list=list()
        for a_job_id in self.job_relations:
            a_job = GridjobModel.load(db, a_job_id)
            job_list.append(a_job)
        return tuple(job_list)
            
    def __setattr__(self, name, value):
        if name == 'status':
            assert value in self.POSSIBLE_STATUS, 'Invalid status. \
            Only the following are valid, %s'%(' ,'.join(self.POSSIBLE_STATUS))
        super(GridtaskModel, self).__setattr__(name, value)
    
    @classmethod
    def view(cls, db, viewname, **options):
        from couchdb.design import ViewDefinition
        viewnames = cls.sync_views(db, only_names=True)
        assert viewname in viewnames
        a_view = super(cls, cls).view(db, '%s/%s'%(cls.VIEW_PREFIX, viewname), **options)
        #a_view=.view(db, 'all/%s'%viewname, **options)
        return a_view
    
    @classmethod
    def sync_views(cls, db,  only_names=False):
        from couchdb.design import ViewDefinition
        if only_names:
            viewnames=('all', 'by_author', 'by_status', 'by_title', 'get_jobs')
            return viewnames
        else:
            all = ViewDefinition(cls.VIEW_PREFIX, 'all', map_func_all, wrapper=cls, language='python')
            by_author = ViewDefinition(cls.VIEW_PREFIX, 'by_author', map_func_author, wrapper=cls, language='python')
            by_status = ViewDefinition(cls.VIEW_PREFIX, 'by_status', map_func_status, wrapper=cls,  language='python')
            by_title = ViewDefinition(cls.VIEW_PREFIX, 'by_title', map_func_title, wrapper=cls, language='python')
            #get_jobs = ViewDefinition(cls.VIEW_PREFIX, 'get_jobs', map_func_task_owns, reduce_fun=reduce_func_task_owns, wrapper=cls, language='python')
            
            views=[all, by_author, by_status, by_title]
            ViewDefinition.sync_many( db,  views)
        return views
