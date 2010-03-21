from couchdb import schema as sch
from couchdb.schema import  Schema
from gridrun import GridrunModel
from couchdb import client as client
import time
'''When you need to query for a key like this ('sad','saq') do this:
self.view(db,'who_task_owns',startkey=[self.id],endkey=[self.id,{}])
when you want to match a key ('sad','sad') do this:
a_job.view(db,'by_task_ownership',keys=[[a_task.id,a_task.id]])
'''

map_func_all = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'BaseroleModel':
            yield doc['_id'], doc
    '''
map_func_author = '''
def mapfun(doc):
     if 'base_type' in doc:
        if doc['base_type'] == 'BaseroleModel':
            yield doc['author'], doc
    '''
map_func_title = '''
def mapfun(doc):
    if 'base_type' and 'title' in doc:
        if doc['base_type'] == 'BaseroleModel':
            yield doc['title'], doc
    '''

class BaseroleModel(sch.Document):
    VIEW_PREFIX = 'BaseroleModel'
    author = sch.TextField()
    title = sch.TextField()
    dat = sch.DateTimeField(default=time.gmtime())
    # Type defined by the user
    base_type = sch.TextField(default='BaseroleModel')
    sub_type = sch.TextField()
    user_defined_type = sch.TextField(default='gamess')
    
    # This works like a dictionary, but allows you to go a_job.test.application_to_run='a app' as well as a_job.test['application_to_run']='a app'
    #test=sch.DictField(Schema.build(application_to_run=sch.TextField(default='gamess')))
    
    def create(self, db, author, title):
        pass
    
    def commit(self, db):
        pass
    
    @staticmethod
    def generate_new_docid():
        from uuid import uuid4
        return uuid4().hex
    
    def copy(self):
        import copy
        '''We want to make a copy of this job, so we can use the same setting to 
        create another job.'''
        new_record=copy.deepcopy(self)
        del new_record['_id']
        del new_record['_rev']
        return new_record

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
            viewnames=('all', 'by_author', 'by_title')
            return viewnames
        else:
            all = ViewDefinition(cls.VIEW_PREFIX, 'all', map_func_all, wrapper=cls, language='python')
            by_author = ViewDefinition(cls.VIEW_PREFIX, 'by_author', map_func_author, wrapper=cls, language='python')
            by_title = ViewDefinition(cls.VIEW_PREFIX, 'by_title', map_func_title, wrapper=cls, language='python')
            views=[all, by_author, by_title]
            ViewDefinition.sync_many( db,  views)
        return views
    
