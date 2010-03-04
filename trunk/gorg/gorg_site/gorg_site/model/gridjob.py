from couchdb import schema as sch
from couchdb import client as client
import time

map_func_all = '''
    def mapfun(doc):
        if 'type' in doc:
            if doc['type'] == 'GridjobModel':
                yield None, doc
    '''
map_func_status = '''
    def mapfun(doc):
        if 'type' and 'status' in doc:
            if doc['type'] == 'GridjobModel':
                yield doc['status'], doc
    '''
map_func_author = '''
    def mapfun(doc):
         if 'type' and 'author' in doc:
            if doc['type'] == 'GridjobModel':
                yield doc['author'], doc
    '''
map_func_title = '''
    def mapfun(doc):
        if 'type' and 'title' in doc:
            if doc['type'] == 'GridjobModel':
                yield doc['title'], doc
    '''

class GridjobModel(sch.Document):
    POSSIBLE_STATUS = ('FINISHED',  'SUBMITTED', 'ERROR', 'RUNNING', 'RETRIEVING')
    author = sch.TextField()
    title = sch.TextField()
    dat = sch.DateTimeField(default=time.gmtime())
    type = sch.TextField(default='GridjobModel')
    status = sch.TextField(default = 'SUBMITTED')
    # Type defined by the user
    defined_type = sch.TextField(default='GridjobModel')
    run_params = sch.DictField(default=dict(selected_resource='',  cores=None, memory=None, walltime=None))
      
#    def __init__(self, author=None, title=None, defined_type=None):
#        super(Gridjobdata, self).__init__()
#        self.author = author
#        self.title = title
#        self.defined_type = defined_type
    
    def __setattr__(self, name, value):
        if name == 'status':
            assert value in self.POSSIBLE_STATUS, 'Invalid status. \
            Only the following are valid, %s'%(' ,'.join(POSSIBLE_STATUS))
        super(GridjobModel, self).__setattr__(name, value)
    
    def set_run_param(self, param, value):
        assert param in self.run_params, 'Invalid run parameter. \
        Only the following are valid, %s'%(' ,'.join(self.run_params))
        self.run_params[param]=value
    
    def  put_attachment(self, db, content, filename=None, content_type=None):
        # The doc needs to be in the database before we can attach anything to it
        if not self.id:
            self.store(db)
        return db.put_attachment(self, content, filename, content_type)
        
    def get_attachment(self, db, filename, default=None):
        return db.get_attachment(self, filename, default)
    
    def delete_attachment(self, db, filename):
        return db.delete_attachment(doc, filename)
    
    @classmethod
    def view(cls, db, viewname, **options):
        from couchdb.design import ViewDefinition
        viewnames = cls.sync_views(db, only_names=True)
        assert viewname in viewnames
        a_view = super(cls, cls).view(db, 'all/%s'%viewname, **options)
        #a_view=.view(db, 'all/%s'%viewname, **options)
        return a_view
    
    @staticmethod
    def sync_views(db,  only_names=False):
        from couchdb.design import ViewDefinition
        if only_names:
            viewnames=('all', 'by_author', 'by_status', 'by_title')
            return viewnames
        else:
            all = ViewDefinition('all', 'all', map_func_all, wrapper=GridjobModel, language='python')
            by_author = ViewDefinition('all', 'by_author', map_func_author, wrapper=GridjobModel, language='python')
            by_status = ViewDefinition('all', 'by_status', map_func_status, wrapper=GridjobModel,  language='python')
            by_title = ViewDefinition('all', 'by_title', map_func_title, wrapper=GridjobModel, language='python')
            views=[all, by_author, by_status, by_title]
            ViewDefinition.sync_many( db,  views)
        return views
    
