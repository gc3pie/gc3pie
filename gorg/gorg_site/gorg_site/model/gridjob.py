from couchdb import schema as sch
from couchdb import client as client

from gorg_site.lib.mydb import Mydb

import time

class GridjobModel(object):   
    
    def __init__(self, couchdb_database=None, couchdb_url=None):    
        mydb=Mydb(couchdb_database, couchdb_url)
        if mydb.createdatabase() or True:
            from couchdb.design import ViewDefinition
            self.db=mydb.cdb()
            ViewDefinition.sync_many( self.db,  Gridjobdata.couchdb_views())
        else:
            self.db=mydb.cdb()

    def save(self, job_data):
        job_data.store(self.db)
    
    def attach(self, job_data, name,  file):
        if not job_data.id:
            job_data.store(self.db)
        self.db.put_attachment(job_data, file, name)
    
    def create(self, title,  author,  user_type,  attach_name, myfile):
        '''This method is used to create a new job and 
        save it to the database.
        
        myfile is a file like object that will be attached to the 
        job using the attach_name its the key.
        '''
        job_data = Gridjobdata(title, author, user_type)
        job_data.store(self.db)
        self.attach(job_data, attach_name, myfile)
        return job_data
    
    def load(self, id):
        return Gridjobdata.load(self.db, id)
    
    def view_all(self, maxnumber):
        return Gridjobdata.view_map_fun_all(self.db, maxnumber)


map_func_all = '''
    def function(doc):
        if doc.type == 'GridjobModel':
            emit(null, doc)
    '''
map_func_status = '''
    def function(doc):
        if doc.type == 'GridjobModel':
            emit(doc.status, doc)
    '''
map_func_author = '''
    def function(doc):
        if doc.type == 'GridjobModel':
            emit(doc.author, doc)
    '''
map_func_title = '''
    def function(doc):
        if doc.type == 'GridjobModel':
            emit(doc.title, doc)
    '''
    
STATUS = ('FINISHED',  'ERROR', 'RUNNING', 'RETRIEVING')

class Gridjobdata(sch.Document):   
    author = sch.TextField()
    title = sch.TextField()
    dat = sch.DateTimeField(default=time.gmtime())
    type = sch.TextField(default='GridjobModel')
    status = sch.TextField()
    # Type defined by the user
    user_type = sch.TextField(default='GridjobModel')
      
    def __init__(self, author=None, title=None, user_type=None):
        super(Gridjobdata, self).__init__()
        self.author = author
        self.title = title
        self.user_type = user_type
    
    @staticmethod
    def view_map_fun_all(db, maxnumber=None):
        from couchdb.design import ViewDefinition
            #a_view=Gridjobdata.view(db, 'all/all', limit=maxnumber)
        all = ViewDefinition('all', 'all', map_func_all, wrapper=Gridjobdata.wrap)
        a_view=all.view(db, 'all/all', limit=maxnumber)
        return a_view
    
    @staticmethod
    def couchdb_views():
        from couchdb.design import ViewDefinition
        views = list()
        map_func_all = '''
            function(doc){
                if (doc.type == 'GridjobModel')
                emit(null, doc);
            }
            '''
        all = ViewDefinition('all', 'all', map_func_all, wrapper=Gridjobdata.wrap,  language='python')
        by_author = ViewDefinition('all', 'by_author', map_func_author, wrapper=Gridjobdata.wrap,  language='python')
        by_status = ViewDefinition('all', 'by_status', map_func_status, wrapper=Gridjobdata.wrap,  language='python')
        by_title = ViewDefinition('all', 'by_title', map_func_title, wrapper=Gridjobdata.wrap,  language='python')
        views.append(all)
        return views
