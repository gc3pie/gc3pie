from pylons.config import config
from couchdb import client

class DatabaseConnection(object):   
    
    def __init__(self, couchdb_database=None, couchdb_url=None):    
        mydb=Mydb(couchdb_database, couchdb_url)
        mydb.createdatabase()
        self.db = mydb.cdb()
        
    def save(self, model_obj):
        model_obj.store(self.db)
    
    def syn_views(self, model_obj):
        from couchdb.design import ViewDefinition
        ViewDefinition.sync_many( self.db,  model_obj.couchdb_views())

    def attach(self, model_obj, name,  file):
        if not model_obj.id:
            self.save(model_obj)
        self.db.put_attachment(model_obj, file, name)
       
    def load(self, model_obj, id):
        return model_obj.load(self.db, id)
    
    def run_view(self, model_obj, viewname):
        '''See the model_object for possible views for all posible view names'''
        return model_obj.run_view(self.db, viewname)
        

class Mydb(object):
    def __init__(self, couchdb_database=None, couchdb_url =None):
        if not couchdb_database:
            couchdb_database = config.get('database.name')
        if not couchdb_url:
            couchdb_url = config.get('database.url')
        self.couchdb_database = couchdb_database
        self.couchdb_url = couchdb_url
    
    def cdb(self):
            return client.Server( self.couchdb_url )[self.couchdb_database]

    def createdatabase(self):
        created = True
        try:
            client.Server( self.couchdb_url ).create(self.couchdb_database)
        except:
            created = False
        return created
