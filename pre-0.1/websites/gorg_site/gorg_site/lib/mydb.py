from pylons.config import config
from couchdb import client

class Mydb(object):
    def __init__(self, couchdb_user=None, couchdb_database=None, couchdb_url =None):
        if not couchdb_database:
            couchdb_database = config.get('database.name')
        if not couchdb_url:
            couchdb_url = config.get('database.url')
        if not couchdb_user:
            couchdb_user = config.get('database.username')
        assert couchdb_database,  'No database name found.'
        assert couchdb_url, 'No database url found.'
        self.couchdb_database = couchdb_database        
        self.couchdb_url = couchdb_url
        self.username = couchdb_user
    
    def cdb(self):
        db = client.Server( self.couchdb_url )[self.couchdb_database]
        db.username = self.username
        return db

    def createdatabase(self):
        created = True
        try:
            client.Server( self.couchdb_url ).create(self.couchdb_database)
        except:
            created = False
        return created
