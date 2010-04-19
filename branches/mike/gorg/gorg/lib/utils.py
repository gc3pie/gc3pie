from couchdb import client

class Mydb(object):
    def __init__(self, couchdb_user, couchdb_database, couchdb_url):
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
