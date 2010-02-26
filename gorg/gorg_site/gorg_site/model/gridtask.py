from couchdb import schema as sch
from couchdb import client as client

import time

from gorg_site.lib.mydb import Mydb

class GridtaskModel(sch.Document):   
    from couchdb import schema as sch
    import time
    author = sch.TextField()
    title = sch.TextField()
    dat = sch.DateTimeField(default=time.gmtime())
    type = sch.TextField(default='GridtaskModel') 
    # Each new entry is added as a parent.
    # A parent may or may not have childern
    # A parent without childern has an empty list
    job_relations = sch.DictField()

    def save(self):
        mydb = Mydb()
        db = mydb.cdb()
        self.store(db)
    
    def attach(self, name,  file):
        mydb = Mydb()
        db = mydb.cdb()
        if not self.id:
            self.store(db)
        db.put_attachment(self, file, name)
    
    def status(self):
        task = GridtaskModel.load(id)
        pass
    
    @staticmethod
    def load(id):
        mydb = Mydb()
        db = mydb.cdb()
        return sch.Document.load(db, id);
