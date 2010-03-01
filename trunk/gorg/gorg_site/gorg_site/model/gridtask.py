from couchdb import schema as sch
from couchdb import client as client

import time

from gorg_site.lib.mydb import Mydb
from gorg_site.model.gridjob import GridjobModel

class GridtaskModel(sch.Document):   
    from couchdb import schema as sch
    import time
    author = sch.TextField()
    title = sch.TextField()
    dat = sch.DateTimeField(default=time.gmtime())
    type = sch.TextField(default='GridtaskModel')
    # Type defined by the user
    user_type = sch.TextField(default='GridtaskModel')
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
    
    def add_job(self, job_id, my_parent=None):
        if not id in self.job_relations:
            self.job_relations[job_id]=list()
        for a_parent in my_parent:
            self.job_relations[a_parent].append(job_id)
        self.save()
    
    def create(self, title,  author,  user_type):
        '''This method is used to create a new task and 
        save it to the database.
        
        myfile is a file like object that will be attached to the 
        job using the attach_name its the key.
        '''        
        self.title = title
        self.author = author
        self.user_type = user_type
        self.save()

    @staticmethod
    def load(id):
        mydb = Mydb()
        db = mydb.cdb()
        return sch.Document.load(db, id);
