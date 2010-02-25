from couchdb import schema as sch
from couchdb import client as client
from pylons.config import config

import time

class GridtaskModel(sch.Document):   
    author = sch.TextField()
    title = sch.TextField()
    dat = sch.DateTimeField(default=time.gmtime())
    type = sch.TextField() 
    job_relations = sch.ListField( sch.DictField(sch.Schema.build(
                                                                  parent = sch.TextField(), 
                                                                  child = sch.ListField(sch.TextField())
                                                                  ))) 
    
