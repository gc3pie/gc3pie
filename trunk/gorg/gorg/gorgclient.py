import os
import sys
sys.path.append('/home/mmonroe/apps/gorg')
from gorg_site.gorg_site.model.gridjob import GridjobModel, JobInterface
from gorg_site.gorg_site.model.baserole import BaseroleModel
from gorg_site.gorg_site.model.gridtask import GridtaskModel, TaskInterface
from gorg_site.gorg_site.model.gridrun import GridrunModel

from gorg_site.gorg_site.lib.mydb import Mydb

def main():
    # We add a job to our database lke this
    db=Mydb('gorg_site','http://127.0.0.1:5984').createdatabase()
    db=Mydb('gorg_site','http://127.0.0.1:5984').cdb()
    GridjobModel.sync_views(db)
    GridrunModel.sync_views(db)
    GridtaskModel.sync_views(db)
    BaseroleModel.sync_views(db)

    a_task = TaskInterface(db)
    a_task = a_task.create('mark', 'hope')
    a_job = JobInterface(db)
    myfile =  open('/home/mmonroe/apps/ase-patched/exam01.inp', 'rb')
    a_job = a_job.create('mark', 'hope', myfile)
    myfile.seek(0)
    a_job.run
    parent = JobInterface(db)
    parent = parent.create('mark', 'hope', myfile)
    parent.add_child(a_job)
    
    me = JobInterface(db)
    me.a_job = GridjobModel.load_job(db,a_job.a_job.id)
    myfile.close()

    print 'saved small job to db'    
    
    
if __name__ == "__main__":
    main()
    sys.exit()

'''
from couchdb import schema as sch
from couchdb import client as client

class MyMatrix(sch.Document):
    np_matrix = sch.ListField(sch.ListField(sch.FloatField))

from numpy import *
a_matrix=array([[1,2,3],[1,2,5]])
array([[1,2,3],[1,2,5]]).tolist()
array([[1,2,3],[1,2,5]]).tofile()
array([[1,2,3],[1,2,5]]).dumps()

big_list=[]
small_list=[]
for i in range(1000):
    small_list.append(i)

for i in range(1000):
    big_list.append(small_list)
'''
