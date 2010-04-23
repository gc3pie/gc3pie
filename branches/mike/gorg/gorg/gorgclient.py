import os
import sys

from gorg.model.gridjob import *
from gorg.model.baserole import BaseroleModel
from gorg.model.gridtask import GridtaskModel, TaskInterface

from gorg.lib.utils import Mydb, create_file_logger, write_to_file

def main():
    # We add a job to our database lke this
    create_file_logger(10)

    db=Mydb('mark','gorg_site','http://130.60.144.211:5984').createdatabase()
    db=Mydb('mark','gorg_site','http://130.60.144.211:5984').cdb()
    BaseroleModel.sync_views(db)
    GridjobModel.sync_views(db)
    GridrunModel.sync_views(db)
    GridtaskModel.sync_views(db)

#    a_task = TaskInterface(db)
#    a_task = a_task.create('a title')
#    a_task.user_data_dict['me']=12
#    a_task.user_data_dict['me']
#    myfile =  open('./gorg/examples/exam01.inp', 'rb')
#    for i in range(5):
#        a_job = JobInterface(db)
#        a_job = a_job.create('a title', 'myparser', myfile)
#        myfile.seek(0)
#        a_task.add_child(a_job)
#    a_task.status_overall
#    a_job.run
#    a_job.task
#    parent = JobInterface(db)
#    parent = parent.create('a title', 'myparser', myfile)
#    parent.add_child(a_job)
#    
#    view = GridtaskModel.view_author(db)
#    
#    me = JobInterface(db)
#    me = JobInterface(db).load(a_job.id)
#    me.run.attachments_to_files(db)
#    myfile.close()
#
#    print 'saved small job to db'    
    
    
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
