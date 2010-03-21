import os
import sys
sys.path.append('/home/mmonroe/apps/gorg')
from gorg_site.gorg_site.model.gridjob import GridjobModel
from gorg_site.gorg_site.model.baserole import BaseroleModel
from gorg_site.gorg_site.model.gridtask import GridtaskModel, commit_all
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

    a_task = GridtaskModel().create('mark', 'a title')
    
    a_job = GridjobModel().create('mark', 'a title', a_task)
    myfile =  open('/home/mmonroe/apps/ase-patched/exam01.inp', 'rb')
    a_run = a_job.create_run(db, myfile)
    myfile.close()
    parent1=a_job
    
    a_job = GridjobModel().create('mark', 'a title', a_task)
    myfile =  open('/home/mmonroe/apps/ase-patched/exam01.inp', 'rb')
    a_job.add_parents(parent1)
    a_run = a_job.create_run(db, myfile)
    myfile.close()

    commit_all(db, a_task, (parent1, a_job))
    
    print a_job.get_run(db)
    print a_job.get_status(db)
    print a_task.get_jobs(db)
    print a_task.get_status(db)
    GridtaskModel.view_by_author(db, 'mark')

    print 'saved small job to db'    
    
    
if __name__ == "__main__":
    main()
    sys.exit()
