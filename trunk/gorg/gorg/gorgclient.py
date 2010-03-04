import os
import sys
from gorg_site.gorg_site.model.gridjob import GridjobModel
from gorg_site.gorg_site.lib.mydb import Mydb

def main():

    db=Mydb('gorg_site','http://127.0.0.1:5984').cdb()
    a_job = GridjobModel()
    GridjobModel.sync_views(db)
    a_job.author = 'mark'
    a_job.status = 'SUBMITTED'
    a_job.defined_type = 'GAMESS'
    a_view=GridjobModel.view(db, 'all')
    myfile =  open('/home/mmonroe/Desktop/python-crontab-0.9.2.tar.gz', 'rb')
    a_job.put_attachment(db, myfile, 'a zip')
    myfile.close()
    print 'saved small job to db'
    
if __name__ == "__main__":
    main()

