import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to
from gorg_site.lib.base import BaseController, render

import os
import shutil

from gorg_site.model.gridjob import GridJobModel

log = logging.getLogger(__name__)
PERMANENT_STORE = '/home/mmonroe/uploads/'

class GridjobController(BaseController):

    def index(self):
        # Return a rendered template
        #return render('/gridjob.mako')
        # or, return a response
        return 'Hello World'

    def create(self):
        """Post / users: Create a new job in the database."""
        return None
    
    def submit_form(self):
        return render('/submit_form.mako')

    def upload(self):
        new_job = GridJobModel()
        myfile = request.POST['myfile']
        new_job.title = request.POST['title']
        new_job.author = request.POST['author']
        new_job.type = 'GAMESS'
        permanent_file = open(os.path.join(PERMANENT_STORE,
                                           myfile.filename.lstrip(os.sep)),'w')
        shutil.copyfileobj(myfile.file, permanent_file)
        myfile.file.close()
        permanent_file.close()
        
        return 'Successfully uploaded: %s, title: %s' % \
            (myfile.filename, request.POST['title'])


    GRID_RESOURCE='gc3'

    def batch_job_finished(filepath, jobID, logger):    
        job_status=myGcli.gstat(jobID)
        logger.info('Job status is %s'%(job_status))
        if job_status[1][0][1].find('FINISHED')  != -1:        
            myGcli.gget(jobID)
            shutil.copy( glob.glob('%s/*.dat'%jobID)[0], filepath)
            shutil.copy( glob.glob('%s/*.stdout'%jobID)[0], filepath)
            shutil.copy( glob.glob('%s/*.stderr'%jobID)[0], filepath)
            return True
        else:
            return False
