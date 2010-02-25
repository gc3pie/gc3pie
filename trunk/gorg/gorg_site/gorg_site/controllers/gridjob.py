import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to
from gorg_site.lib.base import BaseController, render
from pylons.controllers import XMLRPCController
from gorg_site.model.gridjob import GridjobModel

import os
import shutil

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
        xmlController = XMLGridjobController()        
        myfile = request.POST['myfile']
        title = request.POST['title']
        author = request.POST['author']
        type = 'GAMESS'
        xmlController.upload(myfile,  title,  author,  type)
        c.mess = 'Successfully uploaded: %s, title: %s' % \
                (myfile, title)
        return render('/upload_result.mako')
   
class XMLGridjobController(XMLRPCController):
        def upload(self, myfile,  title,  author,  type):
            new_job = GridjobModel()            
            new_job.title = title
            new_job.author = author
            new_job.type = type            
            new_job.attach(myfile.name, myfile.file)            
            myfile.file.close()
            return ('Successfully uploaded: %s, title: %s' % \
                (myfile, title), 201)
        upload.signature = [['string','string', 'string', 'string'],  
                              ['string', 'int']]
