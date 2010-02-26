import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to
from gorg_site.lib.base import BaseController, render
from gorg_site.controllers.xmlgridjob import XmlgridjobController
from pylons.decorators import jsonify
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
    
    @jsonify
    def submit_form(self):
        if request.environ['CONTENT_TYPE'] == 'application/json':
            return {'response':'I am json'}
        return render('/submit_job_form.mako')
    
    def upload(self):
        xmlController = XmlgridjobController()        
        myfile = request.POST['myfile']
        title = request.POST['title']
        author = request.POST['author']
        xmlController.create(title,  author,  myfile.name, myfile.file)
        c.mess = 'Successfully uploaded: %s, title: %s' % \
                (myfile, title)
        return c.mess #render('/submit_job_finish.mako')
   

