import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from gorg_site.lib.base import BaseController, render
from gorg_site.controllers.xmlgridtask import XmlgridtaskController

log = logging.getLogger(__name__)

class GridtaskController(BaseController):

    def index(self):
        # Return a rendered template
        #return render('/gridtask.mako')
        # or, return a response
        return 'Hello World'

    def submit_form(self):
        return render('/submit_task_form.mako')

    def upload(self):
        xmlController = XmlgridtaskController()        
        myfile = request.POST['myfile']
        title = request.POST['title']
        author = request.POST['author']
        type = 'GAMESS'
        xmlController.upload(myfile,  title,  author,  type)
        c.mess = 'Successfully uploaded: %s, title: %s' % \
                (myfile, title)
        return render('/submit_task_finish.mako')
