import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from gorg_site.lib.base import BaseController, render
from gorg_site.model.gridjob import GridtaskModel

log = logging.getLogger(__name__)

class GridtaskController(BaseController):

    def index(self):
        # Return a rendered template
        #return render('/gridtask.mako')
        # or, return a response
        return 'Hello World'

    def submit_form(self):
        return render('/submit_task_form.mako')

    def create(self):
        new_task = GridtaskModel()
        myfile = request.POST['myfile']
        title = request.POST['title']
        author = request.POST['author']
        user_type = 'GAMESS'
        new_task.create(title, title, user_type)
        c.mess = 'Successfully uploaded: %s, title: %s' % \
                (myfile, title)
        return render('/submit_task_finish.mako')
