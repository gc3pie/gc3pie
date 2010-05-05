import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from gorg_site.lib.base import BaseController, render

log = logging.getLogger(__name__)

class DirectorController(BaseController):

    def index(self):
        c.title = 'Greetings'
        c.heading = 'Sample Page'
        return render('/derived/login.html')
    
    def view_user_overview(self):
        author = request.GET['author']
        session['author']=author
        session.save()
        return render('/derived/user_overview.html')
        
        
