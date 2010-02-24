import logging

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect_to

from gorg_site.lib.base import BaseController, render

log = logging.getLogger(__name__)

class GridjobcontrollerController(BaseController):

    def index(self):
        # Return a rendered template
        #return render('/GridJobController.mako')
        # or, return a response
        return 'Hello World'
