import logging

from pylons.controllers.util import abort, redirect_to
from pylons.controllers import XMLRPCController

from gorg_site.model.gridjob import GridjobModel

log = logging.getLogger(__name__)

class XmlgridtaskController(XMLRPCController):

    def index(self):
        # Return a rendered template
        #return render('/xmlgridtask.mako')
        # or, return a response
        return 'Hello World, I am XmlgridtaskController'
        
    
