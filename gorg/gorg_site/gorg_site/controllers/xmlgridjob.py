import logging

from pylons.controllers.util import abort, redirect_to
from pylons.controllers import XMLRPCController
from pylons.config import config

from gorg_site.model.gridjob import GridjobModel

log = logging.getLogger(__name__)

class XmlgridjobController(XMLRPCController):
    
    def __init__(self):
        XMLRPCController.__init__(self)
        # The max size of the xml the server can receive server
        # Must be increased here to handle file transfers 
        # over the xml-rcp call.
        self.max_body_length= config.get('xml_controller.max_xml_size')
        
    def index(self):
        # Return a rendered template
        #return render('/xmlgridjob.mako')
        # or, return a response
        return 'Hello World, I am XmlgridjobController' 
    index.signature = [['string']]
    
    def create(self, title,  author,  attach_name, myfile):
        '''This method is used to create a new task and 
        save it to the database.'''        
        new_job = GridjobModel()            
        new_job.title = title
        new_job.author = author
        new_job.save()
        new_job.attach(attach_name, myfile)      
#        newfile=open('/home/mmonroe/uploads/%s'%attach_name, 'wb')
#        newfile.write(myfile.data)
#        newfile.close()
        return ('Successfully create a new job')
    create.signature = [['string','string','string', 'string', 'base64']]
    
    def retrieve(self, id):
        '''Retrieves a job from the database.
        The job is pickled then returned.'''
        job = GridjobModel.load(id)
        return XmlgridjobController._pickle_object(job)
    retrieve.signature = [['string', 'string']]
    
    @staticmethod
    def _pickle_object(obj):
        import cPickle  
        return cPickle.dumps(obj)
