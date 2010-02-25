import logging

from pylons.controllers.util import abort, redirect_to
from pylons.controllers import XMLRPCController

from gorg_site.model.gridjob import GridjobModel

log = logging.getLogger(__name__)

class XmlgridjobController(XMLRPCController):

    def index(self):
        # Return a rendered template
        #return render('/xmlgridjob.mako')
        # or, return a response
        return 'Hello World, I am XmlgridjobController' 
    index.signature = [['string']]
    
    def upload(self, myfile,  title,  author,  type):
        '''This method is used to upload a file to the database.'''
        
        new_job = GridjobModel()            
        new_job.title = title
        new_job.author = author
        new_job.type = type            
        new_job.attach(myfile.name, myfile.file)            
        myfile.file.close()
        return ('Successfully uploaded: %s, title: %s' % \
            (myfile, title))
    upload.signature = [['string','string','string', 'string', 'string']]
    
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
