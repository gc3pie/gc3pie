import logging

from pylons.controllers.util import abort, redirect_to
from pylons.controllers import XMLRPCController

from gorg_site.model.gridjob import GridjobModel
import cPickle

log = logging.getLogger(__name__)

class XmlgridtaskController(XMLRPCController):

    def index(self):
        # Return a rendered template
        #return render('/xmlgridtask.mako')
        # or, return a response
        return 'Hello World, I am XmlgridtaskController'

    def add_job(self, id, my_parent=None):
        task = GridjobModel.load(id)
        if not id in task.job_relations:
            task.job_relations[id]=list()
        if my_parent:
            task.job_relations[my_parent].append(id)
        task.save()
        return XmlgridtaskController._pickle_object(task)
    retrieve.signature = [['string','string', 'string']]

    def retrieve(self, id):
        '''Retrieves a job from the database.
        The job is pickled then returned.'''
        task = GridtaskModel.load(id)
        return XmlgridtaskController._pickle_object(task)
    retrieve.signature = [['string', 'string']]
    
    def status(self, id):
        pass
    
    @staticmethod
    def _pickle_object(obj):
        import cPickle  
        return cPickle.dumps(obj)
