#import logging
#
#from pylons.controllers.util import abort, redirect_to
#from pylons.controllers import XMLRPCController
#
#from gorg_site.model.gridjob import GridjobModel
#import cPickle
#
#log = logging.getLogger(__name__)
#
#class XmlgridtaskController(XMLRPCController):
#    def __init__(self):
#        XMLRPCController.__init__(self)
#        # The max size of the xml the server can receive server
#        # Must be increased here to handle file transfers 
#        # over the xml-rcp call.
#        self.max_body_length= config.get('xml_controller.max_xml_size')
#        
#    def index(self):
#        # Return a rendered template
#        #return render('/xmlgridtask.mako')
#        # or, return a response
#        return 'Hello World, I am XmlgridtaskController'
#
#    def add_job(self, id, my_parent=None):
#        task = GridjobModel.load(id)
#        if not id in task.job_relations:
#            task.job_relations[id]=list()
#        for a_parent in my_parent:
#            task.job_relations[a_parent].append(id)
#        task.save()
#        return XmlgridtaskController._pickle_object(task)
#    add_job.signature = [['string','string'], 
#                                       ['string','string', 'struct']]
#
#    def retrieve(self, id):
#        '''Retrieves a job from the database.
#        The job is pickled then returned.'''
#        task = GridtaskModel.load(id)
#        return XmlgridtaskController._pickle_object(task)
#    retrieve.signature = [['string', 'string']]
#    
#    def status(self, id):
#        pass
#    
#    @staticmethod
#    def _pickle_object(obj):
#        import cPickle  
#        return cPickle.dumps(obj)
