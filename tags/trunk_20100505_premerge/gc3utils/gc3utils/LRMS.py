class LRMS:

    def __init__(self, resource): 
        if resource.has_key('transport'):
            if resource.transport is 'local':
                transport = LocalTransport()
            elif resource.transport is 'ssh':
                transport = SshTransport()
            elif resource.transport is 'globus':
                transport = GlobusTransport()
            else:
                raise TransportException('Could not initialize transport')
        else:
            raise LRMSException('Invalid resource description: missing transport')
    
    def submit_job(self, application): abstract

    def check_status(self, job):  abstract

    def get_results(self, job): abstract
    
    def cancel_job(self, job): abstract
    
    def get_resource_status(self): abstract
        # this method should return an object of type Resource
        # containing a dictionary with resource information
        
    def tail(self, std_type): abstract

    def is_valid(): abstract
