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
        """
        Submit a single job.
        Return a Job object.
        """
        
    def check_status(self, job):  abstract
        """
        Check the status of a single job.
        Return a Job object.
        """
        
    def get_results(self, job): abstract
        """
        Retrieve results from a single job.
        Return a Job object.
        """
        
    def cancel_job(self, job): abstract
        """
        Cancel a single running job.
        Return a Job object.
        """
    
    def get_resource_status(self): abstract
        """
        Get the status of a single resource.
        Return a Resource object.
        """
        
    def tail(self, std_type): abstract
        """
        todo : not permanent yet
        Gets the output of a running job, similar to ngcat.
        Return Job object.
        
        examples:
        print Job.stdout
        print Job.stderr
        
        """
        
    def is_valid(): abstract
        """
        Determine if a provided LRMS instance is valid.
        Returns True or False.
        """
