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
    
    def submit_job(self, application):
        """
        Submit a single job.
        Return a Job object.
        """
        raise NotImplementedError("Abstract method `submit_job()` called - this should have been defined in a derived class.")

    def check_status(self, job):
        """
        Check the status of a single job.
        Return a Job object.
        """
        raise NotImplementedError("Abstract method `check_status()` called - this should have been defined in a derived class.")
    
    def get_results(self, job):
        """
        Retrieve results from a single job.
        Return a Job object.
        """
        raise NotImplementedError("Abstract method `get_results()` called - this should have been defined in a derived class.")
    
    def cancel_job(self, job):
        """
        Cancel a single running job.
        Return a Job object.
        """
        raise NotImplementedError("Abstract method `cancel_job()` called - this should have been defined in a derived class.")
    
    def get_resource_status(self):
        """
        Get the status of a single resource.
        Return a Resource object.
        """
        raise NotImplementedError("Abstract method `get_resource_status()` called - this should have been defined in a derived class.")
    
    def tail(self, std_type):
        """
        todo : not permanent yet
        Gets the output of a running job, similar to ngcat.
        Return Job object.
        
        examples:
        print Job.stdout
        print Job.stderr
        
        """
        raise NotImplementedError("Abstract method `tail()` called - this should have been defined in a derived class.")
    
    def is_valid():
        """
        Determine if a provided LRMS instance is valid.
        Returns True or False.
        """
        raise NotImplementedError("Abstract method `is_valid()` called - this should have been defined in a derived class.")
