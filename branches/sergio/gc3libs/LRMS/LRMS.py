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
        raise NotImplementedError("Abstract method `LRMS.submit_job()` called - this should have been defined in a derived class.")

    def check_status(self, job):
        """
        Check the status of a single job.
        Return a Job object.
        """
        raise NotImplementedError("Abstract method `LRMS.check_status()` called - this should have been defined in a derived class.")
    
    def get_results(self, job):
        """
        Retrieve results from a single job.
        Return a Job object.
        """
        raise NotImplementedError("Abstract method `LRMS.get_results()` called - this should have been defined in a derived class.")
    
    def cancel_job(self, job):
        """
        Cancel a single running job.
        Return a Job object.
        """
        raise NotImplementedError("Abstract method `LRMS.cancel_job()` called - this should have been defined in a derived class.")
    
    def get_resource_status(self):
        """
        Get the status of a single resource.
        Return a Resource object.
        """
        raise NotImplementedError("Abstract method `LRMS.get_resource_status()` called - this should have been defined in a derived class.")
    
    def tail(self, job, remote_filename):
        """
        Gets the output of a running job, similar to ngcat.
        Return Job object.
        
        examples:
        print Job.stdout
        print Job.stderr
                                                
        Copy a remote file belonging to the job sandbox and return the content of the file as a string.
        Primarly conceived for stdout and stderr.
        Any exception raised by operations will be passed through.
        @param job: the job object
        @type job: gc3utils.Job
        @param remote_filename: the remote file to copy
        @type remote_filename: string
        @since: 0.2
        """
        raise NotImplementedError("Abstract method `LRMS.tail()` called - this should have been defined in a derived class.")
    
    def is_valid(self):
        """
        Determine if a provided LRMS instance is valid.
        Returns True or False.
        """
        raise NotImplementedError("Abstract method `LRMS.is_valid()` called - this should have been defined in a derived class.")
