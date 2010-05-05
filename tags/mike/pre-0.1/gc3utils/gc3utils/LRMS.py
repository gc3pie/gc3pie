class LRMS:

    def __init__(self, resource): 
        abstract 
    
    def CheckAuthentication(self): 
        abstract
    
    def SubmitJob(self, unique_token, application, input_file): 
        """
        return LRMS specific lrms_jobid
        stages input files if necessary
        dumps submission stdout to lrms_log string
        """
        abstract

    def CheckStatus(self,lrms_jobid): 
        """
        Check the status of a single job.
        Return either 'FINISHED' or 'RUNNING'.
        """
        # todo : also return ERROR?
        abstract

    def GetResults(self,lrms_jobid,job_dir): 
        """
        Retrieve results from a single job.
        Return a list containing 2 elements: True/False, output
        """
        abstract

    def KillJob(self,lrms_jobid,job_dir): 
        """
        Kill a single job at the LRMS level.
        Return output of delete command.
        """
        abstract

    def GetResourceStatus(self): 
        """
        This method should return an object of type Resource
        containing a dictionary with resource information.
        """
        abstract
    
