class Task:

    def __init__(self): 
        abstract 
    
    
    def SubmitTask(self, unique_token, application, input_file): 
        """
        Return Task id
        stages input files if necessary
        dumps submission stdout to lrms_log string
        """
        abstract

    def CheckStatus(self,taskid,detail_level): 
        """
        Check the status of a task.
        If detail_level is 0, return 'FINISHED' or 'RUNNING'.
        If detail_level is 1, return list of jobs and their status.
        """
        # todo : also return ERROR?
        abstract

    def GetResults(self,taskid,job_dir): 
        """
        Retrieve results from all finished jobs in a task.
        Return a list containing 2 elements: True/False, output
        """
        abstract

    def Kill(self,taskid,job_dir): 
        """
        Kill all jobs in a task at the LRMS level.
        Return nothing.
        """
        abstract

    def GetResourceStatus(self): 
        """
        This method should return an object of type Resource
        containing a dictionary with resource information.
        """
        abstract
    