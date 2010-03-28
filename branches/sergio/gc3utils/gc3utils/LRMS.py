class LRMS:

    def __init__(self, resource): abstract

    def check_authentication(self): abstract

    def submit_job(self, unique_token, application, input_file): abstract
        # return LRMS specific lrms_jobid
        # stages input files if necessary
        # dumps submission stdout to lrms_log string

    def check_status(self,lrms_jobid):  abstract


    def get_results(self,lrms_jobid,job_dir): abstract

    def GetResourceStatus(self): abstract
        # this method should return an object of type Resource
        # containing a dictionary with resource information
    
