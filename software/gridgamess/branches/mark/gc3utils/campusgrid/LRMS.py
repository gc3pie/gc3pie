class LRMS:

    def __init__(self, resource):
        pass

    def check_authentication(self):
        pass

    def submit_job(self, unique_token, application, input_file):
        # return LRMS specific lrms_jobid
        # stages input files if necessary
        # dumps submission stdout to lrms_log string
        pass

    def check_status(self,lrms_jobid):
        pass

    def get_results(self,lrms_jobid,job_dir):
        pass
