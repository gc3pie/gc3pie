import types
# -----------------------------------------------------
# Job
#

class Job():
    # unique_token, lrms_jobid,  status, used_walltime, used_cores, used_memory, submitted_at, finished_at
    
    def __init__(self, **resource_info):
        self.__dict__.update(resource_info)

    def __repr__(self):
        args = ['%s=%s' % (k, repr(v)) for (k,v) in vars(self).items()]
        return 'Job(%s)' % ', '.join(args)
    
    def update(self, **resource_info):
        self.__dict__.update(resource_info)




