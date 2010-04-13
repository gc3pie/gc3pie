import types
# -----------------------------------------------------
# Resource lrms
#

class Resource():
    '''
    int max_cores_per_node
    int max_walltime
    int max_memory_per_node
    int queued
    int running
    int max_user_queue
    int max_user_run
    int user_queued
    int user_run
    string frontend
    int total_cores
    int max_quota
    int used_quota
    int type
    '''


    def __init__(self, **resource_info):
        self.__dict__.update(resource_info)

    def __repr__(self):
        args = ['%s=%s' % (k, repr(v)) for (k,v) in vars(self).items()]
        return 'Resource(%s)' % ', '.join(args)
    
    def update(self, **resource_info):
        self.__dict__.update(resource_info)
        
    


