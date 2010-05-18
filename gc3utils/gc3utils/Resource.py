import types
from InformationContainer import *

# -----------------------------------------------------
# Resource
#

class Resource(InformationContainer):
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
    string name
    int total_cores
    int max_quota
    int used_quota
    int type
    '''

    def is_valid(self):
        if self.has_key('max_cores_per_node') and self.has_key('type') and self.has_key('frontend') and self.has_key('name') and self.has_key('max_walltime') and self.has_key('max_memory_per_node') and self.has_key('total_cores'):
            return True
        else:
            return False
