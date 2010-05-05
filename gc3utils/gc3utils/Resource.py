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
        if ('max_cores_per_node' in self.__dict__) and ('type' in self.__dict__) and ('frontend' in self.__dict__) and ('name' in self.__dict__) and ('max_walltime' in self.__dict__) and ('max_memory_per_node' in self.__dict__) and ('total_cores' in self.__dict__):
            return True
        else:
            return False
