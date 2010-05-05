import types
from InformationContainer import *

# -----------------------------------------------------
# Job
#

class Job(InformationContainer):

    FINISHED = 1
    RUNNING = 2
    FAILED = 3
    SUBMITTED = 4
    COMPLETED = 5

    def is_valid(self):
        if self.__dict__.has_key('status') and self.__dict__.has_key('resource_name') and self.__dict__.has_key('lrms_jobid'):
            return True



