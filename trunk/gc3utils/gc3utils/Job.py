import types
from InformationContainer import *

# -----------------------------------------------------
# Job
#
JOB_STATE_FINISHED = 1
JOB_STATE_RUNNING = 2
JOB_STATE_FAILED = 3
JOB_STATE_SUBMITTED = 4
JOB_STATE_COMPLETED = 5


class Job(InformationContainer):

    def is_valid(self):
        if self.__dict__.has_key('status') and self.__dict__.has_key('resource_name') and self.__dict__.has_key('lrms_jobid'):
            return True



