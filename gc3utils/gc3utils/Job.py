import types
from InformationContainer import *
import utils
import Exceptions
import gc3utils

# -----------------------------------------------------
# Job
#

# Job is finished on grid or cluster, results have not yet been retrieved.
# User or Application can now call gget.
JOB_STATE_FINISHED = 1

# Job is currently running on a grid or cluster.
# Gc3utils must wait until it finishes to proceed.
JOB_STATE_RUNNING = 2

# Could mean several things:
#   - LRMS failed to accept the job for some reason.
#   - LRMS killed the job.
#   - Job exited with non-zero exit status.
# The User can decide whether to do gget or resubmit or nothing.
JOB_STATE_FAILED = 3

# Job is between the submit phase and confirmed in the LRMS scheduler.
# For example, this applies to ARC jobs after they are submitted but not yet officially queued according to the ARC scheduler.
# SGE+SSH jobs do not have this.
# No action required.
JOB_STATE_SUBMITTED = 4

# Job is finished and results successfully retrieved.
# User or Application can do whatever it wants with the results.
JOB_STATE_COMPLETED = 5

# This is the default initial state of a job instance before it has been updated by a gsub/gstat/gget/etc.
# No action required; the state will be set to something else as the code executes.
JOB_STATE_UNKNOWN = 6

#JOB_STATE_HOLD = 7 # Initial state
#JOB_STATE_READY = 8 # Ready for gsub
#JOB_STATE_WAIT = 9 # equivalent to SUBMITTED
#JOB_STATE_OUTPUT = 10 # equivalent to FINISHED
#JOB_STATE_UNREACHABLE = 11 # AuthError
#JOB_STATE_NOTIFIED = 12 # User Notified of AuthError
#JOB_STATE_ERROR = 13 # Equivalent to FAILED


class Job(InformationContainer):

    def __init__(self,initializer=None,**keywd):
        """
        Create a new Job object.
        
        Examples::
        
        >>> df = Job()
        """
        # create_unique_token
        if (not keywd.has_key('unique_token')) and not (initializer is not None and hasattr(initializer, 'has_key') and initializer.has_key('unique_token')):
            gc3utils.log.debug('creating new unique_token')
            keywd['unique_token'] = utils.create_unique_token()
        InformationContainer.__init__(self,initializer,**keywd)

    def is_valid(self):
        if (self.has_key('status') 
            and self.has_key('resource_name') 
            and self.has_key('lrms_jobid') 
            and self.has_key('unique_token')):
            return True
