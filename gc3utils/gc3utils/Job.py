import types
from InformationContainer import *

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


class Job(InformationContainer):

    def is_valid(self):
        if self.__dict__.has_key('status') and self.__dict__.has_key('resource_name') and self.__dict__.has_key('lrms_jobid'):
            return True



