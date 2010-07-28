import types
import os
import os.path
from InformationContainer import *

# -----------------------------------------------------
# Default
#

HOMEDIR = os.path.expandvars('$HOME')
RCDIR = HOMEDIR + "/.gc3"
CONFIG_FILE_LOCATION = RCDIR + "/gc3utils.conf"
LOG_FILE_LOCATION = RCDIR + '/logging.conf'
JOBLIST_FILE = RCDIR + "/.joblist"
JOBLIST_LOCK = RCDIR + "/.joblist_lock"
JOB_FOLDER_LOCATION= os.path.expandvars("$PWD")
AAI_CREDENTIAL_REPO = RCDIR + "/aai_credential"
GAMESS_XRSL_TEMPLATE = os.path.expandvars("$HOME/.gc3/gamess_template.xrsl")
JOBS_DIR = RCDIR + "/jobs"
JOB_FILE = ".lrms_id"
JOB_FINISHED_FILE = ".finished"
JOB_LOG = ".log"
CACHE_TIME = 90 # only update ARC resources status every this seconds

ARC_LRMS = 1
SGE_LRMS = 2

SMSCG_AUTHENTICATION = 1
SSH_AUTHENTICATION = 2
NONE_AUTHENTICATION = 3

class Default(InformationContainer):

    def __init__(self,initializer=None,**keywd):
        """
        Create a new Defaults object.

        Examples::

          >>> df = Default()
        """
        InformationContainer.__init__(self,initializer,**keywd)
#        super(InformationContainer)
        self.update(homedir=HOMEDIR)
        self.update(config_file_location=CONFIG_FILE_LOCATION)
        self.update(joblist_location=JOBLIST_FILE)
        self.update(joblist_lock=JOBLIST_LOCK)
        self.update(job_folder_location=JOB_FOLDER_LOCATION)

    def is_valid(self):
        return True


if __name__ == '__main__':
    import doctest
    doctest.testmod()
