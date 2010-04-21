import types
from InformationContainer import *

# -----------------------------------------------------
# Default
#

class Default(InformationContainer):

    HOMEDIR = os.path.expandvars('$HOME')
    RCDIR = homedir + "/.gc3"
    CONFIG_FILE_LOCATION = rcdir + "/config"
    JOBLIST_FILE = rcdir + "/.joblist"
    JOBLIST_LOCK = rcdir + "/.joblist_lock"
    JOB_FOLDER_LOCATION="$PWD"

    def is_valid(self):
        return True
