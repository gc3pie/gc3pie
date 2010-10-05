from InformationContainer import InformationContainer
#import sys
#from utils import Struct

# -----------------------------------------------------
# Resource
#
class Resource(InformationContainer):
    '''
    `Resource` objects are dictionaries, comprised of the following keys.
    
    Statically provided, i.e., specified at construction time and changed never after:

      arc_ldap             string   
      authorization_type   string
      frontend             string
      gamess_location      string
      max_cores_per_job    int     *
      max_memory_per_core  int     *
      max_walltime         int     *
      name                 string  *
      ncores               int
      type                 int     *
      walltime             int(?)

    Starred attributes are required for object construction.
     
    Dynamically provided (i.e., defined by the `get_resource_status()` method or similar):
      free_slots          int
      user_run            int
      user_queued         int
      queued              int
    '''

    def is_valid(self):
        if (self.has_key('max_cores_per_job') 
            and self.has_key('max_memory_per_core') 
            and self.has_key('type') 
            and self.has_key('name') 
            and self.has_key('max_walltime') 
            ):
            return True
        else:
            return False

    # def __getattr__(self, key):
    #     sys.stderr.write("Resource: query for attribute '%s'\n" %key)
    #     return Struct.__getattr__(self, key)
    # def __getitem__(self, key):
    #     sys.stderr.write("Resource: query for attribute '%s'\n" % key)
    #     return dict.__getitem__(self, key)
