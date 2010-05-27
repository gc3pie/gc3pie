from Exceptions import *
from InformationContainer import *
import os
import os.path
import types


# -----------------------------------------------------
# Applications
#

class Application(InformationContainer):

    def is_valid(self):
        if self.has_key('input_file_name'):
            if not os.path.exists(self.input_file_name):
                raise InputFileError("Input file '%s' does not exist" 
                                     % self.input_file_name)
        return True

