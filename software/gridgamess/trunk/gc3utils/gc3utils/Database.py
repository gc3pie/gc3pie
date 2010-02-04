import logging
import os 
import utils
from DatabaseModel import *

class Database:

    def __init__(self):
        pass

    def create_database(self,application,location=''):
        try:
            configure_logging()
            homedir = os.path.expandvars('$HOME')
            database_filename = application + "_database"
            print database_filename
            default_database_location = homedir + "/.gc3/" + database_filename

            if location == '':
                database_location = default_database_location
            else:
                database_location = location

            logging.debug('database_location: ' + database_location)
            print database_location
            metadata.bind = "sqlite://database_location"
            setup_all(True)
            create_all()
            return
        except:
            return 

