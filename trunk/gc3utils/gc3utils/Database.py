import os 
import utils
from DatabaseModel import *

class Database:

    def __init__(self):
        pass

    def create_database(self,dbfile_location):
        try:
            # sqlite url example:
            # sqlite:////absolute/path/to/file.db
            sqlite_url = "sqlite:///" + dbfile_location

            metadata.bind = sqlite_url
            setup_all(True)
            create_all()
            return
        except:
            raise

