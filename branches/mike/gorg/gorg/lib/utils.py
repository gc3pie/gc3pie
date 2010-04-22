from couchdb import client

class Mydb(object):
    def __init__(self, couchdb_user, couchdb_database, couchdb_url):
        self.couchdb_database = couchdb_database
        self.couchdb_url = couchdb_url
        self.username = couchdb_user

    def cdb(self):
        db = client.Server( self.couchdb_url )[self.couchdb_database]
        db.username = self.username
        return db

    def createdatabase(self):
        created = True
        try:
            client.Server( self.couchdb_url ).create(self.couchdb_database)
        except:
            created = False
        return created

def create_file_logger(verbosity,file_prefix = 'gc3utils'):
    '''
    Create a file logger object.
     * Requires logger name, file_prefix, verbosity
     * Returns logger object.
     
    '''
    import logging
    import os
    if ( verbosity > 5):
        logging_level = 10
    else:
        logging_level = (( 6 - verbosity) * 10)
    log_filename = ('%s/%s_log'%(os.path.abspath(''), file_prefix))
    logger = logging.basicConfig(filename = log_filename, level = logging_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def formatExceptionInfo(maxTBlevel=5):
    '''Make the exception output pretty'''
    import traceback
    import sys

    cla, exc, trbk = sys.exc_info()
    excName = cla.__name__
    try:
        excArgs = exc.__dict__["args"]
    except KeyError:
        excArgs = "<no args>"
    excTb = traceback.format_tb(trbk, maxTBlevel)
    #(excName, excArgs, excTb)
    return '%s %s\n%s'%(excName, excArgs, ''.join(excTb))
