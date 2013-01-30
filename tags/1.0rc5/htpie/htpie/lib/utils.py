import htpie
import logging
import logging.handlers
import os
import ConfigParser


import time

def print_timing(func):
    def wrapper(*arg):
        t1 = time.time()
        res = func(*arg)
        t2 = time.time()
        htpie.log.debug( '%s took %0.3f ms' % (func.func_name, (t2-t1)*1000.0))
        return res
    return wrapper

class Struct(dict):
    """
    A `dict`-like object, whose keys can be accessed with the usual
    '[...]' lookup syntax, or with the '.' get attribute syntax.

    Examples::

      >>> a = Struct()
      >>> a['x'] = 1
      >>> a.x
      1
      >>> a.y = 2
      >>> a['y']
      2

    Values can also be initially set by specifying them as keyword
    arguments to the constructor::

      >>> a = Struct(z=3)
      >>> a['z']
      3
      >>> a.z
      3
    """
    def __init__(self, initializer=None, **kw):
        if initializer is None:
            dict.__init__(self, **kw)
        else:
            dict.__init__(self, initializer, **kw)
    def __setattr__(self, key, val):
        self[key] = val
    def __getattr__(self, key):
        if self.has_key(key):
            return self[key]
        else:
            raise AttributeError, "No attribute '%s' on object %s" % (key, self)
    def __hasattr__(self, key):
        return self.has_key(key)

class InformationContainer(Struct):

    def __init__(self, initializer=None, **keywd):
        Struct.__init__(self, initializer, **keywd)
        if not self.is_valid():
            raise InvalidInformationContainerError('Object `%s` of class `%s` failed validity check.' % (self, self.__class__.__name__))

    def is_valid(self):
        raise NotImplementedError("Abstract method `is_valid()` called - this should have been defined in a derived class.")

class GC3Param(InformationContainer):
    def __init__(self,initializer=None,**keywd):
        if not keywd.has_key('requested_cores'):
            keywd['requested_cores'] = 2
        if not keywd.has_key('requested_memory'):
            keywd['requested_memory'] = 2
        if not keywd.has_key('requested_walltime'):
            keywd['requested_walltime'] = 2
        InformationContainer.__init__(self,initializer,**keywd)

def generate_temp_dir(uid=None, subdir=None):
    import tempfile
    import os
    rootdir = tempfile.gettempdir()
    if uid:
        rootdir += '/' + uid
    if subdir:
        rootdir += '/'+subdir    
    if uid or subdir:
        try:
            os.makedirs(rootdir)
        except OSError:
            if not os.path.isdir(rootdir):
                raise
    return rootdir

def verify_file_container(f_container, mode='r'):
    def open_str(a_file, mode):
        if isinstance(a_file, str) or isinstance(a_file, unicode):
            return open(a_file, mode)
        else:
            # We have a file like object
            if a_file.closed:
                return open(a_file.name, mode)
            return a_file

    if isinstance(f_container, list) or \
       isinstance(f_container, tuple):
        f_open = []
        for a_file in f_container:
            f_open.append(open_str(a_file, mode))
    else:
        f_open = open_str(f_container, mode)
    
    return f_open
    
def format_exception_info(maxTBlevel=5):
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
    return '%s %s\n%s'%(excName, excArgs, ''.join(excTb))
    
def configure_logger(verbosity, log_file_name='~/.htpie/gc3utils_log.log'):
    """
    Configure the logger.

    - Input is the logging level and a filename to use.
    - Returns nothing.
    """
    # We have to check the logger in here, and not in __init__ to see if it is empty.
    # when we check in __init__ it doesn't work. We want to execute this once per process
    if not htpie.log.handlers:
        from cloghandler import ConcurrentRotatingFileHandler
        logfile = os.path.abspath(os.path.expanduser(log_file_name))
        rotateHandler = ConcurrentRotatingFileHandler(logfile, "a", maxBytes=2000000, backupCount=5)
        logging_level = 10 * max(1, 5-verbosity)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        rotateHandler.setFormatter(formatter)
        
        htpie.log.setLevel(logging_level)
        htpie.log.addHandler(rotateHandler)
        
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        htpie.log.addHandler(stream_handler )
        # This can be uncommented to turn on gc3utils logging
        #from gc3utils.gcommands import _configure_logger
        #import gc3utils
        #gc3utils.log.addHandler(file_handler)
        #gc3utils.log.addHandler(stream_handler)
        #gc3utils.log.setLevel(0)
        #_configure_logger(0)

#def configure_logger(verbosity, log_file_name='gc3utils_log'):
#    """
#    Configure the logger.
#
#    - Input is the logging level and a filename to use.
#    - Returns nothing.
#    """
#    from htpie.lib import multiprocessinglog
#    global _logger_proxy
#    manager = multiprocessinglog.LoggingManager()
#    manager.start()
#    LOG_FILENAME  = os.path.expanduser('~/.htpie/gc3utils.log')
#    LOGGING_LEVEL =  10 * max(1, 5-verbosity)
#    _logger_proxy = manager.setup_logger(LOGGING_LEVEL, LOG_FILENAME)


def split_seq(iterable, size):
    """ Split a interable into chunks of the given size
        tuple(split_seq([1,2,3,4], size=2)
                        returns ([1,2],[3,4])
        
    """
    import itertools
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))

def read_config(config_file_location):
    """
    Read configuration file.
    """
    
    class Configuration(object):
        pass
    
    _configFileLocation = os.path.expandvars(config_file_location)

    # Config File exists; read it
    config = ConfigParser.ConfigParser()
    try:
        config_file = open(_configFileLocation)
        config.readfp(config_file)
    except:
        raise NoConfigurationFile("Configuration file '%s' is unreadable or malformed. Aborting." 
                                  % _configFileLocation)
    finally:
        config_file.close()
    
    ret = Configuration()
    ret.database_user = config.get('SETTINGS','database_user')
    ret.database_name = config.get('SETTINGS','database_name')
    ret.database_uri = config.get('SETTINGS','database_uri')
    ret.database_port = config.getint('SETTINGS','database_port')
    ret.verbosity = config.getint('SETTINGS','verbosity')

    return ret