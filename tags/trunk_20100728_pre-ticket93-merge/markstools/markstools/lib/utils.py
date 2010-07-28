import markstools
import logging
import logging.handlers
import os
import ConfigParser

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

def configure_logger(verbosity, log_file_name='gc3utils_log'):
    """
    Configure the logger.

    - Input is the logging level and a filename to use.
    - Returns nothing.
    """
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging_level = 10 * max(1, 5-verbosity)

    markstools.log.setLevel(logging_level)
    file_handler = logging.handlers.RotatingFileHandler(os.path.expanduser(log_file_name), maxBytes=2000000, backupCount=5)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    markstools.log.addHandler(file_handler)
    markstools.log.addHandler(stream_handler)
    
    import gorg.lib.utils
    gorg.lib.utils.configure_logger(verbosity, log_file_name)
    from gc3utils.gcommands import _configure_logger
    import gc3utils
    gc3utils.log.addHandler(file_handler)
    gc3utils.log.addHandler(stream_handler)
    #gc3utils.log.setLevel(logging_level)
    _configure_logger(verbosity)

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
    ret.database_url = config.get('SETTINGS','database_url')
    ret.verbosity = config.getint('SETTINGS','verbosity')
    ret.temp_directory = os.path.expanduser(config.get('SETTINGS','temp_directory'))

    return ret
