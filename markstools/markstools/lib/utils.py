import markstools
import logging
import logging.handlers

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
    
    if ( verbosity > 5):
        logging_level = 10
    else:
        logging_level = (( 6 - verbosity) * 10)

    markstools.log.setLevel(logging_level)
    handler = logging.handlers.RotatingFileHandler(log_file_name, maxBytes=200, backupCount=5)
    markstools.log.addHandler(handler)
