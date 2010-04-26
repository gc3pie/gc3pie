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
