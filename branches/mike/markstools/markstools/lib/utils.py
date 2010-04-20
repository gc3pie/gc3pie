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
    
def create_logger( logging_level):
    import logging
    import logging.handlers
    logger = logging.getLogger(self.__class__.__name__)
    logger.setLevel(logging_level)
    file_handler = logging.handlers.RotatingFileHandler(
              self.LOG_FILENAME, maxBytes=100000, backupCount=5)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger
