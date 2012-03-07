import multiprocessing
import multiprocessing.managers
import logging
import logging.handlers

#
#   Create logger object
#
def setup_shared_logger(LOGGING_LEVEL, LOG_FILENAME):
    """
    Function to setup logger shared between all processes
    The logger object will be created within a separate (special) process
        run by multiprocessing.BaseManager.start()
    """

    #
    #   Log file name with logger level
    #
    my_ruffus_logger = logging.getLogger('htpie')
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    my_ruffus_logger.setLevel(LOGGING_LEVEL)

    #
    #   Add handler to print to file, with the specified format
    #
    file_handler = logging.handlers.RotatingFileHandler(
                  LOG_FILENAME, maxBytes=2000000, backupCount=5)
    file_handler.setFormatter(formatter)
    my_ruffus_logger.addHandler(file_handler)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    my_ruffus_logger.addHandler(stream_handler)

    return my_ruffus_logger

#
#   Proxy object for logging
#       Logging messages will be marshalled (forwarded) to the process where the
#       shared log lives
#
class LoggerProxy(multiprocessing.managers.BaseProxy):
    _exposed_ = ('debug', 'info', 'critical', 'warning', 'error', '__str__')
    def debug(self, message):
        return self._callmethod('debug', [message])
    def info(self, message):
        return self._callmethod('info', [message])
    def critical(self, message):
        return self._callmethod('critical', [message])
    def warning(self, message):
        return self._callmethod('warning', [message])
    def error(self, message):
        return self._callmethod('error', [message])
    def __str__ (self):
        return "Logging proxy"

#
#   Register the setup_logger function as a proxy for setup_logger
#
#   We use SyncManager as a base class so we can get a lock proxy for synchronising
#       logging later on
#
class LoggingManager(multiprocessing.managers.SyncManager):
    """
    Logging manager sets up its own process and will create the real Log object there
    We refer to this (real) log via proxies
    """
    pass

LoggingManager.register('setup_logger', setup_shared_logger, proxytype=LoggerProxy)

if __name__ == '__main__':

    #
    #   make shared log and proxy
    #
    manager = LoggingManager()
    #manager.register('setup_logger', setup_shared_logger,
    #                 proxytype=LoggerProxy, exposed = ('info', 'debug'))

    manager.start()
    LOG_FILENAME  = os.path.expanduser('~/.htpie/gc3utils.log')
    LOGGING_LEVEL = logging.DEBUG
    logger_proxy = manager.setup_logger(LOGGING_LEVEL, LOG_FILENAME)
    logger_proxy.debug('12')
    #
    #   make sure we are not logging at the same time in different processes
    #
    logging_mutex = manager.Lock()
