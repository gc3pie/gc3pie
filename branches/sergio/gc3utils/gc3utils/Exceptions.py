import gc3utils

class Error(Exception):
    """A non-fatal error."""
    def __init__(self, msg, do_log=True):
        if do_log: gc3utils.log.error(msg)
        
class Critical(Exception):
    """
    A fatal error;
    execution cannot continue and program should report to user and then stop.
    """
    def __init__(self, msg, do_log=True):
        if do_log: gc3utils.log.critical(msg)
        
class BrokerException(Error):
    pass

class LRMSException(Error):
    pass    

class TransportException(Error):
    pass

class AuthenticationException(Error):
    pass

class SLCSException(Error):
    pass

class VOMSException(Error):
    pass
