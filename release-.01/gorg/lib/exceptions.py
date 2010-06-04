import gorg

class UnhandledStateError(Exception):
    def __init__(msg, report=True):
       super(Exception).__init__(msg)
       if report:
           gorg.log.critical(msg)

class CriticalError(Exception):
    def __init__(msg, report=True):
       super(Exception).__init__(msg)
       if report:
           gorg.log.critical(msg)

class DocumentError(Exception):
    '''Raised when a couchdb document is inconsistant
    '''
    def __init__(self, msg, report=True):
       super(Exception, self).__init__(msg)
       if report:
           gorg.log.critical(msg)

class ViewWarning(UserWarning):
    '''Raised when a user requests a query that does not return the expected result
    '''
    def __init__(self, msg, report=True):
       super(UserWarning, self).__init__(msg)
       if report:
           gorg.log.critical(msg)

class JobWarning(UserWarning):
    '''Raised when a user requests a query that does not return the expected result
    '''
    def __init__(self, msg, report=True):
       super(UserWarning, self).__init__(msg)
       if report:
           gorg.log.critical(msg)
