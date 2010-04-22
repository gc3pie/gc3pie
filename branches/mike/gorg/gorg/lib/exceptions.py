class UnhandledStateError(Exception):
    def __init__(msg, report=True):
       super(Exception).__init__()
       if report:
           _log.critical(msg)

class CriticalError(Exception):
    def __init__(msg, report=True):
       super(Exception).__init__()
       if report:
           _log.critical(msg)

class DocumentError(Exception):
    '''Raised when a couchdb document is inconsistant
    '''
    def __init__(msg, report=True):
       super(Exception).__init__()
       if report:
           _log.critical(msg)

class ViewWarning(UserWarning):
    '''Raised when a user requests a query that does not return the expected result
    '''
    def __init__(msg, report=True):
       super(Exception).__init__()
       if report:
           _log.critical(msg)
