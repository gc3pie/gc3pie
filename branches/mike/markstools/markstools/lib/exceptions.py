import logging
_log = logging.getLogger('markstools')

class UnhandledStateError(Exception):
    def __init__(msg, report=True):
       super(Exception).__init__(msg)
       if report:
           _log.critical(msg)
