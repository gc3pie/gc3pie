import markstools

class UnhandledStateError(Exception):
    def __init__(msg, report=True):
       super(Exception).__init__(msg)
       if report:
           log.critical(msg)
