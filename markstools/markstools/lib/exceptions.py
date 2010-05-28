import markstools

class UnhandledStateError(Exception):
    def __init__(self, msg, report=True):
       super(Exception, self).__init__(msg)
       if report:
           markstools.log.critical(msg)

class MyTypeError(TypeError):
    def __init__(self, msg, report=True):
       super(TypeError, self).__init__(msg)
       if report:
           markstools.log.critical(msg)
