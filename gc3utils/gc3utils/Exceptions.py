#! /usr/bin/env python
#
"""
Exceptions specific to the `gc3utils` package.
"""
__docformat__ = 'reStructuredText'


import gc3utils


class FatalError(Exception):
    """
    A fatal error; execution cannot continue and program should report
    to user and then stop.

    The message is sent to the logs at CRITICAL level
    when the exception is first constructed.

    This is the base class for all fatal exceptions.
    """
    def __init__(self, msg, do_log=True):
        if do_log:
            gc3utils.log.critical(msg)
        Exception.__init__(self, msg)


class Error(Exception):
    """
    A non-fatal error.

    Depending on the nature of the task, steps could be taken to
    continue, but users *must* be aware that an error condition
    occurred, so the message is sent to the logs at the ERROR level.

    This is the base class for all error-level exceptions.
    """
    def __init__(self, msg, do_log=True):
        if do_log: 
            gc3utils.log.error(msg)
        Exception.__init__(self, msg)
        

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

class UniqueTokenError(FatalError):
    pass

class JobRetrieveError(Error):
    pass


class InvalidUsage(FatalError):
    """
    Raised when a command is not provided all required arguments on
    the command line, or the arguments do not match the expected
    syntax.

    Since the exception message is the last thing a user will see,
    try to be specific about what is wrong on the command line.
    """
    pass



## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="exceptions",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
