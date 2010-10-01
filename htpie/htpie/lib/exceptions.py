#! /usr/bin/env python
#
"""
Exceptions specific to the `htpie` package.
"""
__docformat__ = 'reStructuredText'


import htpie


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
            htpie.log.critical(msg)
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
            htpie.log.error(msg)
        Exception.__init__(self, msg)

class ChildNodeException(FatalError):
    pass

class UnhandledStateException(FatalError):
    pass

class AuthorizationException(Error):
    pass

class GC3Exception(Error):
    pass

class ValidationException(Error):
    pass
