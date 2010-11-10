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
        

class AuthenticationException(Error):
    pass

class BrokerException(Error):
    pass

class InvalidJobid(FatalError):
    pass

class InputFileError(FatalError):
    """
    Raised when an input file is specified, which does not exist or
    cannot be read.
    """
    pass

class InternalError(Error):
    """
    Raised when some function cannot fulfill its duties, for reasons
    that do not depend on the library client code.  For instance, when
    a response string gotten from an external command cannot be parsed
    as expected.
    """
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

class JobRetrieveError(Error):
    pass

class LRMSException(Error):
    pass    

class NoConfigurationFile(FatalError):
    """
    Raised when the configuration file cannot be read (e.g., does not
    exist or has wrong permissions), or cannot be parsed (e.g., is
    malformed).
    """
    pass

class NoResources(Error):
    """
    Raised to signal that no resources are defined, or that none are
    compatible with the request.
    """
    # FIXME: should we have a separate `NoCompatibleResources` exception?
    pass

class SLCSException(AuthenticationException):
    pass

class SshSubmitException(Error):
    pass

class TransportException(Error):
    pass

class VOMSException(AuthenticationException):
    pass

class InvalidInformationContainerError(Error):
    pass

class LRMSUnrecoverableError(Error):
    pass

class ResourceNotFoundError(Error):
    pass

class XRSLNotFoundError(Error):
    pass

class LRMSSubmitError(Error):
    pass

class LRMSSshError(Error):
    pass

class ConfigurationError(Error):
    pass

class OutputNotAvailableError(Error):
    pass

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="exceptions",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
