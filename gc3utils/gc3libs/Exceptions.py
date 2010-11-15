#! /usr/bin/env python
"""
Exceptions specific to the `gc3libs` package.
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
#
# Includes parts adapted from the ``bzr`` code, which is
# copyright (C) 2005, 2006, 2007, 2008, 2009 Canonical Ltd
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__docformat__ = 'reStructuredText'
__version__ = '$Revision$'


import gc3libs


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
            gc3libs.log.critical(msg)
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
            gc3libs.log.error(msg)
        Exception.__init__(self, msg)
        

class AuthenticationException(Error):
    pass

class BrokerException(Error):
    pass

class DataStagingError(Error):
    """
    Raised when problems with copying data to or from the remote
    execution site occurred.
    """
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

class InvalidJobid(FatalError):
    pass

class InvalidOperation(Error):
    """
    Raised when an operation is attempted, that is not considered
    valid according to the system state.  For instance, trying to
    retrieve the output of a job that has not yet been submitted.
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

class UnknownJobState(Error):
    """
    Raised when a job state is gotten from the Grid middleware, that
    is not handled by the GC3Libs code.  Might actually mean that
    there is a version mismatch between GC3Libs and the Grid
    middleware used.
    """
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

class TransportError(Error):
    pass

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="exceptions",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
