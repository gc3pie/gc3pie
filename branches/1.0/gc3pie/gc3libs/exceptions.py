#! /usr/bin/env python
"""
Exceptions specific to the `gc3libs` package.
"""
# Copyright (C) 2009-2011 GC3, University of Zurich. All rights reserved.
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
__version__ = '1.0rc1 (SVN $Revision$)'


import gc3libs

## base error classes

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
    def __init__(self, msg, do_log=False):
        if do_log: 
            gc3libs.log.error(msg)
        Exception.__init__(self, msg)
        

## derived exceptions

class RecoverableAuthError(Error):
    pass

class UnrecoverableAuthError(Error):
    pass

class ConfigurationError(FatalError):
    """
    Raised when the configuration file (or parts of it) could not be
    read/parsed.  Also used to signal that a required parameter is
    missing or has an unknown/invaliud value.
    """
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

class InvalidArgument(Error): # XXX: should this be fatal? should it be a descendant of `AssertionError`?
    """
    Raised when the arguments passed to a function do not honor some
    required contract.  For instance, either one of two optional
    arguments must be provided, but none of them was.
    """
    pass

class DuplicateEntryError(InvalidArgument):
    """
    Raised by `Application.__init__` if not all (local or remote)
    entries in the input or output files are distinct.
    """
    pass

class InvalidInformationContainerError(Error):
    pass

class InvalidOperation(Error):
    """
    Raised when an operation is attempted, that is not considered
    valid according to the system state.  For instance, trying to
    retrieve the output of a job that has not yet been submitted.
    """
    pass

class InvalidResourceName(Error):
    """
    Raised to signal that no computational resource with the given
    name is defined in the configuration file.
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

class LoadError(Error):
    """
    Raised upon errors loading a job from the persistent storage.
    """
    pass

class LRMSException(Error):
    pass    

class LRMSSubmitError(Error):
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

class OutputNotAvailableError(Error):
    """
    Raised upon attempts to retrieve the output for jobs that are
    still in `NEW` or `SUBMITTED` state.
    """
    pass

class TransportError(Error):
    pass

class UnknownJobState(Error):
    """
    Raised when a job state is gotten from the Grid middleware, that
    is not handled by the GC3Libs code.  Might actually mean that
    there is a version mismatch between GC3Libs and the Grid
    middleware used.
    """
    pass


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="exceptions",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
