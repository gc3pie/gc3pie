#! /usr/bin/env python
"""Exceptions specific to the `gc3libs` package.

In addition to the exceptions listed here, `gc3libs`:mod: functions
try to use Python builtin exceptions with the same meaning they have
in core Python, namely:

* `TypeError` is raised when an argument to a function or method has an
  incompatible type or does not implement the required protocol (e.g.,
  a number is given where a sequence is expected).

* `ValueError`is  raised when an argument to a function or method has
  the correct type, but fails to satisfy other constraints in the
  function contract (e.g., a positive number is required, and `-1` is
  passed instead).

* `AssertionError` is raised when some internal assumption regarding
  state or function/method calling contract is violated.  Informally,
  this indicates a bug in the software.

"""
# Copyright (C) 2009-2012 GC3, University of Zurich. All rights reserved.
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
__version__ = '2.0.0 version (SVN $Revision$)'


import gc3libs

## base error classes

class Error(Exception):
    """
    Base class for all error-level exceptions in GC3Pie.

    Generally, this indicates a non-fatal error: depending on the
    nature of the task, steps could be taken to continue, but users
    *must* be aware that an error condition occurred, so the message
    is sent to the logs at the ERROR level.

    Exceptions indicating an error condition after which the program
    cannot continue and should immediately stop, should use the
    `FatalError`:class: base class.
    """
    def __init__(self, msg, do_log=False):
        if do_log:
            gc3libs.log.error(msg)
        Exception.__init__(self, msg)


## mark errors as "Recoverable" (meaning that a retry a ta later time
## could succeed), or "Unrecoverable" (meaning there's no point in
## retrying).

class RecoverableError(Error):
    """
    Used to mark transient errors: retrying the same action at a later
    time could succeed.

    This exception should *never* be instanciated: it is only to be used
    in `except` clauses to catch "try again" situations.
    """
    pass


class UnrecoverableError(Error):
    """
    Used to mark permanent errors: there's no point in retrying the same
    action at a later time, because it will yield the same error again.

    This exception should *never* be instanciated: it is only to be used
    in `except` clauses to exclude "try again" situations.
    """
    pass


class FatalError(UnrecoverableError):
    """
    A fatal error: execution cannot continue and program should report
    to user and then stop.

    The message is sent to the logs at CRITICAL level
    when the exception is first constructed.

    This is the base class for all fatal exceptions.
    """
    def __init__(self, msg, do_log=True):
        if do_log:
            gc3libs.log.critical(msg)
        Exception.__init__(self, msg)


## derived exceptions

class AuthError(Error):
    """
    Base class for Auth-related errors.

    Should *never* be instanciated: create a specific error class
    describing the actual error condition.
    """
    pass

class RecoverableAuthError(AuthError, RecoverableError):
    pass

class UnrecoverableAuthError(AuthError, UnrecoverableError):
    pass


class ConfigurationError(FatalError):
    """
    Raised when the configuration file (or parts of it) could not be
    read/parsed.  Also used to signal that a required parameter is
    missing or has an unknown/invalid value.
    """
    pass


class DataStagingError(Error):
    """
    Base class for data staging and movement errors.

    Should *never* be instanciated: create a specific error class
    describing the actual error condition.
    """
    pass

class RecoverableDataStagingError(DataStagingError, RecoverableError):
    """
    Raised when transient problems with copying data to or from the
    remote execution site occurred.

    This error is considered to be transient (e.g., network
    connectivity interruption), so trying again at a later time could
    solve the problem.
    """
    pass

class UnrecoverableDataStagingError(DataStagingError, UnrecoverableError):
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


class InvalidArgument(Error, AssertionError): # XXX: should this be fatal?
    """
    Raised when the arguments passed to a function do not honor some
    required contract.  For instance, either one of two optional
    arguments must be provided, but none of them was.
    """
    pass

class InvalidType(InvalidArgument, TypeError):
    """
    A specialization of`InvalidArgument` for cases when the type of
    the passed argument does not match expectations.
    """
    pass

class InvalidValue(InvalidArgument, ValueError):
    """
    A specialization of`InvalidArgument` for cases when the type of
    the passed argument does not match expectations.
    """
    pass

class DuplicateEntryError(InvalidArgument):
    """
    Raised by `Application.__init__` if not all (local or remote)
    entries in the input or output files are distinct.
    """
    pass


class InvalidOperation(Error):
    """
    Raised when an operation is attempted, that is not considered
    valid according to the system state.  For instance, trying to
    retrieve the output of a job that has not yet been submitted.
    """
    pass


class InvalidResourceName(Error, ValueError):
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


class LRMSError(Error):
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


class OutputNotAvailableError(InvalidOperation):
    """
    Raised upon attempts to retrieve the output for jobs that are
    still in `NEW` or `SUBMITTED` state.
    """
    pass


class TaskError(Error):
    """
    Generic error condition in a `Task` object.
    """
    pass


class DetachedFromGridError(TaskError):
    """
    Raised when a method (other than :meth:`attach`) is called on
    a detached `Task` instance.
    """
    pass


class UnexpectedStateError(TaskError):
    """
    Raised by :meth:`Task.progress` when a job lands in `STOPPED`
    or `TERMINATED` state.
    """
    pass


class TransportError(Error):
    pass

class RecoverableTransportError(RecoverableError):
    pass

class UnrecoverableTransportError(UnrecoverableError):
    pass

class UnknownJob(Error, ValueError):
    """
    Raised when an operation is attempted on a task, which is
    unknown to the remote server or backend.
    """
    pass


class UnknownJobState(Error, AssertionError):
    """
    Raised when a job state is gotten from the Grid middleware, that
    is not handled by the GC3Libs code.  Might actually mean that
    there is a version mismatch between GC3Libs and the Grid
    middleware used.
    """
    pass

class ApplicationDescriptionError(FatalError):
    """
    Raised when the dumped description on a given Application produces something that the LRMS backend cannot process.
    As an example, for arc backends, this error is raised when the parsing of the Application's XRSL fails
    """
    pass


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="exceptions",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
