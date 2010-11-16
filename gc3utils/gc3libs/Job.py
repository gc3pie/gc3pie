#! /usr/bin/env python
#
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
"""
Object-oriented interface for computational job control.

A GC3Libs' `Job` is an abstraction of an independent asynchronous
computation, i.e., a GC3Libs' `Job` behaves much like an independent
UNIX process. Indeed, GC3Libs' `Job` objects mimick the POSIX process
interface: `Job`s are started by a parent process, run independently
of it, and need to have their final exit code and output reaped by the
calling process.

The following table makes the correspondence between POSIX processes
and GC3Libs' Job objects explicit.

====================   ================   =====================
`os` module function   GC3Libs function   purpose
====================   ================   =====================
exec                   Gcli.gsub          start new job
kill (SIGTERM)         Gcli.gkill         terminate executing job
wait (WNOHANG)         Gcli.gstat         get job status (running, terminated)
-                      Gcli.gget          retrieve output 

At any given moment, a GC3Libs job is in any one of a set of
pre-defined states, listed in the table below.

GC3Libs' Job state   purpose                                                         can change to
==================   ==============================================================  ======================
NEW                  Job has not yet been submitted/started (i.e., gsub not called)  SUBMITTED (by gsub)
SUBMITTED            Job has been sent to execution resource                         RUNNING, STOPPED
STOPPED              Trap state: job needs manual intervention (either user- \
                     or sysadmin-level) to resume normal execution                   TERMINATED (by gkill), SUBMITTED (by miracle)
RUNNING              Job is executing on remote resource                             TERMINATED
TERMINATED           Job execution is finished (correctly or not) \
                     and will not be resumed                                         None: final state

A job that is not in the NEW or TERMINATED state is said to be a "live" job.

When a Job object is first created, it is assigned the state NEW.
After a successful invocation of `Gcli.gsub()`, the Job object is
transitioned to state SUBMITTED.  Further transitions to RUNNING or
STOPPED or TERMINATED state, happen completely independently of the
creator program.  The `Gcli.gstat()` call provides updates on the
status of a job. (Somewhat like the POSIX `wait(..., WNOHANG)` system
call, except that GC3Libs provide explicit RUNNING and STOPPED states,
instead of encoding them into the return value.)

The STOPPED state is a kind of generic "run time error" state: a job
can get into the STOPPED state if its execution is stopped (e.g., a
SIGSTOP is sent to the remote process) or delayed indefinitely (e.g.,
the remote batch system puts the job "on hold"). There is no way a job
can get out of the STOPPED state automatically: all transitions from the
STOPPED state require manual intervention, either by the submitting
user (e.g., cancel the job), or by the remote systems administrator
(e.g., by releasing the hold).

The TERMINATED state is the final state of a job: once a job reaches
it, it cannot get back to any other state. Jobs reach TERMINATED state
regardless of their exit code, or even if a system failure occurred
during remote execution; actually, jobs can reach the TERMINATED
status even if they didn't run at all! Just like POSIX encodes process
termination information in the "return code", the GC3Libs encode
information about abnormal process termination using a set of
pseudo-signal codes in a job's returncode attribute: i.e., if
termination of a job is due to some gird/batch system/middleware
error, the job's `os.WIFSIGNALED(job.returncode)` will be True and the
signal code (as gotten from `os.WTERMSIG(job.returncode)`) will be one
of the following:

======  ============================================================
signal  error condition
======  ============================================================
125     submission to batch system failed
124     remote error (e.g., execution node crashed, batch system misconfigured)
123     data staging failure
122     job killed by batch system / sysadmin
121     job canceled by user

In addition, each GC3Libs' Job object in TERMINATED state is
guaranteed to have these additional attributes:

    * output_retrieved: boolean flag, indicating whether job output
      has been fetched from the remote resource; use the Gcli.gget()
      function to retrieve the output. (Note: for jobs in TERMINATED
      state, the output can be retrieved only once!)

    * ... 

"""

import os
import shelve
import sys
import time
import types

from Exceptions import *
import gc3libs
from gc3libs.utils import defproperty, Enum, Log, Struct, safe_repr
import Default


State = Enum(
    'NEW',       # Job has not yet been submitted/started
    'SUBMITTED', # Job has been sent to execution resource
    'STOPPED',   # trap state: job needs manual intervention
    'RUNNING',   # job is executing on remote resource
    'TERMINATED',# job execution finished (correctly or not) and will not be resumed
    'UNKNOWN',   # job info not found or lost track of job (e.g., network error or invalid job ID)
    )


class _Signal(object):
    """
    Base class for representing fake signals encoding the failure
    reason for GC3Libs jobs.
    """
    def __init__(self, name, signum, description):
        self._name = name
        self._signum = signum
        self.__doc__ = description
    # conversion to integer types
    def __int__(self):
        return self._signum
    def __long__(self):
        return self._signum
    # human-readable explanation
    def __str__(self):
        return "SIG%s(%d) - %s" % (self._name, self._signum, self.__doc__)

class Signals(object):
    """
    Collection of (fake) signals used to encode termination reason in `Job.returncode`.
    """
    Cancelled = _Signal('CANCEL', 121, "Job canceled by user")
    RemoteKill = _Signal('BATCHKILL', 122, "Job killed by batch system or sysadmin")
    DataStagingFailure = _Signal('STAGE', 123, "Data staging failure")
    RemoteError = _Signal('BATCHERR', 124, 
                          "Unspecified remote error, e.g., execution node crashed"
                          " or batch system misconfigured")
    SubmissionFailed = _Signal('SUBMIT', 125, "Submission to batch system failed.")


class Job(Struct):
    """A specialized `dict`-like object that keeps information about a GC3Libs job. 

    A `Job` object is guaranteed to have the following attributes:

      * `state`: Current state of the job, initially `State.NEW`; 
         see `Job.State` for a list of the possible values.

      * `output_dir`: path to the directory where output has been
        downloaded, or `None` if no output has been retrieved yet.

      * `output_retrieved`: Initially, `False`, set to `True` after a
        successful call to `Gcli.gget`

    `Job` objects support attribute lookup by both the ``[...]`` and the ``.`` syntax;
    see `gc3libs.utils.Struct` for examples.
    """
    def __init__(self,initializer=None,**keywd):
        """
        Create a new Job object; constructor accepts the same
        arguments as the `dict` constructor.
        
        Examples:
        
          1. Create a new job with default parameters::

            >>> j1 = Job()
            >>> j1.returncode
            None
            >>> j1.state
            'NEW'

          2. Create a new job with additional attributes::

            >>> j2 = Job(application='GAMESS', version='2010R1')
            >>> j2.state
            'NEW'
            >>> j2.application
            'GAMESS'
            >>> j2['version']
            '2010R1'

          3. Clone an existing job object::

            >>> j3 = Job(j2)
            >>> j3.application
            'GAMESS'
            >>> j3['version']
            '2010R1'
            
        """
        if not hasattr(self, '_state'): self._state = State.NEW
        self._exitcode = None
        self._signal = None

        Struct.__init__(self, initializer, **keywd)

        if 'log' not in self: self.log = Log()
        if 'output_retrieved' not in self: self.output_retrieved = False
        if 'timestamp' not in self: self.timestamp = { }


    def __str__(self):
        try:
            return str(self._id) # XXX: keep in sync with what `persistence.Store` does
        except AttributeError:
            return safe_repr(self)

    def __repr__(self):
        return "Job(%s)" % str.join(', ', 
                                    [ ("%s=%s" % (k,repr(v))) 
                                      for k,v in self.items() ])


    @defproperty
    def state():
        """
        The state a `Job` is in; see `Job.State` for possible values.
        The value of `Job.state` must always be a value from the
        `Job.State` enumeration.
        """
        def fget(self):
            return self._state
        def fset(self, value):
            if value not in State:
                raise ValueError("Value '%s' is not a legal `gc3libs.Job.State` value." % value)
            if self._state != value:
                self.timestamp[value] = time.time()
                self.log.append('%s on %s' % (value, time.asctime()))
            self._state = value
        return locals()


    @defproperty
    def signal():
        """
        The "signal number" part of a `Job.returncode`, see
        `os.WTERMSIG` for details. 

        The "signal number" is a 7-bit integer value in the range
        0..127; value `0` is used to mean that no signal has been
        received during the application runtime (i.e., the application
        terminated by calling ``exit()``).  

        The value represents either a real UNIX system signal, or a
        "fake" one that GC3Libs uses to represent Grid middleware
        errors (see `Job.Signals`).
        """
        def fget(self): 
            return self._signal
        def fset(self, value): 
            if value is None:
                self._signal = None
            else:
                self._signal = int(value) & 0x7f
        return (locals())


    @defproperty
    def exitcode():
        """
        The "exit code" part of a `Job.returncode`, see
        `os.WEXITSTATUS`.  This is an 8-bit integer, whose meaning is
        entirely application-specific.  However, the value `-1` is
        used to mean that an error has occurred and the application
        could not end its execution normally.
        """
        def fget(self):
            return self._exitcode
        def fset(self, value):
            if value is None:
                self._exitcode = None
            else:
                self._exitcode = int(value) & 0xff
        return (locals())


    @defproperty
    def returncode():
        """
        The `returncode` attribute of this job object encodes the
        `Job` termination status in a manner compatible with the POSIX
        termination status as implemented by `os.WIFSIGNALED()` and
        `os.WIFEXITED()`.

        However, in contrast with POSIX usage, the `exitcode` and the
        `signal` part can *both* be significant: in case a Grid
        middleware error happened *after* the application has
        successfully completed its execution.  In other words,
        `os.WEXITSTATUS(job.returncode)` is meaningful iff
        `os.WTERMSIG(job.returncode)` is 0 or one of the
        pseudo-signals listed in `Job.Signals`.
        
        `Job.exitcode` and `Job.signal` are combined to form the
        return code 16-bit integer as follows (the convention appears
        to be obyed on every known system)::

           Bit     Encodes...
           ======  ====================================
           0..7    signal
           8       1 if program is stopped; 0 otherwise
           9..16   exitcode 

        *Note:* the "stop bit" is always 0.

        Setting the `returncode` property sets `exitcode` and
        `signal`; you can either assign a `(signal, exitcode)` pair to
        `returncode`, or set `returncode` to an integer from which the
        correct `exitcode` and `signal` attribute values are
        extracted::

           >>> j = Job()
           >>> j.returncode = (42, 56)
           >>> j.signal
           42
           >>> j.exitcode
           56

           >>> j.returncode = 137
           >>> j.signal
           9
           >>> j.exitcode
           0

        See also `Job.exitcode` and `Job.signal`.
        """
        def fget(self): 
            if self.exitcode is None and self.signal is None:
                return None
            if self.exitcode is None:
                exitcode = -1
            else:
                exitcode = self.exitcode
            if self.signal is None:
                signal = 0
            else: 
                signal = self.signal
            return (exitcode << 8) | signal
        def fset(self, value):
            try:
                # `value` can be a tuple `(signal, exitcode)`
                self.signal = int(value[0])
                self.exitcode = int(value[1])
            except TypeError:
                self.exitcode = (int(value) >> 8) & 0xff
                self.signal = int(value) & 0x7f
            # ensure values are within allowed range
            self.exitcode &= 0xff
            self.signal &= 0x7f
        return (locals())
