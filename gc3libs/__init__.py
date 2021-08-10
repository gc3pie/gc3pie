#! /usr/bin/env python

# Copyright (C) 2021       Google LLC. All rights reserved.
# Copyright (C) 2009-2019  University of Zurich. All rights reserved.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
GC3Libs is a python package for controlling the life-cycle of a Grid
or batch computational job.

GC3Libs provides services for submitting computational jobs to Grids
and batch systems, controlling their execution, persisting job
information, and retrieving the final output.

GC3Libs takes an application-oriented approach to batch computing. A
generic :class:`Application` class provides the basic operations for
controlling remote computations, but different :class:`Application`
subclasses can expose adapted interfaces, focusing on the most
relevant aspects of the application being represented.

"""
from __future__ import absolute_import, print_function, unicode_literals
from __future__ import division
from builtins import str
from past.utils import old_div
from builtins import object
__docformat__ = 'reStructuredText'

__version__ = '2.6.8'


import os
import os.path
import platform
import string
import sys
import time
import subprocess
import shlex
import uuid

import logging
import logging.config
log = logging.getLogger("gc3.gc3libs")
log.propagate = True

try:
    # Python 2
    from types import StringTypes as string_types
except ImportError:
    # Python 3
    string_types = (str,)

import gc3libs.defaults
from gc3libs.quantity import MB, hours, minutes, seconds, MiB
from gc3libs.events import TaskStateChange, TermStatusChange
from gc3libs.compat._collections import OrderedDict
from gc3libs.compat._inspect import getargspec

import gc3libs.exceptions
from gc3libs.persistence import Persistable
from gc3libs.url import UrlKeyDict, UrlValueDict
from gc3libs.utils import (deploy_configuration_file, Enum,
                           History, Struct, safe_repr, to_str)

#
# global constants
#

ANY_OUTPUT = '*'
"""
When used in the `output` attribute of an application,
it stands for 'fetch the whole contents of the remote
directory'.
"""

# for compatibility with GC3Pie <2.6.8
Default = gc3libs.defaults


# utility functions
#

def configure_logger(
        level=logging.ERROR,
        name=None,
        format=(os.path.basename(sys.argv[0])
                + ': [%(asctime)s] %(levelname)-8s: %(message)s'),
        datefmt='%Y-%m-%d %H:%M:%S',
        colorize='auto'):
    """
    Configure the ``gc3.gc3libs`` logger.

    Arguments `level`, `format` and `datefmt` set the corresponding
    arguments in the `logging.basicConfig()` call.

    Argument `colorize` controls the use of the `coloredlogs`_ module to
    color-code log output lines.  The default value ``auto`` enables log
    colorization iff the `sys.stderr` stream is connected to a terminal;
    a ``True`` value will enable it regardless of the log output stream
    terminal status, and any ``False`` value will disable log
    colorization altogether.  Note that log colorization can anyway be
    disabled if `coloredlogs`_ thinks that the terminal is not capable
    of colored output; see `coloredlogs.terminal_supports_colors`__.
    If the `coloredlogs`_ module cannot be imported, a warning is logged
    and log colorization is disabled.

    .. _coloredlogs: https://coloredlogs.readthedocs.org/en/latest/#
    .. __: http://humanfriendly.readthedocs.org/en/latest/index.html#humanfriendly.terminal.terminal_supports_colors

    A user configuration file named ``NAME.log.conf`` or
    ``gc3pie.log.conf`` is searched for in the directory pointed to by
    environment variable ``GC3PIE_CONF``, and then in ``~/.gc3``; if
    found, it is read and used for more advanced configuration; if it
    does not exist, then a sample one is created in location
    ``~/.gc3/gc3pie.log.conf``
    """
    # ensure basic logger configuration is there, and load more
    # complex config from file if it exists
    logging.basicConfig(level=level, format=format, datefmt=datefmt)
    _load_logging_configuration_file(name)
    # initialize a logger so we can complain about missing
    # `coloredlogs` later on
    log = logging.getLogger(name)
    log.setLevel(level)
    log.propagate = True
    # try to colorize logs going to the console
    if colorize == 'auto':
        # set if STDERR is connected to a terminal
        colorize = sys.stderr.isatty()
    if colorize:
        try:
            import coloredlogs
            for name in set([
                    #None,  # root logger
                    "gc3.gc3libs",
                    name,
            ]):
                coloredlogs.install(
                    logger=logging.getLogger(name),
                    reconfigure=True,
                    stream=sys.stderr,
                    level=level,
                    fmt=format,
                    datefmt=datefmt,
                    programname=name)
        except ImportError as err:
            log.warning("Could not import `coloredlogs` module: %s", err)
    return log


def _load_logging_configuration_file(name=None):
    if name is None:
        name = os.path.basename(sys.argv[0])
    # determine where to look for conf files
    log_conf_dirs = [gc3libs.defaults.RCDIR]
    gc3pie_conf_path = os.environ.get('GC3PIE_CONF', '')
    if os.path.exists(gc3pie_conf_path):
        if not os.path.isdir(gc3pie_conf_path):
            gc3pie_conf_path = os.path.dirname(gc3pie_conf_path)
        log_conf_dirs.insert(0, gc3pie_conf_path)
    # use first logging configuration found
    for log_conf_file in [
            name + '.log.conf',
            'gc3pie.log.conf',
    ]:
        for log_conf_dir in log_conf_dirs:
            log_conf = os.path.join(log_conf_dir, log_conf_file)
            if os.path.exists(log_conf):
                break
    else:
        deploy_configuration_file(log_conf, "logging.conf.example")
    # actually read the file that was found or deployed above
    logging.config.fileConfig(log_conf, {
        'RCDIR': gc3libs.defaults.RCDIR,
        'HOMEDIR': os.path.expandvars('$HOME'),
        'HOSTNAME': platform.node(),
    })
    return log_conf


UNIGNORE_ERRORS = set(os.environ.get('GC3PIE_NO_CATCH_ERRORS', '').split(','))
if 'ALL' in UNIGNORE_ERRORS:
    UNIGNORE_ALL_ERRORS = True
else:
    UNIGNORE_ALL_ERRORS = False


def error_ignored(*ctx):
    """
    Return ``True`` if no object in list `ctx` matches the contents of the
    ``GC3PIE_NO_CATCH_ERRORS`` environment variable.

    Note that the list of un-ignored errors is determined when the `gc3libs`
    module is initially loaded and is thus insensitive to changes in the
    environment that happen afterwards.

    The calling interface is so designed, that a list of keywords
    describing -or related- to the error are passed; if any of them
    has been mentioned in the environment variable
    ``GC3PIE_NO_CATCH_ERRORS`` then this function returns ``False`` --
    i.e., the error is never ignored by GC3Pie and always propagated
    to the top-level handler.

    """
    if UNIGNORE_ALL_ERRORS:
        return False
    else:
        words = set(str(word).lower() for word in ctx)
        return not UNIGNORE_ERRORS.intersection(words)


# Task and Application classes
class Task(Persistable, Struct):

    """
    Mix-in class implementing a facade for job control.

    A `Task` can be described as an "active" job, in the sense that
    all job control is done through methods on the `Task` instance
    itself; contrast this with operating on `Application` objects
    through a `Core` or `Engine` instance.

    The following pseudo-code is an example of the usage of the `Task`
    interface for controlling a job.  Assume that `GamessApplication` is
    inheriting from `Task` (it actually is)::

        t = GamessApplication(input_file)
        t.submit()
        # ... do other stuff
        t.update_state()
        # ... take decisions based on t.execution.state
        t.wait() # blocks until task is terminated

    Each `Task` object has an `execution` attribute: it is an instance
    of class :class:`Run`, initialized with a new instance of `Run`,
    and at any given time it reflects the current status of the
    associated remote job.  In particular, `execution.state` can be
    checked for the current task status.

    After successful initialization, a `Task` instance will have the
    following attributes:

    `changed`
      evaluates to `True` if the `Task` has been changed since last
      time it has been saved to persistent storage (see
      :mod:`gclibs.persistence`)

    `execution`
      a `Run` instance; its state attribute is initially set to ``NEW``.

    """

    # assume that a generic `Task` produces no output -- this should
    # be changed in subclasses!
    would_output = False

    def __init__(self, **extra_args):
        """
        Initialize a `Task` instance.

        The following attributes are defined on a valid `Task` instance:

        * `execution`: a `gc3libs.Run`:class: instance

        :param grid: A :class:`gc3libs.Engine` or
                     :class:`gc3libs.Core` instance, or anything
                     implementing the same interface.
        """
        Persistable.__init__(self, **extra_args)
        Struct.__init__(self, **extra_args)
        self.execution = Run(attach=self)
        # `_controller` and `_attached` are set by `attach()`/`detach()`
        self._attached = False
        self._controller = None
        self.changed = True
        TaskStateChange.connect(self._on_state_change, sender=self)

    def __hash__(self):
        # FIXME: this might be incorrect, still `__hash__
        # = id` is what Python 2 does, and it has not broken
        # GC3Pie so far... For more details, see:
        # - https://docs.python.org/3/reference/datamodel.html#object.__hash__
        # - https://docs.python.org/2/reference/datamodel.html#object.__hash__
        return id(self)


    # manipulate the "controller" interface used to control the associated job
    def attach(self, controller):
        """
        Use the given Grid interface for operations on the job
        associated with this task.
        """
        if self._controller != controller:
            if self._attached:
                self.detach()
            controller.add(self)
            self._attached = True
            self._controller = controller

    class __NoController(object):
        """
        A mock "controller", that just throws a `DetachedFromController`
        exception when any of its methods is used.

        We use this as a safeguard for detached `Task` objects,
        in order to get sensible error reporting.

        .. note::

          Lookup of *any* attribute on an instance of this class
          returns a function object; should be fine since a
          "controller" interface should just contain methods, but one
          never knows...
        """
        def __getattr__(self, name):
            def throw_error(*args, **kwargs):
                raise gc3libs.exceptions.DetachedFromControllerError(
                    "Task object is not attached to any controller.")
            return throw_error

    def detach(self):
        """
        Remove any reference to the current grid interface.  After
        this, calling any method other than :meth:`attach` results in
        an exception :class:`TaskDetachedFromControllerError` being thrown.
        """
        if self._attached:
            try:
                self._controller.remove(self)
            except:
                pass
            self._controller = self.__NoController()
            self._attached = False

    # interface with pickle/gc3libs.persistence: do not save the
    # attached grid/engine/core as well: it definitely needs to be
    # saved separately.

    def __getstate__(self):
        state = self.__dict__.copy()
        state['_controller'] = None
        state['_attached'] = None
        state['changed'] = False
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self.detach()
        TaskStateChange.connect(self._on_state_change, sender=self)

    # grid-level actions on this Task object are re-routed to the
    # grid/engine/core instance
    def submit(self, resubmit=False, targets=None, **extra_args):
        """
        Start the computational job associated with this `Task` instance.
        """
        assert self._attached, ("Task.submit() called on detached task %s." %
                                self)
        assert hasattr(self._controller, 'submit'), \
            ("Invalid `_controller` object '%s' in Task %s" %
             (self._controller, self))
        self._controller.submit(self, resubmit, targets, **extra_args)

    def update_state(self, **extra_args):
        """
        In-place update of the execution state of the computational
        job associated with this `Task`.  After successful completion,
        `.execution.state` will contain the new state.

        After the job has reached the `TERMINATING` state, the following
        attributes are also set:

        `execution.duration`
          Time lapse from start to end of the job at the remote
          execution site, as a `gc3libs.quantity.Duration`:class: value.
          (This is also often referred to as the 'wall-clock time' or
          `walltime`:term: of the job.)

        `execution.max_used_memory`
          Maximum amount of RAM used during job execution, represented
          as a `gc3libs.quantity.Memory`:class: value.

        `execution.used_cpu_time`
          Total time (as a `gc3libs.quantity.Duration`:class: value) that the
          processors has been actively executing the job's code.

        The execution backend may set additional attributes; the exact
        name and format of these additional attributes is
        backend-specific.  However, you can easily identify the
        backend-specific attributes because their name is prefixed
        with the (lowercased) backend name; for instance, the
        `PbsLrms`:class: backend sets attributes `pbs_queue`,
        `pbs_end_time`, etc.

        """
        assert self._attached, (
            "Task.update_state() called on detached task %s." % self)
        assert hasattr(self._controller, 'update_job_state'), \
            ("Invalid `_controller` object '%s' in Task %s" %
             (self._controller, self))
        self._controller.update_job_state(self, **extra_args)

    def kill(self, **extra_args):
        """
        Terminate the computational job associated with this task.

        See :meth:`gc3libs.Core.kill` for a full explanation.
        """
        assert self._attached, ("Task.kill() called on detached task %s." %
                                self)
        assert hasattr(self._controller, 'kill'), \
            ("Invalid `_controller` object '%s' in Task %s" %
             (self._controller, self))
        self._controller.kill(self, **extra_args)

    def fetch_output(self, output_dir=None,
                     overwrite=False, changed_only=True, **extra_args):
        """
        Retrieve the outputs of the computational job associated with
        this task into directory `output_dir`, or, if that is `None`,
        into the directory whose path is stored in instance attribute
        `.output_dir`.

        If the execution state is `TERMINATING`, transition the state to
        `TERMINATED` (which runs the appropriate hook).

        See :meth:`gc3libs.Core.fetch_output` for a full explanation.

        :return: Path to the directory where the job output has been
                 collected.
        """
        if self.execution.state == Run.State.TERMINATED:
            return self.output_dir
        if self.execution.state == Run.State.TERMINATING:
            # advance state to TERMINATED
            self.output_dir = self._get_download_dir(output_dir)
            if self.output_dir:
                self.execution.info = (
                    "Final output downloaded to '%s'" % self.output_dir)
            self.execution.state = Run.State.TERMINATED
            self.changed = True
            return self.output_dir
        else:
            download_dir = self._get_download_dir(output_dir)
            self.execution.info = (
                "Output snapshot downloaded to '%s'" % download_dir)
            return download_dir

    def _get_download_dir(self, download_dir):
        """
        Return a directory path where to download this Task's output files,
        or ``None`` if this Task is not expected to produce any output.

        For output-producing tasks, if the given `download_dir` is not
        None, return that.  Otherwise, return the directory saved on
        this object in attribute `output_dir`.  If all else fails,
        raise an `InvalidArgument` exception.
        """
        # XXX: the try/except block below is there for compatibility
        # with saved jobs -- should be removed after release 2.3
        try:
            if not self.would_output:
                return None
        except AttributeError:
            pass
        # determine download dir
        if download_dir is not None:
            return download_dir
        else:
            try:
                return self.output_dir
            except AttributeError:
                raise gc3libs.exceptions.InvalidArgument(
                    "`Task._get_download_dir()` called with no explicit"
                    " download directory, but object '%s' (%s) has no `output_dir`"
                    " attribute set either."
                    % (self, type(self)))

    def peek(self, what='stdout', offset=0, size=None, **extra_args):
        """
        Download `size` bytes (at offset `offset` from the start) from
        the associated job standard output or error stream, and write them
        into a local file.  Return a file-like object from which the
        downloaded contents can be read.

        See :meth:`gc3libs.Core.peek` for a full explanation.
        """
        assert self._attached, ("Task.peek() called on detached task %s." %
                                self)
        assert hasattr(self._controller, 'peek'), \
            ("Invalid `_controller` object '%s' in Task %s" %
             (self._controller, self))
        return self._controller.peek(self, what, offset, size, **extra_args)

    def free(self, **extra_args):
        """
        Release any remote resources associated with this task.

        See :meth:`gc3libs.Core.free` for a full explanation.
        """
        assert self._attached, ("Task.free() called on detached task %s." %
                                self)
        assert hasattr(self._controller, 'free'), \
            ("Invalid `_controller` object '%s' in Task %s" %
             (self._controller, self))
        self._controller.free(self, **extra_args)

    # convenience methods, do not really add any functionality over
    # what's above
    def progress(self):
        """
        Advance the associated job through all states of a regular
        lifecycle. In detail:

          1. If `execution.state` is `NEW`, the associated job is started.
          2. The state is updated until it reaches `TERMINATED`
          3. Output is collected and the final returncode is returned.

        An exception `TaskError` is raised if the job hits state
        `STOPPED` or `UNKNOWN` during an update in phase 2.

        When the job reaches `TERMINATING` state, the output is
        retrieved; if this operation is successfull, state is advanced
        to `TERMINATED`.

        Once the job reaches `TERMINATED` state, the return code
        (stored also in `.returncode`) is returned; if the job is not
        yet in `TERMINATED` state, calling `progress` returns `None`.

        :raises: exception :class:`UnexpectedStateError` if the
                 associated job goes into state `STOPPED` or `UNKNOWN`

        :return: final returncode, or `None` if the execution
                 state is not `TERMINATED`.

        """
        # first update state, we'll submit NEW jobs last, so that the
        # state is not updated immediately after submission as ARC
        # does not cope well with this...
        if self.execution.state in [Run.State.SUBMITTED,
                                    Run.State.RUNNING,
                                    Run.State.STOPPED,
                                    Run.State.UNKNOWN]:
            self.update_state()
        # now "do the right thing" based on actual state
        if self.execution.state in [Run.State.STOPPED,
                                    Run.State.UNKNOWN]:
            raise gc3libs.exceptions.UnexpectedStateError(
                "Task '%s' entered `%s` state." % (self, self.execution.state))
        elif self.execution.state == Run.State.NEW:
            self.submit()
        elif self.execution.state == Run.State.TERMINATING:
            self.fetch_output()
            return self.execution.returncode

    def redo(self, *args, **kwargs):
        """
        Reset the state of this Task instance to ``NEW``.

        This is only allowed for tasks which are already in a terminal
        state, or one of ``STOPPED``, ``UNKNOWN``, or ``NEW``;
        otherwise an `AssertionError` is raised.

        The task should then be resubmitted to actually resume
        execution.

        See also `SequentialTaskCollection.redo`:meth:.

        :raises AssertionError: if this Task's state is not terminal.
        """
        assert self.execution.state in [
            Run.State.NEW,  # allow re-doing partially run TaskCollections
            Run.State.STOPPED,
            Run.State.TERMINATED,
            Run.State.TERMINATING,
            Run.State.UNKNOWN,
        ], ("Can only re-do a Task which is in a terminal state;"
            " task {0} is in state {1} instead."
            .format(self, self.execution.state))
        self.execution.state = Run.State.NEW

    def wait(self, interval=60):
        """
        Block until the associated job has reached `TERMINATED` state,
        then return the job's return code.  Note that this does not
        automatically fetch the output.

        :param integer interval: Poll job state every this number of seconds
        """
        # FIXME: I'm not sure how to deal with this... Ideally, this
        # call should suspend the current thread and wait for
        # notifications from the Engine, but:
        #  - there's no way to tell if we are running threaded,
        #  - `self._controller` could be a `Core` instance, thus not capable
        #    of running independently.
        # For now this is a poll+sleep loop, but we certainly need to revise
        # it.
        while True:
            self.update_state()
            if self.execution.state == Run.State.TERMINATED:
                return self.returncode
            time.sleep(interval)

    # State transition handlers.
    #

    def _on_state_change(self, task, from_state, to_state):
        if id(task) != id(self):
            return
        handler_name = to_state.lower()
        gc3libs.log.debug(
            "Calling state-transition handler '%s' on %s ...",
            handler_name, self)
        handler = getattr(self, handler_name, None)
        if handler is not None:
            return handler()

    def new(self):
        """
        Called when the job state is (re)set to `NEW`.

        Note this will not be called when the application object is
        created, rather if the state is reset to `NEW` after it has
        already been submitted.

        The default implementation does nothing, override in derived
        classes to implement additional behavior.
        """
        pass

    def submitted(self):
        """
        Called when the job state transitions to `SUBMITTED`, i.e.,
        the job has been successfully sent to a (possibly) remote
        execution resource and is now waiting to be scheduled.

        The default implementation does nothing, override in derived
        classes to implement additional behavior.
        """
        pass

    def running(self):
        """
        Called when the job state transitions to `RUNNING`, i.e., the
        job has been successfully started on a (possibly) remote
        resource.

        The default implementation does nothing, override in derived
        classes to implement additional behavior.
        """
        pass

    def stopped(self):
        """
        Called when the job state transitions to `STOPPED`, i.e., the
        job has been remotely suspended for an unknown reason and
        cannot automatically resume execution.

        The default implementation does nothing, override in derived
        classes to implement additional behavior.
        """
        pass

    def terminating(self):
        """
        Called when the job state transitions to `TERMINATING`, i.e.,
        the remote job has finished execution (with whatever exit
        status, see `returncode`) but output has not yet been
        retrieved.

        The default implementation does nothing, override in derived
        classes to implement additional behavior.
        """
        pass

    def terminated(self):
        """
        Called when the job state transitions to `TERMINATED`, i.e.,
        the job has finished execution (with whatever exit status, see
        `returncode`) and the final output has been retrieved.

        The location where the final output has been stored is
        available in attribute `self.output_dir`.

        The default implementation does nothing, override in derived
        classes to implement additional behavior.
        """
        pass

    def unknown(self):
        """
        Called when the job state transitions to `UNKNOWN`, i.e.,
        the job has not been updated for a certain period of time
        thus it is placed in UNKNOWN state.

        Two possible ways of changing from this state:
        1) next update cycle, job status is updated from the remote
        server
        2) derive this method for Application specific logic to deal
        with this case

        The default implementation does nothing, override in derived
        classes to implement additional behavior.
        """
        pass


class Application(Task):

    """
    Support for running a generic application with the GC3Libs.

    The following parameters are *required* to create an `Application`
    instance:

    `arguments`
      List or sequence of program arguments. The program to execute is
      the first one.; any object in the list will be converted to
      string via Python's `str()`.

    `inputs`
      Files that will be copied to the remote execution node before
      execution starts.

      There are two possible ways of specifying the `inputs` parameter:

      * It can be a Python dictionary: keys are local file paths or
        URLs, values are remote file names.

      * It can be a Python list: each item in the list should be a pair
        `(source, remote_file_name)`: the `source` can be a local file
        or a URL; `remote_file_name` is the path (relative to the
        execution directory) where `source` will be downloaded.  If
        `remote_file_name` is an absolute path, an
        :class:`InvalidArgument` error is raised.

      A single string `file_name` is allowed instead of the pair
      and results in the local file `file_name` being copied to
      `file_name` on the remote host.

    `outputs`
      Files and directories that will be copied from the remote
      execution node back to the local computer (or a
      network-accessible server) after execution has completed.
      Directories are copied recursively.

      There are three possible ways of specifying the `outputs` parameter:

      * It can be a Python dictionary: keys are remote file or directory
        paths (relative to the execution directory), values are
        corresponding local names.

      * It can be a Python list: each item in the list should be a pair
        `(remote_file_name, destination)`: the `destination` can be a
        local file or a URL; `remote_file_name` is the path (relative to
        the execution directory) that will be uploaded to `destination`.
        If `remote_file_name` is an absolute path, an
        :class:`InvalidArgument` error is raised.

      A single string `file_name` is allowed instead of the pair
      and results in the remote file `file_name` being copied to
      `file_name` on the local host.

      * The constant `gc3libs.ANY_OUTPUT` which instructs GC3Libs to
        copy every file in the remote execution directory back to the
        local output path (as specified by the `output_dir` attribute).

      Note that no errors will be raised if an output file is not present.
      Override the `terminated`:meth: method to raise errors for reacting
      on this kind of failures.

    `output_dir`
      Path to the base directory where output files will be downloaded.
      Output file names are interpreted relative to this base directory.

    `requested_cores`,`requested_memory`,`requested_walltime`
      Specify resource requirements for the application:

      * the number of independent execution units (CPU cores; all are
        required to be in the same execution node);

      * amount of memory (as a `gc3libs.quantity.Memory`:class: object)
        for the task as a whole, i.e., independent of number of CPUs
        allocated;

      * amount of wall-clock time to allocate for the computational job
        (as a `gc3libs.quantity.Duration`:class: object).

    The following optional parameters may be additionally
    specified as keyword arguments and will be given special
    treatment by the `Application` class logic:

    `requested_architecture`
      specify that this application can only be executed on a certain
      processor architecture; see `Run.Arch`:class: for a list of
      possible values.  The default value `None` means that any
      architecture is valid, i.e., there are no requirements on the
      processor architecture.

    `environment`
      a dictionary defining environment variables and the values to
      give them in the task execution setting.  Keys of the dictionary
      are environmental variables names, and dictionary values define
      the corresponding variable content.  Both keys and values must
      be strings or convertible to string; keys (environment variable
      names) must be ASCII-only or a ``UnicodeDecodeError`` will be
      raised.

      For example, to run the application in an environment where the
      variable ``LC_ALL`` has the value ``C`` and the variable ``HZ``
      has the value ``100``, one would use::

        Application(...,
          environment={'LC_ALL':'C', 'HZ':100},
        ...)

    `output_base_url`
      if not `None`, this is prefixed to all output files (except
      stdout and stderr, which are always retrieved), so, for
      instance, having `output_base_url="gsiftp://example.org/data"`
      will upload output files into that remote directory.

    `stdin`
      file name of a file whose contents will be fed as
      standard input stream to the remote-executing process.

    `stdout`
      name of a file where the standard output stream of
      the remote executing process will be redirected to; will be
      automatically added to `outputs`.

    `stderr`
      name of a file where the standard error stream of
      the remote executing process will be redirected to; will be
      automatically added to `outputs`.

    `join`
      if this evaluates to `True`, then standard error is
      redirected to the file specified by `stdout` and `stderr` is
      ignored.  (`join` has no effect if `stdout` is not given.)

    `jobname`
      a string to display this job in user-oriented listings

    `tags`
      list of tag names (string) that must be present on a
      resource in order to be eligible for submission.

    Any other keyword arguments will be set as instance attributes,
    but otherwise ignored by the `Application` constructor.

    After successful construction, an `Application` object is
    guaranteed to have the following instance attributes:

    `arguments`
    list of strings specifying command-line arguments for executable
    invocation. The first element must be the executable.

    `inputs`
      dictionary mapping source URL (a `gc3libs.url.Url`:class:
      object) to a remote file name (a string); remote file names are
      relative paths (root directory is the remote job folder)

    `outputs`
      dictionary mapping remote file name (a string) to a destination
      (a `gc3libs.url.Url`:class:); remote file names are relative
      paths (root directory is the remote job folder)

    `output_dir`
      Path to the base directory where output files will be
      downloaded.  Output file names (those which are not URLs) are
      interpreted relative to this base directory.

    `execution`
      a `Run` instance; its state attribute is initially set to ``NEW``
      (Actually inherited from the `Task`:class:)

    `environment`
      dictionary mapping environment variable names to the requested
      value (string); possibly empty

    `stdin`
      ``None`` or a string specifying a (local) file name.  If `stdin`
      is not None, then it matches a key name in `inputs`

    `stdout`
      ``None`` or a string specifying a (remote) file name.  If `stdout`
      is not None, then it matches a key name in `outputs`

    `stderr`
      ``None`` or a string specifying a (remote) file name.  If `stdout`
      is not None, then it matches a key name in `outputs`

    `join`
      boolean value, indicating whether `stdout` and `stderr` are
      collected into the same file

    `tags`
      list of strings specifying the tags to request in each resource
      for submission; possibly empty.
    """

    application_name = 'generic'
    """
    A name for applications of this class.

    This string is used as a prefix for configuration items related to
    this application in configured resources.  For example, if the
    `application_name` is ``foo``, then the application interface code
    in GC3Pie might search for ``foo_cmd``, ``foo_extra_args``, etc.
    See `qsub_sge`:meth: for an actual example.
    """

    def __init__(self, arguments, inputs, outputs, output_dir, **extra_args):
        # required parameters
        if isinstance(arguments, string_types):
            arguments = shlex.split(arguments)

        if 'executable' in extra_args:
            gc3libs.log.warning(
                "The `executable` argument is not supported anymore in"
                " GC3Pie 2.0. Please adapt your code and use `arguments`"
                " only.")
            arguments = [extra_args['executable']] + list(arguments)

        self.arguments = [to_str(x, 'filesystem') for x in arguments]

        self.inputs = Application._io_spec_to_dict(UrlKeyDict, inputs, True)
        self.outputs = Application._io_spec_to_dict(
            UrlValueDict, outputs, False)

        # check that remote entries are all distinct
        # (can happen that two local paths are mapped to the same remote one)
        if len(list(self.inputs.values())) != len(set(self.inputs.values())):
            # try to build an exact error message
            inv = {}
            for l, r in self.inputs.items():
                if r in inv:
                    raise gc3libs.exceptions.DuplicateEntryError(
                        "Local inputs '%s' and '%s'"
                        " map to the same remote path '%s'" %
                        (l, inv[r], r))
                else:
                    inv[r] = l

        # ensure remote paths are not absolute
        for r_path in self.inputs.values():
            if os.path.isabs(r_path):
                raise gc3libs.exceptions.InvalidArgument(
                    "Remote paths not allowed to be absolute: %s" % r_path)

        # check that local entries are all distinct
        # (can happen that two remote paths are mapped to the same local one)
        if len(list(self.outputs.values())) != len(set(self.outputs.values())):
            # try to build an exact error message
            inv = {}
            for r, l in self.outputs.items():
                if l in inv:
                    raise gc3libs.exceptions.DuplicateEntryError(
                        "Remote outputs '%s' and '%s'"
                        " map to the same local path '%s'" %
                        (r, inv[l], l))
                else:
                    inv[l] = r

        # ensure remote paths are not absolute
        for r_path in self.outputs.keys():
            if os.path.isabs(r_path):
                raise gc3libs.exceptions.InvalidArgument(
                    "Remote paths not allowed to be absolute")

        self.output_dir = to_str(output_dir, 'filesystem')

        # optional params
        self.output_base_url = extra_args.pop('output_base_url', None)

        self.requested_cores = int(extra_args.pop('requested_cores', 1))
        self.requested_memory = extra_args.pop('requested_memory', None)
        assert (self.requested_memory is None
                or isinstance(self.requested_memory,
                              gc3libs.quantity.Memory)), \
            ("Expected `Memory` instance for `requested_memory,"
             " got %r %s instead."
             % (self.requested_memory, type(self.requested_memory)))
        self.requested_walltime = extra_args.pop('requested_walltime', None)
        assert (self.requested_walltime is None
                or isinstance(self.requested_walltime,
                              gc3libs.quantity.Duration)), \
            ("Expected `Duration` instance for `requested_walltime,"
             " got %r %s instead."
             % (self.requested_walltime, type(self.requested_walltime)))
        self.requested_architecture = extra_args.pop(
            'requested_architecture', None)
        if self.requested_architecture is not None \
                and self.requested_architecture not in [
                    Run.Arch.X86_32,
                    Run.Arch.X86_64]:
            raise gc3libs.exceptions.InvalidArgument(
                "Architecture must be either '%s' or '%s'"
                % (Run.Arch.X86_32, Run.Arch.X86_64))

        self.environment = dict(
            (to_str(k), to_str(v, 'terminal'))
            for k, v in list(extra_args.pop(
                'environment', dict()).items()))

        self.join = extra_args.pop('join', False)
        self.stdin = to_str(extra_args.pop('stdin', None), 'filesystem')
        if self.stdin and (self.stdin not in self.inputs):
            self.inputs[self.stdin] = os.path.basename(self.stdin)
        self.stdout = to_str(extra_args.pop('stdout', None), 'filesystem')
        if self.stdout is not None and os.path.isabs(self.stdout):
            raise gc3libs.exceptions.InvalidArgument(
                "Absolute path '%s' passed as `Application.stdout`"
                % self.stdout)
        if ((self.stdout is not None)
                and (gc3libs.ANY_OUTPUT not in self.outputs)
                and (self.stdout not in self.outputs)):
            self.outputs[self.stdout] = self.stdout

        self.stderr = to_str(extra_args.pop('stderr', None), 'filesystem')
        join_stdout_and_stderr = (self.join
                                  or self.stderr == self.stdout
                                  or self.stderr == subprocess.STDOUT)
        if join_stdout_and_stderr:
            self.join = True
            self.stderr = self.stdout

        if self.stderr is not None and os.path.isabs(self.stderr):
            raise gc3libs.exceptions.InvalidArgument(
                "Absolute path '%s' passed as `Application.stderr`"
                % self.stderr)
        if ((self.stderr is not None)
                and (gc3libs.ANY_OUTPUT not in self.outputs)
                and (self.stderr not in self.outputs)):
            self.outputs[self.stderr] = self.stderr

        if (self.outputs or self.stdout or self.stderr):
            self.would_output = True

        self.tags = extra_args.pop('tags', list())

        if 'jobname' in extra_args:
            # constrain job name to be ASCII-only, or some
            # batch-queuing systems might reject it
            jobname = to_str(extra_args['jobname'], 'ascii')
            # Check whether the first character of a jobname is an
            # integer. SGE does not allow job names to start with a
            # number, so add a prefix...
            if len(jobname) == 0:
                gc3libs.log.warning(
                    "Empty string passed as jobname to %s,"
                    " generating UUID job name", self)
                jobname = ("GC3Pie.%s.%s"
                           % (self.__class__.__name__, uuid.uuid4()))
            elif str(jobname)[0] not in string.ascii_letters:
                gc3libs.log.warning(
                    "Supplied job name `%s` for %s does not start"
                    " with a letter; changing it to `GC3Pie.%s`"
                    % (jobname, self, jobname))
                jobname = "GC3Pie.%s" % jobname
            extra_args['jobname'] = jobname
        # task setup; creates the `.execution` attribute as well
        Task.__init__(self, **extra_args)

        # for k,v in self.outputs.iteritems():
        #     gc3libs.log.debug("outputs[%s]=%s", repr(k), repr(v))
        # for k,v in self.inputs.iteritems():
        #     gc3libs.log.debug("inputs[%s]=%s", repr(k), repr(v))

    @staticmethod
    def _io_spec_to_dict(ctor, spec, force_abs):
        """
        (This function is only used for internal processing of `input`
        and `output` fields.)

        Return a dictionary formed by pairs `URL:name` or `name:URL`.
        The `URL` part is a tuple as returned by functions `urlparse`
        and `urlsplit` in the Python standard module
        `urlparse`:module: -- `name` is a string that should be
        interpreted as a filename (relative to the job execution
        directory).

        Argument `ctor` is the constructor for the dictionary class to
        return; `gc3libs.url.UrlKeyDict` and
        `gc3libs.url.UrlValueDict` are valid values here.

        Argument `spec` is either a list or a Python `dict` instance.

        If a Python `dict` is given, then it is copied into an
        `gc3libs.url.UrlDict`, and that copy is returned::

          >>> d1 = { '/tmp/1':'1', '/tmp/2':'2' }
          >>> d2 = Application._io_spec_to_dict(
          ...     gc3libs.url.UrlKeyDict, d1, True)
          >>> isinstance(d2, gc3libs.url.UrlKeyDict)
          True
          >>> for key in sorted(d2.keys()): print(key.path)
          /tmp/1
          /tmp/2

        If `spec` is a list, each element can either be a tuple
        `(path, name)`, or a string `path`, which is converted to a
        tuple `(path, name)` by setting `name =
        os.path.basename(path)`::

          >>> l1 = [ ('/tmp/1', '1'), '/tmp/2' ]
          >>> d3 = Application._io_spec_to_dict(
          ...     gc3libs.url.UrlKeyDict, l1, True)
          >>> d3 == d2
          True

        If `force_abs` is `True`, then all paths are converted to
        absolute ones in the dictionary keys; otherwise they are
        stored unchanged.
        """
        try:
            # is `spec` dict-like?
            return ctor(((to_str(k, 'filesystem'), to_str(v, 'filesystem'))
                         for k, v in spec.items()),
                        force_abs=force_abs)
        except AttributeError:
            # `spec` is a list-like
            return ctor((Application.__convert_to_tuple(x) for x in spec),
                        force_abs=force_abs)

    @staticmethod
    def __convert_to_tuple(val):
        """Auxiliary method for `io_spec_to_dict`:meth:, which see."""
        if isinstance(val, string_types):
            l = to_str(val, 'filesystem')
            r = os.path.basename(l)
            return (l, r)
        else:
            return (to_str(val[0], 'filesystem'),
                    to_str(val[1], 'filesystem'))

    def __str__(self):
        try:
            return str(self.persistent_id)
        except AttributeError:
            return safe_repr(self)

    def compatible_resources(self, resources):
        """
        Return a list of compatible resources.
        """
        selected = []
        for lrms in resources:
            assert (lrms is not None), \
                "Application.compatible_resources():" \
                " expected `LRMS` object, got `None` instead."
            gc3libs.log.debug(
                "Checking resource '%s' for compatibility with application"
                " requirements" % lrms.name)
            # Checking whether resource is 'enabled'. Discard otherwise
            if (not lrms.enabled):
                gc3libs.log.info(
                    "Rejecting resource '%s': resource currently disabled" %
                    lrms.name)
                continue
            # if architecture is specified, check that it matches the resource
            # one
            if (self.requested_architecture is not None
                    and self.requested_architecture not in lrms.architecture):
                gc3libs.log.info(
                    "Rejecting resource '%s': requested a different"
                    " architecture (%s) than what resource provides (%s)" %
                    (lrms.name, self.requested_architecture,
                     ', '.join([str(arch) for arch in lrms.architecture])))
                continue
            # check that Application requirements are within resource limits
            if self.requested_cores > lrms.max_cores_per_job:
                gc3libs.log.info(
                    "Rejecting resource '%s': requested more cores (%d) that"
                    " resource provides (%d)" %
                    (lrms.name, self.requested_cores, lrms.max_cores_per_job))
                continue
            if (self.requested_memory is not None and self.requested_memory >
                    self.requested_cores * lrms.max_memory_per_core):
                gc3libs.log.info(
                    "Rejecting resource '%s': requested more memory (%s) that"
                    " resource provides (%s, %s per CPU core)" %
                    (lrms.name,
                     self.requested_memory,
                     self.requested_cores * lrms.max_memory_per_core,
                     lrms.max_memory_per_core))
                continue
            if (self.requested_walltime is not None
                    and self.requested_walltime > lrms.max_walltime):
                gc3libs.log.info(
                    "Rejecting resource '%s': requested a longer duration (%s)"
                    " that resource provides (%s)" %
                    (lrms.name, self.requested_walltime, lrms.max_walltime))
                continue
            if not lrms.validate_data(
                    list(self.inputs.keys())) or not lrms.validate_data(
                    list(self.outputs.values())):
                gc3libs.log.info(
                    "Rejecting resource '%s': input/output data protocol"
                    " not supported." % lrms.name)
                continue

            selected.append(lrms)

        return selected

    @staticmethod
    def _resource_sorting_key(rsc):
        """
        Return the value to be used for comparing resource `rsc` with others.

        See `rank_resources`:meth: and Python's `sorted()` builtin.
        """
        return (rsc.user_queued, -rsc.free_slots, rsc.queued, rsc.user_run)

    def rank_resources(self, resources):
        """
        Sort the given resources in order of preference.

        By default, computational resource `a` is preferred over `b`
        if it has less queued jobs from the same user; failing that,
        if it has more free slots; failing that, if it has less queued
        jobs (in total); finally, should all preceding parameters
        compare equal, `a` is preferred over `b` if it has less
        running jobs from the same user.

        Resources where the job has already attempted to run (the
        resource front-end name is recorded in
        `.execution._execution_targets`) are then moved to the back of
        the list, to avoid resubmitting to a faulty resource.
        """
        selected = sorted(resources, key=self._resource_sorting_key)
        # shift lrms that are already in application.execution_targets
        # to the bottom of the list
        if '_execution_targets' in self.execution:
            for lrms in selected:
                if (hasattr(lrms, 'frontend')
                        and lrms.frontend in
                        self.execution._execution_targets):
                    # append resource to the bottom of the list
                    selected.remove(lrms)
                    selected.append(lrms)
        return selected


    ##
    # backend interface methods
    ##

    def cmdline(self, resource):
        """
        Return list of command-line arguments for invoking the application.

        This is exactly the *argv*-vector of the application process:
        the application command name is included as first item (index
        0) of the list, further items are command-line arguments.

        Hence, to get a UNIX shell command-line, just concatenate the
        elements of the list, separating them with spaces.
        """
        if self.environment:
            return (['/usr/bin/env']
                    + [('%s=%s' % (k, v)) for k, v in list(self.environment.items())]
                    + self.arguments[:])
        else:
            return self.arguments[:]

    def qsub_sge(self, resource, **extra_args):
        """
        Get an SGE ``qsub`` command-line invocation for submitting an
        instance of this application.

        Return a pair `(cmd_argv, app_argv)`.  Both `cmd_argv` and
        `app_argv` are *argv*-lists: the command name is included as
        first item (index 0) of the list, further items are
        command-line arguments; `cmd_argv` is the *argv*-list for the
        submission command (excluding the actual application command
        part); `app_argv` is the *argv*-list for invoking the
        application.  By overriding this method, one can add futher
        resource-specific options at the end of the `cmd_argv`
        *argv*-list.

        In the construction of the command-line invocation, one should
        assume that all the input files (as named in `Application.inputs`)
        have been copied to the current working directory, and that output
        files should be created in this same directory.

        The default implementation just prefixes any output from the
        `cmdline` method with an SGE ``qsub`` invocation of the form
        ``qsub -cwd -S /bin/sh`` + resource limits.  Note that
        *there is no generic way of requesting a certain number of cores*
        in SGE: it all depends on the installed parallel environment, and
        these are totally under control of the local sysadmin;
        therefore, any request for cores is ignored and a warning is
        logged.

        Override this method in application-specific classes to
        provide appropriate invocation templates and/or add different
        submission options.
        """
        qsub = list(resource.qsub)
        qsub += ['-cwd', '-S', '/bin/sh']
        if self.requested_walltime:
            # SGE uses `s_rt` for wall-clock time limit, expressed in seconds
            qsub += ['-l', 's_rt=%d' % self.requested_walltime.amount(seconds)]
        if self.requested_memory:
            # SGE uses `mem_free` for memory limits; 'M' suffix allowed for
            # Megabytes
            # XXX: there are a number of problems here:
            #   - `mem_free` might not be requestable, i.e., submission
            #     will fail
            #   - `mem_free` might be a JOB consumable, meaning the value
            #     should be the total amount of memory requested by the job
            #   - in the end it's all matter of local configuration, so we
            #     might need to request `h_vmem` (job total) *and*
            #     `virtual_free` (per-slot consumable) ...
            # Let's make whatever works in our cluster, and see how we can
            # extend/change it when issue reports come...
            qsub += ['-l', ('mem_free=%dM' %
                            max(1, int(old_div(self.requested_memory.amount(MB),
                                       self.requested_cores))))]
        if self.join:
            qsub += ['-j', 'yes']
        if self.stdout:
            qsub += ['-o', '%s' % self.stdout]
        if self.stdin:
            # `self.stdin` is the full pathname on the GC3Pie client host;
            # it is copied to its basename on the execution host
            qsub += ['-i', '%s' % os.path.basename(self.stdin)]
        if self.stderr:
            # from the qsub(1) man page: "If both the -j y and the -e
            # options are present, Grid Engine sets but ignores the
            # error-path attribute."
            qsub += ['-e', '%s' % self.stderr]
        if self.requested_cores != 1:
            pe_cfg_name = (self.application_name + '_pe')
            if pe_cfg_name in resource:
                pe_name = resource.get(pe_cfg_name)
            else:
                pe_name = resource.get('default_pe')
                if pe_name is not None:
                    # XXX: overly verbose reporting?
                    log.info(
                        "Application %s requested %d cores, but no '%s'"
                        " configuration item is defined on resource '%s';"
                        " using the 'default_pe' setting to submit the"
                        " parallel job.",
                        self,
                        self.requested_cores,
                        pe_cfg_name,
                        resource.name)
                else:
                    raise gc3libs.exceptions.InternalError(
                        "Application %s requested %d cores, but neither '%s'"
                        " nor 'default_pe' appear in the configuration of"
                        " resource '%s'.  Please fix the configuration and"
                        " retry." %
                        (self, self.requested_cores, pe_cfg_name,
                         resource.name))
            # XXX: it is the cluster admin's responsibility to ensure
            # that this PE allocates all cores on the same machine
            qsub += ['-pe', pe_name, ('%d' % self.requested_cores)]
        if 'jobname' in self and self.jobname:
            qsub += ['-N', '%s' % self.jobname]
        if self.environment:
            for name, value in self.environment.items():
                qsub += ['-v', '{0}={1}'.format(name, value)]
        return (qsub, self.cmdline(resource))

    def bsub(self, resource, **extra_args):
        """
        Get an LSF ``qsub`` command-line invocation for submitting an
        instance of this application.  Return a pair `(cmd_argv,
        app_argv)`, where `cmd_argv` is a list containing the
        *argv*-vector of the command to run to submit an instance of
        this application to the LSF batch system, and `app_argv` is
        the *argv*-vector to use when invoking the application.

        In the construction of the command-line invocation, one should
        assume that all the input files (as named in `Application.inputs`)
        have been copied to the current working directory, and that output
        files should be created in this same directory.

        The default implementation just prefixes any output from the
        `cmdline` method with an LSF ``bsub`` invocation of the form
        ``bsub -cwd . -L /bin/sh`` + resource limits.

        Override this method in application-specific classes to
        provide appropriate invocation templates and/or add
        resource-specific submission options.
        """
        bsub = list(resource.bsub)
        bsub += ['-cwd', '.', '-L', '/bin/sh']
        if self.requested_cores > 1:
            bsub += [
                '-n', ('%d' % self.requested_cores),
                # require that all cores reside on the same host
                '-R', 'span[hosts=1]'
            ]
        if self.requested_walltime:
            # LSF wants walltime as HH:MM (days expressed as many hours)
            hs = int(self.requested_walltime.amount(hours))
            ms = int(self.requested_walltime.amount(minutes)) % 60
            bsub += ['-W', ('%02d:%02d' % (hs, ms))]
        if self.requested_memory:
            # LSF uses `rusage[mem=...]` for memory limits (number of MBs)
            mem_mb = max(1, int(self.requested_memory.amount(MB)))
            bsub += ['-R', ('rusage[mem=%d]' % mem_mb)]
        if self.stdout:
            bsub += ['-oo', ('%s' % self.stdout)]
            if not self.join and not self.stderr:
                # LSF joins STDERR and STDOUT by default, so redirect STDERR
                # away
                bsub += ['-eo', '/dev/null']
        if self.stdin:
            # `self.stdin` is the full pathname on the GC3Pie client host;
            # it is copied to its basename on the execution host
            bsub += ['-i', ('%s' % os.path.basename(self.stdin))]
        if self.stderr:
            bsub += ['-eo', ('%s' % self.stderr)]
        if 'jobname' in self and self.jobname:
            bsub += ['-J', ('%s' % self.jobname)]
        if self.environment:
            # I could not find a way to define environment variables
            # on `bsub`'s command-line. However, according to
            # https://www.ibm.com/support/knowledgecenter/en/SSETD4_9.1.3/lsf_config_ref/lsf_envars_job_exec.html
            # LSF will copy environment variables from the user
            # environment to the execution environment. So, let's use
            # the `env` prefix trick.
            bsub = (['/usr/bin/env']
                    + ['{0}={1}'.format(name, value)
                       for name, value in self.environment.items()]
                    + bsub)
        return (bsub, self.cmdline(resource))

    def qsub_pbs(self, resource, **extra_args):
        """
        Similar to `qsub_sge()`, but for the PBS/TORQUE resource manager.
        """
        qsub = list(resource.qsub)
        if self.requested_walltime:
            qsub += ['-l', 'walltime=%s' %
                     (self.requested_walltime.amount(seconds))]
        if self.requested_memory:
            mem_mb = max(1, int(self.requested_memory.amount(MB)))
            qsub += ['-l', ('mem=%dmb' % mem_mb)]
        if self.stdin:
            # `self.stdin` is the full pathname on the GC3Pie client host;
            # it is copied to its basename on the execution host
            # XXX: this is wrong, as it redirects STDIN of the `qsub` process!
            # qsub += ['<', '%s' % os.path.basename(self.stdin)]
            raise NotImplementedError(
                "STDIN redirection is currently not handled by the"
                " PBS/TORQUE backend!")
        if self.stderr:
            # from the qsub(1) man page: "If both the -j y and the -e
            # options are present, Grid Engine sets but ignores the
            # error-path attribute."
            qsub += ['-e', '%s' % self.stderr]
        if self.stdout:
            # from the qsub(1) man page: "If both the -j y and the -e
            # options are present, Grid Engine sets but ignores the
            # error-path attribute."
            qsub += ['-o', '%s' % self.stdout]
        if self.requested_cores > 1:
            # require that all cores are on the same node
            qsub += ['-l', 'nodes=1:ppn=%d' % self.requested_cores]
        if 'jobname' in self and self.jobname:
            qsub += ['-N', '%s' % self.jobname[:15]]
        if self.environment:
            # XXX: is the `-v` option also supported by older versions of PBS/TORQUE?
            for name, value in self.environment.items():
                qsub += ['-v', '{0}={1}'.format(name, value)]
        return (qsub, self.cmdline(resource))

    def sbatch(self, resource, **extra_args):
        """
        Get a SLURM ``sbatch`` command-line invocation for submitting an
        instance of this application.

        Return a pair `(cmd_argv, app_argv)`.  Both `cmd_argv` and
        `app_argv` are *argv*-lists: the command name is included as
        first item (index 0) of the list, further items are
        command-line arguments; `cmd_argv` is the *argv*-list for the
        submission command (excluding the actual application command
        part); `app_argv` is the *argv*-list for invoking the
        application.  By overriding this method, one can add futher
        resource-specific options at the end of the `cmd_argv`
        *argv*-list.

        In the construction of the command-line invocation, one should
        assume that all the input files (as named in `Application.inputs`)
        have been copied to the current working directory, and that output
        files should be created in this same directory.

        Override this method in application-specific classes to
        provide appropriate invocation templates and/or add different
        submission options.
        """
        cmdline = self.cmdline(resource)
        sbatch = list(resource.sbatch)
        sbatch += ['--no-requeue']
        if self.requested_walltime:
            # SLURM uses `--time` for wall-clock time limit, expressed in
            # minutes
            sbatch += ['--time', ('%d' % self.requested_walltime.amount(minutes))]
        if self.stdout:
            sbatch += ['--output', ('%s' % self.stdout)]
        if self.stdin:
            # `self.stdin` is the full pathname on the GC3Pie client host;
            # it is copied to its basename on the execution host
            sbatch += ['--input', ('%s' % os.path.basename(self.stdin))]
        if self.stderr:
            sbatch += ['-e', ('%s' % self.stderr)]
        if self.requested_cores != 1:
            sbatch += ['--ntasks', '1',
                       # require that all cores are on the same node
                       '--cpus-per-task', ('%d' % self.requested_cores)]
            # we have to run the command through `srun` otherwise
            # SLURM launches every task as a single-CPU
            cmdline = (list(resource.srun)
                       + ['--cpus-per-task', ('%d' % self.requested_cores)]
                       + cmdline)
        if self.requested_memory:
            # SLURM uses `mem_free` for memory limits;
            # 'M' suffix allowed for Megabytes
            mem_mb = max(1, int(self.requested_memory.amount(MB)))
            sbatch += ['--mem', ('%d' % mem_mb)]
        if 'jobname' in self and self.jobname:
            sbatch += ['--job-name', ('%s' % self.jobname)]
        # ensure `sbatch` environment is propagated to jobs
        # (should be the default, but "explicit is better than implicit")
        sbatch += ['--export', 'ALL']
        if self.environment:
            # ensure all the required env vars are set in `sbatch`'s
            # environment, so they will be propagated to the job
            sbatch = (['/usr/bin/env']
                      + ['{0}={1}'.format(name, value)
                         for name, value in self.environment.items()]
                      + sbatch)

        return (sbatch, cmdline)

    # Operation error handlers; called when transition from one state
    # to another fails.  The names are formed by suffixing the
    # corresponding `Core` method (operation) with ``_error``.

    def submit_error(self, exs):
        """
        Invocation of `Core.submit()` on this object failed;
        `exs` is a list of `Exception` objects, one for each attempted
        submission.

        If this method returns an exception object, that is raised as
        a result of the `Core.submit()`, otherwise the return value is
        ignored and `Core.submit` returns `None`.

        Default is to always return the first exception in the list
        (on the assumption that it is the root of all exceptions or
        that at least it refers to the preferred resource).  Override
        in derived classes to change this behavior.
        """
        assert len(exs) > 0, \
            "Application.submit_error called with empty list of exceptions."
        # XXX: should we choose the first or the last exception occurred?
        # vote for choosing the first, on the basis that it refers to the
        # "best" submission target
        return exs[0]

    # XXX: this method might be dangerous in that it can break the
    # `update_job_state` semantics; it's here for completeness, but we
    # should consider removing...
    def update_job_state_error(self, ex):
        """Handle exceptions that occurred during a `Core.update_job_state`
        call.

        If this method returns an exception object, that exception is
        processed in `Core.update_job_state()` instead of the original
        one.  Any other return value is ignored and
        `Core.update_job_state` proceeds as if no exception had
        happened.

        Argument `ex` is the exception that was raised by the backend
        during job state update.

        Default is to return `ex` unchanged; override in derived
        classes to change this behavior.

        """
        return ex

    def fetch_output_error(self, ex):
        """
        Invocation of `Core.fetch_output()` on this object failed;
        `ex` is the `Exception` that describes the error.

        If this method returns an exception object, that is raised as
        a result of the `Core.fetch_output()`, otherwise the return
        value is ignored and `Core.fetch_output` returns `None`.

        Default is to return `ex` unchanged; override in derived classes
        to change this behavior.
        """
        return ex


class _Signal(object):

    """
    Base class for representing fake signals encoding the failure
    reason for GC3Libs jobs.
    """

    def __init__(self, signum, description):
        self._signum = signum
        self.__doc__ = description
    # conversion to integer types

    def __int__(self):
        return self._signum

    def __long__(self):
        return self._signum
    # human-readable explanation

    def __str__(self):
        return "Signal %d: %s" % (self._signum, self.__doc__)


class _Signals(object):

    """
    Collection of (fake) signals used to encode termination reason in
    `Run.returncode`.

    ======  ============================================================
    signal  error condition
    ======  ============================================================
    125     submission to batch system failed
    124     remote error (e.g., execution node crashed, batch system
            misconfigured)
    123     data staging failure
    122     job killed by batch system / sysadmin
    121     job canceled by user
    ======  ============================================================

    """

    Lost = _Signal(120, "Remote site reports no information about the job")
    Cancelled = _Signal(121, "Job canceled by user")
    RemoteKill = _Signal(122, "Job killed by batch system or sysadmin")
    DataStagingFailure = _Signal(123, "Data staging failure")
    RemoteError = _Signal(124, "Unspecified remote error,"
                          " e.g., execution node crashed"
                          " or batch system misconfigured")
    SubmissionFailed = _Signal(125, "Submission to batch system failed.")

    def __contains__(self, signal):
        if (signal is not None and 120 <= int(signal) <= 125):
            return True
        else:
            return False

    def __getitem__(self, signal_num):
        if signal_num == 120:
            return _Signals.Lost
        elif signal_num == 121:
            return _Signals.Cancelled
        elif signal_num == 122:
            return _Signals.RemoteKill
        elif signal_num == 123:
            return _Signals.DataStagingFailure
        elif signal_num == 124:
            return _Signals.RemoteError
        elif signal_num == 125:
            return _Signals.SubmissionFailed
        else:
            raise gc3libs.exceptions.InvalidArgument(
                "Unknown signal number %d" % signal_num)


class Run(Struct):

    """
    A specialized `dict`-like object that keeps information about
    the execution state of an `Application` instance.

    A `Run` object is guaranteed to have the following attributes:

      `log`
        A `gc3libs.utils.History` instance, recording human-readable text
        messages on events in this job's history.

      `info`
        A simplified interface for reading/writing messages to
        `Run.log`.  Reading from the `info` attribute returns the last
        message appended to `log`.  Writing into `info` appends a
        message to `log`.

      `timestamp`
        Dictionary, recording the most recent timestamp when a certain
        state was reached.  Timestamps are given as UNIX epochs.

    For properties `state`, `signal` and `returncode`, see the
    respective documentation.

    `Run` objects support attribute lookup by both the ``[...]`` and
    the ``.`` syntax; see `gc3libs.utils.Struct` for examples.
    """

    def __init__(self, initializer=None, attach=None, **keywd):
        """
        Create a new Run object; constructor accepts the same
        arguments as the `dict` constructor.

        Examples:

          1. Create a new job with default parameters::

            >>> j1 = Run()
            >>> print (j1.returncode)
            None
            >>> j1.state == 'NEW'
            True

          2. Create a new job with additional attributes::

            >>> j2 = Run(application='GAMESS', version='2010R1')
            >>> j2.state
            'NEW'
            >>> j2.application == 'GAMESS'
            True
            >>> j2['version'] == '2010R1'
            True

          3. Clone an existing job object::

            >>> j3 = Run(j2)
            >>> j3.application == 'GAMESS'
            True
            >>> j3['version'] == '2010R1'
            True

        """
        self._ref = attach
        self._state = Run.State.NEW
        self._exitcode = None
        self._signal = None

        # to overcome the "black hole" effect
        self._execution_targets = []

        Struct.__init__(self, initializer, **keywd)

        if 'history' not in self:
            self.history = History()
        if 'timestamp' not in self:
            self.timestamp = OrderedDict()

    @property
    def info(self):
        """
        A simplified interface for reading/writing entries into `history`.

        Setting the `info` attribute appends a message to the log::

           >>> j1 = Run()
           >>> j1.info = 'a message'
           >>> j1.info = 'a second message'

        Getting the value of the `info` attribute returns the last
        message entered in the log::

          >>> print(j1.info) # doctest: +ELLIPSIS
          a second message ...

        """
        return locals()

    @info.setter
    def info(self, value):
        try:
            self.history.append(str(value, errors='replace'))
        except (TypeError, ValueError):
            try:
                self.history.append(str(value))
            except Exception as err:
                log.error("Cannot append `%s` to history of task %s", value, self)

    @info.getter
    def info(self):
        return self.history.last()

    # states that a `Run` can be in
    State = Enum(
        'NEW',       # Job has not yet been submitted/started
        'SUBMITTED',  # Job has been sent to execution resource
        'STOPPED',   # trap state: job needs manual intervention
        'RUNNING',   # job is executing on remote resource
        # remote job execution finished, output not yet retrieved
        'TERMINATING',
        # job execution finished (correctly or not) and will not be resumed
        'TERMINATED',
        # job info not found or lost track of job (e.g., network error or
        # invalid job ID)
        'UNKNOWN',
    )

    class Arch(object):

        """
        Processor architectures, for use as values in the
        `requested_architecture` field of the `Application` class
        constructor.

        The following values are currently defined:

        `X86_64`
          64-bit Intel/AMD/VIA x86 processors in 64-bit mode.

        `X86_32`
          32-bit Intel/AMD/VIA x86 processors in 32-bit mode.
        """
        X86_64 = "x86_64"
        X86_32 = "i686"

        # the following method makes this class read-only,
        # thus preventing users accidentally overwriting the
        # value of constants above...
        def __setattr__(self, name, value):
            raise TypeError("Cannot overwrite value of constant '%s'" % name)

    @property
    def state(self):
        """
        The state a `Run` is in.

        The value of `Run.state` must always be a value from the
        `Run.State` enumeration, i.e., one of the following values.

        =============== ============================================= ======================
        Run.State value purpose                                       can change to
        =============== ============================================= ======================
        NEW             Job has not yet been submitted/started        SUBMITTED

        SUBMITTED       Job has been sent to execution resource       RUNNING, STOPPED

        STOPPED         Trap state: job needs manual intervention     TERMINATING(by gkill),
                        (either user- or sysadmin-level)              SUBMITTED (by miracle)
                        to resume normal execution

        RUNNING         Job is executing on resource                  TERMINATING

        TERMINATING     Job has finished execution                    TERMINATED
                        on (possibly remote) resource;
                        output not yet retrieved

        TERMINATED      Job execution is finished (correctly or not)  None: **final state**
                        and output has been retrieved

        UNKNOWN         GC3Pie can no longer monitor Job at the       Any other state except
                        remote site: job may not need manual          for NEW
                        intervention.
        =============== ============================================= ======================

        When a :class:`Run` object is first created, it is assigned
        the state NEW.  After a successful invocation of
        `Core.submit()`, it is transitioned to state SUBMITTED.
        Further transitions to RUNNING or STOPPED or TERMINATED state,
        happen completely independently of the creator progra; the
        `Core.update_job_state()` call provides updates on the status
        of a job.

        The STOPPED state is a kind of generic "run time error" state:
        a job can get into the STOPPED state if its execution is
        stopped (e.g., a SIGSTOP is sent to the remote process) or
        delayed indefinitely (e.g., the remote batch system puts the
        job "on hold"). There is no way a job can get out of the
        STOPPED state automatically: all transitions from the STOPPED
        state require manual intervention, either by the submitting
        user (e.g., cancel the job), or by the remote systems
        administrator (e.g., by releasing the hold).

        The TERMINATED state is the final state of a job: once a job
        reaches it, it cannot get back to any other state. Jobs reach
        TERMINATED state regardless of their exit code, or even if a
        system failure occurred during remote execution; actually,
        jobs can reach the TERMINATED status even if they didn't run
        at all, for example, in case of a fatal failure during the
        submission step.
        """
        return self._state

    @state.setter
    def state(self, value):
        assert value in Run.State, \
            ("Value '{0}' is not a legal `gc3libs.Run.State` value."
             .format(value))
        if self._state == value:
            # no changes
            return
        self.state_last_changed = time.time()
        self.timestamp[value] = time.time()
        # record state-transition in Task execution history
        # (can be later queried with `ginfo` for e.g. debugging)
        if value == Run.State.TERMINATED:
            self.history.append(
                "Transition from state {0} to state {1} (returncode: {2})"
                .format(self._state, value, self.returncode))
        else:
            self.history.append(
                "Transition from state {0} to state {1}"
                .format(self._state, value))
        if self._ref is not None:
            self._ref.changed = True
            # signal state-transition
            TaskStateChange.send(
                self._ref, from_state=self._state, to_state=value)
        # finally, update state
        self._state = value

    def in_state(self, *names):
        """
        Return `True` if the `Run` state matches any of the given names.

        In addition to the states from `Run.State`:class:, the two
        additional names ``ok`` and ``failed`` are also accepted, with
        the following meaning:

        * ``ok``: state is `TERMINATED` and `returncode` is 0.

        * ``failed``: state is `TERMINATED` and `returncode` is non-zero.
        """
        state = self.state
        if state in names:
            return True
        elif ('ok' in names
              and state == Run.State.TERMINATED and self.returncode == 0):
            return True
        elif ('failed' in names
              and state == Run.State.TERMINATED and self.returncode != 0):
            return True
        else:
            return False

    @property
    def signal(self):
        """
        The "signal number" part of a `Run.returncode`, see
        `os.WTERMSIG` for details.

        The "signal number" is a 7-bit integer value in the range
        0..127; value `0` is used to mean that no signal has been
        received during the application runtime (i.e., the application
        terminated by calling ``exit()``).

        The value represents either a real UNIX system signal, or a
        "fake" one that GC3Libs uses to represent Grid middleware
        errors (see `Run.Signals`).
        """
        return self._signal

    @signal.setter
    def signal(self, value):
        self._signal = None if value is None else int(value) & 0x7f

    @property
    def exitcode(self):
        """
        The "exit code" part of a `Run.returncode`, see `os.WEXITSTATUS`.
        This is an 8-bit integer, whose meaning is entirely
        application-specific.  (However, the value `255` is often used
        to mean that an error has occurred and the application could
        not end its execution normally.)
        """
        return self._exitcode

    @exitcode.setter
    def exitcode(self, value):
        self._exitcode = None if value is None else int(value) & 0xff

    @property
    def returncode(self):
        """
        The `returncode` attribute of this job object encodes the
        `Run` termination status in a manner compatible with the POSIX
        termination status as implemented by `os.WIFSIGNALED` and
        `os.WIFEXITED`.

        However, in contrast with POSIX usage, the `exitcode` and the
        `signal` part can *both* be significant: in case a Grid
        middleware error happened *after* the application has
        successfully completed its execution.  In other words,
        `os.WEXITSTATUS(returncode)` is meaningful iff
        `os.WTERMSIG(returncode)` is 0 or one of the pseudo-signals
        listed in `Run.Signals`.

        `Run.exitcode` and `Run.signal` are combined to form the
        return code 16-bit integer as follows (the convention appears
        to be obeyed on every known system):

           +------+------------------------------------+
           |Bit   |Encodes...                          |
           +======+====================================+
           |0..7  |signal number                       |
           +------+------------------------------------+
           |8     |1 if program dumped core.           |
           +------+------------------------------------+
           |9..16 |exit code                           |
           +------+------------------------------------+

        *Note:* the "core dump bit" is always 0 here.

        Setting the `returncode` property sets `exitcode` and
        `signal`; you can either assign a `(signal, exitcode)` pair to
        `returncode`, or set `returncode` to an integer from which the
        correct `exitcode` and `signal` attribute values are
        extracted::

           >>> j = Run()
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

        See also `Run.exitcode` and `Run.signal`.
        """
        return self._make_termstatus(self.exitcode, self.signal)

    @returncode.setter
    def returncode(self, value):
        old_exitcode, old_signal = self.exitcode, self.signal
        if value is None:
            self.signal = None
            self.exitcode = None
        else:
            try:
                # `value` can be a tuple `(signal, exitcode)`;
                # ensure values are within allowed range
                self.signal = int(value[0]) & 0x7f
                self.exitcode = int(value[1]) & 0xff
            except (TypeError, ValueError):
                self.exitcode = (int(value) >> 8) & 0xff
                self.signal = int(value) & 0x7f
        if self._ref is not None:
            if self.exitcode != old_exitcode and self.signal != old_signal:
                TermStatusChange.send(
                    self._ref,
                    from_returncode=self._make_termstatus(old_exitcode, old_signal),
                    to_returncode=self._make_termstatus(self.exitcode, self.signal))

    @staticmethod
    def _make_termstatus(exitcode, signal=0):
        if exitcode is None and signal is None:
            return None
        if exitcode is None:
            exitcode = -1
        if signal is None:
            signal = 0
        return (exitcode << 8) | signal

    # `Run.Signals` is an instance of global class `_Signals`
    Signals = _Signals()

    @staticmethod
    def shellexit_to_returncode(rc):
        """
        Convert shell exit code to POSIX process return code.
        The "return code" is represented as a pair `(signal,
        exitcode)` suitable for setting the ``returncode`` property.

        A POSIX shell represents the return code of the last-run
        program within its exit code as follows:

        * If the program was terminated by signal ``K``, the shell exits
          with code ``128+K``,

        * otherwise, if the program terminated with exit code ``X``,
          the shell exits with code ``X``.  (Yes, the mapping is not
          bijective and it is possible that a program wants to exit
          with, e.g., code 137 and this is mistaken for it having been
          killed by signal 9.  Blame the original UNIX implementors
          for this.)

        Examples:

        * Shell exit code 137 means that the last program got a
          SIGKILL. Note that in this case there is no well-defined
          "exit code" of the program; we use ``-1`` in the place of
          the exit code to mark it::

            >>> Run.shellexit_to_returncode(137)
            (9, -1)

        * Shell exit code 75 is a valid program exit code::

            >>> Run.shellexit_to_returncode(75)
            (0, 75)

        * ...and so is, of course, 0::

            >>> Run.shellexit_to_returncode(0)
            (0, 0)

        """
        # only the less significant 8 bits matter
        rc &= 0xff
        if rc > 128:
            # terminated by signal N is encoded as 128+N
            return (rc - 128, -1)
        else:
            # regular exit
            return (0, rc)


# Factory functions to create Core and Engine instances

def _split_specific_args(fn, argdict):
    """
    Pop keys from `argdict` which appear as argument names in the definition of
    function `fn` out into a separate dictionary, and return it.

    *Note:* Argument `argdict` is modified in-place!
    """
    args, varargs, _, _ = getargspec(fn)
    specific_args = {}
    for n, argname in enumerate(args):
        if n == 0 and argname == 'self':
            continue
        if argname in argdict:
            specific_args[argname] = argdict.pop(argname)
    if varargs is not None:
        if varargs in argdict:
            specific_args[varargs] = argdict.pop(varargs)
    return specific_args


def create_core(*conf_files, **extra_args):
    """Make and return a `gc3libs.core.Core`:class: instance.

    It accepts an optional list of configuration filenames and a dictionary
    to create a configuration object from. Filenames containing a `~` or
    an environment variable reference, will be expanded automatically.
    If called without arguments, the paths specified in
    `gc3libs.defaults.CONFIG_FILE_LOCATIONS` will be used.

    Any keyword argument matching the name of a parameter used by
    `Core.__init__` is passed to it.  Any leftover keyword argument is
    passed unchanged to the `gc3libs.config.Configuration`:class:
    constructor.  In particular,
    a `cfg_dict` keyword argument can be used to initialize a GC3Pie Core
    from a dictionary of configuration values, without reading in any files.
    """
    from gc3libs.config import Configuration
    from gc3libs.core import Core
    conf_files = [
        os.path.expandvars(
            os.path.expanduser(fname)) for fname in conf_files]
    if not conf_files:
        conf_files = gc3libs.defaults.CONFIG_FILE_LOCATIONS[:]

    # extract params specific to the `Core` instance
    core_specific_args = _split_specific_args(Core.__init__, extra_args)

    # params specific to the `Configuration` instance
    if 'auto_enable_auth' not in extra_args:
        extra_args['auto_enable_auth'] = True
    if 'cfg_dict' not in extra_args:
        extra_args['cfg_dict'] = None

    # make 'em all
    cfg = Configuration(*conf_files, **extra_args)
    return Core(cfg, **core_specific_args)


def create_engine(*conf_files, **extra_args):
    """
    Make and return a `gc3libs.core.Engine`:class: instance.

    It accepts an optional list of configuration filenames and a dictionary
    to create a configuration object from. Filenames containing a `~` or
    an environment variable reference, will be expanded automatically.
    If called without arguments, the paths specified in
    `gc3libs.Default.CONFIG_FILE_LOCATIONS` will be used.

    Any keyword argument that matches the name of a parameter of the
    constructor for :class:`Engine` is passed to that constructor.
    Likewise, any keyword argument that matches the name of a parameter
    used by `Core.__init__` is passed to it.  Any leftover keyword
    argument is passed unchanged to the
    `gc3libs.config.Configuration`:class: constructor.  In particular,
    a `cfg_dict` keyword argument can be used to initialize a GC3Pie Engine
    from a dictionary of configuration values, without reading in any files.
    """
    from gc3libs.core import Engine

    # extract `Engine`-specific construction params
    engine_specific_args = _split_specific_args(Engine.__init__, extra_args)

    if 'cfg_dict' not in extra_args:
        extra_args['cfg_dict'] = None

    core = create_core(*conf_files, **extra_args)
    return Engine(core, **engine_specific_args)


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
