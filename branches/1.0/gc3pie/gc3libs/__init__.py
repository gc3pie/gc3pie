#! /usr/bin/env python
#
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
__docformat__ = 'reStructuredText'
__version__ = '1.0rc3 (SVN $Revision$)'


import copy
import os
import os.path
import sys
import time
import types

import logging
import logging.config
log = logging.getLogger("gc3.gc3libs")

import gc3libs.exceptions

# this needs to be defined before we import other GC3Libs modules, as
# they may depend on it
class Default(object):
    """
    A namespace for all constants and default values used in the
    GC3Libs package.
    """
    RCDIR = os.path.join(os.path.expandvars('$HOME'), ".gc3")
    CONFIG_FILE_LOCATIONS = [
        # system-wide config file
        "/etc/gc3/gc3pie.conf",
        # user-private virtualenv config file
        os.path.join(os.path.expandvars("$VIRTUAL_ENV"), "etc/gc3/gc3pie.conf"),
        # user-private config file
        os.path.join(RCDIR, "gc3pie.conf")
        ]
    JOBS_DIR = os.path.join(RCDIR, "jobs")
    
    ARC_LRMS = 'arc'
    ARC_CACHE_TIME = 90 #: only update ARC resources status every this seconds
    
    SGE_LRMS = 'sge'
    # Transport information
    SSH_PORT = 22
    SSH_CONNECT_TIMEOUT = 30
    
    FORK_LRMS = 'fork'

    # Proxy
    PROXY_VALIDITY_THRESHOLD = 600 #: Proxy validity threshold in seconds. If proxy is expiring before the thresold, it will be marked as to be renewed.


from gc3libs.exceptions import *
from gc3libs.persistence import Persistable
from gc3libs.utils import defproperty, deploy_configuration_file, get_and_remove, Enum, Log, Struct, safe_repr


def configure_logger(level=logging.ERROR,
                     name=None,
                     format=(os.path.basename(sys.argv[0]) 
                             + ': [%(asctime)s] %(levelname)-8s: %(message)s'),
                     datefmt='%Y-%m-%d %H:%M:%S'):
    """
    Configure the ``gc3.gc3libs`` logger.

    Arguments `level`, `format` and `datefmt` set the corresponding
    arguments in the `logging.basicConfig()` call.  

    If a user configuration file exists in file NAME.log.conf in the
    ``Default.RCDIR`` directory (usually ``~/.gc3``), it is read and
    used for more advanced configuration; if it does not exist, then a
    sample one is created.
    """
    if name is None:
        name = os.path.basename(sys.argv[0])
    log_conf = os.path.join(Default.RCDIR, name + '.log.conf')
    logging.basicConfig(level=level, format=format, datefmt=datefmt)
    deploy_configuration_file(log_conf, "logging.conf.example")
    logging.config.fileConfig(log_conf, {
        'RCDIR': Default.RCDIR,
        'HOMEDIR': os.path.expandvars('$HOME'),
        })
    log = logging.getLogger("gc3.gc3libs")
    log.setLevel(level)
    log.propagate = 1
    # Up to Python 2.5, the `logging` library disables all existing
    # loggers upon reconfiguration, and fails to re-create them when
    # getLogger() is called again.  We work around this the hard way:
    # using an undocumented internal variable, ignore errors, and hope
    # for the best.
    try:
        log.disabled = 0
    except:
        pass
    # due to a bug in Python 2.4.x (see 
    # https://bugzilla.redhat.com/show_bug.cgi?id=573782 )
    # we need to disable `logging` reporting of exceptions.
    try:
        version_info = sys.version_info
    except AttributeError:
        version_info = (1, 5) # 1.5 or earlier
    if version_info < (2, 5):
        logging.raiseExceptions = False


class Task(object):
    # XXX: alternative design: we could make Task take an additional
    # `job` parameter, which is the controlled job (i.e., the one that
    # `submit()` and friends act upon), defaulting to `self`.  This
    # way, Task would become a mediator object binding a "grid" (core
    # || engine) and a "job" (Application or whatnot).
    """
    Mix-in class implementing a facade for job control.

    A `Task` can be described as an "active" job, in the sense that
    all job control is done through methods on the `Task` instance
    itself; contrast this with operating on `Application` objects
    through a `Core` or `Engine` instance.

    The following pseudo-code is an example of the usage of the `Task`
    interface for controlling a job.  Assume that `GamessApplication` is
    inheriting from `Task` (and it actually is)::

        t = GamessApplication(input_file)
        t.submit()
        # ... do other stuff 
        t.update()
        # ... take decisions based on t.execution.state
        t.wait() # blocks until task is terminated

    Each `Task` object has an `execution` attribute: it is an instance
    of class :class:`Run`, initialized with a new instance of `Run`,
    and at any given time it reflects the current status of the
    associated remote job.  In particular, `execution.state` can be
    checked for the current task status.

    """

    def __init__(self, grid=None):
        """
        Initialize a `Task` instance.

        :param grid: A :class:`gc3libs.Engine` or
                     :class:`gc3libs.Core` instance, or anything
                     implementing the same interface.
        """
        if grid is not None:
            self.attach(grid)
        else:
            self.detach()
        # `self.execution` could have been initialized by the
        # `Application` class, so chek before re-initializing it.
        if not hasattr(self, 'execution'):
            self.execution = Run()

    class Error(gc3libs.exceptions.Error):
        """
        Generic error condition in a `Task` object.
        """
        pass
    class DetachedFromGridError(Error):
        """
        Raised when a method (other than :meth:`attach`) is called on
        a detached `Task` instance.
        """
        pass
    class UnexpectedStateError(Error):
        """
        Raised by :meth:`Task.progress` when a job lands in `STOPPED`
        or `TERMINATED` state.
        """
        pass

    # manipulate the "grid" interface used to control the associated job
    def attach(self, grid):
        """
        Use the given Grid interface for operations on the job
        associated with this task.
        """
        self._grid = grid
        self._attached = True

    # create a class-shared fake "grid" object, that just throws a
    # DetachedFromGrid exception when any of its methods is used.  We
    # use this as a safeguard for detached `Task` objects, in order to
    # get sensible error reporting.
    class __NoGrid(object):
        # XXX: this returns a function object for whatever `name`;
        # should be fine since a "grid" interface should just contain
        # methods, but one never knows...
        def __getattr__(self, name):
            def throw_error():
                raise DetachedFromGridError("Task object has been detached from a Grid interface.")
            return throw_error
    __no_grid = __NoGrid()

    def detach(self):
        """
        Remove any reference to the current grid interface.  After
        this, calling any method other than :meth:`attach` results in
        an exception :class:`Task.DetachedFromGridError` being thrown.
        """
        self._grid = Task.__no_grid
        self._attached = False


    # interface with pickle/gc3libs.persistence: do not save the
    # attached grid/engine/core as well: it definitely needs to be
    # saved separately.

    def __getstate__(self):
        state = self.__dict__.copy()
        state['_grid'] = None
        state['_attached'] = None
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self.detach()


    # grid-level actions on this Task object are re-routed to the
    # grid/engine/core instance
    def submit(self):
        """
        Start the computational job associated with this `Task` instance.
        """
        self._grid.submit(self)

    def update_state(self):
        """
        In-place update of the execution state of the computational
        job associated with this `Task`.  After successful completion,
        `.execution.state` will contain the new state.
        """
        self._grid.update_job_state(self)

    def kill(self):
        """
        Terminate the computational job associated with this task.

        See :meth:`gc3libs.Core.kill` for a full explanation.
        """
        self._grid.kill(self)

    def fetch_output(self, output_dir=None, overwrite=False):
        """
        Retrieve the outputs of the computational job associated with
        this task into directory `output_dir`, or, if that is `None`,
        into the directory whose path is stored in instance attribute
        `.output_dir`.

        See :meth:`gc3libs.Core.fetch_output` for a full explanation.

        :return: Path to the directory where the job output has been
                 collected.
        """
        return self._grid.fetch_output(self, output_dir, overwrite)

    def peek(self, what='stdout', offset=0, size=None):
        """
        Download `size` bytes (at offset `offset` from the start) from
        the associated job standard output or error stream, and write them
        into a local file.  Return a file-like object from which the
        downloaded contents can be read. 

        See :meth:`gc3libs.Core.peek` for a full explanation.
        """
        return self._grid.peek(self, what, offset, size)

    # convenience methods, do not really add any functionality over
    # what's above
    
    def progress(self):
        """
        Advance the associated job through all states of a regular
        lifecycle. In detail:

          1. If `execution.state` is `NEW`, the associated job is started.
          2. The state is updated until it reaches `TERMINATED`
          3. Output is collected and the final returncode is returned.

        An exception `Task.Error` is raised if the job hits state
        `STOPPED` or `UNKNOWN` during an update in phase 2.

        When the job reaches `TERMINATED` state, the output is
        retrieved, and the return code (stored also in `.returncode`)
        is returned; if the job is not yet in `TERMINATED` state,
        calling `progress` returns `None`.

        :raises: exception :class:`Task.UnexpectedStateError` if the
                 associated job goes into state `STOPPED` or `UNKNOWN`

        :return: job final returncode, or `None` if the execution
                 state is not `TERMINATED`.

        """
        # first update state, we'll submit NEW jobs last, so that the
        # state is not updated immediately after submission as ARC
        # does not cope well with this...
        if self.execution.state in [ Run.State.SUBMITTED, 
                                     Run.State.RUNNING, 
                                     Run.State.STOPPED, 
                                     Run.State.UNKNOWN ]:
            self.update_state()
        # now "do the right thing" based on actual state
        if self.execution.state in [ Run.State.STOPPED, 
                                     Run.State.UNKNOWN ]:
            raise Task.UnexpectedStateError("Job '%s' entered `%s` state." 
                                            % (self, self.execution.state))
        elif self.execution.state == Run.State.NEW:
            self.submit()
        elif self.execution.state == Run.State.TERMINATED:
            self.fetch_output()
            return self.execution.returncode


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
        #  - `self._grid` could be a `Core` instance, thus not capable 
        #    of running independently.
        # For now this is a poll+sleep loop, but we certainly need to revise it.
        while True:
            self.update()
            if self.execution.state == Run.State.TERMINATED:
                return self.returncode
            time.sleep(interval)



class Application(Struct, Persistable, Task):
    """
    Support for running a generic application with the GC3Libs.
    The following parameters are *required* to create an `Application`
    instance:

    `executable`
      (string) name of the application binary to be
      launched on the remote resource; the specifics of how this is
      handled are dependent on the submission backend, but you may
      always run a script that you upload through the `inputs`
      mechanism by specifying ``./scriptname`` as `executable`.

    `arguments`
      list of command-line arguments to pass to
      `executable`; any object in the list will be converted to
      string via Python ``str()``. Note that, in contrast with the
      UNIX ``execvp()`` usage, the first argument in this list
      will be passed as ``argv[1]``, i.e., ``argv[0]`` will always
      be equal to `executable`.

    `inputs`
      files that will be copied from the local computer to the remote
      execution node before execution starts.

      There are two possible ways of specifying the `inputs` parameter:

        * It can be a Python dictionary: keys are local file paths, 
          values are remote file names.

        * It can be a Python list: each item in the list should be a
          pair `(local_file_name, remote_file_name)`; a single string
          `file_name` is allowed as a shortcut and will result in both
          `local_file_name` and `remote_file_name` being equal.  If an
          absolute path name is specified as `remote_file_name`, then
          an :class:`InvalidArgument` exception is thrown.

    `outputs`
      list of files that will be copied back from the remote execution
      node back to the local computer after execution has completed.

      There are two possible ways of specifying the `outputs` parameter:

      * It can be a Python dictionary: keys are local file paths, 
        values are remote file names.

      * It can be a Python list: each item in the list should be a
        pair `(remote_file_name, local_file_name)`; a single string
        `file_name` is allowed as a shortcut and will result in both
        `local_file_name` and `remote_file_name` being equal.  If an
        absolute path name is specified as `remote_file_name`, then an
        :class:`InvalidArgument` exception is thrown.

    `output_dir`
      Path to the base directory where output files will be downloaded.
      Output file names are interpreted relative to this base directory.

    `requested_cores`,`requested_memory`,`requested_walltime`
      specify resource requirements for the application: the
      number of independent execution units (CPU cores), amount of
      memory (in GB; will be converted to a whole number by
      truncating any decimal digits), amount of wall-clock time to
      allocate for the computational job (in hours; will be
      converted to a whole number by truncating any decimal
      digits).

    The following optional parameters may be additionally
    specified as keyword arguments and will be given special
    treatment by the `Application` class logic:

    `environment`
      a list of pairs `(name, value)`: the
      environment variable whose name is given by the contents of
      the string `name` will be defined as the content of string
      `value` (i.e., as if "export name=value" was executed prior
      to starting the application).  Alternately, one can pass in
      a list of strings of the form "name=value".

    `grid`
      if not `None`, equivalent to calling `Task.attach(self, grid)`;
      enables the use of the `Task`/active job control interface

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

    `tags`
      list of tag names (string) that must be present on a
      resource in order to be eligible for submission; in the ARC
      backend, tags are interpreted as run-time environments (RTE) to
      request.

    Any other keyword arguments will be set as instance attributes,
    but otherwise ignored by the `Application` constructor.

    After successful construction, an `Application` object is
    guaranteed to have the following instance attributes:

    `executable`
      a string specifying the executable name

    `arguments`
      list of strings specifying command-line arguments for executable
      invocation; possibly empty

    `inputs`
      dictionary mapping local file name (a string) to a remote file name (a string);
      remote file names are relative paths (root directory is the remote job folder)

    `outputs`
      dictionary mapping remote file name (a string) to a local file name (a string);
      remote file names are relative paths (root directory is the remote job folder)

    `output_dir`
      Path to the base directory where output files will be downloaded.
      Output file names are interpreted relative to this base directory.

    `final_output_retrieved` 
      boolean, indicating whether job output has been fetched from the
      remote resource; if `True`, you cannot assume that data is still
      available remotely.  (Note: for jobs in ``TERMINATED`` state, the
      output can be retrieved only once!)

    `execution`
      a `Run` instance; its state attribute is initially set to ``NEW``

    `environment`
      dictionary mapping environment variable names to the requested
      value (string); possibly empty

    `stdin`
      `None` or a string specifying a (local) file name.  If `stdin`
      is not None, then it matches a key name in `inputs`

    `stdout`
      `None` or a string specifying a (remote) file name.  If `stdout`
      is not None, then it matches a key name in `outputs`

    `stderr`
      `None` or a string specifying a (remote) file name.  If `stdout`
      is not None, then it matches a key name in `outputs`

    `join`
      boolean value, indicating whether `stdout` and `stderr` are
      collected into the same file

    `tags`
      list of strings specifying the tags to request in each resource
      for submission; possibly empty.
    """
    def __init__(self, executable, arguments, inputs, outputs, output_dir, **kw):
        # required parameters
        self.executable = executable
        self.arguments = [ str(x) for x in arguments ]

        
        self.inputs = Application._io_spec_to_dict(inputs)
        # check that remote entries are all distinct
        # (can happen that two local paths apre mapped to the same remote one)
        if len(self.inputs.values()) != len(set(self.inputs.values())):
            # try to build an exact error message
            inv = { }
            for l, r in self.inputs:
                if r in inv:
                    raise DuplicateEntryError("Local input files '%s' and '%s'"
                                              " map to the same remote path '%s'"
                                              % (l, inv[r], r))
                else:
                    inv[r] = l
        # ensure remote paths are not absolute
        for r_path in self.inputs.itervalues():
            if os.path.isabs(r_path):
                raise gc3libs.exceptions.InvalidArgument("Remote paths not allowed to be absolute")

        self.outputs = Application._io_spec_to_dict(outputs)
        # check that local entries are all distinct
        # (can happen that two remote paths apre mapped to the same local one)
        if len(self.outputs.values()) != len(set(self.outputs.values())):
            # try to build an exact error message
            inv = { }
            for r, l in self.outputs:
                if l in inv:
                    raise DuplicateEntryError("Remote output files '%s' and '%s'"
                                              " map to the same local path '%s'"
                                              % (r, inv[l], l))
                else:
                    inv[l] = r
        # ensure remote paths are not absolute
        for r_path in self.outputs.iterkeys():
            if os.path.isabs(r_path):
                raise gc3libs.exceptions.InvalidArgument("Remote paths not allowed to be absolute")

        self.output_dir = output_dir

        # optional params
        # FIXME: should use appropriate unit classes for requested_*
        self.requested_cores = get_and_remove(kw, 'requested_cores')
        self.requested_memory = get_and_remove(kw, 'requested_memory')
        self.requested_walltime = get_and_remove(kw, 'requested_walltime')

        self.environment = get_and_remove(kw, 'environment', dict())
        def to_env_pair(val):
            if isinstance(val, tuple):
                return val
            else:
                # assume `val` is a string
                return tuple(val.split('=', 1))
        self.environment = dict(to_env_pair(x) for x in self.environment.items())

        self.join = get_and_remove(kw, 'join', False)
        self.stdin = get_and_remove(kw, 'stdin')
        if self.stdin and (self.stdin not in self.inputs):
            self.inputs[self.stdin] = os.path.basename(self.stdin)
        self.stdout = get_and_remove(kw, 'stdout')
        if self.stdout and (self.stdout not in self.outputs):
            self.outputs[self.stdout] = os.path.basename(self.stdout)
        self.stderr = get_and_remove(kw, 'stderr')
        if self.stderr and (self.stderr not in self.outputs):
            self.outputs[self.stderr] = os.path.basename(self.stderr)

        self.tags = get_and_remove(kw, 'tags', list())

        # job name
        self.jobname = get_and_remove(kw, 'jobname', self.__class__.__name__)

        # task setup; creates the `.execution` attribute as well
        Task.__init__(self, get_and_remove(kw, 'grid', None))
        
        # output management
        self.final_output_retrieved = False

        # any additional param
        Struct.__init__(self, **kw)

    @staticmethod
    def _io_spec_to_dict(spec):
        """
        Return a dictionary formed by pairs path:name.  (Only used for
        internal processing of `input` and `output` fields.)

        Argument `spec` is either a list or a Python `dict` instance,
        in which case a copy of it is returned::
        
          >>> d1 = { '/tmp/1':'1', '/tmp/2':'2' }
          >>> d2 = Application._io_spec_to_dict(d1)
          >>> d2 == d1
          True
          >>> d2 is d1
          False

        If `spec` is a list, each element can be either a tuple
        `(path, name)`, or a string `path`, which is converted to a
        tuple `(path, name)` by setting `name =
        os.path.basename(path)`::

          >>> l1 = [ ('/tmp/1', '1'), '/tmp/2' ]
          >>> d3 = Application._io_spec_to_dict(l1)
          >>> d3 == d2
          True

        """
        try:
            # is `spec` dict-like?
            return dict((k, v) for k,v in spec.iteritems())
        except AttributeError:
            # `spec` is a list-like
            def convert_to_tuple(val):
                if isinstance(val, types.StringTypes):
                    l = str(val) # XXX: might throw enconding error if `val` is Unicode?
                    r = os.path.basename(l)
                    return (l, r)
                else: 
                    return tuple(val)
            return dict(convert_to_tuple(x) for x in spec)
        

    def __str__(self):
        try:
            return str(self.persistent_id)
        except AttributeError:
            return safe_repr(self)

        
    def clone(self):
        """
        Return a deep copy of this `Application` object, with the
        `.execution` instance variable reset to a fresh new instance
        of `Run`.
        """
        new = copy.deepcopy(self)
        new.execution = Run()
        return new


    def xrsl(self, resource):
        """
        Return a string containing an xRSL sequence, suitable for
        submitting an instance of this application through ARC's
        ``ngsub`` command.

        The default implementation produces XRSL content based on 
        the construction parameters; you should override this method
        to produce XRSL tailored to your application.
        """
        xrsl= str.join(' ', [
                '&',
                '(executable="%s")' % self.executable,
                '(gmlog=".arc")', # FIXME: should check if conflicts with any input/output files
                ])
        # treat 'arguments' separately
        if self.arguments:
            xrsl += '(arguments=%s)' % str.join(' ', [('"%s"' % x) for x in self.arguments])
        if (os.path.basename(self.executable) in self.inputs
            or './'+os.path.basename(self.executable) in self.inputs):
            xrsl += '(executables="%s")' % os.path.basename(self.executable)
        if self.stdin:
            xrsl += '(stdin="%s")' % self.stdin
        if self.join:
            xrsl += '(join="yes")'
        else:
            xrsl += '(join="no")'
        if self.stdout:
            xrsl += '(stdout="%s")' % self.stdout
        if self.stderr and not self.join:
            xrsl += '(stderr="%s")' % self.stderr
        if len(self.inputs) > 0:
            xrsl += ('(inputFiles=%s)' 
                     % str.join(' ', [ ('("%s" "%s")' % (r,l)) for (l,r) in self.inputs.items() ]))
        if len(self.outputs) > 0:
            # filter out stdout/stderr (they are automatically
            # retrieved) and then check again
            outputs_ = [ ('("%s" "")' % r) 
                         for (r,l) in [ (remotename, localname)
                                        for remotename,localname 
                                        in self.outputs.iteritems() 
                                        if (remotename != self.stdout 
                                            and remotename != self.stderr)]]
            if len(outputs_) > 0:
                xrsl += ('(outputFiles=%s)' % str.join(' ', outputs_))
        if len(self.tags) > 0:
            xrsl += str.join('\n', [
                    ('(runTimeEnvironment="%s")' % rte) for rte in self.tags ])
        if len(self.environment) > 0:
            xrsl += ('(environment=%s)' % 
                     str.join(' ', [ ('("%s" "%s")' % kv) for kv in self.environment ]))
        if self.requested_walltime:
            xrsl += '(wallTime="%d hours")' % self.requested_walltime
        if self.requested_memory:
            xrsl += '(memory="%d")' % (1000 * self.requested_memory)
        if self.requested_cores:
            xrsl += '(count="%d")' % self.requested_cores
        if self.jobname:
            xrsl += '(jobname="%s")' % self.jobname

        return xrsl


    def cmdline(self, resource):
        """
        Return a string, suitable for invoking the application from a
        UNIX shell command-line.

        The default implementation just concatenates `executable` and
        `arguments` separating them with whitespace; this is hardly
        correct for any application, so you should override this
        method in derived classes to provide appropriate invocation
        templates.
        """
        return str.join(" ", [self.executable] + self.arguments)


    def qsub(self, resource, _suppress_warning=False, **kw):
        # XXX: the `_suppress_warning` switch is only provided for
        # some applications to make use of this generic method without
        # logging the user-level warning, because, e.g., it has already
        # been taken care in some other way (cf. GAMESS' `qgms`).
        # Use with care and don't depend on it!
        """
        Get an SGE ``qsub`` command-line invocation for submitting an
        instance of this application.  Return a pair `(cmd, script)`,
        where `cmd` is the command to run to submit an instance of
        this application to the SGE batch system, and `script` --if
        it's not `None`-- is written to a new file, whose name is then
        appended to `cmd`.

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
        provide appropriate invocation templates.
        """
        qsub = 'qsub -cwd -S /bin/sh '
        if self.requested_walltime:
            # SGE uses `s_rt` for wall-clock time limit, expressed in seconds
            qsub += ' -l s_rt=%d' % (3600 * self.requested_walltime)
        if self.requested_memory:
            # SGE uses `mem_free` for memory limits; 'G' suffix allowed for Gigabytes
            qsub += ' -l mem_free=%dG' % self.requested_memory
        if self.requested_cores and not _suppress_warning:
            # XXX: should this be an error instead?
            log.warning("Application requested %d cores,"
                        " but there is no generic way of expressing this requirement in SGE!"
                        " Ignoring request, but this will likely result in malfunctioning later on.", 
                        self.requested_cores)
        if self.job_name:
            qsub += " -N '%s'" % self.job_name
        return (qsub, self.cmdline(resource))


    # State transition handlers.
    #
    # XXX: should these be moved to the `Task` interface?
                      
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

    def terminated(self):
        """
        Called when the job state transitions to `TERMINATED`, i.e.,
        the job has finished execution (with whatever exit status, see
        `returncode`) and its execution cannot resume.

        The default implementation does nothing, override in derived
        classes to implement additional behavior.
        """
        pass

    def postprocess(self, dir):
        """
        Called when the final output of the job has been retrieved to
        local directory `dir`.

        The default implementation does nothing, override in derived
        classes to implement additional behavior.
        """
        pass
    
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

        Default is to always return `None`; override in derived
        classes to change this behavior.
        """
        return None

    # XXX: this method might be dangerous in that it can break the
    # `update_job_state` semantics; it's here for completeness, but we
    # should consider removing...
    def update_job_state_error(self, ex):
        """
        Invocation of `Core.update_job_state()` on this object failed;
        `ex` is the `Exception` that describes the error.

        If this method returns an exception object, that is raised as
        a result of the `Core.update_job_state()`, otherwise the
        return value is ignored and `Core.update_job_state` returns
        `None`.

        Note that returning an `Exception` instance interrupts the
        normal flow of `Core.update_job_state`: in particular, the
        execution state is not updated and state transition methods
        will not be called.

        Default is to return `None`; override in derived classes to
        change this behavior.
        """
        return None

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
    Collection of (fake) signals used to encode termination reason in `Run.returncode`.

    ======  ============================================================
    signal  error condition
    ======  ============================================================
    125     submission to batch system failed
    124     remote error (e.g., execution node crashed, batch system misconfigured)
    123     data staging failure
    122     job killed by batch system / sysadmin
    121     job canceled by user
    ======  ============================================================

    """

    Cancelled = _Signal(121, "Job canceled by user")
    RemoteKill = _Signal(122, "Job killed by batch system or sysadmin")
    DataStagingFailure = _Signal(123, "Data staging failure")
    RemoteError = _Signal(124, "Unspecified remote error,"
                          " e.g., execution node crashed"
                          " or batch system misconfigured")
    SubmissionFailed = _Signal(125, "Submission to batch system failed.")

    def __contains__(self, signal):
        if 121 <= int(signal) <= 125:
            return True
        else:
            return False
    def __getitem__(self, signal_num):
        if signal_num == 121:
            return Signals.Cancelled
        if signal_num == 122:
            return Signals.RemoteKill
        if signal_num == 123:
            return Signals.DataStagingFailure
        if signal_num == 124:
            return Signals.RemoteError
        if signal_num == 125:
            return Signals.SubmissionFailed
        raise gc3libs.exceptions.InvalidArgument("Unknown signal number %d" % signal_num)


class Run(Struct):
    """
    A specialized `dict`-like object that keeps information about
    the execution state of an `Application` instance.

    A `Run` object is guaranteed to have the following attributes:

      `log`
        A `gc3libs.utils.Log` instance, recording human-readable text
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
    def __init__(self,initializer=None,**keywd):
        """
        Create a new Run object; constructor accepts the same
        arguments as the `dict` constructor.
        
        Examples:
        
          1. Create a new job with default parameters::

            >>> j1 = Run()
            >>> j1.returncode
            None
            >>> j1.state
            'NEW'

          2. Create a new job with additional attributes::

            >>> j2 = Run(application='GAMESS', version='2010R1')
            >>> j2.state
            'NEW'
            >>> j2.application
            'GAMESS'
            >>> j2['version']
            '2010R1'

          3. Clone an existing job object::

            >>> j3 = Run(j2)
            >>> j3.application
            'GAMESS'
            >>> j3['version']
            '2010R1'
            
        """
        if not hasattr(self, '_state'): self._state = Run.State.NEW
        self._exitcode = None
        self._signal = None

        Struct.__init__(self, initializer, **keywd)

        if 'log' not in self: self.log = Log()
        if 'timestamp' not in self: self.timestamp = { }


    @defproperty
    def info():
        """
        A simplified interface for reading/writing entries into `log`.

        Setting the `info` attribute appends a message to the log::

           >>> j1.info = 'a message'
           >>> j1.info = 'a second message'

        Getting the value of the `info` attribute returns the last
        message entered in the log::

          >>> j1.info
          'a second message'

        """
        def fget(self):
            return self.log.last()
        def fset(self, value):
            self.log.append(str(value))
        return locals()
    
    # states that a `Run` can be in
    State = Enum(
        'NEW',       # Job has not yet been submitted/started
        'SUBMITTED', # Job has been sent to execution resource
        'STOPPED',   # trap state: job needs manual intervention
        'RUNNING',   # job is executing on remote resource
        'TERMINATED',# job execution finished (correctly or not) and will not be resumed
        'UNKNOWN',   # job info not found or lost track of job (e.g., network error or invalid job ID)
        )

    @defproperty
    def state():
        """
        The state a `Run` is in.  

        The value of `Run.state` must always be a value from the
        `Run.State` enumeration, i.e., one of the following values.

        +---------------+--------------------------------------------------------------+----------------------+
        |Run.State value|purpose                                                       |can change to         |
        +===============+==============================================================+======================+
        |NEW            |Job has not yet been submitted/started (i.e., gsub not called)|SUBMITTED (by gsub)   |
        +---------------+--------------------------------------------------------------+----------------------+
        |SUBMITTED      |Job has been sent to execution resource                       |RUNNING, STOPPED      |
        +---------------+--------------------------------------------------------------+----------------------+
        |STOPPED        |Trap state: job needs manual intervention (either user-       |TERMINATED (by gkill),|
        |               |or sysadmin-level) to resume normal execution                 |SUBMITTED (by miracle)|
        +---------------+--------------------------------------------------------------+----------------------+
        |RUNNING        |Job is executing on remote resource                           |TERMINATED            |
        +---------------+--------------------------------------------------------------+----------------------+
        |TERMINATED     |Job execution is finished (correctly or not)                  |None: final state     |
        |               |and will not be resumed                                       |                      |
        +---------------+--------------------------------------------------------------+----------------------+

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
        def fget(self):
            return self._state
        def fset(self, value):
            if value not in Run.State:
                raise ValueError("Value '%s' is not a legal `gc3libs.Run.State` value." % value)
            if self._state != value:
                self.timestamp[value] = time.time()
                self.log.append('%s on %s' % (value, time.asctime()))
            self._state = value
        return locals()


    @defproperty
    def signal():
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
        The "exit code" part of a `Run.returncode`, see
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
        `Run` termination status in a manner compatible with the POSIX
        termination status as implemented by `os.WIFSIGNALED` and
        `os.WIFEXITED`.

        However, in contrast with POSIX usage, the `exitcode` and the
        `signal` part can *both* be significant: in case a Grid
        middleware error happened *after* the application has
        successfully completed its execution.  In other words,
        `os.WEXITSTATUS(job.returncode)` is meaningful iff
        `os.WTERMSIG(job.returncode)` is 0 or one of the
        pseudo-signals listed in `Run.Signals`.
        
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
            except (TypeError, ValueError):
                self.exitcode = (int(value) >> 8) & 0xff
                self.signal = int(value) & 0x7f
            # ensure values are within allowed range
            self.exitcode &= 0xff
            self.signal &= 0x7f
        return (locals())

    # `Run.Signals` is an instance of global class `_Signals`
    Signals = _Signals()    

        
## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
