#!/usr/bin/env python
"""
Top-level interface to Grid functionality.
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
__version__ = 'development version (SVN $Revision$)'
__date__ = '$Date$'


from fnmatch import fnmatch
import os
import posix
import re
import sys
import time
import ConfigParser
import tempfile
import warnings
warnings.simplefilter("ignore")

from gc3libs.compat.collections import defaultdict

import gc3libs
import gc3libs.debug
from gc3libs import Application, Run, Task
from gc3libs.backends.sge import SgeLrms
from gc3libs.backends.lsf import LsfLrms
from gc3libs.backends.fork import ForkLrms
from gc3libs.backends.subprocess import SubprocessLrms
from gc3libs.authentication import Auth
import gc3libs.exceptions
import gc3libs.Resource as Resource
import gc3libs.scheduler as scheduler
import gc3libs.utils as utils 


class Core:
    def __init__(self, resource_list, auths, auto_enable_auth):
        if len(resource_list) == 0:
            raise gc3libs.exceptions.NoResources("Resource list has length 0")
        self._resources = resource_list
        self.auths = auths
        self.auto_enable_auth = auto_enable_auth
        self._lrms_list = []
        self._init_backends()

    def get_backend(self, resource_name):
        for lrms in self._lrms_list:
            if fnmatch(lrms._resource.name, resource_name):
                return lrms
        raise gc3libs.exceptions.InvalidResourceName(
            "Cannot find computational resource '%s'" % 
            resource_name)

    def _init_backends(self):
        for _resource in self._resources:
            if not _resource.enabled:
                gc3libs.log.info("Ignoring disabled resource '%s'.", resource_name)
                continue
            try:
                _lrms = self._get_backend(_resource.name)
                # self.auths.get(_lrms._resource.auth)
                self._lrms_list.append(_lrms)
            except Exception, ex:
                # log exceptions but ignore them
                gc3libs.log.warning("Failed creating LRMS for resource '%s' of type '%s': %s: %s",
                                    _resource.name, _resource.type,
                                    ex.__class__.__name__, str(ex), exc_info=True)
                continue

    def select_resource(self, match):
        """
        Alter the configured list of resources, and retain only those
        that satisfy predicate `match`.

        Argument `match` can be:

          - either a function (or a generic callable) that is passed
            each `Resource` object in turn, and should return a
            boolean indicating whether the resources should be kept
            (`True`) or not (`False`);

          - or it can be a string: only resources whose name matches
            (wildcards ``*`` and ``?`` are allowed) are retained.
        """
        try:
            self._resources = [ res for res in self._resources if match(res) ]
            self._lrms_list = [ lrms for lrms in self._lrms_list if match(lrms._resource) ]
        except:
            # `match` is not callable, then assume it's a 
            # glob pattern and select resources whose name matches
            self._resources = [ res for res in self._resources
                                if fnmatch(res.name, match) ]
            self._lrms_list = [ lrms for lrms in self._lrms_list
                                if fnmatch(lrms._resource.name, match) ]
        # return len(self._resources)
        return len(self._lrms_list)


    def free(self, app, **kw):
        """
        Free up any remote resources used for the execution of `app`.
        In particular, this should delete any remote directories and
        files.

        It is an error to call this method if `app.execution.state` is
        anything other than `TERMINATED`: an `InvalidOperation` exception
        will be raised in this case.

        :raise: `gc3libs.exceptions.InvalidOperation` if `app.execution.state`
                differs from `Run.State.TERMINATED`.
        """
        assert isinstance(app, Task), \
            "Core.free: passed an `app` argument which is not a `Task` instance."
        if isinstance(app, Application):
            return self.__free_application(app, **kw)
        else:
            # must be a `Task` instance
            return self.__free_task(app, **kw)
        
    def __free_application(self, app, **kw):
        """Implementation of `free` on `Application` objects."""
        if app.execution.state not in [ Run.State.TERMINATING, Run.State.TERMINATED ]:
            raise gc3libs.exceptions.InvalidOperation(
                "Attempting to free resources of job '%s',"
                " which is in non-terminal state." % app)

        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        lrms =  self.get_backend(app.execution.resource_name)
        lrms.free(app)

    def __free_task(self, task, **kw):
        """Implementation of `free` on generic `Task` objects."""
        return task.free(**kw)


    def submit(self, app, **kw):
        """
        Submit a job running an instance of the given `app`.  Upon
        successful submission, call the `submitted` method on the
        `app` object.

        :raise: `gc3libs.exceptions.InputFileError` if an input file
                does not exist or cannot otherwise be read.
        """
        assert isinstance(app, Task), \
            "Core.submit: passed an `app` argument which is not a `Task` instance."
        if isinstance(app, Application):
            return self.__submit_application(app, **kw)
        else:
            # must be a `Task` instance
            return self.__submit_task(app, **kw)

    def __submit_application(self, app, **kw):
        """Implementation of `submit` on `Application` objects."""

        gc3libs.log.debug("Submitting %s ..." % str(app))

        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
        
        # XXX: we obsolete this check as we now move this responsibility
        # within the LRMS
        # check that all input files can be read
        #for local_path in app.inputs:
        #    gc3libs.utils.test_file(local_path, os.R_OK,
        #                            gc3libs.exceptions.InputFileError)

        job = app.execution

        if len(self._lrms_list) == 0:
            raise gc3libs.exceptions.NoResources(
                "Could not initialize any computational resource"
                " - please check log and configuration file.")

        # # XXX: auth should probably become part of LRMS and controlled within it
        # for _lrms in self._lrms_list:
        #     try:
        #         self.auths.get(_lrms._resource.auth)
        #     except Exception, ex:
        #         gc3libs.log.warning('Failed obtaining auth. Error type %s, message %s' % (ex.__class__,str(ex)))
        #         continue

        gc3libs.log.debug('Performing brokering ...')
        # decide which resource to use
        # (Resource)[] = (Scheduler).PerformBrokering((Resource)[],(Application))
        _selected_lrms_list = app.compatible_resources(self._lrms_list)
        gc3libs.log.debug('Application scheduler returned %d matching resources',
                           len(_selected_lrms_list))
        if 0 == len(_selected_lrms_list):
            raise gc3libs.exceptions.NoResources(
                "No available resource can accomodate the application requirements")

        if len(_selected_lrms_list) <= 1:
            # shortcut: no brokering to do, just use what we've got
            updated_resources = _selected_lrms_list
        else:
            # update status of selected resources
            updated_resources = []
            for r in _selected_lrms_list:
                try:
                    # in-place update of resource status
                    gc3libs.log.debug("Trying to update status of resource '%s' ..."
                                      % r._resource.name)
                    r.get_resource_status()
                    updated_resources.append(r)
                except Exception, x:
                    # ignore errors in update, assume resource has a problem
                    # and just drop it
                    gc3libs.log.error("Cannot update status of resource '%s', dropping it."
                                      " See log file for details."
                                      % r._resource.name)
                    gc3libs.log.debug("Got error from get_resource_status(): %s: %s",
                                      x.__class__.__name__, x.args, exc_info=True)

        # sort resources according to Application's preferences
        _selected_lrms_list = app.rank_resources(updated_resources)

        exs = [ ]
        # Scheduler.do_brokering returns a sorted list of valid lrms
        for lrms in _selected_lrms_list:
            gc3libs.log.debug("Attempting submission to resource '%s'..." 
                              % lrms._resource.name)
            try:
                # self.auths.get(lrms._resource.auth)
                lrms.submit_job(app)
            except Exception, ex:
                gc3libs.log.info(
                    "Error in submitting job to resource '%s': %s: %s", 
                    lrms._resource.name, ex.__class__.__name__, str(ex),
                    exc_info=True)
                exs.append(ex) 
                continue
            gc3libs.log.info("Successfully submitted %s to: %s",
                             str(app), lrms._resource.name)
            job.state = Run.State.SUBMITTED
            job.resource_name = lrms._resource.name
            job.info = ("Submitted to '%s' at %s"
                        % (job.resource_name, 
                           time.ctime(job.timestamp[Run.State.SUBMITTED])))
            app.changed = True
            app.submitted()
            # job submitted; return to caller
            return
        # if wet get here, all submissions have failed; call the
        # appropriate handler method if defined
        ex = app.submit_error(exs)
        if isinstance(ex, Exception):
            app.execution.info = ("Submission failed: %s" % str(ex))
            raise ex
        else:
            return

    def __submit_task(self, task, **kw):
        """Implementation of `submit` on generic `Task` objects."""
        kw.setdefault('auto_enable_auth', self.auto_enable_auth)
        task.submit(**kw)


    def update_job_state(self, *apps, **kw):
        """
        Update state of all applications passed in as arguments.
        
        If keyword argument `update_on_error` is `False` (default),
        then application execution state is not changed in case a
        backend error happens; it is changed to `UNKNOWN` otherwise.

        Note that if state of a job changes, the `Run.state` calls the
        appropriate handler method on the application/task object.

        :raise: `gc3libs.exceptions.InvalidArgument` in case one of
                the passed `Application` or `Task` objects is
                invalid. This can stop updating the state of other
                objects in the argument list.

        :raise: `gc3libs.exceptions.ConfigurationError` if the
                configuration of this `Core` object is invalid or
                otherwise inconsistent (e.g., a resource references a
                non-existing auth section).
        
        """
        self.__update_application((app for app in apps if isinstance(app, Application)), **kw)
        self.__update_task((app for app in apps if not isinstance(app, Application)), **kw)

    def __update_application(self, apps, **kw):
        """Implementation of `update_job_state` on `Application` objects."""
        update_on_error = kw.get('update_on_error', False)
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        for app in apps:
            state = app.execution.state
            old_state = state
            gc3libs.log.debug("About to update state of application: %s (currently: %s)", app, state)
            try:
                if state not in [ Run.State.NEW,
                                  Run.State.TERMINATING,
                                  Run.State.TERMINATED,
                                  ]:
                    lrms = self.get_backend(app.execution.resource_name)
                    try:
                        # self.auths.get(lrms._resource.auth)
                        state = lrms.update_job_state(app)
                    except Exception, ex:
                        gc3libs.log.debug(
                            "Error getting status of application '%s': %s: %s",
                            app, ex.__class__.__name__, str(ex), exc_info=True)
                        state = Run.State.UNKNOWN
                        # run error handler if defined
                        ex = app.update_job_state_error(ex)
                        if isinstance(ex, Exception):
                            raise ex
                    if state != old_state:
                        app.changed = True
                        # set log information accordingly
                        if (app.execution.state == Run.State.TERMINATING 
                            and app.execution.returncode != 0):
                            # there was some error, try to explain
                            signal = app.execution.signal
                            if signal in Run.Signals:
                                app.execution.info = ("Abnormal termination: %s" % signal)
                            else:
                                if os.WIFSIGNALED(app.execution.returncode):
                                    app.execution.info = ("Remote job terminated by signal %d" % signal)
                                else:
                                    app.execution.info = ("Remote job exited with code %d" 
                                                          % app.execution.exitcode)
                    if state != Run.State.UNKNOWN or update_on_error:
                        app.execution.state = state

            except (gc3libs.exceptions.InvalidArgument, 
                    gc3libs.exceptions.ConfigurationError,
                    gc3libs.exceptions.UnrecoverableAuthError,
                    gc3libs.exceptions.FatalError):
                # Unrecoverable; no sense in continuing --
                # pass immediately on to client code and let
                # it handle this...
                raise

            except gc3libs.exceptions.UnknownJob:
                # information about the job is lost, mark it as failed
                app.execution.returncode = (Run.Signals.Lost, -1)
                app.execution.state = Run.State.TERMINATED
                app.changed = True
                continue

            # XXX: Re-enabled the catch-all clause otherwise the loop stops at the first erroneous iteration
            except Exception, ex:
                gc3libs.log.warning("Ignored error in Core.update_job_state(): %s", str(ex))
                gc3libs.log.debug("Ignored error in Core.update_job_state(): %s: %s",
                                  ex.__class__.__name__, str(ex), exc_info=True)
                continue

    def __update_task(self, tasks, **kw):
        """Implementation of `update_job_state` on generic `Task` objects."""
        for task in tasks:
            assert isinstance(task, Task), \
                   "Core.update_job_state: passed an argument which is not a `Task` instance."
            task.update_state()


    def fetch_output(self, app, download_dir=None, overwrite=False, **kw):
        """
        Retrieve output into local directory `app.output_dir`;
        optional argument `download_dir` overrides this.

        The download directory is created if it does not exist.  If it
        already exists, and the optional argument `overwrite` is
        `False` (default), it is renamed with a `.NUMBER` suffix and a
        new empty one is created in its place.  Otherwise, if
        'overwrite` is `True`, files are downloaded over the ones
        already present.

        If the task is in TERMINATING state, the state is changed to
        `TERMINATED`, attribute `output_dir`:attr: is set to the
        absolute path to the directory where files were downloaded,
        and the `terminated` transition method is called on the `app`
        object.

        Task output cannot be retrieved when `app.execution` is in one
        of the states `NEW` or `SUBMITTED`; an
        `OutputNotAvailableError` exception is thrown in these cases.

        :raise: `gc3libs.exceptions.OutputNotAvailableError` if no
                output can be fetched from the remote job (e.g., the
                Application/Task object is in `NEW` or `SUBMITTED`
                state, indicating the remote job has not started
                running).
        """
        assert isinstance(app, Task), \
            "Core.fetch_output: passed an `app` argument which is not a `Task` instance."
        if isinstance(app, Application):
            self.__fetch_output_application(app, download_dir, overwrite, **kw)
        else:
            # generic `Task` object
            self.__fetch_output_task(app, download_dir, overwrite, **kw)

    def __fetch_output_application(self, app, download_dir, overwrite, **kw):
        """Implementation of `fetch_output` on `Application` objects."""
        job = app.execution
        if job.state in [ Run.State.NEW, Run.State.SUBMITTED ]:
            raise gc3libs.exceptions.OutputNotAvailableError(
                "Output not available: '%s' currently in state '%s'"
                % (app, app.execution.state))

        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        # Prepare/Clean download dir
        if download_dir is None:
            try:
                download_dir = app.output_dir
            except AttributeError:
                raise gc3libs.exceptions.InvalidArgument(
                    "`Core.fetch_output` called with no explicit download directory,"
                    " but object '%s' has no `output_dir` attribute set either."
                    % (app, type(app)))
        try:
            if overwrite:
                if not os.path.exists(download_dir):
                    os.makedirs(download_dir)
            else:
                utils.mkdir_with_backup(download_dir)
        except Exception, ex:
            gc3libs.log.error("Failed creating download directory '%s': %s: %s",
                              download_dir, ex.__class__.__name__, str(ex))
            raise

        # download job output
        try:
            lrms = self.get_backend(job.resource_name)
            # self.auths.get(lrms._resource.auth)
            lrms.get_results(app, download_dir)
            # clear previous data staging errors
            if job.signal == Run.Signals.DataStagingFailure:
                job.signal = 0
        except gc3libs.exceptions.RecoverableDataStagingError, rex:
            job.info = ("Temporary failure when retrieving results: %s. Ignoring error, try again." % str(rex))
            return
        except gc3libs.exceptions.UnrecoverableDataStagingError, ex:
            job.signal = Run.Signals.DataStagingFailure
            ex = app.fetch_output_error(ex)
            if isinstance(ex, Exception):
                job.info = ("No output could be retrieved: %s" % str(ex))
                raise ex
            else:
                return
        except Exception, ex:
            ex = app.fetch_output_error(ex)
            if isinstance(ex, Exception):
                raise ex
            else:
                return
        
        # successfully downloaded results
        gc3libs.log.debug("Downloaded output of '%s' (which is in state %s)"
                          % (str(app), job.state))

        app.output_dir = os.path.abspath(download_dir)
        app.changed = True

        if job.state == Run.State.TERMINATING:
            job.info = ("Final output downloaded to '%s'" % download_dir)
            job.state = Run.State.TERMINATED
            gc3libs.log.debug("Final output of '%s' retrieved" % str(app))
        else:
            job.info = ("Output snapshot downloaded to '%s'" % download_dir)


    def __fetch_output_task(self, task, download_dir, overwrite, **kw):
        """Implementation of `fetch_output` on generic `Task` objects."""
        return task.fetch_output(download_dir, overwrite, **kw)


    def get_resources(self, **kw):
        """
        Return list of resources configured into this `Core` instance.
        """
        return [ lrms._resource for lrms in self._lrms_list ]


    def kill(self, app, **kw):
        """
        Terminate a job.

        Terminating a job in RUNNING, SUBMITTED, or STOPPED state
        entails canceling the job with the remote execution system;
        terminating a job in the NEW or TERMINATED state is a no-op.
        """
        assert isinstance(app, Task), \
            "Core.kill: passed an `app` argument which is not a `Task` instance."
        if isinstance(app, Application):
            self.__kill_application(app, **kw)
        else:
            self.__kill_task(app, **kw)
            
    def __kill_application(self, app, **kw):
        """Implementation of `kill` on `Application` objects."""
        job = app.execution
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
        lrms = self.get_backend(job.resource_name)
        # self.auths.get(lrms._resource.auth)
        lrms.cancel_job(app)
        gc3libs.log.debug("Setting job '%s' status to TERMINATED"
                          " and returncode to SIGCANCEL" % job)
        app.changed = True
        # setting the state runs the state-transition handlers,
        # which may raise an error -- ignore them, but log nonetheless
        try:
            job.state = Run.State.TERMINATED
        except Exception, ex:
            gc3libs.log.info("Ignoring error in state transition"
                             " since task is being killed: %s",
                             str(ex))
        job.signal = Run.Signals.Cancelled
        job.log.append("Cancelled.")

    def __kill_task(self, task, **kw):
        kw.setdefault('auto_enable_auth', self.auto_enable_auth)
        task.kill(**kw)
    

    def peek(self, app, what='stdout', offset=0, size=None, **kw):
        """
        Download `size` bytes (at `offset` bytes from the start) from
        the remote job standard output or error stream, and write them
        into a local file.  Return file-like object from which the
        downloaded contents can be read.

        If `size` is `None` (default), then snarf all available
        contents of the remote stream from `offset` unto the end.
        
        The only allowed values for the `what` arguments are the
        strings `'stdout'` and `'stderr'`, indicating that the
        relevant section of the job's standard output resp. standard
        error should be downloaded.
        """
        assert isinstance(app, Task), \
            "Core.peek: passed an `app` argument which is not a `Task` instance."
        if isinstance(app, Application):
            return self.__peek_application(app, what, offset, size, **kw)
        else:
            return self.__peek_task(app, what, offset, size, **kw)
        
    def __peek_application(self, app, what, offset, size, **kw):
        """Implementation of `peek` on `Application` objects."""
        job = app.execution
        if what == 'stdout':
            remote_filename = job.stdout_filename
        elif what == 'stderr':
            remote_filename = job.stderr_filename
        else:
            raise Error("File name requested to `Core.peek` must be"
                        " 'stdout' or 'stderr', not '%s'" % what)

        # Check if local data available
        if job.state == Run.State.TERMINATED:
            # FIXME: local data could be stale!!
            filename = os.path.join(app.output_dir, remote_filename)
            local_file = open(filename, 'r')
        else:
            # Get authN
            auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
            lrms = self.get_backend(job.resource_name)
            local_file = tempfile.NamedTemporaryFile(suffix='.tmp', prefix='gc3libs.')
            lrms.peek(app, remote_filename, local_file, offset, size)
            local_file.flush()
            local_file.seek(0)
        
        return local_file

    def __peek_task(self, task, what, offset, size, **kw):
        """Implementation of `peek` on generic `Task` objects."""
        return task.peek(what, offset, size, **kw)
    

    def update_resources(self, **kw):
        """
        Update the state of resources configured into this `Core` instance.

        Each resource object in the returned list will have its `updated` attribute
        set to `True` if the update operation succeeded, or `False` if it failed.
        """
        for lrms in self._lrms_list:
            try:
                auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
                resource = lrms.get_resource_status()
                resource.updated = True
            except Exception, ex:
                gc3libs.log.error("Got error while updating resource '%s': %s."
                                  % (lrms._resource.name, str(ex)))
                lrms._resource.updated = False


    def close(self):
        """
        Used to invoke explicitly the distructor on objects
        e.g. LRMS
        """
        for lrms in self._lrms_list:
            lrms.close()


    ## compatibility with the `Engine` interface
    
    def add(self, task):
        """
        This method is here just to allow `Core` and `Engine` objects
        to be used interchangeably.  It's effectively a no-op, as it makes
        no sense in the synchronous/blocking semantics implemented by `Core`.
        """
        pass


    def remove(self, task):
        """
        This method is here just to allow `Core` and `Engine` objects
        to be used interchangeably.  It's effectively a no-op, as it makes
        no sense in the synchronous/blocking semantics implemented by `Core`.
        """
        pass


    ## internal methods

    def _get_backend(self,resource_name):
        _lrms = None

        for _resource in self._resources:
            if _resource.name == resource_name:
                # there's a matching resource
                try:
                    if _resource.type == gc3libs.Default.ARC0_LRMS:
                        from gc3libs.backends.arc0 import ArcLrms
                        _lrms = ArcLrms(_resource, self.auths)
                    elif _resource.type == gc3libs.Default.ARC1_LRMS:
                        from gc3libs.backends.arc1 import Arc1Lrms
                        _lrms = Arc1Lrms(_resource, self.auths)
                    elif _resource.type == gc3libs.Default.SGE_LRMS:
                        _lrms = SgeLrms(_resource, self.auths)
                    elif _resource.type == gc3libs.Default.LSF_LRMS:
                        _lrms = LsfLrms(_resource, self.auths)
                    elif _resource.type == gc3libs.Default.FORK_LRMS:
                        _lrms = ForkLrms(_resource, self.auths)
                    elif _resource.type == gc3libs.Default.SUBPROCESS_LRMS:
                        _lrms = SubprocessLrms(_resource, self.auths)
                    else:
                        raise gc3libs.exceptions.ConfigurationError(
                            "Unknown resource type '%s'" % _resource.type)
                except Exception, ex:
                    gc3libs.log.error(
                        "Error in creating resource %s: %s."
                        " Configuration file problem?"
                        % (_resource.name, str(ex)))
                    raise

        if _lrms is None:
            raise gc3libs.exceptions.InvalidResourceName(
                "Cannot find computational resource '%s'" % resource_name)

        return _lrms


# === Configuration File

def import_config(config_file_locations, auto_enable_auth=True):
    (resources, auths) = read_config(*config_file_locations)
    return (get_resources(resources), 
            get_auth(auths,auto_enable_auth), 
            auto_enable_auth)


def get_auth(auths,auto_enable_auth):
    try:
        return Auth(auths, auto_enable_auth)
    except Exception, ex:
        gc3libs.log.critical('Failed initializing Auth module: %s: %s',
                             ex.__class__.__name__, str(ex))
        raise


def get_resources(resources_list):
    # build Resource objects from the list returned from read_config
    #        and match with selectd_resource from comand line
    #        (optional) if not options.resource_name is None:
    resources = [ ]
    for key in resources_list.keys():
        resource = resources_list[key]
        try:
            tmpres = gc3libs.Resource.Resource(resource)
        except Exception, x:
            gc3libs.log.error("Could not create resource '%s': %s."
                              " Please check configuration file.",
                               key, str(x))
            continue
        if tmpres.type == 'arc':
            gc3libs.log.warning("Resource type 'arc' was renamed to 'arc0',"
                                " please change all occurrences of 'type=arc' to 'type=arc0'"
                                " in your configuration file.")
            tmpres.type = gc3libs.Default.ARC0_LRMS
        elif tmpres.type == 'ssh':
            gc3libs.log.warning("Resource type 'ssh' was renamed to '%s',"
                                " please change all occurrences of 'type=ssh' to 'type=%s'"
                                " in your configuration file.",
                                gc3libs.Default.SGE_LRMS, gc3libs.Default.SGE_LRMS)
            tmpres.type = gc3libs.Default.SGE_LRMS
        if tmpres.type not in [
            gc3libs.Default.ARC0_LRMS,
            gc3libs.Default.ARC1_LRMS,
            gc3libs.Default.SGE_LRMS,
            gc3libs.Default.LSF_LRMS,
            gc3libs.Default.FORK_LRMS,
            gc3libs.Default.SUBPROCESS_LRMS,
            ]:
            gc3libs.log.error(
                "Configuration error: '%s' is no valid resource type.", 
                resource['type'])
            continue
        gc3libs.log.debug("Created %s resource '%s' of type %s"
                          % (utils.ifelse(tmpres.is_valid, "valid", "invalid"),
                             tmpres.name, 
                             tmpres.type))
        resources.append(tmpres)
    return resources

                                
def read_config(*locations):
    """
    Read each of the configuration files listed in `locations`, and
    return a `(defaults, resources, auths)` triple that can be passed
    to the `Core` class constructor.
    """
    files_successfully_read = 0
    defaults = { }
    resources = defaultdict(lambda: dict())
    auths = defaultdict(lambda: dict())

    # map values for the `architecture=...` configuration item
    # into internal constants
    architecture_value_map = {
        # 'x86-32', 'x86 32-bit', '32-bit x86' and variants thereof
        re.compile('x86[ _-]+32([ _-]+bits?)?', re.I): Run.Arch.X86_32,
        re.compile('32[ _-]+bits? +[ix]86', re.I):     Run.Arch.X86_32,
        # accept also values printed by `uname -a` on 32-bit x86 archs
        re.compile('i[3456]86', re.I):                 Run.Arch.X86_32,
        # 'x86_64', 'x86 64-bit', '64-bit x86' and variants thereof
        re.compile('x86[ _-]+64([ _-]+bits?)?', re.I): Run.Arch.X86_64,
        re.compile('64[ _-]+bits? +[ix]86', re.I):     Run.Arch.X86_32,
        # also accept commercial arch names
        re.compile('(amd[ -]*64|x64|emt64|intel[ -]*64)( *bits?)?', re.I): \
                                                       Run.Arch.X86_64,
        # finally, map "32-bit" and "64-bit" to i686 and x86_64
        re.compile('32[ _-]+bits?', re.I):             Run.Arch.X86_32,
        re.compile('64[ _-]+bits?', re.I):             Run.Arch.X86_64,
        }

    for location in locations:
        location = os.path.expandvars(location)
        if os.path.exists(location):
            if os.access(location, os.R_OK):
                gc3libs.log.debug("Core.read_config(): Reading file '%s'" % location)
            else:
                gc3libs.log.debug("Core.read_config(): File '%s' cannot be read, ignoring." % location)
                continue # with next `location`
        else:
            gc3libs.log.debug("Core.read_config(): File '%s' does not exist, ignoring." % location)
            continue # with next `location`

        # Config File exists; read it
        config = ConfigParser.ConfigParser()
        if location not in config.read(location):
            gc3libs.log.debug("Configuration file '%s' is unreadable or malformed:"
                              " ignoring." % location)
            continue # with next `location`
        files_successfully_read += 1

        # update `defaults` with the contents of the `[DEFAULTS]` section
        defaults.update(config.defaults())

        for sectname in config.sections():
            if sectname.startswith('auth/'):
                # handle auth section
                gc3libs.log.debug("Core.read_config():"
                                  " Read configuration stanza for auth '%s'." % sectname)
                # extract auth name and register auth dictionary
                auth_name = sectname.split('/', 1)[1]
                auths[auth_name].update(dict(config.items(sectname)))

            elif  sectname.startswith('resource/'):
                # handle resource section
                resource_name = sectname.split('/', 1)[1]
                gc3libs.log.debug("Core.read_config():"
                                  " Read configuration stanza for resource '%s'." % resource_name)
                config_items = dict(config.items(sectname))
                if config_items.has_key('enabled'):
                    config_items['enabled'] = utils.string_to_boolean(config_items['enabled'])
                if config_items.has_key('architecture'):
                    def matching_architecture(value):
                        for matcher, arch in architecture_value_map.items():
                            if matcher.match(value):
                                return arch
                        raise gc3libs.exceptions.ConfigurationError(
                            "Unknown architecture '%s' in resource '%s'"
                            " (reading configuration file '%s')"
                            % (value, resource_name, location))
                    archs = [ matching_architecture(value.strip())
                              for value in config_items['architecture'].split(',') ]
                    if len(archs) == 0:
                        raise gc3libs.exceptions.ConfigurationError(
                            "No architecture specified for resource '%s'" % resource_name)
                resources[resource_name].update(config_items)
                resources[resource_name]['name'] = resource_name

            else:
                # Unhandled sectname
                gc3libs.log.error("Core.read_config(): unknown configuration section '%s' -- ignoring!", 
                                   sectname)

    # remove disabled resources
    disabled_resources = [ ]
    for resource in resources.values():
        if resource.has_key('enabled'):
            if not resource['enabled']:
                disabled_resources.append(resource['name'])
        else:
            # by default, resources are enabled
            resource['enabled'] = True
    for resource_name in disabled_resources:
        gc3libs.log.info("Ignoring computational resource '%s'"
                         " because of 'enabled=False' setting"
                         " in configuration file.",
                         resource_name)
        del resources[resource_name]

    if files_successfully_read == 0:
        raise gc3libs.exceptions.NoConfigurationFile("Could not read any configuration file; tried locations '%s'."
                                  % str.join("', '", locations))

    return (resources, auths)


class Engine(object):
    """
    Submit tasks in a collection, and update their state until a
    terminal state is reached. Specifically:
      
      * tasks in `NEW` state are submitted;

      * the state of tasks in `SUBMITTED`, `RUNNING` or `STOPPED` state is updated;

      * when a task reaches `TERMINATED` state, its output is downloaded.

    The behavior of `Engine` instances can be further customized by
    setting the following instance attributes:

      `can_submit`
        Boolean value: if `False`, no task will be submitted.

      `can_retrieve`
        Boolean value: if `False`, no output will ever be retrieved.

      `max_in_flight`
        If >0, limit the number of tasks in `SUBMITTED` or `RUNNING`
        state: if the number of tasks in `SUBMITTED`, `RUNNING` or
        `STOPPED` state is greater than `max_in_flight`, then no new
        submissions will be attempted.

      `max_submitted` 
        If >0, limit the number of tasks in `SUBMITTED` state: if the
        number of tasks in `SUBMITTED`, `RUNNING` or `STOPPED` state is
        greater than `max_submitted`, then no new submissions will be
        attempted.

      `output_dir`
        Base directory for job output; if not `None`, each task's
        results will be downloaded in a subdirectory named after the
        task's `permanent_id`.

      `fetch_output_overwrites`
        Default value to pass as the `overwrite` argument to
        :meth:`Core.fetch_output` when retrieving results of a
        terminated task.

    Any of the above can also be set by passing a keyword argument to
    the constructor::

      >>> e = Engine(can_submit=False)
      >>> e.can_submit
      False
    """


    def __init__(self, grid, tasks=list(), store=None, 
                 can_submit=True, can_retrieve=True, 
                 max_in_flight=0, max_submitted=0,
                 output_dir=None, fetch_output_overwrites=False):
        """
        Create a new `Engine` instance.  Arguments are as follows: 

        `grid`
          A `gc3libs.Core` instance, that will be used to operate on
          tasks.  This is the only required argument.

        `apps`
          Initial list of tasks to be managed by this Engine.  Tasks can
          be later added and removed with the `add` and `remove`
          methods (which see).  Defaults to the empty list.

        `store`
          An instance of `gc3libs.persistence.Store`, or `None`.  If
          not `None`, it will be used to persist tasks after each
          iteration; by default no store is used so no task state is
          persisted.

        `can_submit`, `can_retrieve`, `max_in_flight`, `max_submitted`
          Optional keyword arguments; see `Engine` for a description.
        """
        # internal-use attributes
        self._new = []
        self._in_flight = []
        self._stopped = []
        self._terminating = []
        self._terminated = []
        self._to_kill = []
        self._core = grid
        self._store = store
        for task in tasks:
            self.add(task)
        # public attributes
        self.can_submit = can_submit
        self.can_retrieve = can_retrieve
        self.max_in_flight = max_in_flight
        self.max_submitted = max_submitted
        self.output_dir = output_dir
        self.fetch_output_overwrites = fetch_output_overwrites


    def add(self, task):
        """
        Add `task` to the list of tasks managed by this Engine.
        Adding a task that has already been added to this `Engine`
        instance results in a no-op.
        """
        state = task.execution.state
        # Work around infinite recursion error when trying to compare
        # `UserDict` instances which can contain each other.  We know
        # that two identical tasks are the same object by
        # construction, so let's use this to check.
        def contained(elt, lst):
            i = id(elt)
            for item in lst:
                if i == id(item):
                    return True
            return False
        if Run.State.NEW == state:
            if not contained(task, self._new): self._new.append(task)
        elif Run.State.SUBMITTED == state or Run.State.RUNNING == state:
            if not contained(task, self._in_flight): self._in_flight.append(task)
        elif Run.State.STOPPED == state:
            if not contained(task, self._stopped): self._stopped.append(task)
        elif Run.State.TERMINATING == state:
            if not contained(task, self._terminating): self._terminating.append(task)
        elif Run.State.TERMINATED == state:
            if not contained(task, self._terminated): self._terminated.append(task)
        else:
            raise AssertionError("Unhandled run state '%s' in gc3libs.core.Engine." % state)
        task.attach(self)


    def remove(self, task):
        """Remove a `task` from the list of tasks managed by this Engine."""
        state = task.execution.state
        if Run.State.NEW == state:
            self._new.remove(task)
        elif Run.State.SUBMITTED == state or Run.State.RUNNING == state:
            self._in_flight.remove(task)
        elif Run.State.STOPPED == state:
            self._stopped.remove(task)
        elif Run.State.TERMINATING == state:
            self._terminating.remove(task)
        elif Run.State.TERMINATED == state:
            self._terminated.remove(task)
        else:
            raise AssertionError("Unhandled run state '%s' in gc3libs.core.Engine." % state)
        task.detach()


    def progress(self):
        """
        Update state of all registered tasks and take appropriate action.
        Specifically:

          * tasks in `NEW` state are submitted;

          * the state of tasks in `SUBMITTED`, `RUNNING` or `STOPPED` state is updated;

          * when a task reaches `TERMINATING` state, its output is downloaded.

          * tasks in `TERMINATED` status are simply ignored.

        The `max_in_flight` and `max_submitted` limits (if >0) are
        taken into account when attempting submission of tasks.
        """
        # prepare 
        currently_submitted = 0
        currently_in_flight = 0
        if self.max_in_flight > 0:
            limit_in_flight = self.max_in_flight
        else:
            limit_in_flight = utils.PlusInfinity()
        if self.max_submitted > 0:
            limit_submitted = self.max_submitted
        else:
            limit_submitted = utils.PlusInfinity()

        # update status of SUBMITTED/RUNNING tasks before launching
        # new ones, otherwise we would be checking the status of
        # some tasks twice...
        #gc3libs.log.debug("Engine.progress: updating status of tasks [%s]"
        #                  % str.join(', ', [str(task) for task in self._in_flight]))
        transitioned = []
        for index, task in enumerate(self._in_flight):
            try:
                self._core.update_job_state(task)
                if self._store and task.changed:
                    self._store.save(task)
                state = task.execution.state
                if state == Run.State.SUBMITTED:
                    # only real applications need to be counted
                    # against the limit; policy tasks are exempt
                    # (this applies to all similar clause below)
                    if isinstance(task, Application):
                        currently_submitted += 1
                        currently_in_flight += 1
                elif state == Run.State.RUNNING:
                    if isinstance(task, Application):
                        currently_in_flight += 1
                elif state == Run.State.STOPPED:
                    transitioned.append(index) # task changed state, mark as to remove
                    self._stopped.append(task)
                elif state == Run.State.TERMINATING:
                    transitioned.append(index) # task changed state, mark as to remove
                    self._terminating.append(task)
                elif state == Run.State.TERMINATED:
                    transitioned.append(index) # task changed state, mark as to remove
                    self._terminated.append(task)
            except gc3libs.exceptions.ConfigurationError:
                # Unrecoverable; no sense in continuing -- pass
                # immediately on to client code and let it handle
                # this...
                raise
            except Exception, x:
                gc3libs.log.error("Ignoring error in updating state of task '%s': %s: %s"
                                  % (task, x.__class__.__name__, str(x)),
                                  exc_info=True)
        # remove tasks that transitioned to other states
        for index in reversed(transitioned):
            del self._in_flight[index]

        # execute kills and update count of submitted/in-flight tasks
        #gc3libs.log.debug("Engine.progress: killing tasks [%s]"
        #                  % str.join(', ', [str(task) for task in self._to_kill]))
        transitioned = []
        for index, task in enumerate(self._to_kill):
            try:
                old_state = task.execution.state
                self._core.kill(task)
                if self._store:
                    self._store.save(task)
                if old_state == Run.State.SUBMITTED:
                    if isinstance(task, Application):
                        currently_submitted -= 1
                        currently_in_flight -= 1
                elif old_state == Run.State.RUNNING:
                    if isinstance(task, Application):
                        currently_in_flight -= 1
                self._terminated.append(task)
                transitioned.append(index)
            except Exception, x:
                gc3libs.log.error("Ignored error in killing task '%s': %s: %s"
                                  % (task, x.__class__.__name__, str(x)),
                                  exc_info=True)
        # remove tasks that transitioned to other states
        for index in reversed(transitioned):
            del self._to_kill[index]

        # update state of STOPPED tasks; again need to make before new
        # submissions, because it can alter the count of in-flight
        # tasks.
        #gc3libs.log.debug("Engine.progress: updating status of stopped tasks [%s]"
        #                  % str.join(', ', [str(task) for task in self._stopped]))
        transitioned = []
        for index, task in enumerate(self._stopped):
            try:
                self._core.update_job_state(task)
                if self._store and task.changed:
                    self._store.save(task)
                state = task.execution.state
                if state in [Run.State.SUBMITTED, Run.State.RUNNING]:
                    if isinstance(task, Application):
                        currently_in_flight += 1
                        if task.execution.state == Run.State.SUBMITTED:
                            currently_submitted += 1
                    self._in_flight.append(task)
                    transitioned.append(index) # task changed state, mark as to remove
                elif state == Run.State.TERMINATING:
                    self._terminating.append(task)
                    transitioned.append(index) # task changed state, mark as to remove
                elif state == Run.State.TERMINATED:
                    self._terminated.append(task)
                    transitioned.append(index) # task changed state, mark as to remove
            except Exception, x:
                gc3libs.log.error("Ignoring error in updating state of STOPPED task '%s': %s: %s"
                                  % (task, x.__class__.__name__, str(x)),
                                  exc_info=True)
        # remove tasks that transitioned to other states
        for index in reversed(transitioned):
            del self._stopped[index]

        # now try to submit NEW tasks
        #gc3libs.log.debug("Engine.progress: submitting new tasks [%s]"
        #                  % str.join(', ', [str(task) for task in self._new]))
        transitioned = []
        if self.can_submit:
            index = 0
            while (currently_submitted < limit_submitted
                   and currently_in_flight < limit_in_flight
                   and index < len(self._new)):
                task = self._new[index]
                # try to submit; go to SUBMITTED if successful, FAILED if not
                if currently_submitted < limit_submitted and currently_in_flight < limit_in_flight:
                    try:
                        self._core.submit(task)
                        if self._store:
                            self._store.save(task)
                        self._in_flight.append(task)
                        transitioned.append(index)
                        if isinstance(task, Application):
                            currently_submitted += 1
                            currently_in_flight += 1
                    except Exception, x:
                        sys.excepthook(*sys.exc_info()) # DEBUG
                        gc3libs.log.error("Ignored error in submitting task '%s': %s: %s"
                                          % (task, x.__class__.__name__, str(x)))
                        task.execution.log("Submission failed: %s: %s" 
                                           % (x.__class__.__name__, str(x)))
                index += 1
        # remove tasks that transitioned to SUBMITTED state
        for index in reversed(transitioned):
            del self._new[index]

        # finally, retrieve output of finished tasks
        #gc3libs.log.debug("Engine.progress: fetching output of tasks [%s]"
        #                  % str.join(', ', [str(task) for task in self._terminating]))
        if self.can_retrieve:
            transitioned = []
            for index, task in enumerate(self._terminating):
                # try to get output
                try:
                    self._core.fetch_output(task)
                except gc3libs.exceptions.UnrecoverableDataStagingError, ex:
                    gc3libs.log.error("Error in fetching output of task '%s',"
                                      " will mark it as TERMINATED"
                                      " (with error exit code %d): %s: %s",
                                      task, posix.EX_IOERR,
                                      ex.__class__.__name__, str(ex), exc_info=True)
                    task.execution.returncode = (Run.Signals.DataStagingFailure,
                                                 posix.EX_IOERR)
                    task.execution.state = Run.State.TERMINATED
                    task.changed = True
                except Exception, x:
                    gc3libs.log.error("Ignored error in fetching output of task '%s': %s: %s" 
                                      % (task, x.__class__.__name__, str(x)), exc_info=True)
                if task.execution.state == Run.State.TERMINATED:
                    self._terminated.append(task)
                    self._core.free(task)
                    transitioned.append(index)
                if self._store and task.changed:
                    self._store.save(task)
            # remove tasks for which final output has been retrieved
            for index in reversed(transitioned):
                del self._terminating[index]


    def stats(self, only=None):
        """
        Return a dictionary mapping each state name into the count of
        tasks in that state. In addition, the following keys are defined:
        
        * `ok`:  count of TERMINATED tasks with return code 0
        
        * `failed`: count of TERMINATED tasks with nonzero return code

        * `total`: total count of managed tasks, whatever their state

        If the optional argument `only` is not None, tasks whose
        class is not contained in `only` are ignored.

        :param tuple only: Restrict counting to tasks of these classes.
        
        """
        if only:
            gc3libs.log.debug("Engine.stats: Restricting to object of class %s", only)
        result = defaultdict(lambda: 0)
        if only:
            result[Run.State.NEW] = len([task for task in self._new
                                                       if isinstance(task, only)])
        else:
            result[Run.State.NEW] = len(self._new)
        for task in self._in_flight:
            if only and not isinstance(task, only):
                continue
            state = task.execution.state
            result[state] += 1
        result[Run.State.STOPPED] = len(self._stopped)
        for task in self._to_kill:
            if only and not isinstance(task, only):
                continue
            # XXX: presumes no task in the `_to_kill` list is TERMINATED
            state = task.execution.state
            result[state] += 1
        if only:
            result[Run.State.TERMINATING] = len([task for task in self._terminating
                                                               if isinstance(task, only)])
        else:
            result[Run.State.TERMINATING] = len(self._terminating)
        if only:
            result[Run.State.TERMINATED] = len([task for task in self._terminated
                                                               if isinstance(task, only)])
        else:
            result[Run.State.TERMINATED] = len(self._terminated)
        # for TERMINATED tasks, compute the number of successes/failures
        for task in self._terminated:
            if only and not isinstance(task, only):
                continue
            if task.execution.returncode == 0:
                result['ok'] += 1
            else:
                gc3libs.log.debug("Task '%s' failed: return code %s (signal %s, exitcode %s)"
                                  % (task, task.execution.returncode,
                                     task.execution.signal, task.execution.exitcode))
                result['failed'] += 1
        result['total'] = (result[Run.State.NEW]
                           + result[Run.State.SUBMITTED]
                           + result[Run.State.RUNNING]
                           + result[Run.State.STOPPED]
                           + result[Run.State.TERMINATING]
                           + result[Run.State.TERMINATED])
        return result

            
    # implement a Core-like interface, so `Engine` objects can be used
    # as substitutes for `Core`.

    def free(self, task, **kw):
        """
        Proxy for `Core.free`, which see.
        """
        self._core.free(task)


    def submit(self, task, **kw):
        """
        Submit `task` at the next invocation of `perform`.  Actually,
        the task is just added to the collection of managed tasks,
        regardless of its state.
        """
        return self.add(task)


    def update_job_state(self, *tasks, **kw):
        """
        Return list of *current* states of the given tasks.  States
        will only be updated at the next invocation of `perform`; in
        particular, no state-change handlers are called as a result of
        calling this method.
        """
        pass


    def fetch_output(self, task, output_dir=None, overwrite=False, **kw):
        """
        Proxy for `Core.fetch_output` (which see).
        """
        if output_dir is None and self.output_dir is not None:
            output_dir = os.path.join(self.output_dir, task.persistent_id)
        if overwrite is None:
            overwrite = self.fetch_output_overwrites
        self._core.fetch_output(task, output_dir, overwrite, **kw)


    def kill(self, task, **kw):
        """
        Schedule a task for killing on the next `progress` run.
        """
        self._to_kill.append(task)


    def peek(self, task, what='stdout', offset=0, size=None, **kw):
        """
        Proxy for `Core.peek` (which see).
        """
        self._core.peek(task, what, offset, size, **kw)

    def close(self):
        """
        Call explicilty finalize methods on relevant objects
        e.g. LRMS
        """
        self._core.close()
