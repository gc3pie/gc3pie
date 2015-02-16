#!/usr/bin/env python
"""
Top-level interface to Grid functionality.
"""
# Copyright (C) 2009-2015 S3IT, Zentrale Informatik, University of Zurich. All rights reserved.
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 2110-1301 USA
#
__docformat__ = 'reStructuredText'
__version__ = 'development version (SVN $Revision$)'
__date__ = '$Date$'


from fnmatch import fnmatch
import os
import posix
import sys
import time
import tempfile
import warnings
warnings.simplefilter("ignore")

from gc3libs.compat._collections import defaultdict

import gc3libs
import gc3libs.debug
from gc3libs import Application, Run, Task
import gc3libs.exceptions
import gc3libs.utils as utils


class MatchMaker(object):

    """Select and sort resources for attempting submission of a `Task`.

    A match-making algorithm must implement two methods:

    - `filter`: given a task and a list of resources, return the list
      of resources that the given task could be submitted to.

    - `rank`: given a task and a list of resources, return a list of
      resources sorted in preference order, i.e., submission of the
      given task will be attempted to the first returned resource,
      then the next one, etc.

    This class implements the default match-making algorithm in
    GC3Pie, which operates as follows:

    - *filter phase:* if `task` has a `compatible_resources` method (as
      instances of `Application`:class: do), retain only those
      resources where it evaluates to ``True``.  Otherwise, return the
      resources list unchanged.

    - *rank phase:* sort resources according to the task's
      `rank_resources` method, or retain the given order if task does
      not define such method.

    """

    def filter(self, task, resources):
        """
        Return the subset of resources to which `task` could be submitted to.

        Note that the result subset could be empty (no resource can
        accomodate task's requirements).

        The default implementation uses the task's
        `compatible_resources` method to retain only the resources
        that satisfy the task's requirements.  If `task` does not
        provide such a method, the resource list is returned
        unchanged.
        """
        gc3libs.log.debug(
            "Performing matching of resource(s) %s to task '%s' ...",
            str.join(',', (r.name for r in resources)), task)
        # keep only compatible resources
        try:
            compatible_resources = task.compatible_resources(resources)
            gc3libs.log.debug(
                'Task compatiblity check returned %d matching resources',
                len(compatible_resources))
        except AttributeError:
            # XXX: should we require that len(resources) > 0?
            compatible_resources = resources
        return compatible_resources

    def rank(self, task, resources):
        """
        Sort the list of `resources` in the preferred order for submitting
        `task`.

        Unless overridden in a derived class, this calls the task's
        `rank_resources` method to sort the list.  If the task does
        not provide such a method, the resources list is returned
        unchanged.
        """
        # sort resources according to the Task's preference, if stated
        try:
            targets = task.rank_resources(resources)
        except AttributeError:
            targets = resources
        return targets


class Core:

    """Core operations: submit, update state, retrieve (a
snapshot of) output, cancel job.

Core operations are *blocking*, i.e., they return only after the
operation has successfully completed, or an error has been detected.

Operations are always performed by a `Core` object.  `Core` implements
an overlay Grid on the resources specified in the configuration file.

    """

    def __init__(self, cfg, matchmaker=MatchMaker()):
        # init auths
        self.auto_enable_auth = cfg.auto_enable_auth

        # init backends
        self.resources = cfg.make_resources()
        if len(self.resources) == 0:
            raise gc3libs.exceptions.NoResources(
                "No resources given to initialize `gc3libs.core.Core` object!")

        # init matchmaker
        self.matchmaker = matchmaker

    def get_backend(self, name):
        try:
            return self.resources[name]
        except KeyError:
            raise gc3libs.exceptions.InvalidResourceName(
                "Cannot find computational resource '%s'" %
                name)

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
        for lrms in self.resources.itervalues():
            try:
                if not match(lrms):
                    lrms.enabled = False
            except:
                if not fnmatch(lrms.name, match):
                    lrms.enabled = False
        return len(self.resources)

    def free(self, app, **extra_args):
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
        assert isinstance(
            app, Task), "Core.free: passed an `app` argument which" \
            " is not a `Task` instance."
        if isinstance(app, Application):
            return self.__free_application(app, **extra_args)
        else:
            # must be a `Task` instance
            return self.__free_task(app, **extra_args)

    def __free_application(self, app, **extra_args):
        """Implementation of `free` on `Application` objects."""
        if app.execution.state not in [
                Run.State.TERMINATING, Run.State.TERMINATED]:
            raise gc3libs.exceptions.InvalidOperation(
                "Attempting to free resources of job '%s',"
                " which is in non-terminal state." % app)

        # auto_enable_auth = extra_args.get(
        #     'auto_enable_auth', self.auto_enable_auth)

        try:
            lrms = self.get_backend(app.execution.resource_name)
            lrms.free(app)
        except AttributeError:
            gc3libs.log.debug(
                "Core.__free_application():"
                " Application `%s` is missing the `execution.resource_name` attribute."
                " This should not happen. I'm assuming the application had been"
                " aborted before submission.",
                app)

    def __free_task(self, task, **extra_args):
        """Implementation of `free` on generic `Task` objects."""
        return task.free(**extra_args)

    def submit(self, app, resubmit=False, targets=None, **extra_args):
        """Submit a job running an instance of the given task `app`.

        Upon successful submission, call the `submitted` method on the
        `app` object.  If `targets` are given, submission of the task
        is attempted to the resources in the order given; the `submit`
        method returns after the first successful attempt.  If
        `targets` is ``None`` (default), a brokering procedure is run
        to determine the best resource among the configured ones.

        At the beginning of the submission process, the
        `app.execution` state is reset to ``NEW``; if submission is
        successful, the task will be in ``SUBMITTED`` or ``RUNNING``
        state when this call returns.

        :raise: `gc3libs.exceptions.InputFileError` if an input file
                does not exist or cannot otherwise be read.

        :param Task app:
          A GC3Pie `Task`:class: instance to be submitted.
        :param resubmit:
          If ``True``, submit task regardless of its execution state;
          if ``False`` (default), submission is a no-op if task is not
          in ``NEW`` state.
        :param list targets:
          A list of `Resource`s to submit the task to; resources are
          tried in the order given.  If ``None`` (default), perform
          brokering among all the configured resources.

        """
        assert isinstance(
            app, Task), "Core.submit: passed an `app` argument" \
            "which is not a `Task` instance."
        if isinstance(app, Application):
            return self.__submit_application(
                app, resubmit, targets, **extra_args)
        else:
            # must be a `Task` instance
            return self.__submit_task(app, resubmit, targets, **extra_args)

    def __submit_application(self, app, resubmit, targets, **extra_args):
        """Implementation of `submit` on `Application` objects."""

        gc3libs.log.debug("Submitting %s ..." % str(app))

        # auto_enable_auth = extra_args.get(
        #     'auto_enable_auth', self.auto_enable_auth)

        job = app.execution
        if resubmit:
            job.state = Run.State.NEW
        elif job.state != Run.State.NEW:
            return

        # Validate Application local input files
        for input_ref in app.inputs:
            if input_ref.scheme == 'file':
                # Local file, check existence before proceeding
                if not os.path.exists(input_ref.path):
                    raise gc3libs.exceptions.UnrecoverableDataStagingError(
                        "Input file '%s' does not exist" % input_ref.path,
                        do_log=True)

        if targets is not None:
            assert len(targets) > 0
        else:  # targets is None
            enabled_resources = [
                r for r in self.resources.itervalues() if r.enabled]
            if len(enabled_resources) == 0:
                raise gc3libs.exceptions.NoResources(
                    "Could not initialize any computational resource"
                    " - please check log and configuration file.")

            # decide which resource to use
            compatible_resources = self.matchmaker.filter(
                app, enabled_resources)
            if 0 == len(compatible_resources):
                raise gc3libs.exceptions.NoResources(
                    "No available resource can accomodate the application"
                    " requirements")
            gc3libs.log.debug(
                "Application compatibility check returned %d matching"
                " resources", len(compatible_resources))

            if len(compatible_resources) <= 1:
                # shortcut: no brokering to do, just use what we've got
                targets = compatible_resources
            else:
                # update status of selected resources
                updated_resources = []
                for r in compatible_resources:
                    try:
                        # in-place update of resource status
                        gc3libs.log.debug(
                            "Trying to update status of resource '%s' ..."
                            % r.name)
                        r.get_resource_status()
                        updated_resources.append(r)
                    except Exception as err:
                        # ignore errors in update, assume resource has
                        # a problem and just drop it
                        gc3libs.log.error(
                            "Cannot update status of resource '%s', dropping"
                            " it. See log file for details." %
                            r.name)
                        gc3libs.log.debug(
                            "Got error from get_resource_status(): %s: %s",
                            err.__class__.__name__,
                            str(err),
                            exc_info=True)

                if len(updated_resources) == 0:
                    raise gc3libs.exceptions.LRMSSubmitError(
                        "No computational resource found reachable during"
                        " update! Aborting submission of task '%s'" %
                        app)

                # sort resources according to Application's preferences
                targets = self.matchmaker.rank(app, updated_resources)

        exs = []
        # after brokering we have a sorted list of valid resource
        for resource in targets:
            gc3libs.log.debug("Attempting submission to resource '%s'..."
                              % resource.name)
            try:
                job.timestamp[Run.State.NEW] = time.time()
                job.info = ("Submitting to '%s'" % (resource.name,))
                resource.submit_job(app)
            except gc3libs.exceptions.LRMSSkipSubmissionToNextIteration as ex:
                gc3libs.log.info(
                    "Submission of job %s delayed" % app)
                # Just raise the exception
                raise
            except Exception as ex:
                gc3libs.log.info(
                    "Error in submitting job to resource '%s': %s: %s",
                    resource.name, ex.__class__.__name__, str(ex),
                    exc_info=True)
                exs.append(ex)
                continue
            gc3libs.log.info("Successfully submitted %s to: %s",
                             str(app), resource.name)
            job.state = Run.State.SUBMITTED
            job.resource_name = resource.name
            job.info = ("Submitted to '%s'" % (job.resource_name,))
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

    def __submit_task(self, task, resubmit, targets, **extra_args):
        """Implementation of `submit` on generic `Task` objects."""
        extra_args.setdefault('auto_enable_auth', self.auto_enable_auth)
        task.submit(resubmit, **extra_args)

    def update_job_state(self, *apps, **extra_args):
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
        self.__update_application(
            (app for app in apps if isinstance(
                app,
                Application)),
            **extra_args)
        self.__update_task(
            (app for app in apps if not isinstance(
                app,
                Application)),
            **extra_args)

    def __update_application(self, apps, **extra_args):
        """Implementation of `update_job_state` on `Application` objects."""
        update_on_error = extra_args.get('update_on_error', False)
        # auto_enable_auth = extra_args.get(
        #     'auto_enable_auth', self.auto_enable_auth)

        for app in apps:
            state = app.execution.state
            old_state = state
            gc3libs.log.debug(
                "About to update state of application: %s (currently: %s)",
                app,
                state)
            try:
                if state not in [Run.State.NEW,
                                 Run.State.TERMINATING,
                                 Run.State.TERMINATED,
                                 ]:
                    lrms = self.get_backend(app.execution.resource_name)
                    try:
                        state = lrms.update_job_state(app)
                    except Exception as ex:
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
                                and app.execution.returncode is not None
                                and app.execution.returncode != 0):
                            # there was some error, try to explain
                            app.execution.info = (
                                "Execution failed on resource: %s" %
                                app.execution.resource_name)
                            signal = app.execution.signal
                            if signal in Run.Signals:
                                app.execution.info = (
                                    "Abnormal termination: %s" % signal)
                            else:
                                if os.WIFSIGNALED(app.execution.returncode):
                                    app.execution.info = (
                                        "Remote job terminated by signal %d" %
                                        signal)
                                else:
                                    app.execution.info = (
                                        "Remote job exited with code %d" %
                                        app.execution.exitcode)

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

            except gc3libs.exceptions.InvalidResourceName as irn:
                # could be the corresponding LRMS has been removed
                # because of an unrecoverable error mark application
                # as state UNKNOWN
                gc3libs.log.warning(
                    "Failed while retrieving resource %s from core.Detailed"
                    " Error message: %s" %
                    (app.execution.resource_name, str(irn)))
                continue

            # XXX: Re-enabled the catch-all clause otherwise the loop stops at
            # the first erroneous iteration
            except Exception as ex:
                if gc3libs.error_ignored(
                        # context:
                        # - module
                        'core',
                        # - class
                        'Core',
                        # - method
                        'update_job_state',
                        # - actual error class
                        ex.__class__.__name__,
                        # - additional keywords
                        'update',
                ):
                    gc3libs.log.warning(
                        "Ignored error in Core.update_job_state(): %s", ex)
                    # print again with traceback at a higher log level
                    gc3libs.log.debug(
                        "(Original traceback follows.)", exc_info=True)
                    continue
                else:
                    # propagate generic exceptions for debugging purposes
                    raise

    def __update_task(self, tasks, **extra_args):
        """Implementation of `update_job_state` on generic `Task` objects."""
        for task in tasks:
            assert isinstance(
                task, Task), "Core.update_job_state: passed an argument" \
                " which is not a `Task` instance."
            task.update_state()

    def fetch_output(self, app, download_dir=None,
                     overwrite=False, changed_only=True, **extra_args):
        """
        Retrieve output into local directory `app.output_dir`.

        If the task is not expected to produce any output (i.e.,
        `app.would_output == False`) then the only effect of this is
        to advance the state of ``TERMINATING`` tasks to
        ``TERMINATED``.

        Optional argument `download_dir` overrides the download location.

        The download directory is created if it does not exist.  If it
        already exists, and the optional argument `overwrite` is
        ``False`` (default), it is renamed with a `.NUMBER` suffix and
        a new empty one is created in its place.  Otherwise, if
        'overwrite` is ``True``, files are downloaded over the ones
        already present; in this case, the `changed_only` argument
        controls which files are overwritten:

        - if `changed_only` is ``True`` (default), then only files for
          which the source has a different size or has been modified
          more recently than the destination are copied;

        - if `changed_only` is ``False``, then *all* files in `source`
          will be copied into `destination`, unconditionally.

        Source files that do not exist at `destination` will be
        copied, independently of the `overwrite` and `changed_only`
        settings.

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
        assert isinstance(
            app, Task), "Core.fetch_output: passed an `app` argument " \
            "which is not a `Task` instance."
        if isinstance(app, Application):
            self.__fetch_output_application(
                app, download_dir, overwrite, changed_only, **extra_args)
        else:
            # generic `Task` object
            self.__fetch_output_task(
                app, download_dir, overwrite, changed_only, **extra_args)

    def __fetch_output_application(
            self, app, download_dir, overwrite, changed_only, **extra_args):
        """Implementation of `fetch_output` on `Application` objects."""
        job = app.execution
        if job.state in [Run.State.NEW, Run.State.SUBMITTED]:
            raise gc3libs.exceptions.OutputNotAvailableError(
                "Output not available: '%s' currently in state '%s'"
                % (app, app.execution.state))

        # auto_enable_auth = extra_args.get(
        #     'auto_enable_auth', self.auto_enable_auth)

        # determine download dir
        download_dir = app._get_download_dir(download_dir)

        if download_dir is not None:
            # Prepare/Clean download dir
            try:
                if overwrite:
                    if not os.path.exists(download_dir):
                        os.makedirs(download_dir)
                else:
                    utils.mkdir_with_backup(download_dir)
            except Exception as ex:
                gc3libs.log.error(
                    "Failed creating download directory '%s': %s: %s",
                    download_dir,
                    ex.__class__.__name__,
                    str(ex))
                raise

            # download job output
            try:
                lrms = self.get_backend(job.resource_name)
                lrms.get_results(app, download_dir, overwrite, changed_only)
                # clear previous data staging errors
                if job.signal == Run.Signals.DataStagingFailure:
                    job.signal = 0
            except gc3libs.exceptions.InvalidResourceName as ex:
                gc3libs.log.warning(
                    "No such resource '%s': %s"
                    % (app.execution.resource_name, str(ex)))
                ex = app.fetch_output_error(ex)
                if isinstance(ex, Exception):
                    job.info = ("No output could be retrieved: %s" % str(ex))
                    raise ex
                else:
                    return
            except gc3libs.exceptions.RecoverableDataStagingError as rex:
                job.info = ("Temporary failure when retrieving results: %s."
                            " Ignoring error, try again." % str(rex))
                return
            except gc3libs.exceptions.UnrecoverableDataStagingError as ex:
                job.signal = Run.Signals.DataStagingFailure
                ex = app.fetch_output_error(ex)
                if isinstance(ex, Exception):
                    job.info = ("No output could be retrieved: %s" % str(ex))
                    raise ex
            except Exception as ex:
                ex = app.fetch_output_error(ex)
                if isinstance(ex, Exception):
                    raise ex

            # successfully downloaded results
            gc3libs.log.debug("Downloaded output of '%s' (which is in state %s)"
                              % (str(app), job.state))

            app.output_dir = os.path.abspath(download_dir)
            app.changed = True

            if job.state == Run.State.TERMINATING:
                gc3libs.log.debug("Final output of '%s' retrieved" % str(app))

        return Task.fetch_output(app, download_dir)

    def __fetch_output_task(
            self, task, download_dir, overwrite, changed_only, **extra_args):
        """Implementation of `fetch_output` on generic `Task` objects."""
        return task.fetch_output(
            download_dir, overwrite, changed_only, **extra_args)

    def get_resources(self, **extra_args):
        """
        Return list of resources configured into this `Core` instance.
        """
        return [lrms for lrms in self.resources.itervalues()]

    def kill(self, app, **extra_args):
        """
        Terminate a job.

        Terminating a job in RUNNING, SUBMITTED, or STOPPED state
        entails canceling the job with the remote execution system;
        terminating a job in the NEW or TERMINATED state is a no-op.
        """
        assert isinstance(
            app, Task), "Core.kill: passed an `app` argument which is not"\
            " a `Task` instance."
        if isinstance(app, Application):
            self.__kill_application(app, **extra_args)
        else:
            self.__kill_task(app, **extra_args)

    def __kill_application(self, app, **extra_args):
        """Implementation of `kill` on `Application` objects."""
        job = app.execution
        # auto_enable_auth = extra_args.get(
        #     'auto_enable_auth', self.auto_enable_auth)
        try:
            lrms = self.get_backend(job.resource_name)
            lrms.cancel_job(app)
        except AttributeError:
            # A job in state NEW does not have a `resource_name`
            # attribute.
            if job.state != Run.State.NEW:
                raise
        except gc3libs.exceptions.InvalidResourceName as irn:
            gc3libs.log.warning(
                "Failed while retrieving resource %s from core.Detailed"
                " Error message: %s" %
                (app.execution.resource_name, str(irn)))
        gc3libs.log.debug(
            "Setting task '%s' status to TERMINATED"
            " and returncode to SIGCANCEL", app)
        app.changed = True
        # setting the state runs the state-transition handlers,
        # which may raise an error -- ignore them, but log nonetheless
        try:
            job.state = Run.State.TERMINATED
        except Exception as ex:
            if gc3libs.error_ignored(
                    # context:
                    # - module
                    'core',
                    # - class
                    'Core',
                    # - method
                    'kill',
                    # - actual error class
                    ex.__class__.__name__,
                    # - additional keywords
                    'state',
                    job.state,
                    'TERMINATED',
            ):
                gc3libs.log.info("Ignoring error in state transition"
                                 " since task is being killed: %s", ex)
            else:
                # propagate exception to caller
                raise
        job.signal = Run.Signals.Cancelled
        job.history.append("Cancelled")

    def __kill_task(self, task, **extra_args):
        extra_args.setdefault('auto_enable_auth', self.auto_enable_auth)
        task.kill(**extra_args)

    def peek(self, app, what='stdout', offset=0, size=None, **extra_args):
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
        assert isinstance(
            app, Task), "Core.peek: passed an `app` argument which is" \
            " not a `Task` instance."
        if isinstance(app, Application):
            return self.__peek_application(
                app, what, offset, size, **extra_args)
        else:
            return self.__peek_task(app, what, offset, size, **extra_args)

    def __peek_application(self, app, what, offset, size, **extra_args):
        """Implementation of `peek` on `Application` objects."""
        if what == 'stdout':
            remote_filename = app.stdout
        elif what == 'stderr':
            remote_filename = app.stderr
        else:
            raise gc3libs.exceptions.Error(
                "File name requested to `Core.peek` must be"
                " 'stdout' or 'stderr', not '%s'" % what)

        # Check if local data available
        job = app.execution
        if job.state == Run.State.TERMINATED:
            # FIXME: local data could be stale!!
            filename = os.path.join(app.output_dir, remote_filename)
            local_file = open(filename, 'r')
        else:
            # Get authN
            # auto_enable_auth = extra_args.get(
            #     'auto_enable_auth', self.auto_enable_auth)
            lrms = self.get_backend(job.resource_name)
            local_file = tempfile.NamedTemporaryFile(
                suffix='.tmp', prefix='gc3libs.')
            lrms.peek(app, remote_filename, local_file, offset, size)
            local_file.flush()
            local_file.seek(0)

        return local_file

    def __peek_task(self, task, what, offset, size, **extra_args):
        """Implementation of `peek` on generic `Task` objects."""
        return task.peek(what, offset, size, **extra_args)

    def update_resources(self, **extra_args):
        """
        Update the state of resources configured into this `Core` instance.

        Each resource object in the returned list will have its `updated`
        attribute set to `True` if the update operation succeeded, or `False`
        if it failed.
        """
        for lrms in self.resources.itervalues():
            try:
                if not lrms.enabled:
                    continue
                # auto_enable_auth = extra_args.get(
                #     'auto_enable_auth', self.auto_enable_auth)
                resource = lrms.get_resource_status()
                resource.updated = True
            except Exception as ex:
                gc3libs.log.error("Got error while updating resource '%s': %s."
                                  % (lrms.name, str(ex)))
                lrms.updated = False

    def close(self):
        """
        Used to invoke explicitly the destructor on objects
        e.g. LRMS
        """
        for lrms in self.resources.itervalues():
            lrms.close()

    # compatibility with the `Engine` interface

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


class Scheduler(object):

    """
    Instances of the `Scheduler` class are used in
    `Engine.progress`:meth: to determine what tasks (among those in
    `Run.State.NEW` state) are to be submitted.

    A `Scheduler` object must implement *both* the context_ protocol
    *and* the iterator_ protocol.

    .. _context:  http://goo.gl/SvWWyw
    .. _iterator: http://goo.gl/ue2zje

    The way a `Scheduler` instance is actually used within `Engine` is
    as follows:

    0. A `Scheduler` instance is created, passing it two arguments: a
       list of tasks in ``NEW`` state, and a dictionary of configured
       resources (keys are resource names, values are actual resource
       objects).

    1. When a new submission cycle starts, the `__enter__`:meth:
       method is called.

    2. The `Engine` iterates by repeatedly calling the `next`:meth:
       method to receive tasks to be submitted.  The `send`:meth: and
       `throw`:meth: methods are used to notify the scheduler of the
       outcome of the submission attempt.

    3. When the submission cycle ends, the `__exit__`:meth: method is called.

    The `Scheduler.schedule` generator is the heart of the submission
    process and has basically complete control over it.  It is
    initialized with the list of tasks in ``NEW`` state, and the list
    of configured resources.  The `next`:meth: method should yield
    pairs *(task index, resource name)*, where the *task index* is the
    position of the task to be submitted next in the given list, and
    --similarly-- the *resource name* is the name of the resource to
    which the task should be submitted.

    For each pair yielded, submission of that task to the selected
    resource is attempted; the state of the task object after
    submission is sent back (via the `send`:meth: method) to the
    `Scheduler` instance; if an exception is raised, that exception is
    thrown (via the `throw`:meth: method) into the scheduler object
    instead.  Submission stops when the `next()` call raises a
    `StopIteration` exception.
    """

    def __init__(self, tasks, resources):
        self.tasks = tasks
        self.resources = resources

    def __enter__(self):
        """Called at the start of a scheduling cycle.

        Implementation of this method should follow Python's context_
        protocol; in particular, this method must return a reference
        to a valid context object.

        By default, just returns a reference to ``self``.

        """
        return self

    def next(self):
        raise NotImplemented(
            "Method `next` of class `%s` has not been implemented."
            % self.__class__.name)

    def send(self, result):
        raise NotImplemented(
            "Method `send` of class `%s` has not been implemented."
            % self.__class__.name)

    def throw(self, *excinfo):
        raise NotImplemented(
            "Method `throw` of class `%s` has not been implemented."
            % self.__class__.name)

    def __exit__(self, *excinfo):
        """Called at the end of a scheduling cycle, when no more submissions
        will be performed by the `Engine`.

        Implementation of this method should follow Python's context_ protocol.

        By default, does nothing.

        :param tuple excinfo:
          This is either:

            - a triple *(exception class, exception value, traceback)*
              like the one returned from the standard `sys.exc_info`
              function, when an unhandled error occurred during the
              submission cycle, or
            - the empty tuple if execution of the submission cycle
              terminated normally without exceptions.

        """
        pass


class scheduler(object):

    """
    Decorate a generator function for use as a `Scheduler`:class: object.
    """
    __slots__ = ['_fn', '_gen']

    def __init__(self, fn):
        self._fn = fn

    def __call__(self, *args, **kwargs):
        self._gen = self._fn(*args, **kwargs)
        return self

    # proxy generator protocol methods

    def __iter__(self):
        return self

    def next(self):
        return self._gen.next()

    def send(self, value):
        return self._gen.send(value)

    def throw(self, *excinfo):
        return self._gen.throw(*excinfo)

    def close(self):
        self._gen.close()

    # add context protocol methods

    def __enter__(self):
        return self

    def __exit__(self, *excinfo):
        self.close()


@scheduler
def first_come_first_serve(tasks, resources, matchmaker=MatchMaker()):
    """First-come first-serve scheduling policy.

    Tasks are submitted to resources in the order they appear in the
    `tasks` list.  Each task is submitted to resources according to
    the order they are sorted by `Application.rank_resources` (if that
    method exists).

    This is the default scheduling policy in GC3Pie's `Engine`:class.

    """
    for task_idx, task in enumerate(tasks):
        # keep only compatible resources
        compatible_resources = matchmaker.filter(task, resources)
        if not compatible_resources:
            gc3libs.log.warning(
                "No compatible resources for task '%s'"
                " - cannot submit it" % task)
            continue
        # sort them according to the Task's preference
        targets = matchmaker.rank(task, compatible_resources)
        # now try submission of the task to each resource until one succeeds
        for target in targets:
            try:
                # result = yield (task_idx, target.name)
                yield (task_idx, target.name)
            except gc3libs.exceptions.LRMSSkipSubmissionToNextIteration:
                # this is not a real error: the resource is adapting
                # for the task and will actually accept it sometime in
                # the future, so continue with next task
                break
            except Exception as err:
                # note error condition but continue with next resource
                gc3libs.log.debug(
                    "Scheduler ignored error in submitting task '%s': %s: %s",
                    task, err.__class__.__name__, str(err), exc_info=True)
            else:
                # submission successful, continue with next task
                break


# Work around infinite recursion error when trying to compare
# `UserDict` instances which can contain each other.  We know
# that two identical tasks are the same object by
# construction, so let's use this to check.
def _contained(elt, lst):
    i = id(elt)
    for item in lst:
        if i == id(item):
            return True
    return False


class Engine(object):

    """
    Submit tasks in a collection, and update their state until a
    terminal state is reached. Specifically:

      * tasks in `NEW` state are submitted;

      * the state of tasks in `SUBMITTED`, `RUNNING` or `STOPPED` state
        is updated;

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

      `scheduler`
        A factory function for creating objects that conform to the
        `Scheduler` interface to control task submission; see the
        `Scheduler`:class: documentation for details.  The default
        value implements a first-come first-serve algorithm: tasks are
        submitted in the order they have been added to the `Engine`.

      `retrieve_running`
        If ``True``, snapshot output from RUNNING jobs at every
        invocation of `progress`:meth:

      `retrieve_overwrites`
        If ``True``, overwrite files in the output directory of any
        job (as opposed to moving destination away and downloading a
        fresh copy). See `Core.fetch_output`:meth: for details.

      `retrieve_changed_only`
        If both this and `overwrite` are ``True``, then only changed
        files are downloaded. See `Core.fetch_output`:meth: for
        details.

    Any of the above can also be set by passing a keyword argument to
    the constructor (assume ``g`` is a `Core`:class: instance)::

      | >>> e = Engine(g, can_submit=False)
      | >>> e.can_submit
      | False
    """

    def __init__(self, controller, tasks=list(), store=None,
                 can_submit=True, can_retrieve=True,
                 max_in_flight=0, max_submitted=0,
                 output_dir=None, fetch_output_overwrites=False,
                 scheduler=first_come_first_serve,
                 retrieve_running=False,
                 retrieve_overwrites=False,
                 retrieve_changed_only=True):
        """
        Create a new `Engine` instance.  Arguments are as follows:

        :param controller:
          A `gc3libs.Core` instance, that will be used to operate on
          tasks.  This is the only required argument.

        :param list apps:
          Initial list of tasks to be managed by this Engine.  Tasks can
          be later added and removed with the `add` and `remove`
          methods (which see).  Defaults to the empty list.

        :param store:
          An instance of `gc3libs.persistence.Store`, or `None`.  If
          not `None`, it will be used to persist tasks after each
          iteration; by default no store is used so no task state is
          persisted.

        :param can_submit:
        :param can_retrieve:
        :param max_in_flight:
        :param max_submitted:
        :param output_dir:
        :param fetch_output_overwrites:
        :param scheduler:
        :param bool retrieve_running:
        :param bool retrieve_overwrites:
        :param bool retrieve_changed_only:
          Optional keyword arguments; see `Engine`:class: for a description.

        """
        # internal-use attributes
        self._new = []
        self._in_flight = []
        self._stopped = []
        self._terminating = []
        self._terminated = []
        self._to_kill = []
        self._core = controller
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
        self.scheduler = scheduler
        self.retrieve_running = retrieve_running
        self.retrieve_overwrites = retrieve_overwrites
        self.retrieve_changed_only = retrieve_changed_only

    def add(self, task):
        """
        Add `task` to the list of tasks managed by this Engine.
        Adding a task that has already been added to this `Engine`
        instance results in a no-op.
        """
        state = task.execution.state
        if Run.State.NEW == state:
            queue = self._new
        elif state in [Run.State.SUBMITTED,
                       Run.State.RUNNING,
                       Run.State.UNKNOWN]:
            queue = self._in_flight
        elif Run.State.STOPPED == state:
            queue = self._stopped
        elif Run.State.TERMINATING == state:
            queue = self._terminating
        elif Run.State.TERMINATED == state:
            queue = self._terminated
        else:
            raise AssertionError(
                "Unhandled state '%s' in gc3libs.core.Engine." % state)
        if not _contained(task, queue):
            queue.append(task)
            task.attach(self)

    def remove(self, task):
        """Remove a `task` from the list of tasks managed by this Engine."""
        state = task.execution.state
        if Run.State.NEW == state:
            self._new.remove(task)
        elif (Run.State.SUBMITTED == state or
                Run.State.RUNNING == state or
                Run.State.UNKNOWN == state):
            self._in_flight.remove(task)
        elif Run.State.STOPPED == state:
            self._stopped.remove(task)
        elif Run.State.TERMINATING == state:
            self._terminating.remove(task)
        elif Run.State.TERMINATED == state:
            self._terminated.remove(task)
        else:
            raise AssertionError(
                "Unhandled state '%s' in gc3libs.core.Engine." % state)
        task.detach()

    def progress(self):
        """
        Update state of all registered tasks and take appropriate action.
        Specifically:

          * tasks in `NEW` state are submitted;

          * the state of tasks in `SUBMITTED`, `RUNNING`, `STOPPED` or
            `UNKNOWN` state is updated;

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
                # elif state == Run.State.RUNNING or state ==
                # Run.State.UNKNOWN:
                elif state == Run.State.RUNNING:
                    if isinstance(task, Application):
                        currently_in_flight += 1
                    if self.can_retrieve and self.retrieve_running:
                        # try to get output
                        try:
                            self._core.fetch_output(
                                task,
                                overwrite=self.retrieve_overwrites,
                                changed_only=self.retrieve_changed_only)
                        except Exception as err:
                            if gc3libs.error_ignored(
                                    # context:
                                    # - module
                                    'core',
                                    # - class
                                    'Engine',
                                    # - method
                                    'progress',
                                    # - actual error class
                                    err.__class__.__name__,
                                    # - additional keywords
                                    'RUNNING',
                                    'fetch_output',
                            ):
                                gc3libs.log.error(
                                    "Ignored error in fetching output of"
                                    " RUNNING task '%s': %s: %s",
                                    task, err.__class__.__name__, err)
                                gc3libs.log.debug(
                                    "(Original traceback follows.)",
                                    exc_info=True)
                            else:
                                # propagate exceptions for debugging purposes
                                raise
                elif state == Run.State.STOPPED:
                    # task changed state, mark as to remove
                    transitioned.append(index)
                    self._stopped.append(task)
                elif state == Run.State.TERMINATING:
                    # task changed state, mark as to remove
                    transitioned.append(index)
                    self._terminating.append(task)
                elif state == Run.State.TERMINATED:
                    # task changed state, mark as to remove
                    transitioned.append(index)
                    self._terminated.append(task)
            except gc3libs.exceptions.ConfigurationError:
                # Unrecoverable; no sense in continuing -- pass
                # immediately on to client code and let it handle
                # this...
                raise
            except Exception as err:
                if gc3libs.error_ignored(
                        # context:
                        # - module
                        'core',
                        # - class
                        'Engine',
                        # - method
                        'progress',
                        # - actual error class
                        err.__class__.__name__,
                        # - additional keywords
                        'state',
                        'update',
                ):
                    gc3libs.log.error(
                        "Ignoring error in updating state of task '%s':"
                        " %s: %s",
                        task,
                        err.__class__.__name__,
                        err,
                        exc_info=True)
                else:
                    # propagate exception to caller
                    raise
        # remove tasks that transitioned to other states
        for index in reversed(transitioned):
            del self._in_flight[index]

        # execute kills and update count of submitted/in-flight tasks
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
            except Exception as err:
                if gc3libs.error_ignored(
                        # context:
                        # - module
                        'core',
                        # - class
                        'Engine',
                        # - method
                        'progress',
                        # - actual error class
                        err.__class__.__name__,
                        # - additional keywords
                        'kill'
                ):
                    gc3libs.log.error(
                        "Ignored error in killing task '%s': %s: %s",
                        task, err.__class__.__name__, err)
                    # print again with traceback info at a higher log level
                    gc3libs.log.debug(
                        "(Original traceback follows.)",
                        exc_info=True)
                else:
                    # propagate exceptions for debugging purposes
                    raise
        # remove tasks that transitioned to other states
        for index in reversed(transitioned):
            del self._to_kill[index]

        # update state of STOPPED tasks; again need to make before new
        # submissions, because it can alter the count of in-flight
        # tasks.
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
                    # task changed state, mark as to remove
                    transitioned.append(index)
                elif state == Run.State.TERMINATING:
                    self._terminating.append(task)
                    # task changed state, mark as to remove
                    transitioned.append(index)
                elif state == Run.State.TERMINATED:
                    self._terminated.append(task)
                    # task changed state, mark as to remove
                    transitioned.append(index)
            except Exception as err:
                if gc3libs.error_ignored(
                        # context:
                        # - module
                        'core',
                        # - class
                        'Engine',
                        # - method
                        'progress',
                        # - actual error class
                        err.__class__.__name__,
                        # - additional keywords
                        'state',
                        'update',
                        'STOPPED',
                ):
                    gc3libs.log.error(
                        "Ignoring error in updating state of"
                        " STOPPED task '%s': %s: %s",
                        task, err.__class__.__name__, err,
                        exc_info=True)
                else:
                    # propagate exception to caller
                    raise
        # remove tasks that transitioned to other states
        for index in reversed(transitioned):
            del self._stopped[index]

        # now try to submit NEW tasks
        # gc3libs.log.debug("Engine.progress: submitting new tasks [%s]"
        #                  % str.join(', ', [str(task) for task in self._new]))
        transitioned = []
        if (self.can_submit and
                currently_submitted < limit_submitted and
                currently_in_flight < limit_in_flight):
            with self.scheduler(self._new,
                                self._core.resources.values()) as _sched:
                # wrap the original generator object so that `send`
                # and `throw` do not yield a value -- we only get new
                # stuff from the call to the `next` method in the `for
                # ... in schedule` line.
                sched = gc3libs.utils.YieldAtNext(_sched)
                for task_index, resource_name in sched:
                    task = self._new[task_index]
                    resource = self._core.resources[resource_name]
                    # try to submit; go to SUBMITTED if successful, FAILED if
                    # not
                    try:
                        self._core.submit(task, targets=[resource])
                        if self._store:
                            self._store.save(task)
                        # XXX: can remove the following assert when
                        # we're sure Issue 419 is fixed
                        assert task_index not in transitioned
                        self._in_flight.append(task)
                        transitioned.append(task_index)
                        if isinstance(task, Application):
                            currently_submitted += 1
                            currently_in_flight += 1

                        sched.send(task.execution.state)
                    except Exception as err1:
                        # record the error in the task's history
                        task.execution.history(
                            "Submission to resource '%s' failed: %s: %s" %
                            (resource.name,
                             err1.__class__.__name__,
                             str(err1)))
                        gc3libs.log.error(
                            "Got error in submitting task '%s', informing"
                            " scheduler: %s: %s",
                            task,
                            err1.__class__.__name__,
                            str(err1))
                        # inform scheduler and let it handle it
                        try:
                            sched.throw(* sys.exc_info())
                        except Exception as err2:
                            if gc3libs.error_ignored(
                                    # context:
                                    # - module
                                    'core',
                                    # - class
                                    'Engine',
                                    # - method
                                    'progress',
                                    # - actual error class
                                    err2.__class__.__name__,
                                    # - additional keywords
                                    'scheduler',
                                    'submit',
                            ):
                                gc3libs.log.debug(
                                    "Ignored error in submitting task '%s':"
                                    " %s: %s",
                                    task,
                                    err2.__class__.__name__,
                                    err2,
                                    exc_info=True)
                            else:
                                # propagate exceptions for debugging purposes
                                raise
                    # enforce Engine limits
                    if (currently_submitted >= limit_submitted
                            or currently_in_flight >= limit_in_flight):
                        break
        # remove tasks that transitioned to SUBMITTED state
        for index in reversed(transitioned):
            del self._new[index]

        # finally, retrieve output of finished tasks
        if self.can_retrieve:
            transitioned = []
            for index, task in enumerate(self._terminating):
                # try to get output
                try:
                    self._core.fetch_output(
                        task,
                        overwrite=self.retrieve_overwrites,
                        changed_only=self.retrieve_changed_only)
                except gc3libs.exceptions.UnrecoverableDataStagingError as ex:
                    gc3libs.log.error(
                        "Error in fetching output of task '%s',"
                        " will mark it as TERMINATED"
                        " (with error exit code %d): %s: %s",
                        task, posix.EX_IOERR,
                        ex.__class__.__name__, str(ex), exc_info=True)
                    task.execution.returncode = (
                        Run.Signals.DataStagingFailure,
                        posix.EX_IOERR)
                    task.execution.state = Run.State.TERMINATED
                    task.changed = True
                except Exception as ex:
                    if gc3libs.error_ignored(
                            # context:
                            # - module
                            'core',
                            # - class
                            'Engine',
                            # - method
                            'progress',
                            # - actual error class
                            ex.__class__.__name__,
                            # - additional keywords
                            'fetch_output',
                    ):
                        gc3libs.log.error(
                            "Ignored error in fetching output of task '%s':"
                            " %s: %s",
                            task,
                            ex.__class__.__name__,
                            ex)
                        gc3libs.log.debug(
                            "(Original traceback follows.)",
                            exc_info=True)
                    else:
                        # propagate exceptions for debugging purposes
                        raise

            for index, task in enumerate(self._terminating):
                try:
                    if task.execution.state == Run.State.TERMINATED:
                        self._terminated.append(task)
                        transitioned.append(index)
                        self._core.free(task)
                except Exception as err:
                    gc3libs.log.error(
                        "Got error freeing up resources used by task '%s': %s: %s."
                        " (For cloud-based resources, it's possible that the VM"
                        " has been destroyed already.)",
                        task, err.__class__.__name__, err)

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
        whose class is not contained in `only` are ignored.
        : param tuple only: Restrict counting to tasks of these classes.
        """
        if only:
            gc3libs.log.debug(
                "Engine.stats: Restricting to object of class '%s'",
                only.__name__)
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
        for task in self._stopped:
            if only and not isinstance(task, only):
                continue
            state = task.execution.state
            result[state] += 1
        for task in self._to_kill:
            if only and not isinstance(task, only):
                continue
            # XXX: presumes no task in the `_to_kill` list is TERMINATED
            state = task.execution.state
            result[state] += 1
        if only:
            _terminating = [task for task in self._terminating
                            if isinstance(task, only)]
            result[Run.State.TERMINATING] += len(_terminating)
        else:
            result[Run.State.TERMINATING] += len(self._terminating)
        if only:
            _terminated = [task for task in self._terminated
                           if isinstance(task, only)]
            result[Run.State.TERMINATED] += len(_terminated)
        else:
            result[Run.State.TERMINATED] += len(self._terminated)

        # for TERMINATED tasks, compute the number of successes/failures
        for task in self._terminated:
            if only and not isinstance(task, only):
                continue
            if task.execution.returncode == 0:
                result['ok'] += 1
            else:
                result['failed'] += 1
        result['total'] = (result[Run.State.NEW]
                           + result[Run.State.SUBMITTED]
                           + result[Run.State.RUNNING]
                           + result[Run.State.STOPPED]
                           + result[Run.State.TERMINATING]
                           + result[Run.State.TERMINATED]
                           + result[Run.State.UNKNOWN])
        return result

    # implement a Core-like interface, so `Engine` objects can be used
    # as substitutes for `Core`.

    def free(self, task, **extra_args):
        """
        Proxy for `Core.free`, which see.
        """
        self._core.free(task)

    def submit(self, task, resubmit=False, targets=None, **extra_args):
        """
        Submit `task` at the next invocation of `progress`.

        The `task` state is reset to ``NEW`` and then added to the
        collection of managed tasks.

        The `targets` argument is only present for interface
        compatiblity with `Core.submit`:meth: but is otherwise
        ignored.

        """
        if resubmit:
            task.execution.state = Run.State.NEW
        return self.add(task)

    def update_job_state(self, *tasks, **extra_args):
        """
        Return list of *current* states of the given tasks.  States
        will only be updated at the next invocation of `progress`; in
        particular, no state-change handlers are called as a result of
        calling this method.
        """
        pass

    def fetch_output(self, task, output_dir=None,
                     overwrite=False, changed_only=True, **extra_args):
        """
        Enqueue task for later output retrieval.

        .. warning:: FIXME

          The `output_dir`, `overwrite`, and `changed_only` parameters
          are currently ignored.
        """
        self.add(task)

    def kill(self, task, **extra_args):
        """
        Schedule a task for killing on the next `progress` run.
        """
        self._to_kill.append(task)

    def peek(self, task, what='stdout', offset=0, size=None, **extra_args):
        """
        Proxy for `Core.peek` (which see).
        """
        return self._core.peek(task, what, offset, size, **extra_args)

    def close(self):
        """
        Call explicilty finalize methods on relevant objects
        e.g. LRMS
        """
        self._core.close()

    # Wrapper methods around `Core` to access the backends directly
    # from the `Engine`.

    @utils.same_docstring_as(Core.select_resource)
    def select_resource(self, match):
        return self._core.select_resource(match)

    @utils.same_docstring_as(Core.get_resources)
    def get_resources(self):
        return self._core.get_resources()

    @utils.same_docstring_as(Core.get_backend)
    def get_backend(self, name):
        return self._core.get_backend(name)
