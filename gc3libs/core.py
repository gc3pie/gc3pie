#!/usr/bin/env python

"""
Top-level classes for task execution and control.
"""
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 2110-1301 USA


from __future__ import absolute_import, print_function, unicode_literals
from builtins import next
from builtins import filter
from builtins import str
from builtins import range
from builtins import object
from collections import defaultdict, deque
from fnmatch import fnmatch
import functools
import itertools
import os
import posix
import sys
import time
import tempfile
from warnings import warn

from dictproxyhack import dictproxy

import gc3libs
from gc3libs import Application, Run, Task
from gc3libs.events import TaskStateChange, TermStatusChange
import gc3libs.exceptions
from gc3libs.quantity import Duration
import gc3libs.utils as utils


__docformat__ = 'reStructuredText'


class MatchMaker(object):
    """
    Select and sort resources for attempting submission of a `Task`.

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

    # pylint: disable=no-self-use
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
            ','.join(r.name for r in resources), task)
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

    # pylint: disable=no-self-use
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


class Core(object):
    """
    Core operations: submit, update state, retrieve (a snapshot of) output,
    cancel job.

    Core operations are *blocking*, i.e., they return only after the
    operation has successfully completed, or an error has been detected.

    Operations are always performed by a `Core` object.  `Core` implements
    an overlay Grid on the resources specified in the configuration file.

    Initialization of a `Core`:class: instance also initializes all
    resources in the passed `Configuration`:class: instance.  By default,
    GC3Pie's `Core` objects will ignore errors in initializing resources,
    and only raise an exception if *no* resources can be initialized.
    This can be changed by either passing an optional argument
    ``resource_errors_are_fatal=True``, or by setting the environmental
    variable ``GC3PIE_RESOURCE_INIT_ERRORS_ARE_FATAL`` to ``yes`` or ``1``.
    """

    def __init__(self, cfg, matchmaker=MatchMaker(),
                 resource_errors_are_fatal=None):
        # propagate resource init errors?
        if resource_errors_are_fatal is None:
            # get result from the environment
            resource_errors_are_fatal = gc3libs.utils.string_to_boolean(
                os.environ.get('GC3PIE_RESOURCE_INIT_ERRORS_ARE_FATAL', 'no'))

        # init auths
        self.auto_enable_auth = cfg.auto_enable_auth

        # init backends
        self.resources = cfg.make_resources(
            ignore_errors=(not resource_errors_are_fatal))
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
                "No configured resource by the name '%s'"
                % (name,))

    def select_resource(self, match):
        """
        Disable resources that *do not* satisfy predicate `match`.
        Return number of enabled resources.

        Argument `match` can be:

          - either a function (or a generic callable) that is passed
            each `Resource` object in turn, and should return a
            boolean indicating whether the resources should be kept
            (`True`) or not (`False`);

          - or it can be a string: only resources whose name matches
            (wildcards ``*`` and ``?`` are allowed) are retained.

        .. note::

          Calling this method modifies the configured list of
          resources in-place.
        """
        enabled = 0
        for lrms in self.resources.values():
            try:
                if not match(lrms):
                    lrms.enabled = False
            # we expect `TypeError: 'str' object is not callable` in case
            # argument `match` is a string
            except TypeError:
                if not fnmatch(lrms.name, match):
                    lrms.enabled = False
            if lrms.enabled:
                enabled += 1
        return enabled

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

    # pylint: disable=unused-argument
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

    # pylint: disable=unused-argument
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
        :param targets:
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

        gc3libs.log.debug("Submitting %s ...", app)

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
                r for r in self.resources.values() if r.enabled]
            if len(enabled_resources) == 0:
                raise gc3libs.exceptions.NoResources(
                    "Could not initialize any computational resource"
                    " - please check log and configuration file.")

            # decide which resource to use
            compatible_resources = self.matchmaker.filter(
                app, enabled_resources)
            if len(compatible_resources) == 0:
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
                self.update_resources(compatible_resources)
                updated_resources = [r for r in compatible_resources if r.updated]
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
            gc3libs.log.debug("Attempting submission to resource '%s'...",
                              resource.name)
            try:
                job.timestamp[Run.State.NEW] = time.time()
                job.info = ("Submitting to '%s'" % (resource.name,))
                resource.submit_job(app)
            except gc3libs.exceptions.LRMSSkipSubmissionToNextIteration as ex:
                gc3libs.log.info("Submission of job %s delayed", app)
                # Just raise the exception
                raise
            # pylint: disable=broad-except
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
        task.submit(resubmit, targets, **extra_args)

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
                if state not in [
                        Run.State.NEW,
                        Run.State.TERMINATING,
                        Run.State.TERMINATED,
                ]:
                    lrms = self.get_backend(app.execution.resource_name)
                    try:
                        state = lrms.update_job_state(app)
                    # pylint: disable=broad-except
                    except Exception as ex:
                        gc3libs.log.debug(
                            "Error getting status of application '%s': %s: %s",
                            app, ex.__class__.__name__, ex, exc_info=True)
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

            except gc3libs.exceptions.InvalidResourceName:
                # could be the corresponding LRMS has been removed
                # because of an unrecoverable error mark application
                # as state UNKNOWN
                gc3libs.log.warning(
                    "Cannot access computational resource '%s',"
                    " marking task '%s' as UNKNOWN.",
                    app.execution.resource_name, app)
                app.execution.state = Run.State.TERMINATED
                app.changed = True
                continue

            # This catch-all clause is needed otherwise the loop stops
            # at the first erroneous iteration
            #
            # pylint: disable=broad-except
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

    # pylint: disable=no-self-use
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

        # determine download directory
        #
        # pylint: disable=protected-access
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
            except gc3libs.exceptions.InvalidResourceName as err:
                ex = app.fetch_output_error(err)
                if isinstance(ex, Exception):
                    job.info = ("No output could be retrieved: %s" % (ex,))
                    raise ex
                else:
                    return
            except gc3libs.exceptions.RecoverableDataStagingError as rex:
                job.info = ("Temporary failure when retrieving results: %s."
                            " Ignoring error, try again." % str(rex))
                return
            except gc3libs.exceptions.UnrecoverableDataStagingError as ex:
                # pylint: disable=redefined-variable-type
                job.signal = Run.Signals.DataStagingFailure
                ex = app.fetch_output_error(ex)
                if isinstance(ex, Exception):
                    job.info = ("No output could be retrieved: %s" % str(ex))
                    raise ex
            # pylint: disable=broad-except
            except Exception as ex:
                ex = app.fetch_output_error(ex)
                if isinstance(ex, Exception):
                    raise ex

            # successfully downloaded results
            gc3libs.log.debug(
                "Downloaded output of '%s' (which is in state %s)",
                app, job.state)

            app.output_dir = os.path.abspath(download_dir)
            app.changed = True

            if job.state == Run.State.TERMINATING:
                gc3libs.log.debug("Final output of '%s' retrieved", app)

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
        return [lrms for lrms in self.resources.values()]

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
        except gc3libs.exceptions.InvalidResourceName:
            gc3libs.log.warning(
                "Cannot access computational resource '%s',"
                " but marking task '%s' as TERMINATED anyway.",
                app.execution.resource_name, app)
        gc3libs.log.debug(
            "Setting task '%s' status to TERMINATED"
            " and returncode to SIGCANCEL", app)
        app.changed = True
        # setting the state runs the state-transition handlers,
        # which may raise an error -- ignore them, but log nonetheless
        try:
            job.state = Run.State.TERMINATED
        # pylint: disable=broad-except
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
                suffix='.tmp', prefix='gc3libs.', mode='w+t')
            lrms.peek(app, remote_filename, local_file, offset, size)
            local_file.flush()
            local_file.seek(0)

        return local_file

    def __peek_task(self, task, what, offset, size, **extra_args):
        """Implementation of `peek` on generic `Task` objects."""
        return task.peek(what, offset, size, **extra_args)

    def update_resources(self, resources=all, **extra_args):
        """
        Update the state of a given set of resources.

        Each resource object in the returned list will have its `updated`
        attribute set to `True` if the update operation succeeded, or `False`
        if it failed.

        Optional argument `resources` should be a subset of the
        resources configured in this `Core` instance (the actual
        `Lrms`:class: objects, not the resource names).  By default,
        all configured resources are updated.
        """
        if resources is all:
            resources = list(self.resources.values())
        for lrms in self.resources.values():
            try:
                if not lrms.enabled:
                    continue
                # auto_enable_auth = extra_args.get(
                #     'auto_enable_auth', self.auto_enable_auth)
                lrms.get_resource_status()
                lrms.updated = True
            except gc3libs.exceptions.UnrecoverableError as err:
                # disable resource -- there's no point in
                # trying it again at a later stage
                lrms.enabled = False
                lrms.updated = False
                gc3libs.log.error(
                    "Unrecoverable error updating status"
                    " of resource '%s': %s."
                    " Disabling resource.",
                    lrms.name, err)
                gc3libs.log.warning(
                    "Resource %s will be ignored from now on.",
                    lrms.name)
                gc3libs.log.debug(
                    "Got error '%s' in updating resource '%s';"
                    " printing full traceback.",
                    err.__class__.__name__, lrms.name,
                    exc_info=True)
            # pylint: disable=broad-except
            except Exception as err:
                gc3libs.log.error(
                    "Ignoring error updating resource '%s': %s.",
                    lrms.name, err)
                gc3libs.log.debug(
                    "Got error '%s' in updating resource '%s';"
                    " printing full traceback.",
                    err.__class__.__name__, lrms.name,
                    exc_info=True)
                lrms.updated = False

    def close(self):
        """
        Used to invoke explicitly the destructor on objects
        e.g. LRMS
        """
        for lrms in self.resources.values():
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

    # pylint: disable=missing-docstring
    def __next__(self):
        raise NotImplementedError(
            "Method `next` of class `%s` has not been implemented."
            % self.__class__.__name__)

    # pylint: disable=missing-docstring
    def send(self, result):
        raise NotImplementedError(
            "Method `send` of class `%s` has not been implemented."
            % self.__class__.__name__)

    # pylint: disable=missing-docstring
    def throw(self, *excinfo):
        raise NotImplementedError(
            "Method `throw` of class `%s` has not been implemented."
            % self.__class__.__name__)

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


class scheduler(object):  # pylint: disable=invalid-name

    """
    Decorate a generator function for use as a `Scheduler`:class: object.
    """
    __slots__ = ['_fn', '_gen']

    def __init__(self, fn):
        self._fn = fn

    def __call__(self, *args, **kwargs):
        # pylint: disable=attribute-defined-outside-init
        self._gen = self._fn(*args, **kwargs)
        return self

    #
    # proxy generator protocol methods
    #

    def __iter__(self):
        return self

    # pylint: disable=missing-docstring
    def __next__(self):
        return next(self._gen)

    # pylint: disable=missing-docstring
    def send(self, value):
        return self._gen.send(value)

    # pylint: disable=missing-docstring
    def throw(self, *excinfo):
        return self._gen.throw(*excinfo)

    # pylint: disable=missing-docstring
    def close(self):
        self._gen.close()

    #
    # add context protocol methods
    #

    def __enter__(self):
        return self

    def __exit__(self, *excinfo):
        self.close()


@scheduler
def first_come_first_serve(task_queue, resources, matchmaker=MatchMaker()):
    """
    First-come first-serve scheduling policy.

    Tasks are submitted to resources in the order they appear in the
    `task_queue` queue.  Each task is submitted to resources according to
    the order they are sorted by `Application.rank_resources` (if that
    method exists).

    This is the default scheduling policy in GC3Pie's `Engine`:class.
    """
    assert resources, "No execution resources available!"
    # make a copy of the `resources` argument, so we can modify it
    # when e.g. disabling resources that are full
    resources = list(resources)
    total = len(task_queue)
    for done in range(total):
        task = task_queue.get()
        # keep only compatible resources
        compatible_resources = matchmaker.filter(task, resources)
        if not compatible_resources:
            gc3libs.log.warning(
                "No compatible resources for task '%s'"
                " - cannot submit it", task)
            task_queue.put(task)
            continue
        # sort them according to the Task's preference
        targets = matchmaker.rank(task, compatible_resources)
        # now try submission of the task to each resource until one succeeds
        for target in targets:
            try:
                yield (task, target.name)
                # submission successful, continue with next task
                break
            except (gc3libs.exceptions.ResourceNotReady,
                    gc3libs.exceptions.MaximumCapacityReached) as exc:
                # this is not a real error: the resource is adapting
                # for the task and will actually accept it sometime in
                # the future, so disable resource and try next one
                gc3libs.log.debug(
                    "Disabling resource `%s` for this scheduling cycle: %s",
                    target.name, exc)
                resources.remove(target)
                continue
            # pylint: disable=broad-except
            except Exception as err:
                # note error condition but continue with next target resource
                gc3libs.log.debug(
                    "Scheduler ignored error in submitting task '%s': %s: %s",
                    task, err.__class__.__name__, err, exc_info=True)
                continue
        else:
            # unsuccessful submission, push task back into queue
            task_queue.put(task)
        if not resources:
            gc3libs.log.debug(
                "No more resources available,"
                " aborting scheduling cycle with %d tasks remaining.",
                total - done)
            return


class Engine(object):  # pylint: disable=too-many-instance-attributes
    """
    Manage a collection of tasks, until a terminal state is reached.
    Specifically:

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
      number of tasks in `SUBMITTED` state is greater than
      `max_submitted`, then no new submissions will be attempted.

    `output_dir`
      Base directory for job output; if not `None`, each task's
      results will be downloaded in a subdirectory named after the
      task's `permanent_id`.

    `scheduler`
      A factory function for creating objects that conform to the
      `Scheduler` interface to control task submission; see the
      `Scheduler`:class: documentation for details.  The default value
      implements a first-come first-serve algorithm: tasks are
      submitted in the order they have been added to the `Engine`.

    `retrieve_running`
      If ``True``, snapshot output from RUNNING jobs at every
      invocation of `progress`:meth:

    `retrieve_overwrites`
      If ``True``, overwrite files in the output directory of any job
      (as opposed to moving destination away and downloading a fresh
      copy). See `Core.fetch_output`:meth: for details.

    `retrieve_changed_only`
      If both this and `overwrite` are ``True``, then only changed
      files are downloaded. See `Core.fetch_output`:meth: for details.

    `forget_terminated`
      When ``True``, `Engine.remove`:meth: is automatically called
      on tasks when their state turns to ``TERMINATED``.

      .. warning::

        For historical reasons, the default for this option is
        ``False`` but this can (and should!) be changed in future
        releases.

    Any of the above can also be set by passing a keyword argument to
    the constructor (assume ``g`` is a `Core`:class: instance)::

    | >>> e = Engine(g, can_submit=False)
    | >>> e.can_submit
    | False
    """

    def __init__(self, controller, tasks=[], store=None,
                 can_submit=True, can_retrieve=True,
                 max_in_flight=0, max_submitted=0,
                 output_dir=None,
                 scheduler=first_come_first_serve,  # pylint: disable=redefined-outer-name
                 retrieve_running=False,
                 retrieve_overwrites=False,
                 retrieve_changed_only=True,
                 forget_terminated=False):
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
        :param scheduler:
        :param bool retrieve_running:
        :param bool retrieve_overwrites:
        :param bool retrieve_changed_only:
          Optional keyword arguments; see `Engine`:class: for a description.

        """
        # internal-use attributes
        self._managed = self._TaskQueueManager(self)
        self._core = controller
        self._store = store
        self._tasks_by_id = {}

        # public attributes
        self.can_submit = can_submit
        self.can_retrieve = can_retrieve
        self.max_in_flight = max_in_flight
        self.max_submitted = max_submitted
        self.output_dir = output_dir
        self.scheduler = scheduler
        self.retrieve_running = retrieve_running
        self.retrieve_overwrites = retrieve_overwrites
        self.retrieve_changed_only = retrieve_changed_only
        self.forget_terminated = forget_terminated

        # init counters/statistics
        self._counts = self._Counters(self)
        # always gather statistics about `Task` instances, as `Task`
        # is the root of the GC3Pie task hierarchy
        self._counts.init_for(Task)
        # but also count `Application` instances to enforce submission
        # and running limits (only real tasks that consume compute
        # resources count for those -- "policy" tasks like
        # `TaskCollection` should not)
        self._counts.init_for(Application)
        TaskStateChange.connect(self._on_state_change)

        # Engine fully initialized, add all tasks
        for task in tasks:
            self.add(task)

    def _on_state_change(self, task, from_state, to_state):
        if task in self._managed:
            #gc3libs.log.debug("Task %s transitioned from %s to %s ...", task, from_state, to_state)
            self._counts.transitioned(task, from_state, to_state)

    class TaskQueue(object):
        def __init__(self):
            # the actual data structure used for keeping tasks
            self._queue = deque()

        def __iter__(self):
            return iter(self._queue)

        def __len__(self):
            return len(self._queue)

        def add(self, task):
            """
            Add task in front of queue.

            Does *not* check if already present.
            """
            self._queue.appendleft(task)

        def get(self):
            """
            Pop the first task from the front of queue.
            """
            task = self._queue.popleft()
            return task

        def put(self, task):
            """
            Add task to back of queue.

            Does *not* check if already present.
            """
            self._queue.append(task)

        def remove(self, task):
            """
            Remove the given task from the queue.
            """
            # FIXME: this relies on `collections.deque` which supports
            # removal of an element by value; Python's `queue.Queue`
            # only provides put/get operations and would make this
            # difficult to do.  We would need to find a different
            # solution or data structure before we can generalize the
            # Engine to span multiple processes...
            self._queue.remove(task)


    class _TaskQueueManager(object):

        def __init__(self, engine):
            # action-based queues
            self.to_kill = engine.TaskQueue()
            self.to_update = engine.TaskQueue()
            self.to_submit = engine.TaskQueue()
            self.to_fetch_output = engine.TaskQueue()
            self.to_cleanup = engine.TaskQueue()
            self.done = engine.TaskQueue()

            # map action names to queues
            self._actions = {
                'kill':         self.to_kill,
                'update':       self.to_update,
                'submit':       self.to_submit,
                'fetch_output': self.to_fetch_output,
                'cleanup':      self.to_cleanup,
                'done':         self.done,
            }

            # map task ID to action
            self._index = {}

        def __contains__(self, task):
            """
            Return ``True`` if `task` is present in some queue.
            """
            return id(task) in self._index

        def get_queue(self, task, _override_state=None):
            """
            Return the "queue" object to which `task` should be added or removed.
            """
            state = _override_state or task.execution.state
            if Run.State.NEW == state:
                return self.to_submit
            elif state in [
                    Run.State.RUNNING,
                    Run.State.STOPPED,
                    Run.State.SUBMITTED,
                    Run.State.UNKNOWN,
            ]:
                return self.to_update
            elif Run.State.TERMINATING == state:
                return self.to_fetch_output
            elif Run.State.TERMINATED == state:
                # FIXME: we should never add here when `self.forget_terminated == True`
                return self.done
            else:
                raise AssertionError(
                    "Unhandled state '%s' in gc3libs.core.Engine." % state)

        def add(self, task, action=None):
            """
            Append a task to the back of the appropriate action queue.
            """
            queue = (self._actions[action] if action else self.get_queue(task))
            queue.put(task)
            self._index[id(task)] = queue

        def remove(self, task):
            """
            Delete the given task from the queue it is in.
            After calling this method, `task in this` will be ``False``.
            """
            queue = self._index.pop(id(task))
            queue.remove(task)

        def requeue(self, task, action=None):
            """
            Move an already-managed task to the back of the queue
            that is appropriate for its state.
            """
            try:
                queue = self._index[id(task)]
                queue.remove(task)
            except (KeyError, ValueError):
                pass
            self.add(task, action)


    class _Counters(object):
        """
        Keep count of how many tasks of a class are in a given state.
        """
        def __init__(self, engine):
            self._engine = engine
            self.totals = {}

        def init_for(self, cls):
            """
            Initialize counters for tasks of class `cls`.

            All statistics are initially computed starting from the
            current collection of tasks managed by the parent `Engine`
            instance; they will be kept up-to-date during task
            addition/removal/progress.
            """
            # FIXME: rewrite using `collections.Counter` when we drop
            # support for Py 2.6?
            counter = self.totals[cls] = defaultdict(int)
            for task in self._engine.iter_tasks(cls):
                counter['total'] += 1
                state = task.execution.state
                counter[state] += 1
                if state == Run.State.TERMINATED:
                    if task.execution.returncode == 0:
                        counter['ok'] += 1
                    else:
                        counter['failed'] += 1
            return counter

        def _update(self, task, increment):
            """
            Update the counts relative to `task`'s state by `increment`.
            """
            state = task.execution.state
            stats_to_update = ['total', state]
            if state == 'TERMINATED':
                if task.execution.returncode == 0:
                    stats_to_update.append('ok')
                else:
                    stats_to_update.append('failed')
            for cls in self.totals:
                if isinstance(task, cls):
                    for stat in stats_to_update:
                        self.totals[cls][stat] += increment

        def add(self, task):
            """
            Adjust totals to include `task`.
            """
            self._update(task, +1)

        def remove(self, task):
            """
            Adjust totals following the removal of `task`.
            """
            self._update(task, -1)

        def transitioned(self, task, from_state, to_state):
            """
            Update the counts, following a `TaskStateChange` event.

            The counters relative to *from_state* are decremented by
            unit, and correspondingly the counters for *to_state* are
            incremented.

            This is functionally equivalent to, but more efficient than::

              self._update(task, from_state, -1)
              self._update(task, to_state, +1)
            """
            stats_to_increment = [to_state]
            if to_state == 'TERMINATED':
                if task.execution.returncode == 0:
                    stats_to_increment.append('ok')
                else:
                    stats_to_increment.append('failed')
                # update counts if exit code is later changed
                TermStatusChange.connect(self._on_termstatus_change, sender=task)
            for cls in self.totals:
                if isinstance(task, cls):
                    self.totals[cls][from_state] -= 1
                    for stat in stats_to_increment:
                        self.totals[cls][stat] += 1

        def _on_termstatus_change(self, task, from_returncode, to_returncode):
            # shortcut
            if from_returncode == to_returncode:
                return
            # decrement the "from" status
            target = 'ok' if from_returncode == 0 else 'failed'
            for cls in self.totals:
                if isinstance(task, cls):
                    self.totals[cls][target] -= 1
            # remove handler if task was resubmitted
            if task.execution.state != 'TERMINATED':
                TermStatusChange.disconnect(self._on_termstatus_change, sender=task)
            # increment the "to" status
            target = 'ok' if to_returncode == 0 else 'failed'
            for cls in self.totals:
                if isinstance(task, cls):
                    self.totals[cls][target] += 1


    def add(self, task):
        """
        Add `task` to the list of tasks managed by this Engine.
        Adding a task that has already been added to this `Engine`
        instance results in a no-op.
        """
        if task not in self._managed:
            self._managed.add(task)
            self._counts.add(task)
            if self._store:
                try:
                    self._tasks_by_id[task.persistent_id] = task
                except AttributeError:
                    gc3libs.log.debug(
                        "Engine %s: Added task %s with no persistent ID!",
                        self, task)
            task.attach(self)


    def remove(self, task):
        """
        Remove a `task` from the list of tasks managed by this Engine.

        Removing a task that is not managed (i.e., already removed or
        never added) is a no-op.
        """
        if task not in self._managed:
            return
        self._managed.remove(task)
        self._counts.remove(task)
        if self._store:
            try:
                del self._tasks_by_id[task.persistent_id]
            except KeyError:
                # already removed
                pass
            except AttributeError:
                gc3libs.log.debug(
                    "Engine %s: Cannot remove task %s: has no persistent ID!",
                    self, task)
        task.detach()


    def find_task_by_id(self, task_id):
        """
        Return the task with the given persistent ID added to this
        `Engine` instance.  If no task has that ID, raise a `KeyError`.
        """
        return self._tasks_by_id[task_id]


    def iter_tasks(self, only_cls=None):
        """
        Iterate over tasks managed by the Engine.

        If argument `only_cls` is ``None`` (default), then iterate over
        *all* tasks managed by this Engine.  Otherwise, only return
        tasks which are instances of a (sub)class `only_cls`.
        """
        if only_cls is None:
            select = self.__iter_all
        else:
            select = self.__iter_only
        return itertools.chain(
            select(self._managed.to_submit, only_cls),
            select(self._managed.to_update, only_cls),
            select(self._managed.to_kill, only_cls),
            select(self._managed.to_fetch_output, only_cls),
            select(self._managed.to_cleanup, only_cls),
            select(self._managed.done, only_cls),
        )

    # helper methods for `iter_tasks`; they are created as
    # "staticmethod"s instead of `lambda`-functions to save creating a
    # closure for each invocation of `iter_tasks`
    @staticmethod
    def __iter_all(queue, _):
        return iter(queue)

    @staticmethod
    def __iter_only(queue, cls):
        return filter(
            (lambda task: isinstance(task, cls)), iter(queue))


    def init_counts_for(self, cls):
        """
        Initialize counters for tasks of class `cls`.

        All statistics are initially computed starting from the current
        collection of tasks managed by this `Engine` instance; they will
        be kept up-to-date during task addition/removal/progress.

        .. warning::

          In a future release, the `Engine` might forget about task
          objects in ``TERMINATED`` state.  Therefore, `init_counts_for`
          should be called before any tasks reaches ``TERMINATED``
          state, or the counts for ``TERMINATED``, ``ok``, and
          ``failed`` jobs will be incorrectly initialized to 0.
        """
        counter = self._counts.init_for(cls)
        if Run.State.TERMINATED in counter and counter[Run.State.TERMINATED] > 0:
            warn("The Engine class will forget TERMINATED tasks in the near future."
                 "In order to get correct results, `init_counts_for`"
                 " should be called before any task reaches TERMINATED state",
                 FutureWarning)


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
        gc3libs.log.debug("Engine.progress(): starting.")

        # pylint: disable=redefined-variable-type
        if self.max_in_flight > 0:
            limit_in_flight = self.max_in_flight
        else:
            limit_in_flight = utils.PlusInfinity()
        if self.max_submitted > 0:
            limit_submitted = self.max_submitted
        else:
            limit_submitted = utils.PlusInfinity()

        # if no resources are enabled, there's no point in running
        # this further
        nr_enabled_resources = sum(int(rsc.enabled)
                                   for rsc in self._core.resources.values())
        if nr_enabled_resources == 0:
            raise gc3libs.exceptions.NoResources(
                "No resources available for running jobs.")

        # execute kills and update count of submitted/in-flight tasks
        queue = self._managed.to_kill
        if queue:
            gc3libs.log.debug("Engine %s about to kill jobs ...", self)
        for _ in range(len(queue)):
            task = queue.get()
            old_state = task.execution.state

            try:
                self._core.kill(task)
                if self._store and task.changed:
                    self._store.save(task)
            # pylint: disable=broad-except
            except Exception as err:
                self.__ignore_or_raise(
                    err, "killing", task,
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
                )

            self._managed.requeue(task)

        # update status of tasks before launching new ones
        queue = self._managed.to_update
        if queue:
            gc3libs.log.debug(
                "Engine %s about to update status of in-flight tasks ...",
                self)
        for _ in range(len(queue)):
            task = queue.get()

            # ensure pre-condition on state is met
            old_state = task.execution.state
            if old_state not in [
                    Run.State.RUNNING,
                    Run.State.STOPPED,
                    Run.State.SUBMITTED,
                    Run.State.UNKNOWN,
            ]:
                # task changed state outside of the Engine, requeue
                self._managed.requeue(task)
                continue

            try:
                self._core.update_job_state(task)
                if self._store and task.changed:
                    self._store.save(task)
            except gc3libs.exceptions.ConfigurationError:
                # Unrecoverable; no sense in continuing -- pass
                # immediately on to client code and let it handle
                # this...
                raise
            # pylint: disable=broad-except
            except Exception as err:
                self.__ignore_or_raise(
                    err, "updating state", task,
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
                )

            # do book-keeping
            state = task.execution.state

            if state in [
                    Run.State.RUNNING,
                    Run.State.STOPPED,
                    Run.State.SUBMITTED,
                    Run.State.UNKNOWN,
            ]:
                queue.put(task)
            else:
                self._managed.requeue(task)

            if self.retrieve_running:
                if (state == Run.State.RUNNING and task.would_output
                        and self.can_retrieve):
                    # try to get output
                    try:
                        self._core.fetch_output(
                            task,
                            overwrite=self.retrieve_overwrites,
                            changed_only=self.retrieve_changed_only)
                    # pylint: disable=broad-except
                    except Exception as err:
                        self.__ignore_or_raise(
                            err, "fetching output", task,
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
                        )

        # reckon how many tasks are "live"; we are only interested in
        # tasks that consume real compute resources (i.e.,
        # `Application` instances) and exclude "policy" classes (e.g.,
        # all `TaskCollections`)
        app_counts = self.counts(Application)
        currently_submitted = app_counts['SUBMITTED']
        currently_in_flight = currently_submitted + app_counts['RUNNING']

        # now try to submit NEW tasks
        if (self.can_submit and
                currently_submitted < limit_submitted and
                currently_in_flight < limit_in_flight):
            submit_allowance = min(
                limit_submitted - currently_submitted,
                limit_in_flight - currently_in_flight
            )
            queue = self._managed.to_submit
            if queue:
                gc3libs.log.debug(
                    "Engine %s about to submit NEW tasks ...", self)
            # update state of all enabled resources, to give a chance to
            # all to get a new job; for a complete discussion, see:
            # https://github.com/uzh/gc3pie/issues/485
            self._core.update_resources()
            # now try to submit
            with self.scheduler(queue,
                                list(self._core.resources.values())) as _sched:
                # wrap the original generator object so that `send`
                # and `throw` do not yield a value -- we only get new
                # stuff from the call to the `next` method in the `for
                # ... in sched:` line
                sched = gc3libs.utils.YieldAtNext(_sched)
                for task, resource_name in sched:
                    # enforce Engine limits
                    if submit_allowance <= 0:
                        # we need to put back the task in the queue
                        # here as we won't go call back into scheduler
                        self._managed.to_submit.put(task)
                        break
                    resource = self._core.resources[resource_name]
                    try:
                        self._core.submit(task, targets=[resource])
                        # if we get to this point, we know state is
                        # either SUBMITTED or RUNNING
                        if self._store and task.changed:
                            self._store.save(task)
                        self._managed.to_update.put(task)
                        if isinstance(task, Application):
                            submit_allowance -= 1
                        # notify scheduler
                        sched.send(task.execution.state)
                    # pylint: disable=broad-except
                    except Exception as err1:
                        # record the error in the task's history
                        task.execution.history(
                            "Submission to resource '%s' failed: %s: %s"
                            % (resource.name, err1.__class__.__name__, err1))
                        gc3libs.log.error(
                            "Got error in submitting task '%s',"
                            " informing scheduler: %s: %s",
                            task, err1.__class__.__name__, err1)
                        # inform scheduler and let it handle it
                        try:
                            sched.throw(* sys.exc_info())
                        # pylint: disable=broad-except
                        except Exception as err2:
                            self.__ignore_or_raise(
                                err2, "submitting task", task,
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
                            )

        # finally, retrieve output of finished tasks
        if self.can_retrieve:
            queue = self._managed.to_fetch_output
            if queue:
                gc3libs.log.debug(
                    "Engine %s about to retrieve output of TERMINATING tasks ...",
                    self)
            for _ in range(len(queue)):
                task = queue.get()
                # try to get output
                try:
                    self._core.fetch_output(
                        task,
                        overwrite=self.retrieve_overwrites,
                        changed_only=self.retrieve_changed_only)
                except gc3libs.exceptions.UnrecoverableDataStagingError as err:
                    gc3libs.log.error(
                        "Error in fetching output of task '%s',"
                        " will mark it as TERMINATED"
                        " (with error exit code %d): %s: %s",
                        task, posix.EX_IOERR,
                        err.__class__.__name__, err, exc_info=True)
                    task.execution.returncode = (
                        Run.Signals.DataStagingFailure,
                        posix.EX_IOERR)
                    task.execution.state = Run.State.TERMINATED
                    task.changed = True
                # pylint: disable=broad-except
                except Exception as err:
                    self.__ignore_or_raise(
                        err, "fetching output", task,
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
                        'fetch_output',
                    )

                if task.execution.state == Run.State.TERMINATED:
                    self._managed.requeue(task, 'cleanup')
                else:
                    assert task.execution.state == 'TERMINATING'
                    queue.put(task)  # retry next time

        # clean up terminated tasks
        queue = self._managed.to_cleanup
        if queue:
            gc3libs.log.debug(
                "Engine %s about to clean up TERMINATED tasks ...", self)
        for _ in range(len(queue)):
            task = queue.get()
            try:
                self._core.free(task)
                self._managed.requeue(task, 'done')
            # pylint: disable=broad-except
            except Exception as err:
                queue.put(task)  # retry next time
                gc3libs.log.error(
                    "Got error freeing up resources used by task '%s': %s: %s."
                    " (For cloud-based resources, it's possible that the VM"
                    " has been destroyed already.)",
                    task, err.__class__.__name__, err)

            if self._store and task.changed:
                self._store.save(task)

        # now remove all terminated tasks
        if self.forget_terminated:
            queue = self._managed.done
            if queue:
                gc3libs.log.debug(
                    "Engine %s now dropping TERMINATED tasks ...", self)
            # the whole purpose of this loop is to call
            # `self.remove()` on each item in the queue, so we cannot
            # use the dequeue/requeue mechanism used in other loops,
            # otherwise `self.remove()` fails since the task is not in
            # `self._managed.done` ...
            for task in list(queue):
                try:
                    self.remove(task)
                    gc3libs.log.debug(
                        "Dropped TERMINATED task %s", task)
                except Exception as err:  # pylint: disable=broad-except
                    gc3libs.log.debug(
                        "Could not forget TERMINATED task '%s': %s: %s",
                        task, err.__class__.__name__, err)

        gc3libs.log.debug("Engine.progress(): done.")


    def __ignore_or_raise(self, err, action, task, *ctx):
        if gc3libs.error_ignored(*ctx):
            gc3libs.log.debug(
                "Ignored error in %s of task '%s': %s: %s",
                action, task, err.__class__.__name__, err)
            gc3libs.log.debug(
                "(Original traceback follows.)",
                exc_info=True)
        else:
            # propagate exceptions for debugging purposes
            raise


    def redo(self, task, *args, **kwargs):
        """
        Reset task's state to NEW so that it will be re-run.

        Any additional arguments will be forwarded to the task's own
        `.redo()` method; this is useful, e.g., to perform partial
        re-runs of `SequentialTaskCollection` instances.
        """
        self.remove(task)
        task.redo(*args, **kwargs)
        self.add(task)


    @property
    def resources(self):
        """
        Get dict of configured resources.

        This mapping object has configured resource names as keys, and the
        actual `gc3libs.backends.LRMS` instances as values. Note that only
        resources whose ``.enabled`` attribute evaluates to ``True`` will be
        considered for scheduling.

        This is just a reference to the ``.resources`` attribute of the
        underlying core object; see `Core.resources` for more information.
        """
        return self._core.resources


    # FIXME: rewrite using `collections.Counter` when we drop support for Py 2.6
    def counts(self, only=Task):
        """
        Return a dictionary mapping each state name into the count of
        tasks in that state. In addition, the following keys are defined:

        * `ok`:  count of TERMINATED tasks with return code 0

        * `failed`: count of TERMINATED tasks with nonzero return code

        * `total`: total count of managed tasks, whatever their state

        If the optional argument `only` is not None, tasks whose
        whose class is not contained in `only` are ignored.

        : param class only: Restrict counting to tasks of these classes.
        """
        assert only in self._counts.totals
        return dictproxy(self._counts.totals[only])


    def stats(self, only=None):
        """
        Please use :meth:`counts` instead.

        .. warning::

          This is deprecated since GC3Pie version 2.5.
        """
        warn("Deprecated method `Engine.stats()` called"
             " -- please use `Engine.counts()` instead",
             DeprecationWarning, stacklevel=2)
        if only is None:
            # adapt to use `.counts()` default
            return self.counts()
        else:
            return self.counts(only)

    # implement a Core-like interface, so `Engine` objects can be used
    # as substitutes for `Core`.

    # pylint: disable=unused-argument
    def free(self, task, **extra_args):
        """
        Proxy for `Core.free`, which see.
        """
        self._core.free(task)

    def submit(self, task, resubmit=False, targets=None, **extra_args):
        """
        Submit `task` at the next invocation of `progress`.

        The `task` state is reset using the task's own method
        `.redo()`, and then the task added to the collection of
        managed tasks.  Note that the use of `redo()` implies that
        only tasks in a terminal state can be resubmitted!

        The `targets` argument is only present for interface
        compatiblity with `Core.submit`:meth: but is otherwise
        ignored.
        """
        if resubmit:
            # since we are going to change the task's state, we need
            # to expunge it from the queues ...
            if task in self._managed:
                self.remove(task)
            task.redo()
        # ... and then add it again with the (possibly) new state
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
        self._managed.requeue(task, 'kill')

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

    # pylint: disable=missing-docstring

    @utils.same_docstring_as(Core.select_resource)
    def select_resource(self, match):
        return self._core.select_resource(match)

    @utils.same_docstring_as(Core.get_resources)
    def get_resources(self):
        return self._core.get_resources()

    @utils.same_docstring_as(Core.get_backend)
    def get_backend(self, name):
        return self._core.get_backend(name)


class BgEngine(object):
    """
    Run a GC3Pie `Engine`:class: instance in the background.

    A `BgEngine` exposes the same interface as a regular `Engine`
    class, but proxies all operations for asynchronous execution by
    the wrapped `Engine` instance.  In practice, this means that all
    invocations of `Engine` operations on a `BgEngine` always succeed:
    errors will only be visible in the background thread of execution.
    """
    def __init__(self, lib, *args, **kwargs):
        """
        Initialize an instance of class `BgEngine`:class:.

        :param str lib:
            framework to use for background thread scheduling;
            any value supported by `gc3libs.utils.get_scheduler_and_lock_factory`:func:
            (which see) is allowed here.

        :param args:
            Either a single `Engine`:class: instance, or a list of positional
            arguments to pass to the `Engine`:class: constructor.  In the former
            case, the instance should be the one and only argument (after `lib`).

        :param kwargs:
            Keyword arguments to forward to the `Engine`:class: constructor
            (unless a pre-built `Engine` instance is passed to `BgEngine`,
            in which case no keyword arguments are allowed.)
        """
        sched, lock = utils.get_scheduler_and_lock_factory(lib)
        self._scheduler = sched()
        self.running = False

        # a queue for Engine ops
        self._queue = []
        self._queue_locked = lock()

        # queues for before/after `Engine.progress()` triggers
        self._after_progress_triggers = []
        self._after_progress_triggers_locked = lock()
        self._before_progress_triggers = []
        self._before_progress_triggers_locked = lock()

        assert len(args) + len(kwargs) > 0, (
            "`BgEngine()` must be called"
            " either with an `Engine` instance as second (and last) argument,"
            " or with a set of parameters to pass on to the `Engine` constructor.")
        if len(args) == 1 and isinstance(args[0], gc3libs.core.Engine):
            # first (and only!) arg is an `Engine` instance, use that
            self._engine = args[0]
        else:
            # use supplied parameters to construct an `Engine`
            self._engine = gc3libs.core.Engine(*args, **kwargs)

        # no result caching until an update is really performed
        self._progress_last_run = 0


    #
    # control main loop scheduling
    #

    def start(self, interval):
        """
        Start triggering the main loop at the given `interval` frequency.

        :param gc3libs.quantity.Duration interval:
          Time span between successive calls of `_perform`:meth:
        """
        self.running = True
        self._scheduler.add_job(
            self._perform,
            'interval', seconds=(interval.amount(Duration.s)))
        self._scheduler.start()
        gc3libs.log.info(
            "Started background execution of Engine %s every %s",
            self._engine, interval)


    def stop(self, wait=False):
        """
        Stop background execution of the main loop.

        Call `start`:meth: to resume running.

        :param bool wait:
          When ``True``, wait until all pending actions
          on the background thread have been completed.
        """
        gc3libs.log.info(
            "Stopping background execution of Engine %s ...", self._engine)
        self.running = False
        self._scheduler.shutdown(wait)


    def _perform(self):
        """
        Main loop: runs in a background thread after `start`:meth: has
        been called.

        There are two tasks that this loop performs:

        - Execute any queued engine commands.

        - Run `Engine.progress()` to ensure that GC3Pie tasks are updated.
        """
        gc3libs.log.debug("%s: _perform() started", self)
        self.__run_delayed_operations()
        self.__run_before_triggers()
        self.__run_engine_progress()
        self.__run_after_triggers()

    def __run_delayed_operations(self):
        # quickly grab a local copy of the command queue, and
        # reset it to the empty list -- we do not want to hold
        # the lock on the queue for a long time, as that would
        # make the API unresponsive
        with self._queue_locked:
            queue = self._queue
            self._queue = []
        self.__run_hooks(queue)

    def __run_before_triggers(self):
        with self._before_progress_triggers_locked:
            before_progress_triggers = self._before_progress_triggers
            self._before_progress_triggers = []
        self.__run_hooks(before_progress_triggers)

    def __run_after_triggers(self):
        with self._after_progress_triggers_locked:
            after_progress_triggers = self._after_progress_triggers
            self._after_progress_triggers = []
        self.__run_hooks(after_progress_triggers)

    @staticmethod
    def __run_hooks(queue):
        """
        Call all the functions listed in `queue`, in the order given.
        Any exceptions raised will be logged at a WARNING level but
        otherwise ignored.
        """
        for func, args, kwargs in queue:
            gc3libs.log.debug(
                "Executing delayed call %s(*%r, **%r) ...",
                func.__name__, args, kwargs)
            try:
                func(*args, **kwargs)
            except Exception as err:  # pylint: disable=broad-except
                gc3libs.log.warning(
                    "Got '%s' while executing delayed call %s(*%r, **%r): %s",
                    err.__class__.__name__,
                    func.__name__, args, kwargs,
                    err, exc_info=__debug__)

    def __run_engine_progress(self):
        """
        Call the `.progress()` method of the wrapped `Engine` instance.
        """
        gc3libs.log.debug(
            "%s: calling `progress()` on Engine %s ...",
            self, self._engine)
        # pylint: disable=broad-except
        try:
            self._engine.progress()
            self._progress_last_run = time.time()
        except Exception as err:
            gc3libs.log.warning(
                "Ignoring '%s' error,"
                "  occurred while running"
                " `Engine.progress()` in the background: %s",
                err.__class__.__name__, err, exc_info=__debug__)
        gc3libs.log.debug("%s: _perform() done", self)


    def trigger_before_progress(self, func, *args, **kwargs):
        """
        Call a function *before* running `Engine.progress()` in the main loop.
        Exceptions raised during the call will be logged at WARNING level but
        otherwise ignored.

        The function call will be triggered only *once* at the next run of the
        main loop; it will not be fired repeatedly at every re-run of the main
        loop.

        Any suppplemental positional arguments or keyword-arguments that are
        supplied will be passed unchanged to the trigger function.
        """
        with self._before_progress_triggers_locked:
            self._before_progress_triggers.append((func, args, kwargs))


    def trigger_after_progress(self, func, *args, **kwargs):
        """
        Call a function *after* running `Engine.progress()` in the main loop.
        Exceptions raised during the call will be logged at WARNING level but
        otherwise ignored.

        The function call will be triggered only *once* at the next run of the
        main loop; it will not be fired repeatedly at every re-run of the main
        loop.

        Any suppplemental positional arguments or keyword-arguments that are
        supplied will be passed unchanged to the trigger function.
        """
        with self._after_progress_triggers_locked:
            self._after_progress_triggers.append((func, args, kwargs))


    @staticmethod
    def at_most_once_per_cycle(fn):  # pylint: disable=invalid-name
        """
        Ensure the decorated function is not executed more than once per
        each poll interval.

        Cached results are returned instead, if `Engine.progress()` has
        not been called in between two separate invocations of the wrapped
        function.

        .. warning::

          *Keyword arguments are ignored when doing a lookup* for
          previously-cached function results. This means that the
          following expressions might all return the same cached
          value::

            f(), f(foo=1), f(bar=2, baz='a')
        """
        # pylint: disable=missing-docstring,protected-access
        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            # no caching if the main loop is not running
            if not self._progress_last_run:
                return fn(self, *args, **kwargs)
            else:
                key = (fn, tuple(id(arg) for arg in args))
                try:
                    update = (self._cache_last_updated[key] < self._progress_last_run)
                except AttributeError:
                    self._cache_last_updated = defaultdict(float)
                    self._cache_value = {}
                    update = True
                if update:
                    self._cache_value[key] = fn(self, *args)
                    self._cache_last_updated[key] = time.time()
                # gc3libs.log.debug("%s(%s, ...): Using cached value '%s'",
                #                  fn.__name__, obj, obj._cache_value[key])
                return self._cache_value[key]
        return wrapper


    #
    # Engine interface
    #

    def add(self, task):
        """Proxy to `Engine.add`:meth: (which see)."""
        if self.running:
            with self._queue_locked:
                self._queue.append((self._engine.add, (task,), {}))
        else:
            self._engine.add(task)


    def close(self):
        """Proxy to `Engine.close`:meth: (which see)."""
        if self.running:
            with self._queue_locked:
                self._queue.append((self._engine.close, tuple(), {}))
        else:
            self._engine.close()


    def counts(self, only=Task):
        """Proxy to `Engine.counts`:meth: (which see)."""
        return self._engine.counts(only)


    def fetch_output(self, task, output_dir=None,
                     overwrite=False, changed_only=True, **extra_args):
        """Proxy to `Engine.fetch_output`:meth: (which see)."""
        if self.running:
            with self._queue_locked:
                self._queue.append((self._engine.fetch_output,
                                    (task, output_dir, overwrite, changed_only),
                                    extra_args))
        else:
            self._engine.fetch_output(task, output_dir, overwrite,
                                      changed_only, **extra_args)


    def find_task_by_id(self, task_id):
        """Proxy to `Engine.find_task_by_id`:meth: (which see)."""
        return self._engine.find_task_by_id(task_id)


    def free(self, task, **extra_args):
        """Proxy to `Engine.free`:meth: (which see)."""
        if self.running:
            with self._queue_locked:
                self._queue.append((self._engine.free, (task,), extra_args))
        else:
            self._engine.free(task, **extra_args)


    def get_resources(self):
        """Proxy to `Engine.get_resources`:meth: (which see)."""
        return self._engine.get_resources()


    def get_backend(self, name):
        """Proxy to `Engine.get_backend`:meth: (which see)."""
        return self._engine.get_backend(name)


    def iter_tasks(self):
        """
        Proxy to `Engine.iter_tasks`:meth: (which see).
        """
        return self._engine.iter_tasks()


    def kill(self, task, **extra_args):
        """Proxy to `Engine.kill`:meth: (which see)."""
        if self.running:
            with self._queue_locked:
                self._queue.append((self._engine.kill, (task,), extra_args))
        else:
            self._engine.kill(task, **extra_args)


    def peek(self, task, what='stdout', offset=0, size=None, **extra_args):
        """Proxy to `Engine.peek`:meth: (which see)."""
        if self.running:
            with self._queue_locked:
                self._queue.append((self._engine.peek,
                                    (task, what, offset, size), extra_args))
        else:
            self._engine.peek(task, what, offset, size, **extra_args)


    def progress(self):
        """
        Proxy to `Engine.progress`.

        If the background thread is already running, this is a no-op,
        as progressing tasks is already taken care of by the
        background thread.  Otherwise, just forward the call to the
        wrapped engine.
        """
        if self.running:
            pass
        else:
            self._engine.progress()


    def remove(self, task):
        """Proxy to `Engine.remove`:meth: (which see)."""
        if self.running:
            with self._queue_locked:
                self._queue.append((self._engine.remove, (task,), {}))
        else:
            self._engine.remove(task)


    def select_resource(self, match):
        """Proxy to `Engine.select_resource`:meth: (which see)."""
        if self.running:
            with self._queue_locked:
                self._queue.append((self._engine.select_resource, (match,), {}))
        else:
            self._engine.select_resource(match)


    def stats(self, only=None):
        """Proxy to `Engine.stats`:meth: (which see)."""
        return self._engine.stats(only)


    def submit(self, task, resubmit=False, targets=None, **extra_args):
        """Proxy to `Engine.submit`:meth: (which see)."""
        if self.running:
            with self._queue_locked:
                self._queue.append((self._engine.submit, (task, resubmit, targets), extra_args))
        else:
            self._engine.submit(task, resubmit, targets, **extra_args)


    def update_job_state(self, *tasks, **extra_args):
        """Proxy to `Engine.update_job_state`:meth: (which see)."""
        if self.running:
            with self._queue_locked:
                self._queue.append((self._engine.update_job_state, tasks, extra_args))
        else:
            self._engine.update_job_state(*tasks, **extra_args)
