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
__version__ = '1.0rc2 (SVN $Revision$)'
__date__ = '$Date$'


from fnmatch import fnmatch
import os
import sys
import time
import ConfigParser
import tempfile
import warnings
warnings.simplefilter("ignore")

import gc3libs
from gc3libs import Application, Run
from gc3libs.backends.sge import SgeLrms
from gc3libs.backends.fork import ForkLrms
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
        lrms = [ lrms for lrms in self._lrms_list
                 if fnmatch(lrms._resource.name, resource_name) ]
        if lrms:
            return lrms[0]
        else:
            raise gc3libs.exceptions.InvalidResourceName(
                "Cannot find computational resource '%s'" % 
                resource_name)

    def _init_backends(self):
        for _resource in self._resources:
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
            (wildcards "*" and "?" are allowed) are retained.
        """
        try:
            # self._resources = [ res for res in self._resources if match(res) ]
            self._lrms_list = [ lrms for lrms in self._lrms_list if match(lrms._resource) ]
        except:
            # `match` is not callable, then assume it's a 
            # glob pattern and select resources whose name matches
            # self._resources = [ res for res in self._resources
            #                     if fnmatch(res.name, match) ]
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
        anything other than `TERMINATED`.
        """

        if app.execution.state != Run.State.TERMINATED:
            raise gc3libs.exceptions.InvalidOperation("Attempting to free resources of job '%s',"
                                   " which is in non-terminal state." % app)

        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        #lrms = self._get_backend(app.execution.resource_name)
        lrms =  self.get_backend(app.execution.resource_name)
        self.auths.get(lrms._resource.auth)
        lrms.free(app)
        

    def submit(self, app, **kw):
        """
        Submit a job running an instance of the given `app`.  Upon
        successful submission, call the `submitted` method on the
        `app` object.

        :raise: `gc3libs.exceptions.InputFileError` if an input file
                does not exist or cannot otherwise be read.
        """
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        # check that all input files can be read
        for local_path in app.inputs:
            if not os.path.exists(local_path):
                raise gc3libs.exceptions.InputFileError("Non-existent input file '%s'"
                                                        % local_path)
            if not os.access(local_path, os.R_OK):
                raise gc3libs.exceptions.InputFileError("Cannot read input file '%s'"
                                                        % local_path)

        job = app.execution

        # gc3libs.log.debug('Instantiating LRMSs ...')
        # _lrms_list = []
        # for _resource in self._resources:
        #     try:
        #         _lrms = self._get_backend(_resource.name)
        #         self.auths.get(_lrms._resource.auth)
        #         _lrms_list.append(_lrms)
        #     except Exception, ex:
        #         # log exceptions but ignore them
        #         gc3libs.log.warning("Failed creating LRMS for resource '%s' of type '%s': %s: %s",
        #                             _resource.name, _resource.type,
        #                             ex.__class__.__name__, str(ex), exc_info=True)
        #         continue

        if len(self._lrms_list) == 0:
            raise gc3libs.exceptions.NoResources("Could not initialize any computational resource"
                              " - please check log and configuration file.")

        # XXX: auth should probably become part of LRMS and controlled within it
        for _lrms in self._lrms_list:
            self.auths.get(_lrms._resource.auth)

        gc3libs.log.debug('Performing brokering ...')
        # decide which resource to use
        # (Resource)[] = (Scheduler).PerformBrokering((Resource)[],(Application))
        _selected_lrms_list = scheduler.do_brokering(self._lrms_list,app)
        gc3libs.log.debug('Scheduler returned %d matching resources',
                           len(_selected_lrms_list))
        if 0 == len(_selected_lrms_list):
            raise gc3libs.exceptions.NoResources("No available resource can accomodate the requested"
                              " CPU/memory/wall-clock time combination.")

        exs = [ ]
        # Scheduler.do_brokering should return a sorted list of valid lrms
        for lrms in _selected_lrms_list:
            gc3libs.log.debug("Attempting submission to resource '%s'..." 
                              % lrms._resource.name)
            try:
                self.auths.get(lrms._resource.auth)
                lrms.submit_job(app)
            except Exception, ex:
                gc3libs.log.debug("Error in submitting job to resource '%s': %s: %s", 
                                  lrms._resource.name, ex.__class__.__name__, str(ex),
                                  exc_info=True)
                exs.append(ex) 
                continue
            gc3libs.log.info('Successfully submitted process to: %s', lrms._resource.name)
            job.state = Run.State.SUBMITTED
            job.resource_name = lrms._resource.name
            job.info = ("Submitted to '%s' at %s"
                        % (job.resource_name, 
                           time.ctime(job.timestamp[Run.State.SUBMITTED])))
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
        

    def update_job_state(self, *apps, **kw):
        """
        Update state of all applications passed in as arguments,
        and return list of updated states.
        
        If keyword argument `update_on_error` is `False` (default),
        then application execution state is not changed in case a
        backend error happens; it is changed to `UNKNOWN` otherwise.

        If state of a job has changed, call the appropriate handler
        method on the job object, if it's defined.  Handler methods
        are named after the (lowercase) name of the state; e.g., if a
        job reaches state `TERMINATED`, then `job.terminated()` is
        called.
        """
        update_on_error = kw.get('update_on_error', False)
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        states = [] 
        for app in apps:
            state = app.execution.state
            old_state = state
            gc3libs.log.debug("Updating state (%s) of application: %s", state, app)
            try:
                if state not in [ Run.State.NEW, Run.State.TERMINATED ]:
                    lrms = self.get_backend(app.execution.resource_name)
                    try:
                        self.auths.get(lrms._resource.auth)
                        state = lrms.update_job_state(app)
                    except (gc3libs.exceptions.InvalidArgument,
                            gc3libs.exceptions.ConfigurationError):
                        # Unrecoverable; no sense in continuing --
                        # pass immediately on to client code and let
                        # it handle this...
                        raise
                    except gc3libs.exceptions.UnrecoverableAuthError:
                        raise
                    except gc3libs.exceptions.RecoverableAuthError:
                        raise
                    except Exception, ex:
                        gc3libs.log.debug("Error getting status of application '%s': %s: %s",
                                          app, ex.__class__.__name__, str(ex), exc_info=True)
                        state = Run.State.UNKNOWN
                        # run error handler if defined
                        ex = app.update_job_state_error(ex)
                        if isinstance(ex, Exception):
                            raise ex
                    if state != Run.State.UNKNOWN or update_on_error:
                        app.execution.state = state
                if app.execution.state != old_state:
                    # set log information accordingly
                    if app.execution.state == Run.State.RUNNING:
                        app.execution.info = ("Running at %s" 
                                              % time.ctime(app.execution.timestamp[Run.State.RUNNING]))
                    elif app.execution.state == Run.State.TERMINATED:
                        if app.execution.returncode == 0:
                            app.execution.info = ("Terminated at %s." 
                                                  % time.ctime(app.execution.timestamp[gc3libs.Run.State.TERMINATED]))
                        else:
                            # there was some error, try to explain
                            signal = app.execution.signal
                            if signal in Run.Signals:
                                app.execution.info = ("Abnormal termination: %s" % signal)
                            else:
                                if os.WIFSIGNALED(app.execution.returncode):
                                    app.execution.info = ("Job terminated by signal %d" % signal)
                                else:
                                    app.execution.info = ("Job exited with code %d" 
                                                          % self.execution.exitcode)
                    # call Application-specific handler
                    handler_name = str(app.execution.state).lower()
                    if hasattr(app, handler_name):
                        getattr(app, handler_name)()
            except (gc3libs.exceptions.InvalidArgument, gc3libs.exceptions.ConfigurationError):
                # Unrecoverable; no sense in continuing --
                # pass immediately on to client code and let
                # it handle this...
                raise
            # XXX: disabling this catch-all clause: I think it just
            # makes it harder to catch code errors; consider
            # re-enabling when the code is more proved and stable...
            #except Exception, ex:
            #    gc3libs.log.error("Error in Core.update_job_state(), ignored: %s: %s",
            #                      ex.__class__.__name__, str(ex), exc_info=True)
            states.append(app.execution.state)

        return states


    def fetch_output(self, app, download_dir=None, overwrite=False, **kw):
        """
        Retrieve job output into local directory `app.output_dir`;
        optional argument `download_dir` overrides this.  Return
        actual download directory.

        The download directory is created if it does not exist.  If it
        already exists, and the optional argument `overwrite` is
        `False`, it is renamed with a `.NUMBER` suffix and a new empty
        one is created in its place.  By default, 'overwrite` is
        `True`, so files are downloaded over the ones already present.

        If the job is in a terminal state, the instance attribute
        `app.final_output_retrieved` is set to `True`, and the
        `postprocess` method is called on the `app` object (if it's
        defined), with the effective `download_dir` as sole argument.

        Job output cannot be retrieved when `app.execution` is in one
        of the states `NEW` or `SUBMITTED`; a
        `OutputNotAvailableError` exception is thrown in these cases.
        """
        job = app.execution
        if job.state in [ Run.State.NEW, Run.State.SUBMITTED ]:
            raise gc3libs.exceptions.OutputNotAvailableError("Output not available:"
                                          " Job '%s' currently in state '%s'"
                                          % (app, app.execution.state))

        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        # Prepare/Clean download dir
        if download_dir is None:
            try:
                download_dir = app.output_dir
            except AttributeError:
                raise gc3libs.exceptions.InvalidArgument("`Core.fetch_output` called with no explicit download directory,"
                                      " but `Application` object '%s' has no `output_dir` set either."
                                      % app)
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
            self.auths.get(lrms._resource.auth)
            lrms.get_results(app, download_dir)
        except gc3libs.exceptions.DataStagingError, ex:
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
        job.info = ("Output downloaded to '%s'" % download_dir)
        app.output_dir = download_dir
        gc3libs.log.debug("Downloaded output of '%s' (which is in state %s)"
                      % (str(job), job.state))
        if job.state == Run.State.TERMINATED:
            app.final_output_retrieved = True
            app.postprocess(download_dir)
            gc3libs.log.debug("Final output of job '%s' retrieved" % str(job))
        return download_dir
        

    def get_all_updated_resources(self, **kw):
        """
        Return a list of resources known by core.
        Core will try to update the status of resources before returning
        If core fails updating a given resource, it will send back the same 
        resource as created from information imported from configurartion file
        marking it with an additional flag 'updated'.
        """

        updated_resources = []

        # for resource in self._resources:
        for lrms in self._lrms_list:
            try:
                auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
                # lrms = self.get_backend(resource.name)
                self.auths.get(lrms._resource.auth)
                resource = lrms.get_resource_status()
                resource.updated = True
                updated_resources.append(resource)
            except Exception, ex:
                gc3libs.log.error("Got error while updating resource '%s': %s."
                                  % (lrms._resource.name, str(ex)))
                lrms._resource.updated = False
                updated_resources.append(lrms._resource)
                
        return updated_resources

#    def update_resource_status(self, resource_name):
#        """Update status of a given resource. Return resource object after update."""
#        for resource in self._resources:
#            if resource_name == resource.name:
#                #if resource_name in [ o.name for o in self._resources ]:
#                try:
#                    auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
#                    lrms = self._get_backend(resource_name)
#                    self.auths.get(lrms._resource.auth)
#                    return  lrms.get_resource_status()
#                except Exception, ex:
#                    gc3libs.log.error('Got error while updating resource '%s': %s.' 
#                                      % (resource.name, str(ex)))
#                    raise
#
#        # if we reach this point it means no resource has been matched
#        raise InvalidResourceName('Resources %s not found' % resource_name)

    def kill(self, app, **kw):
        """
        Terminate a job.

        Terminating a job in RUNNING, SUBMITTED, or STOPPED state
        entails canceling the job with the remote execution system;
        terminating a job in the NEW or TERMINATED state is a no-op.
        """
        job = app.execution
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
        lrms = self.get_backend(job.resource_name)
        self.auths.get(lrms._resource.auth)
        lrms.cancel_job(app)
        gc3libs.log.debug("Setting job '%s' status to TERMINATED"
                          " and returncode to SIGCANCEL" % job)
        job.state = Run.State.TERMINATED
        job.signal = Run.Signals.Cancelled
        job.log.append("Cancelled.")
        app.terminated()


    def peek(self, app, what='stdout', offset=0, size=None, **kw):
        """
        Download `size` bytes (at offset `offset` from the start) from
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
        job = app.execution
        if what == 'stdout':
            remote_filename = job.stdout_filename
        elif what == 'stderr':
            remote_filename = job.stderr_filename
        else:
            raise Error("File name requested to `Core.peek` must be"
                        " 'stdout' or 'stderr', not '%s'" % what)

        # Check if local data available
        # FIXME: local data could be stale!!
        if job.state == Run.State.TERMINATED and app.final_output_retrieved:
            _filename = os.path.join(app.output_dir, remote_filename)
            _local_file = open(_filename)
        else:

            # Get authN
            auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
            _lrms = self.get_backend(job.resource_name)
            self.auths.get(_lrms._resource.auth)

            _local_file = tempfile.NamedTemporaryFile(suffix='.tmp', prefix='gc3libs.')

            _lrms.peek(app, remote_filename, _local_file, offset, size)
            _local_file.flush()
            _local_file.seek(0)
        
        return _local_file


#======= Static methods =======

    def _get_backend(self,resource_name):
        _lrms = None

        for _resource in self._resources:
            if _resource.name == resource_name:
                # there's a matching resource
                # gc3libs.log.debug('Creating instance of type %s for %s', _resource.type, _resource.name)
                try:
                    if _resource.type == gc3libs.Default.ARC_LRMS:
                        from gc3libs.backends.arc import ArcLrms
                        _lrms = ArcLrms(_resource, self.auths)
                    elif _resource.type == gc3libs.Default.SGE_LRMS:
                        _lrms = SgeLrms(_resource, self.auths)
                    elif _resource.type == gc3libs.Default.FORK_LRMS:
                        _lrms = ForkLrms(_resource, self.auths)
                    else:
                        raise gc3libs.exceptions.ConfigurationError("Unknown resource type '%s'" 
                                                 % _resource.type)
                except Exception, ex:
                    gc3libs.log.error("Error in creating resource %s: %s."
                                      " Configuration file problem?"
                                      % (_resource.name, str(ex)))
                    raise

        if _lrms is None:
            raise gc3libs.exceptions.InvalidResourceName("Cannot find computational resource '%s'" 
                                      % resource_name)

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
        if not (tmpres.type == gc3libs.Default.ARC_LRMS or tmpres.type == gc3libs.Default.SGE_LRMS or tmpres.type == gc3libs.Default.FORK_LRMS):
            gc3libs.log.error("Configuration error: '%s' is no valid resource type.", 
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
    resources = gc3libs.utils.defaultdict(lambda: dict())
    auths = gc3libs.utils.defaultdict(lambda: dict())

    for location in locations:
        location = os.path.expandvars(location)
        if os.path.exists(location) and os.access(location, os.R_OK):
            gc3libs.log.debug("Core.read_config(): reading file '%s'" % location)
        else:
            gc3libs.log.debug("Core.read_config(): ignoring non-existent file '%s'" % location)
            continue # with next `location`

        # Config File exists; read it
        config = ConfigParser.ConfigParser()
        if location not in config.read(location):
            gc3libs.log.debug("Configuration file '%s' is unreadable or malformed: ignoring." 
                               % location)
            continue # with next `location`
        files_successfully_read += 1

        # update `defaults` with the contents of the `[DEFAULTS]` section
        defaults.update(config.defaults())

        for sectname in config.sections():
            if sectname.startswith('auth/'):
                # handle auth section
                gc3libs.log.debug("Core.read_config(): adding auth '%s' ", sectname)
                # extract auth name and register auth dictionary
                auth_name = sectname.split('/', 1)[1]
                auths[auth_name].update(dict(config.items(sectname)))

            elif  sectname.startswith('resource/'):
                # handle resource section
                resource_name = sectname.split('/', 1)[1]
                gc3libs.log.debug("Core.read_config(): adding resource '%s' ", resource_name)
                config_items = dict(config.items(sectname))
                if config_items.has_key('enabled'):
                    config_items['enabled'] = utils.string_to_boolean(config_items['enabled'])
                    # Sergio: Irrelevant debug information as the list of disabled resources is already reported later in the code.
                    # gc3libs.log.debug("Resource '%s': enabled=%s in file '%s'", 
                    #                   resource_name, config_items['enabled'], location)
                resources[resource_name].update(config_items)
                resources[resource_name]['name'] = resource_name

            else:
                # Unhandled sectname
                gc3libs.log.error("Core.read_config(): unknown configuration section '%s' -- ignoring!", 
                                   sectname)

        # gc3libs.log.debug("Core.read_config(): read %d resources from configuration file '%s'",
        #                    len(resources), location)

    # remove disabled resources
    disabled_resources = [ ]
    for resource in resources.values():
        if resource.has_key('enabled') and not resource['enabled']:
            disabled_resources.append(resource['name'])
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


    def add(self, app):
        """Add `app` to the list of tasks managed by this Engine."""
        state = app.execution.state
        if Run.State.NEW == state:
            self._new.append(app)
        elif Run.State.SUBMITTED == state or Run.State.RUNNING == state:
            self._in_flight.append(app)
        elif Run.State.STOPPED == state:
            self._stopped.append(app)
        elif Run.State.TERMINATED == state:
            self._terminated.append(app)
        else:
            raise AssertionError("Unhandled run state '%s' in gc3libs.core.Engine." % state)


    def remove(self, app):
        """Remove a `app` from the list of tasks managed by this Engine."""
        state = app.execution.state
        if Run.State.NEW == state:
            self._new.remove(app)
        elif Run.State.SUBMITTED == state or Run.State.RUNNING == state:
            self._in_flight.remove(app)
        elif Run.State.STOPPED == state:
            self._stopped.remove(app)
        elif Run.State.TERMINATED == state:
            self._terminated.remove(app)
        else:
            raise AssertionError("Unhandled run state '%s' in gc3libs.core.Engine." % state)
        

    def progress(self):
        """
        Update state of all registered tasks and take appropriate action.
        Specifically:

          * tasks in `NEW` state are submitted;

          * the state of tasks in `SUBMITTED`, `RUNNING` or `STOPPED` state is updated;

          * when a task reaches `TERMINATED` state, its output is downloaded.

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
                if self._store:
                    self._store.save(task)
                if task.execution.state == Run.State.SUBMITTED:
                    currently_submitted += 1
                    currently_in_flight += 1
                elif task.execution.state == Run.State.RUNNING:
                    currently_in_flight += 1
                elif task.execution.state == Run.State.STOPPED:
                    transitioned.append(index) # task changed state, mark as to remove
                    self._stopped.append(task)
                elif task.execution.state == Run.State.TERMINATED:
                    transitioned.append(index) # task changed state, mark as to remove
                    self._terminated.append(task)
            except gc3libs.exceptions.ConfigurationError:
                # Unrecoverable; no sense in continuing -- pass
                # immediately on to client code and let it handle
                # this...
                raise
            except Exception, x:
                gc3libs.log.error("Ignoring error in updating state of task '%s': %s: %s"
                                  % (task.persistent_id, x.__class__.__name__, str(x)),
                                  exc_info=True)
        # remove tasks that transitioned to other states
        for index in reversed(transitioned):
            del self._in_flight[index]

        # execute kills and update count of submitted/in-flight tasks
        transitioned = []
        for index, task in enumerate(self._to_kill):
            try: 
                self._core.kill(task)
                if self._store:
                    self._store.save(task)
                if task.execution.state == Run.State.SUBMITTED:
                    currently_submitted -= 1
                    currently_in_flight -= 1
                elif task.execution.state == Run.State.RUNNING:
                    currently_in_flight -= 1
                self._terminated.append(task)
                transitioned.append(index)
            except Exception, x:
                gc3libs.log.error("Ignored error in killing task '%s': %s: %s"
                                  % (task.persistent_id, x.__class__.__name__, str(x)),
                                  exc_info=True)
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
                if self._store:
                    self._store.save(task)
                if task.execution.state in [Run.State.SUBMITTED, Run.State.RUNNING]:
                    currently_in_flight += 1
                    if task.execution.state == Run.State.SUBMITTED:
                        currently_submitted += 1
                    self._in_flight.append(task)
                    transitioned.append(index) # task changed state, mark as to remove
                elif task.execution.state == Run.State.TERMINATED:
                    self._terminated.append(task)
                    transitioned.append(index) # task changed state, mark as to remove
            except Exception, x:
                gc3libs.log.error("Ignoring error in updating state of STOPPED task '%s': %s: %s"
                                  % (task.persistent_id, x.__class__.__name__, str(x)),
                                  exc_info=True)
        # remove tasks that transitioned to other states
        for index in reversed(transitioned):
            del self._stopped[index]

        # now try to submit NEW tasks
        transitioned = []
        if self.can_submit:
            for index, task in enumerate(self._new):
                # try to submit; go to SUBMITTED if successful, FAILED if not
                if currently_submitted < limit_submitted and currently_in_flight < limit_in_flight:
                    try:
                        self._core.submit(task)
                        if self._store:
                            self._store.save(task)
                        self._in_flight.append(task)
                        transitioned.append(index)
                        currently_submitted += 1
                        currently_in_flight += 1
                    except Exception, x:
                        sys.excepthook(*sys.exc_info()) # DEBUG
                        gc3libs.log.error("Ignored error in submitting task '%s': %s: %s"
                                          % (task.persistent_id, x.__class__.__name__, str(x)))
                        task.execution.log("Submission failed: %s: %s" 
                                           % (x.__class__.__name__, str(x)))
        # remove tasks that transitioned to SUBMITTED state
        for index in reversed(transitioned):
            del self._new[index]

        # finally, retrieve output of finished tasks
        if self.can_retrieve:
            for index, task in enumerate(self._terminated):
                if not task.final_output_retrieved:
                    # try to get output
                    try:
                        self._core.fetch_output(task)
                        if task.final_output_retrieved == True:
                            self._core.free(task)
                        if self._store:
                            self._store.save(task)
                    except Exception, x:
                        gc3libs.log.error("Ignored error in fetching output of task '%s': %s: %s" 
                                          % (task.persistent_id, x.__class__.__name__, str(x)), exc_info=True)


    def stats(self):
        """
        Return a dictionary mapping each state name into the count of
        jobs in that state. In addition, the following keys are defined:
        
        * `ok`:  count of TERMINATED jobs with return code 0
        
        * `failed`: count of TERMINATED jobs with nonzero return code
        """
        result = utils.defaultdict(lambda: 0)
        result[Run.State.NEW] = len(self._new)
        for task in self._in_flight:
            state = task.execution.state
            result[state] += 1
        result[Run.State.STOPPED] = len(self._stopped)
        for task in self._to_kill:
            # XXX: presumes no task in the `_to_kill` list is TERMINATED
            state = task.execution.state
            result[state] += 1
        result[Run.State.TERMINATED] = len(self._terminated)
        # for TERMINATED tasks, compute the number of successes/failures
        for task in self._terminated:
            if task.execution.returncode == 0:
                result['ok'] += 1
            else:
                result['failed'] += 1
        return result

            
    # implement a Core-like interface, so `Engine` objects can be used
    # as substitutes for `Core`.

    def free(task):
        """
        Proxy for `Core.free` (which see); in addition, remove `task`
        from the list of managed tasks.
        """
        self.remove(task)
        self._core.free(task)


    def submit(self, task):
        """
        Submit `task` at the next invocation of `perform`.  Actually,
        the task is just added to the collection of managed tasks,
        regardless of its state.
        """
        return self.add(task)


    def update_job_state(self, *tasks):
        """
        Return list of *current* states of the given tasks.  States
        will only be updated at the next invocation of `perform`; in
        particular, no state-change handlers are called as a result of
        calling this method.
        """
        return [task.execution.state for task in tasks]


    def fetch_output(self, task, output_dir=None, overwrite=False):
        """
        Proxy for `Core.fetch_output` (which see).
        """
        if output_dir is None and self.output_dir is not None:
            output_dir = os.path.join(self.output_dir, task.persistent_id)
        if overwrite is None:
            overwrite = self.fetch_output_overwrites
        return self._core.fetch_output(task, output_dir, overwrite)


    def kill(self, task):
        """
        Schedule a task for killing on the next `perform` run.
        """
        self._to_kill.append(task)


    def peek(self, task, what='stdout', offset=0, size=None, **kw):
        """
        Proxy for `Core.peek` (which see).
        """
        return self._core.peek(task, what, offset, size, **kw)
