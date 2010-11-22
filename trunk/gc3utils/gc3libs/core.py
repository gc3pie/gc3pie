#!/usr/bin/env python
"""
Top-level interface to Grid functionality.
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
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
from gc3libs.backends.arc import ArcLrms
from gc3libs.authentication import Auth
import gc3libs.Default as Default
from gc3libs.Exceptions import *
import gc3libs.Resource as Resource
import gc3libs.scheduler as scheduler
import gc3libs.utils as utils 


class Core:

    def __init__(self, resource_list, authorization, auto_enable_auth):
        if len(resource_list) == 0:
            raise NoResources("Resource list has length 0")
        self._resources = resource_list
        self.authorization = authorization
        self.auto_enable_auth = auto_enable_auth

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
            self._resources = [ res for res in self._resources if match(res) ]
        except:
            # `match` is not callable, then assume it's a 
            # glob pattern and select resources whose name matches
            self._resources = [ res for res in self._resources
                                if fnmatch(res.name, match) ]

    def free(self, app, **kw):
        """
        Free up any remote resources used for the execution of `app`.
        In particular, this should delete any remote directories and
        files.

        It is an error to call this method if `app.execution.state` is
        anything other than `TERMINATED`.
        """

        if app.execution.state != Run.State.TERMINATED:
            raise InvalidOperation("Attempting to free resources of job '%s',"
                                   " which is in non-terminal state." % app)

        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        lrms = self._get_backend(app.execution.resource_name)
        self.authorization.get(lrms._resource.authorization_type)
        lrms.free(app)
        

    def submit(self, app, **kw):
        """
        Submit a job running an instance of the given `app`.  Return
        the `app` object, modified to refer to the submitted
        computational job.

        Upon successful submission, call the `submitted` method on the
        `app` object.
        """
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        job = app.execution

        gc3libs.log.debug('Instantiating LRMSs ...')
        _lrms_list = []
        for _resource in self._resources:
            try:
                _lrms_list.append(self._get_backend(_resource.name))
            except Exception, ex:
                # log exceptions but ignore them
                gc3libs.log.warning("Failed creating LRMS for resource '%s' of type '%s': %s: %s",
                                    _resource.name, _resource.type,
                                    ex.__class__.__name__, str(ex), exc_info=True)
                continue
        if len(_lrms_list) == 0:
            raise NoResources("Could not initialize any computational resource"
                              " - please check log and configuration file.")

        gc3libs.log.debug('Performing brokering ...')
        # decide which resource to use
        # (Resource)[] = (Scheduler).PerformBrokering((Resource)[],(Application))
        _selected_lrms_list = scheduler.do_brokering(_lrms_list,app)
        gc3libs.log.debug('Scheduler returned %d matching resources',
                           len(_selected_lrms_list))
        if 0 == len(_selected_lrms_list):
            raise NoResources("Could not select any compatible computational resource"
                              " - please check log and configuration file.")

        # Scheduler.do_brokering should return a sorted list of valid lrms
        for lrms in _selected_lrms_list:
            try:
                self.authorization.get(lrms._resource.authorization_type)
                lrms.submit_job(app)
            except AuthenticationException:
                # ignore authentication errors: e.g., we may fail some SSH connections but succeed in others
                gc3libs.log.debug("Authentication error in submitting to resource '%s'" 
                                   % lrms._resource.name)
                continue
            except LRMSException:
                gc3libs.log.error("Error in submitting job to resource '%s'", 
                                   lrms._resource.name, exc_info=True)
                continue
            gc3libs.log.info('Successfully submitted process to LRMS backend')
            job.state = Run.State.SUBMITTED
            job.resource_name = lrms._resource.name
            if hasattr(job, 'submitted'):
                job.submitted()
            # job submitted; leave loop
            break

        return job


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
                lrms = self._get_backend(app.execution.resource_name)
                if state not in [ Run.State.NEW, Run.State.TERMINATED ]:
                    self.authorization.get(lrms._resource.authorization_type)
                    try:
                        state = lrms.update_job_state(app)
                    except Exception, ex:
                        gc3libs.log.debug("Error getting status of application '%s': %s: %s",
                                          app, ex.__class__.__name__, str(ex))
                        state = Run.State.UNKNOWN
                    if state != Run.State.UNKNOWN or update_on_error:
                        app.execution.state = state
                if app.execution.state != old_state:
                    handler_name = str(app.execution.state).lower()
                    if hasattr(app, handler_name):
                        getattr(app, handler_name)()
            except Exception, ex:
                gc3libs.log.error("Error in Core.update_job_state(), ignored: %s: %s",
                                  ex.__class__.__name__, str(ex))
            states.append(app.execution.state)

        return states


    def fetch_output(self, app, download_dir=None, overwrite=False, **kw):
        """
        Retrieve job output into local directory `app.output_dir`;
        optional argument `download_dir` overrides this.

        The download directory is created if it does not exist.  If
        already existent, it is renamed with a `.NUMBER` suffix and a
        new empty one is created in its place, unless the optional
        argument `overwrite` is `True`.

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
            raise OutputNotAvailableError("Output not available:"
                                          " Job '%s' currently in state '%s'"
                                          % (app, app.execution.state))

        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        # Prepare/Clean download dir
        if download_dir is None:
            download_dir = application.output_dir
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

        try:
            lrms = self._get_backend(job.resource_name)
            self.authorization.get(lrms._resource.authorization_type)
            lrms.get_results(app, download_dir)
        except DataStagingError:
            job.signal = Run.Signals.DataStagingFailure
            raise
        
        # successfully downloaded results
        job.log.append("Output downloaded to '%s'" % download_dir)
        app.output_dir = download_dir
        if job.state == Run.State.TERMINATED:
            app.final_output_retrieved = True
            if hasattr(app, 'postprocess'):
                app.postprocess(download_dir)
        return download_dir
        

    def update_resource_status(self,resource_name, **kw):
        """ List status of a given resource."""
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
        lrms = self._get_backend(resource_name)
        self.authorization.get(lrms._resource.authorization_type)
        return  lrms.get_resource_status()


    def kill(self, app, **kw):
        """
        Terminate a job.

        Terminating a job in RUNNING, SUBMITTED, or STOPPED state
        entails canceling the job with the remote execution system;
        terminating a job in the NEW or TERMINATED state is a no-op.
        """
        job = app.execution
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
        lrms = self._get_backend(job.resource_name)
        self.authorization.get(lrms._resource.authorization_type)
        lrms.cancel_job(app)
        gc3libs.log.debug("Setting job '%s' status to TERMINATED"
                          " and returncode to SIGCANCEL" % job)
        job.state = Run.State.TERMINATED
        job.signal = Run.Signals.Cancelled
        job.log.append("Cancelled by `Core.kill`")
        if hasattr(job, 'terminated'):
            job.terminated()
        return job


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

            # Get authorization
            auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
            _lrms = self._get_backend(job.resource_name)
            self.authorization.get(_lrms._resource.authorization_type)

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
                gc3libs.log.debug('Creating instance of type %s for %s', _resource.type, _resource.name)
                try:
                    if _resource.type is Default.ARC_LRMS:
                        _lrms = ArcLrms(_resource, self.authorization)
                    elif _resource.type is Default.SGE_LRMS:
                        _lrms = SgeLrms(_resource, self.authorization)
                    else:
                        gc3libs.log.error('Unknown resource type %s',_resource.type)
                        raise Exception('Unknown resource type')
                except:
                    gc3libs.log.error('Exception creating LRMS instance %s',_resource.type)
                    raise

        if _lrms is None:
            raise ResourceNotFoundError("Cannot find computational resource '%s'" % resource_name)

        return _lrms


# === Configuration File

def import_config(config_file_locations, auto_enable_auth=True):
    (resources, authorizations) = read_config(*config_file_locations)
    return (get_resources(resources), 
            get_authorization(authorizations,auto_enable_auth), 
            auto_enable_auth)


def get_authorization(authorizations,auto_enable_auth):
    try:
        return Auth(authorizations, auto_enable_auth)
    except:
        gc3libs.log.critical('Failed initializing Authorization module')
        raise


def get_resources(resources_list):
    # build Resource objects from the list returned from read_config
    #        and match with selectd_resource from comand line
    #        (optional) if not options.resource_name is None:
    resources = [ ]
    try:
        for key in resources_list.keys():
            resource = resources_list[key]
            gc3libs.log.debug('creating instance of Resource object... ')
            try:
                tmpres = gc3libs.Resource.Resource(resource)
            except Exception, x:
                gc3libs.log.error("rejecting resource '%s': %s: %s",
                                   key, x.__class__.__name__, str(x))
                continue
            gc3libs.log.debug('Checking resource type %s',resource['type'])
            if resource['type'] == 'arc':
                tmpres.type = gc3libs.Default.ARC_LRMS
            elif resource['type'] == 'ssh_sge':
                tmpres.type = gc3libs.Default.SGE_LRMS
            else:
                gc3libs.log.error('No valid resource type %s',resource['type'])
                continue
            gc3libs.log.debug('checking validity with %s',str(tmpres.is_valid()))
            resources.append(tmpres)
    except:
        gc3libs.log.critical('failed creating Resource list')
        raise
    return resources

                                
def read_config(*locations):
    """
    Read each of the configuration files listed in `locations`, and
    return a `(defaults, resources, authorizations)` triple that can
    be passed to the `Core` class constructor.
    """
    files_successfully_read = 0
    defaults = { }
    resources = gc3libs.utils.defaultdict(lambda: dict())
    authorizations = gc3libs.utils.defaultdict(lambda: dict())

    for location in locations:
        location = os.path.expandvars(location)
        if os.path.exists(location) and os.access(location, os.R_OK):
            gc3libs.log.debug("Core.read_config(): reading file '%s' ..." % location)
        else:
            gc3libs.log.debug("Core.read_config(): ignoring non-existent file '%s' ..." % location)
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
            if sectname.startswith('authorization/'):
                # handle authorization section
                gc3libs.log.debug("Core.read_config(): adding authorization '%s' ", sectname)
                # extract authorization name and register authorization dictionary
                auth_name = sectname.split('/', 1)[1]
                authorizations[auth_name].update(dict(config.items(sectname)))

            elif  sectname.startswith('resource/'):
                # handle resource section
                resource_name = sectname.split('/', 1)[1]
                gc3libs.log.debug("Core.read_config(): adding resource '%s' ", resource_name)
                config_items = dict(config.items(sectname))
                if config_items.has_key('enabled'):
                    config_items['enabled'] = utils.string_to_boolean(config_items['enabled'])
                    gc3libs.log.debug("Resource '%s': enabled=%s in file '%s'", 
                                       resource_name, config_items['enabled'], location)
                resources[resource_name].update(config_items)
                resources[resource_name]['name'] = resource_name

            else:
                # Unhandled sectname
                gc3libs.log.error("Core.read_config(): unknown configuration section '%s' -- ignoring!", 
                                   sectname)

        gc3libs.log.debug("Core.read_config(): read %d resources from configuration file '%s'",
                           len(resources), location)

        # remove disabled resources
        disabled_resources = [ ]
        for resource in resources.values():
            if resource.has_key('enabled') and not resource['enabled']:
                disabled_resources.append(resource['name'])
        for resource_name in disabled_resources:
            gc3libs.log.info("Ignoring computational resource '%s'"
                              " because of 'enabled=False' setting.",
                              resource_name)
            del resources[resource_name]

    if files_successfully_read == 0:
        raise NoConfigurationFile("Could not read any configuration file; tried locations '%s'."
                                  % str.join("', '", locations))

    return (resources, authorizations)



class Engine(object):
    """
    Submit jobs in a collection, and update their state until a
    terminal state is reached. Specifically:
      
      * jobs in `NEW` state are submitted;
      * the state of jobs in `SUBMITTED`, `RUNNING` or `STOPPED` state is updated;
      * when a job reaches `TERMINATED` state, its output is downloaded.

    The behavior of `Engine` instances can be further customized by
    setting the following instance attributes:

      `can_submit`
        Boolean value: if `False`, no job will be submitted.

      `can_retrieve`
        Boolean value: if `False`, no output will ever be retrieved.

      `max_in_flight`
        If >0, limit the number of jobs in `SUBMITTED` or `RUNNING`
        state: if the number of jobs in `SUBMITTED`, `RUNNING` or
        `STOPPED` state is greater than `max_in_flight`, then no new
        submissions will be attempted.

      `max_submitted` 
        If >0, limit the number of jobs in `SUBMITTED` state: if the
        number of jobs in `SUBMITTED`, `RUNNING` or `STOPPED` state is
        greater than `max_submitted`, then no new submissions will be
        attempted.

    Any of the above can also be set by passing a keyword argument to
    the constructor::

      >>> e = Engine(can_submit=False)
      >>> e.can_submit
      False
    """


    def __init__(self, grid, jobs=list(), store=None, 
                 can_submit=True, can_retrieve=True, max_in_flight=0, max_submitted=0):
        """
        Create a new `Engine` instance.  Arguments are as follows: 

        `grid`
          A `gc3libs.Core` instance, that will be used to operate on
          jobs.  This is the only required argument.

        `jobs`
          Initial list of jobs to be managed by this Engine.  Jobs can
          be later added and removed with the `add` and `remove`
          methods (which see).  Defaults to the empty list.

        `store`
          An instance of `gc3libs.persistence.Store`, or `None`.  If
          not `None`, it will be used to persist jobs after each
          iteration; by default no store is used so no job state is
          persisted.

        `can_submit`, `can_retrieve`, `max_in_flight`, `max_submitted`
          Optional keyword arguments; see `Engine` for a description.
        """
        # internal-use attributes
        self._new = []
        self._in_flight = []
        self._stopped = []
        self._terminated = []
        self._core = grid
        self._store = store
        for job in jobs:
            self.add(job)
        # public attributes
        self.can_submit = can_submit
        self.can_retrieve = can_retrieve
        self.max_in_flight = max_in_flight
        self.max_submitted = max_submitted

            
    def add(self, job):
        """Add `job` to the list of jobs managed by this Engine."""
        state = job.state
        if Run.State.NEW == state:
            self._new.append(job)
        elif Run.State.SUBMITTED == state or Run.State.RUNNING == state:
            self._in_flight.append(job)
        elif Run.State.STOPPED == state:
            self._stopped.append(job)
        elif Run.State.TERMINATED == state:
            self._terminated.append(job)
        else:
            raise AssertionError("Unhandled job state '%s' in gc3libs.core.Engine." % state)


    def remove(self, job):
        """Remove a `job` from the list of jobs managed by this Engine."""
        state = job.state
        if Run.State.NEW == state:
            self._new.remove(job)
        elif Run.State.SUBMITTED == state or Run.State.RUNNING == state:
            self._in_flight.remove(job)
        elif Run.State.STOPPED == state:
            self._stopped.remove(job)
        elif Run.State.TERMINATED == state:
            self._terminated.remove(job)
        else:
            raise AssertionError("Unhandled job state '%s' in gc3libs.core.Engine." % state)
        

    def progress(self):
        """
        Update state of all registered jobs and take appropriate action.
        Specifically:

          * jobs in `NEW` state are submitted;
          * the state of jobs in `SUBMITTED`, `RUNNING` or `STOPPED` state is updated;
          * when a job reaches `TERMINATED` state, its output is downloaded.

        The `max_in_flight` and `max_submitted` limits (if >0) are
        taken into account when attempting submission of jobs.
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

        # update status of SUBMITTED/RUNNING jobs before launching
        # new ones, otherwise we would be checking the status of
        # some jobs twice...
        transitioned = []
        for index, job in enumerate(self._in_flight):
            try:
                self._core.update_job_state(job)
                if self._store:
                    store.save(job)
                if job.state == Run.State.SUBMITTED:
                    currently_submitted += 1
                    currently_in_flight += 1
                elif job.state == Run.State.RUNNING:
                    currently_in_flight += 1
                elif job.state == Run.State.STOPPED:
                    transitioned.append(index) # job changed state, mark as to remove
                    self._stopped.append(job)
                elif job.state == Run.State.TERMINATED:
                    transitioned.append(index) # job changed state, mark as to remove
                    self._terminated.append(job)
            except Exception, x:
                gc3libs.log.error("Ignoring error in updating state of job '%s': %s: %s"
                                  % (job._id, x.__class__.__name__, str(x)),
                                  exc_info=True)
        # remove jobs that transitioned to other states
        for index in reversed(transitioned):
            del self._in_flight[index]

        # update state of STOPPED jobs; again need to make before new
        # submissions, because it can alter the count of in-flight
        # jobs.
        transitioned = []
        for index, job in enumerate(self._stopped):
            try:
                self._core.update_job_state(job)
                if self._store:
                    store.save(job)
                if job.state in [Run.State.SUBMITTED, Run.State.RUNNING]:
                    currently_submitted += 1
                    currently_in_flight += 1
                    self._in_flight.append(job)
                elif job.state == Run.State.TERMINATED:
                    transitioned.append(index) # job changed state, mark as to remove
                    self._terminated.append(job)
            except Exception, x:
                gc3libs.log.error("Ignoring error in updating state of STOPPED job '%s': %s: %s"
                                  % (job._id, x.__class__.__name__, str(x)),
                                  exc_info=True)
        # remove jobs that transitioned to other states
        for index in reversed(transitioned):
            del self._stopped[index]

        # now try to submit NEW jobs
        transitioned = []
        if self.can_submit:
            for index, job in enumerate(self._new):
                # try to submit; go to SUBMITTED if successful, FAILED if not
                if currently_submitted < limit_submitted and currently_in_flight < limit_in_flight:
                    try:
                        self._core.submit(job)
                        if self._store:
                            store.save(job)
                        transitioned.append(index)
                        self._submitted.append(job)
                        currently_submitted += 1
                        currently_in_flight += 1
                    except Exception, x:
                        gc3libs.log.error("Error in submitting job '%s': %s: %s"
                                          % (job._id, x.__class__.__name__, str(x)))
                        job.log("Submission failed: %s: %s" % (x.__class__.__name__, str(x)))
        # remove jobs that transitioned to SUBMITTED state
        for index in reversed(transitioned):
            del self._new[index]

        # finally, retrieve output of finished jobs
        if self.can_retrieve:
            for index, job in enumerate(self._terminated):
                if not job.final_output_retrieved:
                    # try to get output
                    try:
                        self._core.fetch_output(job)
                        if self._store:
                            store.save(job)
                    except Exception, x:
                        gc3libs.log.error("Got error in fetching output of job '%s': %s: %s" 
                                          % (job._id, x.__class__.__name__, str(x)), exc_info=True)


    # implement a Core-like interface, so `Engine` objects can be used
    # as substitutes for `Core`.

    def free(job):
        """
        Proxy for `Core.free` (which see); in addition, remove `job`
        from the list of managed jobs.
        """
        self.remove(job)
        self._core.free(job)


    def submit(self, job):
        """
        Submit `job` at the next invocation of `perform`.  Actually,
        the job is just added to the collection of managed jobs,
        regardless of its state.
        """
        return self.add(job)


    def update_job_state(self, *jobs):
        """
        Return list of *current* states of the given jobs.  States
        will only be updated at the next invocation of `perform`; in
        particular, no state-change handlers are called as a result of
        calling this method.
        """
        return [job.state for job in jobs]


    def fetch_output(self, job):
        """
        Proxy for `Core.fetch_output` (which see).
        """
        return self._core.fetch_output(job)

    def kill(self, job):
        """
        Proxy for `Core.kill` (which see).
        """
        self._core.kill(job)

    def peek(self, job, what='stdout', offset=0, size=None, **kw):
        """
        Proxy for `Core.peek` (which see).
        """
        return self._core.peek(job, what, offset, size, **kw)
