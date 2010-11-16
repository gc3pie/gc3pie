#!/usr/bin/env python
"""
Top-level interface to Grid functionality.
"""
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
__date__ = '$Date$'


from fnmatch import fnmatch
import os
import sys
import time
import ConfigParser
import tempfile

import gc3libs
from gc3libs.application import Application
from gc3libs.backends.sge import SgeLrms
from gc3libs.backends.arc import ArcLrms
from gc3libs.authentication import Auth
import gc3libs.Default as Default
from gc3libs.Exceptions import *
import gc3libs.Job as Job
import gc3libs.Resource as Resource
import gc3libs.scheduler as scheduler
import gc3libs.utils as utils 


class Gcli:

    def __init__(self, resource_list, authorization, auto_enable_auth):
        if ( len(resource_list) == 0 ):
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


    def gsub(self, application, job=None, **kw):
        """
        Submit a job running an instance of the given `application`.
        Return the `job` object, modified to refer to the submitted computational job,
        or a new instance of the `Job` class if `job` is `None` (default).
        """
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        # gsub workflow:
        #    check input files from application
        #    create list of LRMSs
        #    do Brokering
        #    submit job
        #    return job_obj
        
        # Parsing passed arguments
        gc3libs.log.debug('input_file(s): %s',application.inputs)
        gc3libs.log.debug('application tag: %s',application.application_tag)
        gc3libs.log.debug('application arguments: %s',application.arguments)
        gc3libs.log.debug('requested cores: %s',str(application.requested_cores))
        gc3libs.log.debug('requested memory: %s GB',str(application.requested_memory))
        gc3libs.log.debug('requested walltime: %s hours',str(application.requested_walltime))

        gc3libs.log.debug('Instantiating LRMSs')
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
        if ( len(_lrms_list) == 0 ):
            raise NoResources("Could not initialize any computational resource"
                              " - please check log and configuration file.")

        gc3libs.log.debug('Performing brokering ...')
        # decide which resource to use
        # (Resource)[] = (Scheduler).PerformBrokering((Resource)[],(Application))
        _selected_lrms_list = scheduler.do_brokering(_lrms_list,application)
        gc3libs.log.debug('Scheduler returned %d matching resources',
                           len(_selected_lrms_list))
        if 0 == len(_selected_lrms_list):
            raise NoResources("Could not select any compatible computational resource"
                              " - please check log and configuration file.")

        if job is None:
            job = Job.Job()
        # Scheduler.do_brokering should return a sorted list of valid lrms
        for lrms in _selected_lrms_list:
            try:
                self.authorization.get(lrms._resource.authorization_type)
                job = lrms.submit_job(application, job)
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
            job.state = Job.State.SUBMITTED
            job.resource_name = lrms._resource.name
            #if application.has_key('output_dir'):
            #    job.output_dir = application.output_dir
            if application.has_key('default_output_dir'):
                job.default_output_dir = application.default_output_dir
            else:
                job.default_output_dir = os.getcwd()
            # job submitted; leave loop
            break

        return job


    def gstat(self, *jobs, **kw):
        """
        Update state of all jobs passed in as arguments,
        and return list of updated states.
        
        If `update_on_error` is `False` (default), then job state is
        not changed in case a communication error happens; it is
        changed to `UNKNOWN` otherwise.
        """
        update_on_error = kw.get('update_on_error', False)
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        states = [] 
        for job in jobs:
            state = job.state
            try:
                lrms = self._get_backend(job.resource_name)
                if job.state not in [ Job.State.NEW, Job.State.TERMINATED ]:
                    self.authorization.get(lrms._resource.authorization_type)
                    state = lrms.get_state(job)
                if state != Job.State.UNKNOWN or update_on_error:
                    job.state = state
            except Exception, ex:
                gc3libs.log.error("Error getting status of job '%s': %s",
                                  job, str(ex))
            states.append(state)

        return states


    def gget(self, job, download_dir=None, **kw):
        """
        Retrieve job output into local directory `download_dir`.  If
        `download_dir` is `None` (default), then its path is formed by
        appending the job ID to `default_output_dir` if the `job`
        object has such attribute, or by appending the job id to the
        default download location `gc3libs.Default.JOB_FOLDER_LOCATION`.

        The instance attribute `job.output_dir` is set to the actual
        download directory path, and `job.output_retrieved` is set to
        `True`.

        Directory `download_dir` is created if it does not exist; if
        already existent, it is renamed with a `.NUMBER` suffix and a
        new empty one is created in its place.

        Job output cannot be retrieved when `job` is in one of the
        states `NEW` or `SUBMITTED`; a `OutputNotAvailableError`
        exception is thrown in these cases.
        """
        if job.state in [ Job.State.NEW, Job.State.SUBMITTED ]:
            raise OutputNotAvailableError("Output not available: Job '%s' currently in state '%s'"
                                          % (job, job,state))

        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)

        # Prepare/Clean download dir
        if download_dir is None:
            if hasattr(job, 'output_dir'):
                download_dir = job.output_dir
            elif hasattr(job, 'default_output_dir'):
                download_dir = os.path.join(job.default_output_dir, str(job))
            else:
                download_dir = os.path.join(Default.JOB_FOLDER_LOCATION, str(job))
        try:
            utils.mkdir_with_backup(download_dir)
        except Exception, ex:
            gc3libs.log.error("Failed creating download directory '%s': %s: %s",
                              download_dir, ex.__class__.__name__, str(ex))
            raise

        try:
            lrms = self._get_backend(job.resource_name)
            self.authorization.get(lrms._resource.authorization_type)
            lrms.get_results(job, download_dir)
        except LRMSUnrecoverableError:
            # FIXME: assumes LRMS has correctly set the `returncode` attribute on the job...
            job.state = Job.State.TERMINATED
        
        # successfully downloaded results
        job.output_dir = download_dir
        job.output_retrieved = True
        job.log.append("Output downloaded to '%s'" % download_dir)
        return job
        

    def glist(self,resource_name, **kw):
        """ List status of a given resource."""
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
        lrms = self._get_backend(resource_name)
        self.authorization.get(lrms._resource.authorization_type)
        return  lrms.get_resource_status()


    def gkill(self, job, **kw):
        """Terminate a job.

        Terminating a job in RUNNING, SUBMITTED, or STOPPED state
        entails canceling the job with the remote execution system;
        terminating a job in the NEW or TERMINATED state is a no-op.
        """
        
        auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
        lrms = self._get_backend(job.resource_name)
        self.authorization.get(lrms._resource.authorization_type)
        job = lrms.cancel_job(job)

        gc3libs.log.debug("Setting job '%s' status to TERMINATED"
                          " and returncode to SIGCANCEL" % job)
        job.state = Job.State.TERMINATED
        job.signal = Job.Signals.Cancelled
        job.log.append("Cancelled by `Gcli.gkill`")
        return job


    def tail(self, job, what='stdout', offset=0, size=None, **kw):
        """
        Return job object with .stdout or .stderr containing content of stdout or stderr respectively
        Note: For the time being we allow only stdout or stderr as valid filenames
        """

        if what == 'stdout':
            remote_filename = job.stdout_filename
        elif what == 'stderr':
            remote_filename = job.stderr_filename
        else:
            raise Error("File name requested to `Gcli.tail` must be"
                        " 'stdout' or 'stderr', not '%s'" % what)

        # Check if local data available
        # cross reference check: job status and local data availability
        if job.state == gc3libs.Job.State.TERMINATED and job.output_retrieved:
            _filename = os.path.join(job.output_dir, job.jobid, remote_filename)
            _local_file = open(_filename)
        else:

            # Get authorization
            auto_enable_auth = kw.get('auto_enable_auth', self.auto_enable_auth)
            _lrms = self._get_backend(job.resource_name)
            self.authorization.get(_lrms._resource.authorization_type)

            _local_file = tempfile.NamedTemporaryFile(suffix='.tmp', prefix='gc3libs.')

            _lrms.tail(job, remote_filename, _local_file, offset, size)
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
    be passed to the `Gcli` class constructor.
    """
    files_successfully_read = 0
    defaults = { }
    resources = gc3libs.utils.defaultdict(lambda: dict())
    authorizations = gc3libs.utils.defaultdict(lambda: dict())

    for location in locations:
        location = os.path.expandvars(location)
        if os.path.exists(location) and os.access(location, os.R_OK):
            gc3libs.log.debug("gcli.read_config(): reading file '%s' ..." % location)
        else:
            gc3libs.log.debug("gcli.read_config(): ignoring non-existent file '%s' ..." % location)
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
                gc3libs.log.debug("gcli.read_config(): adding authorization '%s' ", sectname)
                # extract authorization name and register authorization dictionary
                auth_name = sectname.split('/', 1)[1]
                authorizations[auth_name].update(dict(config.items(sectname)))

            elif  sectname.startswith('resource/'):
                # handle resource section
                resource_name = sectname.split('/', 1)[1]
                gc3libs.log.debug("gcli.read_config(): adding resource '%s' ", resource_name)
                config_items = dict(config.items(sectname))
                if config_items.has_key('enabled'):
                    config_items['enabled'] = utils.string_to_boolean(config_items['enabled'])
                    gc3libs.log.debug("Resource '%s': enabled=%s in file '%s'", 
                                       resource_name, config_items['enabled'], location)
                resources[resource_name].update(config_items)
                resources[resource_name]['name'] = resource_name

            else:
                # Unhandled sectname
                gc3libs.log.error("gcli.read_config(): unknown configuration section '%s' -- ignoring!", 
                                   sectname)

        gc3libs.log.debug("gcli.read_config(): read %d resources from configuration file '%s'",
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
