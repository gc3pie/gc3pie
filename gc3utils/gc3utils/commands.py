#!/usr/bin/env python
#
"""
Implementation of the `core` command-line front-ends.
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
__docformat__='reStructuredText'
__version__ = '$Revision$'

__author__="Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>, Riccardo Murri <riccardo.murri@uzh.ch>"
__date__ = '$Date$'
__copyright__="Copyright (c) 2009,2010 Grid Computing Competence Center, University of Zurich"



import sys
import os
import ConfigParser
from optparse import OptionParser
import tarfile
import time

from gc3libs import Application, Run
import gc3libs.application.gamess as gamess
import gc3libs.application.rosetta as rosetta
import gc3libs.Default as Default
from   gc3libs.Exceptions import *
import gc3libs.Job as Job
import gc3libs.core as core
import gc3libs.persistence
import gc3libs.utils as utils


import gc3utils


# defaults - XXX: do they belong in ../core.py instead?
_homedir = os.path.expandvars('$HOME')
_rcdir = _homedir + "/.gc3"
_default_config_file_locations = [ "/etc/gc3/gc3pie.conf", _rcdir + "/gc3pie.conf" ]
_default_joblist_file = _rcdir + "/.joblist"
_default_joblist_lock = _rcdir + "/.joblist_lock"
_default_job_folder_location = os.getcwd()
_default_wait_time = 3 # XXX: does it really make sense to have a default wall-clock time??


class _JobIdFactory(gc3libs.persistence.Id):
    """
    Override :py:class:`Id` behavior and generate IDs starting with a
    lowercase ``job`` prefix.
    """
    def __new__(cls, obj, prefix=None, seqno=None):
        return gc3libs.persistence.Id.__new__(cls, obj, 'job', seqno)

_store = gc3libs.persistence.FilesystemStore(idfactory=_JobIdFactory)


def _configure_logger(verbosity):
    """
    Configure the logger verbosity.
    """
    logging_level = max(1, (4-verbosity)*10)
    gc3libs.log.setLevel(logging_level)
    gc3utils.log.setLevel(logging_level)


def _get_core(config_file_locations, auto_enable_auth=True):
    """
    Return a `gc3libs.core.Core` instance configured by parsing
    the configuration file(s) located at `config_file_locations`.
    Order of configuration files matters: files read last overwrite
    settings from previously-read ones; list the most specific configuration
    files last.

    If `auto_enable_auth` is `True` (default), then `Core` will try to renew
    expired credentials; this requires interaction with the user and will
    certainly fail unless stdin & stdout are connected to a terminal.
    """
    # ensure a configuration file exists in the most specific location
    for location in reversed(config_file_locations):
        if os.access(os.path.dirname(location), os.W_OK|os.X_OK) \
                and not gc3libs.utils.deploy_configuration_file(location, "gc3pie.conf.example"):
            # warn user
            gc3utils.log.warning("No configuration file '%s' was found;"
                                 " a sample one has been copied in that location;"
                                 " please edit it and define resources." % location)
    try:
        gc3utils.log.debug('Creating instance of Core ...')
        return gc3libs.core.Core(* gc3libs.core.import_config(config_file_locations, auto_enable_auth))
    except NoResources:
        raise FatalError("No computational resources defined.  Please edit the configuration file '%s'." 
                         % config_file_locations)
    except:
        gc3utils.log.debug("Failed loading config file from '%s'", 
                           str.join("', '", config_file_locations))
        raise


#====== Main ========


def gclean(*args, **kw):
    """
    The 'glean' command.
    gclean takes a list of jobids and tries to clean each of them.
    if any of the clean requests will fail, gclean will exit with exitcode 1
    gclean will try anyway to process all requests
    """
    parser = OptionParser(usage="Usage: %prog [options] JOBID [JOBID ...]", 
                          version="GC3pie project version 1.0. %prog ")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    parser.add_option("-A", action="store_true", dest="all", default=False, help="Remove all stored jobs.")
    parser.add_option("-f", "--force", action="store_true", dest="force", default=False, help="Remove job even when not in terminal state.")
    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    if options.all and len(args) > 0:
        raise InvalidUsage("Option '-A' conflicts with list of job IDs to remove.")
    
    if options.all:
        args = _store.list()

    _core = _get_core(_default_config_file_locations)
    failed = 0
    for jobid in args:
        try:
            app = _store.load(jobid)
            if app.execution.state != Run.State.TERMINATED:
                if options.force:
                    gc3utils.log.warning("Job '%s' not in terminal state:"
                                         " attempting to kill before cleaning up.")
                    try:
                        _core.kill(app)
                    except Exception, ex:
                        gc3utils.log.warning("Killing job '%s' failed (%s: %s);"
                                             " continuing anyway, but errors might ensue.",
                                             app, ex.__class__.__name__, str(ex))
                else:
                    failed += 1
                    gc3utils.log.error("Job '%s' not in terminal state: ignoring.", jobid)
                    continue
            _core.free(app)
        except JobRetrieveError:
            if options.force:
                pass
            else:
                failed += 1
                gc3utils.log.error("Could not load '%s': ignoring"
                                   " (use option '-f' to remove nonetheless).", jobid)
                continue
        _store.remove(jobid)
        gc3utils.log.info("Removed job '%s'", jobid)

    if 0 == failed:
        return 0
    else:
        return 1


def ginfo(*args, **kw):
    """The 'ginfo' command."""
    parser = OptionParser(usage="Usage: %prog [options] JOBID [JOBID ...]")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    # build list of apps to query status of
    if len(args) == 0:
        apps = _get_jobs()
    else:
        apps = _get_jobs(args)

    print (78 * '=')
    for app in apps:
        for key, value in sorted(app.execution.items()):
            if options.verbosity == 0 and (key.startswith('_') 
                                           or key == 'log' 
                                           or str(value) in ['', '-1']):
                continue
            print("%-20s  %-10s " % (key, value))
        print (78 * '=')


def gsub(*args, **kw):
    """The `gsub` command."""
    # Parse command line arguments
    parser = OptionParser(usage="%prog [options] APPLICATION INPUTFILE [OTHER INPUT FILES]")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    parser.add_option("-r", "--resource", action="store", dest="resource_name", metavar="STRING", default=None, help='Select resource destination')
    parser.add_option("-d", "--jobdir", action="store", dest="output_dir", metavar="STRING", default=gc3libs.Default.JOB_FOLDER_LOCATION, help='Select job local folder location')
    parser.add_option("-c", "--cores", action="store", dest="ncores", metavar="INT", default=0, help='Set number of requested cores')
    parser.add_option("-m", "--memory", action="store", dest="memory_per_core", metavar="INT", default=0, help='Set memory per core request (GB)')
    parser.add_option("-w", "--walltime", action="store", dest="walltime", metavar="INT", default=0, help='Set requested walltime (hours)')
    parser.add_option("-a", "--args", action="store", dest="application_arguments", metavar="STRING", default=None, help='Additional application arguments')

    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    if len(args) < 1:
        raise InvalidUsage('Wrong number of arguments: this commands expects at least 1 arguments: application_tag')

    application_tag = args[0]
    if application_tag == 'gamess':
        if len(args) < 2:
            raise InvalidUsage('Wrong number of arguments: this commands expects at least two arguments.')
        app = gamess.GamessApplication(
            *args[1:], # 1st arg is .INP file path, rest are (optional) additional inputs
            **{ 
                'arguments':options.application_arguments,
                'requested_memory':int(options.memory_per_core),
                'requested_cores':int(options.ncores),
                'requestd_resource':options.resource_name,
                'requested_walltime':int(options.walltime),
                'output_dir':options.output_dir,
                }
            )
    elif application_tag == 'rosetta':
        if len(args) != 4:
            raise InvalidUsage('Wrong number of arguments: this commands expects exactly three arguments.')
        app = rosetta.RosettaApplication(
            application = args[1],
            inputs = { 
                "-in:file:s":args[2],
                "-in:file:native":args[3],
                },
            outputs = [ os.path.splitext(os.path.basename(args[2]))[0] + '.fasc' ],
            arguments=options.application_arguments,
            requested_memory=options.memory_per_core,
            requested_cores=options.ncores,
            requested_resource=options.resource_name,
            requested_walltime=options.walltime,
            output_dir=options.output_dir,
            )
    else:
        raise InvalidUsage("Unknown application '%s'" % application_tag)

    _core = _get_core(_default_config_file_locations)
    if options.resource_name:
        _core.select_resource(options.resource_name)
        gc3utils.log.info("Retained only resources: %s (restricted by command-line option '-r %s')",
                          str.join(",", [res['name'] for res in _core._resources]), 
                          options.resource_name)

    _core.submit(app)
    _store.save(app)

    print("Successfully submitted %s; use the 'gstat' command to monitor its progress." % app)
    return 0


def gresub(*args, **kw):
    """The `gresub` command: resubmit an already-submitted job with different parameters."""
    # Parse command line arguments
    parser = OptionParser(usage="%prog [options] JOBID [JOBID ...]")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    parser.add_option("-r", "--resource", action="store", dest="resource_name", metavar="STRING", default=None, help='Select resource destination')
    parser.add_option("-d", "--jobdir", action="store", dest="output_dir", metavar="STRING", default=gc3libs.Default.JOB_FOLDER_LOCATION, help='Select job local folder location')
    parser.add_option("-c", "--cores", action="store", dest="ncores", metavar="INT", default=0, help='Set number of requested cores')
    parser.add_option("-m", "--memory", action="store", dest="memory_per_core", metavar="INT", default=0, help='Set memory per core request (GB)')
    parser.add_option("-w", "--walltime", action="store", dest="walltime", metavar="INT", default=0, help='Set requested walltime (hours)')

    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    if len(args) < 1:
        raise InvalidUsage('Wrong number of arguments: this commands expects at least 1 argument: JOBID')

    _core = _get_core(_default_config_file_locations)
    if options.resource_name:
        _core.select_resource(options.resource_name)
        gc3utils.log.info("Retained only resources: %s (restricted by command-line option '-r %s')",
                          str.join(",", [res['name'] for res in _core._resources]), 
                          options.resource_name)

    failed = 0
    for jobid in args:
        app = _store.load(jobid.strip())
        try:
            _core.update_state(app) # update state
        except Exception, ex:
            # ignore errors, and proceed to resubmission anyway
            gc3utils.log.warning("Could not update state of %s: %s: %s", 
                                 jobid, ex.__class__.__name__, str(ex))
        # kill remote job
        try:
            app = _core.kill(app)
        except Exception, ex:
            # ignore errors, but alert user...
            pass

        try:
            app = _core.submit(app)
            print("Successfully re-submitted %s; use the 'gstat' command to monitor its progress." % job)
            _store.replace(jobid, app)
        except Exception, ex:
            failed += 1
            gc3utils.log.error("Failed resubmission of job '%s': %s: %s", 
                               jobid, ex.__class__.__name__, str(ex))
    return failed


def gstat(*args, **kw):                        
    """The `gstat` command."""
    parser = OptionParser(usage="Usage: %prog [options] JOBID [JOBID ...]")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    _core = _get_core(_default_config_file_locations)

    # build list of apps to query status of
    if len(args) == 0:
        apps = _get_jobs()
    else:
        apps = _get_jobs(args)

    _core.update_state(*apps)
        
    # Print result
    if len(apps) == 0:
        print ("No jobs submitted with GC3Utils.")
    else:
        print("%-16s  %-10s" % ("Job ID", "State"))
        print("===========================")
        def cmp_job_ids(a,b):
            return cmp(a._id, b._id)
        for app in sorted(apps, cmp=cmp_job_ids):
            print("%-16s  %-10s" % (app, app.execution.state))

    # save jobs back to disk
    for app in apps:
        _store.replace(app._id, app)

    return 0


def gget(*args, **kw):
    """ The `gget` command."""
    parser = OptionParser(usage="Usage: %prog [options] JOBID")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    parser.add_option("-d", "--download-dir", action="store", dest="download_dir", default=None,
                      help="Destination directory (job id will be appended to it); default is '.'")
    parser.add_option("-f", "--overwrite", action="store_true", dest="overwrite", default=False, 
                      help="Overwrite files in destination directory")
    (options, args) = parser.parse_args(list(args))

    _configure_logger(options.verbosity)

    _core = _get_core(_default_config_file_locations)
    for jobid in args:
        app = _store.load(jobid)

        if app.final_output_retrieved:
            raise InvalidOperation("Output of '%s' already downloaded to '%s'" 
                                   % (app._id, app.output_dir))

        if options.download_dir is None:
            download_dir = os.path.join(os.getcwd(), app._id)
        else:
            download_dir = options.download_dir

        _core.fetch_output(app, download_dir, overwrite=options.overwrite)
        print("Job results successfully retrieved in '%s'\n" % app.output_dir)
        _store.replace(app._id, app)


def gkill(*args, **kw):
    """
    The `gkill` command.
    gkill takes a list of jobids and tries to kill each of them.
    if any of the clean requests will fail, gkill will exit with exitcode 1
    gkill will try anyway to process all requests
    """
    parser = OptionParser(usage="%prog [options] JOBID")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    parser.add_option("-f", "--force", action="store_true", dest="force", default=False, help="Force removing job")
    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    try:
        _core = _get_core(_default_config_file_locations)

        # Assume args are all jobids
        for jobid in args:
            try:
                app = _store.load(jobid)

                gc3utils.log.debug("gkill: Job '%s' in state %s" % (jobid, app.execution.state))
                if app.execution.state == Run.State.TERMINATED:
                    raise InvalidOperation("Job '%s' is already in terminal state" % app)
                else:
                    _core.kill(app)
                    _store.replace(jobid, app)

                    # or shall we simply return an ack message ?
                    print("Sent request to cancel job '%s'."% jobid)

            except Exception, ex:
                gc3utils.log.error("gkill: Failed canceling Job '%s': %s: %s", 
                                   jobid, ex.__class__.__name__, str(ex))
                continue
    
    except Exception, ex:
        raise FatalError("gkill failed: %s: %s" % (ex.__class__.__name__, str(ex)))
        

def gtail(*args, **kw):
    """The 'gtail' command."""
    parser = OptionParser(usage="Usage: %prog [options] JOBID")
    parser.add_option("-v", "--verbose", action="count", dest="verbosity", default=0, help="Set verbosity level")
    parser.add_option("-e", "--stderr", action="store_true", dest="stderr", default=False, help="show stderr of the job")
    parser.add_option("-o", "--stdout", action="store_true", dest="stdout", default=True, help="show stdout of the job (default)")
    parser.add_option("-n", "--lines", dest="num_lines", type=int, default=10, help="output  the  last N lines, instead of the last 10")
    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    if len(args) != 1:
        raise InvalidUsage("This command requires exactly one argument: the job ID.")

    jobid = args[0]

    if options.stderr:
        std = 'stderr'
    else:
        std = 'stdout'

    _core = _get_core(_default_config_file_locations)

    app = _store.load(jobid)
    if app.execution.state == Run.State.UNKNOWN \
            or app.execution.state == Run.State.SUBMITTED:
        raise Exception('Job output not yet available')

    file_handle = _core.tail(app, std)
    for line in file_handle.readlines()[-(options.num_lines):]:
        print line.strip()

    file_handle.close()

    return 0


def gnotify(*args, **kw):
    """The gnotify command"""
    parser = OptionParser(usage="Usage: %prog [options] JOBID")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    parser.add_option("-i", "--include", action="store_true", dest="include_job_results", default=False, help="Include Job's results in notification package")
    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    if len(args) != 1:
        raise InvalidUsage("This command requires exactly one argument: the Job ID.")
    jobid = args[0]

    app = _store.load(jobid)
    try:
        # create tgz with job information
        tar_filename = gc3libs.Default.NOTIFY_DESTINATIONFOLDER + '/' + jobid + '.tgz'
        tar = tarfile.open(tar_filename, "w:gz")
        if options.include_job_results:
            try:
                for filename in os.listdir(app.output_dir):
                    tar.add(os.path.join(app.output_dir,filename))
            except Exception, ex:
                gc3libs.log.error("Could not add file '%s/%s' to tar file '%s': %s: %s", 
                                  app.output_dir, filename, tar_filename,
                                  ex.__class__.__name__, str(ex))
        # FIXME: this requires knowledge of how the persistence layer is saving jobs...
        tar.add(os.path.join(gc3libs.Default.JOBS_DIR, jobid))
        tar.close()

        # send notification email to gc3admin
        utils.send_mail(gc3libs.Default.NOTIFY_USER_EMAIL,
                        gc3libs.Default.NOTIFY_GC3ADMIN,
                        gc3libs.Default.NOTIFY_SUBJECTS,
                        gc3libs.Default.NOTIFY_MSG,
                        [tar_filename])

        return 0

    except Exception, ex:
        gc3libs.log.error('Failed creating report: %s: %s',
                          ex.__class__.__name__, str(ex))
        raise
    

def glist(*args, **kw):
    """The `glist` command."""
    parser = OptionParser(usage="Usage: %prog [options] resource_name")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    parser.add_option("-s", "--short", action="store_true", dest="shortview", help="Short view.")
    parser.add_option("-l", "--long", action="store_false", dest="shortview", help="Long view.")
    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    # FIXME: should take possibly a list of resource IDs and get files for all of them
    if len(args) != 1:
        raise InvalidUsage("This command requires exactly one argument: the resource name.")
    resource_name = args[0]

    _core = _get_core(_default_config_file_locations)
    resource_object = _core.update_resource_status(resource_name)
    if not resource_object is None:
        if resource_object.has_key("name"):
            sys.stdout.write('Resource Name: '+resource_object.name+'\n')
        if resource_object.has_key("total_cores") and resource_object.has_key("free_slots"):
            sys.stdout.write('Cores Total/Free: '+str(resource_object.total_cores)+'/'+str(resource_object.free_slots)+'\n')
        if resource_object.has_key("user_run") and resource_object.has_key("user_queued"):
            sys.stdout.write('User Jobs Running/Queued: '+str(resource_object.user_run)+'/'+str(resource_object.user_queued)+'\n')
        sys.stdout.flush()
    else:
        raise Exception("glist terminated")



## helper functions

def _get_jobs(job_ids=None, ignore_failures=True):
    """
    Return list of jobs (gc3libs.Application objects) corresponding to
    the given Job IDs.

    If `ignore_failures` is `True` (default), errors retrieving a
    job from the persistence layer are ignored and the jobid is
    skipped, therefore the returned list can be shorter than the
    list of Job IDs given as argument.  If `ignore_failures` is
    `False`, then any errors result in the relevant exception being
    re-raised.

    If `job_ids` is `None` (default), then load all jobs available
    in the persistent storage; if persistent storage does not
    implement the `list` operation, then an empty list is returned
    (when `ignore_failures` is `True`) or a `NotImplementedError`
    exception is raised (when `ignore_failures` is `False`).
    """
    if job_ids is None:
        try:
            job_ids = _store.list()
        except NotImplementedError, ex:
            if ignore_failures:
                return [ ]
            else:
                raise
    apps = [ ]
    for jobid in job_ids:
        try:
            apps.append(_store.load(jobid))
        except Exception, ex:
            if ignore_failures:
                gc3libs.log.error("Could not retrieve job '%s' (%s: %s). Ignoring.", 
                                  jobid, ex.__class__.__name__, str(ex))
                continue
            else:
                raise
    return apps


