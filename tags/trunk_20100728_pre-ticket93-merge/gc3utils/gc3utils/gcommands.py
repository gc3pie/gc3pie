#!/usr/bin/env python
"""
Implementation of the `gcli` command-line front-ends.
"""
__version__="0.4"

__author__="Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>, Riccardo Murri <riccardo.murri@uzh.ch>"
__date__="01 May 2010"
__copyright__="Copyright (c) 2009,2010 Grid Computing Competence Center, University of Zurich"

__docformat__='reStructuredText'


import sys
import os
import ConfigParser
from optparse import OptionParser

import gc3utils
import gc3utils.Application
import gc3utils.ArcLRMS
import gc3utils.Default
from gc3utils.Exceptions import *
import gc3utils.Job
import gc3utils.Resource
import gc3utils.SshLRMS
import gc3utils.gcli
import gc3utils.utils


# defaults - XXX: do they belong in ../gcli.py instead?
_homedir = os.path.expandvars('$HOME')
_rcdir = _homedir + "/.gc3"
_default_config_file_location = _rcdir + "/gc3utils.conf"
_default_joblist_file = _rcdir + "/.joblist"
_default_joblist_lock = _rcdir + "/.joblist_lock"
_default_job_folder_location = os.getcwd()
_default_wait_time = 3 # XXX: does it really make sense to have a default wall-clock time??


def _configure_logger(verbosity):
    """
    Configure the logger verbosity.
    """
    logging_level = max(1, (5-verbosity)*10)
    gc3utils.log.setLevel(logging_level)


def _get_gcli(config_file_path = _default_config_file_location, auto_enable_auth=True):
    """
    Return a `gc3utils.gcli.Gcli` instance configured by parsing
    the configuration file located at `config_file_path`.
    (Which defaults to `Defaults.config_file`.)

    If `auto_enable_auth` is `True` (default), then `Gcli` will try to renew
    expired credentials; this requires interaction with the user and will
    certainly fail unless stdin & stdout are not connected to a terminal.
    """
    try:
        (default, resources, auto_enable_auth) = gc3utils.utils.import_config(config_file_path,
                                                                              auto_enable_auth)
        gc3utils.log.debug('Creating instance of Gcli')
        return gc3utils.gcli.Gcli(default, resources, auto_enable_auth)
    except NoResources:
        raise FatalError("No computational resources defined.  Please edit the configuration file '%s'." 
                         % config_file_path)
    except:
        gc3utils.log.debug("Failed loading config file from '%s'", config_file_path)
        raise

def _print_job_info(job_obj):
    for key in job_obj.keys():
        if not key == 'log' and not ( str(job_obj[key]) == '-1' or  str(job_obj[key]) == '' ):
            if key == 'status':
                print("%-20s  %-10s " % (key, gc3utils.utils.job_status_to_string(job_obj[key])))
            else:
                print("%-20s  %-10s " % (key,job_obj[key]))
    return 0
        
def _print_job_status(job_list,job_status_filter):
    if len(job_list) > 0:
        print("%-16s  %-10s" % ("Job ID", "Status"))
        print("===========================")
        # FIXME: this is FRAGILE as it makes an assumption about how the
        # job.unique_token is formed; it would be *much* better if the
        # JobId was made into an object, and this becomes its __cmp__
        # method; that way all the code manipulating a job ID would be
        # in the `JobId` class...
        def cmp_job_ids(a,b):
            """
            Compare two job IDs `job.XXX` and `job.YYY`
            by numerical comparison of XXX and YYY.
            """
            return cmp(int(a.unique_token[4:]), 
                       int(b.unique_token[4:]))
        for _job in sorted(job_list, cmp=cmp_job_ids):
            gc3utils.log.debug('displaying job status %d',_job.status)
            if job_status_filter > 0:
                if not _job.status == job_status_filter:
                    continue
            print("%-16s  %-10s" 
                  % (_job.unique_token, 
                     gc3utils.utils.job_status_to_string(_job.status)))


#====== Main ========

def gclean(*args, **kw):
    """The 'glean' command."""
    parser = OptionParser(usage="Usage: %prog [options] JOBID")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    if len(args) != 1:
        raise InvalidUsage('Wrong number of arguments: this commands expects exactly one  arguments.')


    job = gc3utils.utils.get_job(args[0])
    if job.status == gc3utils.Job.JOB_STATE_COMPLETED or job.status == gc3utils.Job.JOB_STATE_FAILED or job.status == gc3utils.Job.JOB_STATE_DELETED:
        return gc3utils.utils.clean_job(args[0])
    else:
        raise Exception('Job is not in terminal state')


def ginfo(*args, **kw):
    """The 'ginfo' command."""
    parser = OptionParser(usage="Usage: %prog [options] JOBID")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    if len(args) != 1:
        raise InvalidUsage('Wrong number of arguments: this commands expects exactly one  arguments.')

    try:
        _print_job_info(gc3utils.utils.get_job(args[0]))
        return 0
    except:
        raise


def gsub(*args, **kw):
    """The `gsub` command."""
    # Parse command line arguments
    parser = OptionParser(usage="%prog [options] APPLICATION INPUTFILE")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    parser.add_option("-r", "--resource", action="store", dest="resource_name", metavar="STRING", default=None, help='Select resource destination')
    parser.add_option("-d", "--jobdir", action="store", dest="job_local_dir", metavar="STRING", default=gc3utils.Default.JOB_FOLDER_LOCATION, help='Select job local folder location')
    parser.add_option("-c", "--cores", action="store", dest="ncores", metavar="INT", default=0, help='Set number of requested cores')
    parser.add_option("-m", "--memory", action="store", dest="memory_per_core", metavar="INT", default=0, help='Set memory per core request (GB)')
    parser.add_option("-w", "--walltime", action="store", dest="walltime", metavar="INT", default=0, help='Set requested walltime (hours)')
    parser.add_option("-a", "--args", action="store", dest="application_arguments", metavar="STRING", default=None, help='Application arguments')

    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    if len(args) < 1:
        raise InvalidUsage('Wrong number of arguments: this commands expects at least 1 arguments: application_tag')

    application_tag = args[0]

    if application_tag == 'gamess':
        if len(args) != 2:
            raise InvalidUsage('Wrong number of arguments: this commands expects exactly two arguments.')
        application = gc3utils.Application.Application(
            application_tag=application_tag,
            inputs=[args[1]],
            job_local_dir=options.job_local_dir,
            requested_memory=options.memory_per_core,
            requested_cores=options.ncores,
            requestd_resource=options.resource_name,
            requested_walltime=options.walltime,
            application_arguments=options.application_arguments
            )
    elif application_tag == 'rosetta':
        if len(args) != 4:
            raise InvalidUsage('Wrong number of arguments: this commands expects exactly three arguments.')
        application = gc3utils.Application.Rosetta(
            application_tag=application_tag,
            executable=args[1],
            inputs=[args[2],args[3]],
            outputs=[os.path.splitext(os.path.basename(args[2]))[0]],
            job_local_dir=options.job_local_dir,
            requested_memory=options.memory_per_core,
            requested_cores=options.ncores,
            requestd_resource=options.resource_name,
            requested_walltime=options.walltime,
            application_arguments=options.application_arguments
            )

    _gcli = _get_gcli(_default_config_file_location)
    if options.resource_name:
        _gcli.select_resource(options.resource_name)
        gc3utils.log.info("Retained only resources: %s (restricted by command-line option '-r %s')",
                          str.join(",", [res['name'] for res in _gcli._resources]), 
                          options.resource_name)

    job = _gcli.gsub(application)

    if job.is_valid():
        print("Successfully submitted %s; use the 'gstat' command to monitor its progress." 
              % job.unique_token)
        gc3utils.utils.persist_job(job)
        return 0
    else:
        raise Exception('Job object not valid')


def grid_credential_renew(*args, **kw):                                        
    parser = OptionParser(usage="Usage: %prog [options] USERNAME")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)
    
    try:
        _aai_username = args[0]
    except IndexError:
        raise InvalidUsage("Missing required argument USERNAME; this command requires your AAI/SWITCH username.")
        
    gc3utils.log.debug('Checking grid credential')
    _gcli = _get_gcli(_default_config_file_location)
    if not _gcli.check_authentication(gc3utils.Default.SMSCG_AUTHENTICATION):
        return _gcli.enable_authentication(gc3utils.Default.SMSCG_AUTHENTICATION)
    else:
        return True


def gstat(*args, **kw):                        
    """The `gstat` command."""
    # FIXME: should accept list of JOBIDs and return status of all of them!
    parser = OptionParser(usage="Usage: %prog [options] JOBID")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    parser.add_option("-s", action="store", dest="job_status_filter", metavar="INT", default=0, help="only select jobs whose status is INT")
    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    try:
        _gcli = _get_gcli(_default_config_file_location)
        if len(args) == 0:
            job_list = _gcli.gstat(None)
        elif len(args) == 1:
            job_list = _gcli.gstat(gc3utils.utils.get_job(args[0]))
        else:
            raise InvalidUsage("This command requires either one argument or no arguments at all.")
    except Exception, x:
        # FIXME: this `if` can go away once all exceptions do the logging in their ctor.
        if isinstance(x, InvalidUsage):
            raise
        else:
            gc3utils.log.critical('Failed retrieving job status')
            raise

    # Check validity of returned list
    for _job in job_list:
        if not _job.is_valid():
            # FIXME: under what conditions a returned job can be
            # invalid? My first guess is that this should not happen,
            # i.e., it is a bug in Gcli.gstat() if returned jobs are
            # invalid...
            gc3utils.log.error('Returned job not valid. Removing from list')
            job_list.remove(_job)
        #else:
        #    gc3utils.utils.persist_job(_job)
        #    # gc3utils.utils.persist_job_filesystem(_job)
                                        
    try:
        # Print result
        _print_job_status(job_list,int(options.job_status_filter))
    except:
        gc3utils.log.error('Failed displaying job status results')
        raise

    for _job in job_list:
        gc3utils.utils.persist_job(_job)         

    return 0


def gget(*args, **kw):
    """ The `gget` command."""
    parser = OptionParser(usage="Usage: %prog [options] JOBID")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)
    
    # FIXME: should take possibly a list of JOBIDs and get files for all of them
    if len(args) != 1:
        raise InvalidUsage("This command requires either one argument (the JOBID) or none.")
    unique_token = args[0]

    _gcli = _get_gcli(_default_config_file_location)
    # FIXME: gget should raise exception when something goes wrong; does it indeed?
    job_obj = gc3utils.utils.get_job(unique_token)

    gc3utils.log.debug('job status [%d]',job_obj.status)
    
    if job_obj.status == gc3utils.Job.JOB_STATE_COMPLETED or job_obj.status == gc3utils.Job.JOB_STATE_FAILED:
        if job_obj.has_key('download_dir'):
            sys.stdout.write('Job already retrieved in [ '+job_obj.download_dir+' ]\n')
        else:
            # when this happen ?
            sys.stdout.write('Job cold not be retirieved any furhter\n')
        sys.stdout.flush
    else:
        if  job_obj.status == gc3utils.Job.JOB_STATE_FINISHED or job_obj.status == gc3utils.Job.JOB_STATE_DELETED:
            try:
                gc3utils.log.debug('running gcli.gget')
                job_obj = _gcli.gget(job_obj)
            except:
                gc3utils.log.error('gget failed ')
                raise
            
            if job_obj.status == gc3utils.Job.JOB_STATE_COMPLETED or job_obj.status == gc3utils.Job.JOB_STATE_FAILED:
                gc3utils.utils.persist_job(job_obj)
                if job_obj.has_key('download_dir'):
                    print("Job results successfully retrieved in '%s'\n" % job_obj.download_dir)
                else:
                    raise Exception('Job marked as completed but no results fetched')
        else:
            raise Exception("job status not ready for retrieving results")


def gkill(*args, **kw):
    """The `gkill` command."""
    parser = OptionParser(usage="%prog [options] unique_token")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    (options, args) = parser.parse_args(list(args))
    _configure_logger(options.verbosity)

    shortview = True

    if len(args) != 1:
        raise InvalidUsage("This command expects exactly one argument.")
    unique_token = args[0]

    try:
        _gcli = _get_gcli(_default_config_file_location)
        # FIXME: gget should raise exception when something goes wrong; does it indeed?
        job_obj = gc3utils.utils.get_job(unique_token)

        if not (job_obj.status == gc3utils.Job.JOB_STATE_COMPLETED or job_obj.status == gc3utils.Job.JOB_STATE_FAILED or job_obj.status == gc3utils.Job.JOB_STATE_DELETED):
            job_obj = _gcli.gkill(job_obj)
            gc3utils.utils.persist_job(job_obj)

        # or shall we simply return an ack message ?
        sys.stdout.write('Sent request to kill job ' + unique_token +'\n')
        sys.stdout.write('It may take a few moments for the job to finish.\n\n')
        sys.stdout.flush()
        # raise Exception('cannot cancel a finished job')
        
    except:
        gc3utils.log.critical('program terminated due to:  %s',sys.exc_info()[1])
        raise Exception("gkill terminated")


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

    _gcli = _get_gcli(_default_config_file_location)
    resource_object = _gcli.glist(resource_name)
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
