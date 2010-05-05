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
#import gc3utils.ArcLRMS
import gc3utils.Default
from gc3utils.Exceptions import *
import gc3utils.Job
import gc3utils.Resource
#import gc3utils.SshLRMS
#import gc3utils.gcli
#import gc3utils.utils


# defaults - XXX: do they belong in ../gcli.py instead?
_homedir = os.path.expandvars('$HOME')
_rcdir = _homedir + "/.gc3"
_default_config_file_location = _rcdir + "/config"
_default_joblist_file = _rcdir + "/.joblist"
_default_joblist_lock = _rcdir + "/.joblist_lock"
_default_job_folder_location = os.getcwd()
_default_wait_time = 3 # XXX: does it really make sense to have a default wall-clock time??


def _get_gcli(config_file_path = _default_config_file_location):
    """
    Return a `gc3utils.gcli.Gcli` instance configured by parsing
    the configuration file located at `config_file_path`.
    """
    try:
        # Read configuration file to create Resource lists and default values
        # resources_list is a list of resource dictionaries
        (defaults,resources_list) = gc3utils.utils.read_config(config_file_path)
    except:
        gc3utils.log.debug("Failed loading config file from '%s'", config_file_path)
        raise
    # build Gcli object
    resources = _get_resources(options, resources_list)
    default = _get_defaults(defaults)
    gc3utils.log.debug('Creating instance of Gcli')
    return gc3utils.gcli.Gcli(default, resources)


def _get_defaults(defaults):
    # Create an default object for the defaults
    # defaults is a list[] of values
    try:
        # Create default values
        default = gc3utils.Default.Default(
            homedir=gc3utils.Default.HOMEDIR,
            config_file_location=gc3utils.Default.CONFIG_FILE_LOCATION,
            joblist_location=gc3utils.Default.JOBLIST_FILE,
            joblist_lock=gc3utils.Default.JOBLIST_LOCK,
            job_folder_location=gc3utils.Default.JOB_FOLDER_LOCATION
            )
        
        # Overwrite with what has been read from config 
        for default_values in defaults:
            default.insert(default_values,defaults[default_values])
            if not default.is_valid():
                raise Exception('defaults not valid')
    except:
        gc3utils.log.critical('Failed loading default values')
        raise

    return default


def _get_resources(options, resources_list):
    # build Resource objects from the list returned from read_config
    #        and match with selectd_resource from comand line
    #        (optional) if not options.resource_name is None:
    resources = []

    try:
        for resource in resources_list:
            if (options.resource_name):
                if (not options.resource_name is resource['name']):
                    gc3utils.log.debug("Ignoring resource '%s', because resource '%s' was explicitly requested.",
                                       resource['name'], options.resource_name)
                    continue
            gc3utils.log.debug('creating instance of Resource object... ')
            tmpres = gc3utils.Resource.Resource()

            for items in resource:
                gc3utils.log.debug('Updating with %s %s',items,resource[items])
                tmpres.insert(items,resource[items])

            gc3utils.log.debug('Checking resource type %s',resource['type'])
            if resource['type'] == 'arc':
                tmpres.insert("type",gc3utils.Default.ARC_LRMS)
            elif resource['type'] == 'ssh_sge':
                tmpres.insert("type",gc3utils.Default.SGE_LRMS)
            else:
                gc3utils.log.error('No valid resource type %s',resource['type'])
                continue

            gc3utils.log.debug('checking validity with %s',str(tmpres.is_valid()))
            
            if tmpres.is_valid():
                resources.append(tmpres)
            else:
                gc3utils.log.warning("Resource '%s' failed validity test - rejecting it.",
                                     resource['name'])
                    
    except:
        gc3utils.log.critical('failed creating Resource list')
        raise

    return resources


#====== Main ========

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

    if len(args) != 2:
        raise InvalidUsage('Wrong number of arguments: this commands expects exactly two.')

    application_tag = args[0]

    # check input file
    if ( not gc3utils.utils.check_inputfile(args[1]) ):
        gc3utils.log.critical('Cannot find input file: '+args[1])
        raise Exception('invalid input-file argument')
    input_file_name = args[1]

    # Create Application obj
    application = gc3utils.Application.Application(
        application_tag=application_tag,
        input_file_name=input_file_name,
        job_local_dir=options.job_local_dir,
        requested_memory=options.memory_per_core,
        requested_cores=options.ncores,
        requestd_resource=options.resource_name,
        requested_walltime=options.walltime,
        application_arguments=options.application_arguments
        )

    if not application.is_valid():
        raise Exception('Failed creating application object')

    job = _gcli.gsub(application)

    if job.is_valid():
        gc3utils.utils.display_job_status([job])
        return 0
    else:
        raise Exception('Job object not valid')


def grid_credential_renew(*args, **kw):                                        
    parser = OptionParser(usage="Usage: %prog [options] USERNAME")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    (options, args) = parser.parse_args(list(args))
    
    try:
        _aai_username = args[0]
    except IndexError:
        raise InvalidUsage("Missing required argument USERNAME; this command requires your AAI/SWITCH username.")
        
    gc3utils.log.debug('Checking grid credential')
    if not _gcli.check_authentication(gc3utils.Default.SMSCG_AUTHENTICATION):
        return _gcli.enable_authentication(gc3utils.Default.SMSCG_AUTHENTICATION)
    else:
        return True


def gstat(*args, **kw):                        
    """The `gstat` command."""
    # FIXME: should accept list of JOBIDs and return status of all of them!
    parser = OptionParser(usage="Usage: %prog [options] JOBID")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    (options, args) = parser.parse_args(list(args))

    try:
        if len(args) == 0:
            job_list = _gcli.gstat()
        if len(args) == 1:
            unique_token = args[1]
            job_list = _gcli.gstat(gc3utils.utils.get_job_from_filesystem(unique_token,default.job_file))
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
            gc3utils.log.error('Returned job not valid. Removing from list')
            #job_list.
            #### SERGIO: STOPPED WORKING HERE
    try:
        # Print result
        gc3utils.utils.display_job_status(job_list)
    except:
        gc3utils.log.error('Failed displaying job status results')
        raise

    return 0


def gget(*args, **kw):
    """ The `gget` command."""
    parser = OptionParser(usage="Usage: %prog [options] JOBID")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    (options, args) = parser.parse_args(list(args))
    
    # FIXME: should take possibly a list of JOBIDs and get files for all of them
    if len(args) != 1:
        raise InvalidUsage("This command requires either one argument (the JOBID) or none.")
    unique_token = args[0]

    # FIXME: gget should raise exception when something goes wrong; does it indeed?
    retval = _gcli.gget(unique_token)
    sys.stdout.write('Job results successfully retrieved in directory: '+unique_token+'\n')
    sys.stdout.flush()


def gkill(*args, **kw):
    raise NotImplementedError("Command 'gkill' has not been implemented yet.")



def glist(*args, **kw):
    """The `glist` command."""
    parser = OptionParser(usage="Usage: %prog [options] resource_name")
    parser.add_option("-v", action="count", dest="verbosity", default=0, help="Set verbosity level")
    (options, args) = parser.parse_args()

    # FIXME: should take possibly a list of JOBIDs and get files for all of them
    if len(args) != 1:
        raise InvalidUsage("This command requires either one argument (the JOBID) or none.")
    resource_name = args[0]

    # FIXME: gcli.glist should throw exception, we should not check return value here
    (retval,resource_object) = _gcli.glist(resource_name)
    if (not retval):
        if resource_object.__dict__.has_key("resource_name"):
            sys.stdout.write('Resource Name: '+resource_object.__dict__["resource_name"]+'\n')
        if resource_object.__dict__.has_key("total_slots"):
            sys.stdout.write('Total cores: '+str(resource_object.__dict__["total_slots"])+'\n')
        if resource_object.__dict__.has_key("total_runnings"):
            sys.stdout.write('Total runnings: '+str(resource_object.__dict__["total_runnings"])+'\n')
        if resource_object.__dict__.has_key("total_queued"):
            sys.stdout.write('Total queued: '+str(resource_object.__dict__["total_queued"])+'\n')
        if resource_object.__dict__.has_key("memory_per_core"):
            sys.stdout.write('Memory per core: '+str(resource_object.__dict__["memory_per_core"])+'\n')
        sys.stdout.flush()
    else:
        raise Exception("glist terminated")
