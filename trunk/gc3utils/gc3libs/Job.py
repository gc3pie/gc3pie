#! /usr/bin/env python
"""
Object-oriented interface for computational job control.
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

import os
import shelve
import sys
import types

from Exceptions import *
import gc3libs
import gc3libs.utils
from InformationContainer import InformationContainer
import Default


# -----------------------------------------------------
# Job
#

# Job is finished on grid or cluster, results have not yet been retrieved.
# User or Application can now call gget.
JOB_STATE_FINISHED = 1

# Job is currently running on a grid or cluster.
# Gc3libs must wait until it finishes to proceed.
JOB_STATE_RUNNING = 2

# Could mean several things:
#   - LRMS failed to accept the job for some reason.
#   - LRMS killed the job.
#   - Job exited with non-zero exit status.
# The User can decide whether to do gget or resubmit or nothing.
JOB_STATE_FAILED = 3

# Job is between the submit phase and confirmed in the LRMS scheduler.
# For example, this applies to ARC jobs after they are submitted but not yet officially queued according to the ARC scheduler.
# SGE+SSH jobs do not have this.
# No action required.
JOB_STATE_SUBMITTED = 4

# Job is finished and results successfully retrieved.
# User or Application can do whatever it wants with the results.
JOB_STATE_COMPLETED = 5

# Job has been deleted with gkill
JOB_STATE_DELETED = 6

# This is the default initial state of a job instance before it has been updated by a gsub/gstat/gget/etc.
# No action required; the state will be set to something else as the code executes.
JOB_STATE_UNKNOWN = 7


#JOB_STATE_HOLD = 7 # Initial state
#JOB_STATE_READY = 8 # Ready for gsub
#JOB_STATE_WAIT = 9 # equivalent to SUBMITTED
#JOB_STATE_OUTPUT = 10 # equivalent to FINISHED
#JOB_STATE_UNREACHABLE = 11 # AuthError
#JOB_STATE_NOTIFIED = 12 # User Notified of AuthError
#JOB_STATE_ERROR = 13 # Equivalent to FAILED


def job_status_to_string(job_status):
    try:
        return {
#            JOB_STATE_HOLD:    'HOLD',
#            JOB_STATE_WAIT:    'WAITING',
#            JOB_STATE_READY:   'READY',
#            JOB_STATE_ERROR:   'ERROR',
            JOB_STATE_FAILED:  'FAILED',
#            JOB_STATE_OUTPUT:  'OUTPUTTING',
            JOB_STATE_RUNNING: 'RUNNING',
            JOB_STATE_FINISHED:'FINISHED',
#            JOB_STATE_NOTIFIED:'NOTIFIED',
            JOB_STATE_SUBMITTED:'SUBMITTED',
            JOB_STATE_COMPLETED:'COMPLETED',
            JOB_STATE_DELETED: 'DELETED'
            }[job_status]
    except KeyError:
        gc3libs.log.error('job status code %s unknown', job_status)
        return 'UNKNOWN'


class Job(InformationContainer):

    def __init__(self,initializer=None,**keywd):
        """
        Create a new Job object.
        
        Examples::
        
        >>> df = Job()
        """
        # create_unique_token
        if ((not keywd.has_key('unique_token')) 
                and not (initializer is not None 
                         and hasattr(initializer, 'has_key') 
                         and initializer.has_key('unique_token'))):
            gc3libs.log.debug('Creating new unique_token ...')
            keywd['unique_token'] = gc3libs.utils.create_unique_token()
            gc3libs.log.debug('... got "%s"' % keywd['unique_token'])
        InformationContainer.__init__(self,initializer,**keywd)

    def is_valid(self):
        if (self.has_key('unique_token')
            #and self.has_key('status') 
            #and self.has_key('resource_name') 
            #and self.has_key('lrms_jobid') 
            ):
            return True


def get_job(unique_token):
    return get_job_filesystem(unique_token)

def get_job_filesystem(unique_token):
    job_file = os.path.join(Default.JOBS_DIR, unique_token)
    gc3libs.log.debug('retrieving job from %s', job_file)

    if not os.path.exists(job_file):
        raise JobRetrieveError("No '%s' file found in directory '%s'" 
                               % (unique_token, Default.JOBS_DIR))
    # XXX: this should become `with handler = ...:` as soon as we stop
    # supporting Python 2.4
    handler = None
    try:
        handler = shelve.open(job_file)
        job = Job(handler) 
        handler.close()
    except Exception, x:
        if handler is not None:
            try:
                handler.close()
            except:
                pass # ignore errors
        raise JobRetrieveError("Failed retrieving job from filesystem: %s: %s"
                               % (x.__class__.__name__, str(x)))
    if job.is_valid():
        return job
    else:
        raise JobRetrieveError("Got invalid job from file '%s'" % job_file)

def persist_job(job_obj):
    return persist_job_filesystem(job_obj)

def persist_job_filesystem(job_obj):
    job_file = os.path.join(Default.JOBS_DIR, job_obj.unique_token)
    gc3libs.log.debug("dumping job into file '%s'", job_file)
    if not os.path.exists(Default.JOBS_DIR):
        try:
            os.makedirs(Default.JOBS_DIR)
        except Exception, x:
            # raise same exception but add context message
            gc3libs.log.error("Could not create jobs directory '%s': %s" 
                               % (Default.JOBS_DIR, x))
            raise
    handler = None
    try:
        handler = shelve.open(job_file)
        handler.update(job_obj)
        handler.close()
    except Exception, x:
        gc3libs.log.error("Could not persist job %s to '%s': %s: %s" 
                           % (job_obj.unique_token, Default.JOBS_DIR, x.__class__.__name__, x))
        if handler is not None:
            try:
                handler.close()
            except:
                pass # ignore errors
        raise

def clean_job(job):
    return clean_job_filesystem(job.unique_token)

def clean_job_filesystem(unique_token):
    job_file_path = os.path.join(Default.JOBS_DIR,unique_token)
    if os.path.isfile(job_file_path):
        return os.remove(job_file_path)
    else:
        raise RetrieveJobsFilesystemError('Job file %s not found' % job_file_path)

def prepare_job_dir(_download_dir):
    try:
        if os.path.isdir(_download_dir):
            # directory exists; find a suitable extension and rename
            parent_dir = os.path.dirname(_download_dir)
            prefix = os.path.dirname(_download_dir) + '.'
            l = len(prefix)
            suffix = 1
            for name in [ x for x in os.listdir(parent_dir) if x.startswith(prefix) ]:
                try:
                    n = int(name[l:])
                    suffix = max(suffix, n)
                except:
                    # ignore non-numeric suffixes
                    pass
            os.rename(_download_dir, "%s.%d" % (_download_dir, suffix))

        os.makedirs(_download_dir)
        return True
    except:
        gc3libs.log.error('Failed creating folder %s ' % _download_dir)
        gc3libs.log.debug('%s %s',sys.exc_info()[0], sys.exc_info()[1])
        return False
