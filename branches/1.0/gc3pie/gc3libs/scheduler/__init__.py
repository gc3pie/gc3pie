#! /usr/bin/env python
"""
Simple-minded scheduling for GC3Libs.
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
__version__ = '1.0rc4 (SVN $Revision$)'


import sys

import gc3libs
from gc3libs.exceptions import *


def _compatible_resources(lrms_list, application):
    """
    Return list of resources in `lrms_list` that match the requirements
    in `application`.
    """
    _selected_lrms_list = []
    for lrms in lrms_list:
        assert(lrms is not None), \
            "Scheduler._compatible_resources(): expected `LRMS` object, got `None` instead."
        if not lrms.is_valid():
            gc3libs.log.debug("Ignoring invalid LRMS object '%s'" % lrms)
            continue
        gc3libs.log.debug("Checking resource '%s' for compatibility with application requirements",
                           lrms._resource.name)
        if not ( # check that Application requirements are within resource limits
            int(application.requested_cores) > int(lrms._resource.max_cores_per_job or sys.maxint) 
            or int(application.requested_memory) > int(lrms._resource.max_memory_per_core or sys.maxint) 
            or int(application.requested_walltime) > int(lrms._resource.max_walltime or sys.maxint)
           ):
            _selected_lrms_list.append(lrms)
        else:
            gc3libs.log.info("Rejecting resource '%s':"
                              " no match with application requirements", 
                              lrms._resource.name)
    return _selected_lrms_list


def _cmp_resources(a,b):
    """
    Compare resources `a` and `b` and return -1,0,1 accordingly
    (see doc for the Python standard function `cmp`).

    Computational resource `a` is preferred over `b` if it has less
    queued jobs from the same user; failing that, if it has more free
    slots; failing that, if it has less queued jobs (in total);
    finally, should all preceding parameters compare equal, `a` is
    preferred over `b` if it has less running jobs from the same user.
    """
    a_ = (a._resource.user_queued, -a._resource.free_slots, 
          a._resource.queued, a._resource.user_run)
    b_ = (b._resource.user_queued, -b._resource.free_slots, 
          b._resource.queued, b._resource.user_run)
    return cmp(a_, b_)


def do_brokering(lrms_list, application):
    assert (application is not None), \
        "Scheduler.do_brokering(): expected valid `Application` object, got `None` instead."
    rs = _compatible_resources(lrms_list, application)
    if len(rs) <= 1:
        # shortcut: no brokering to do, just use the only resource we've got
        return rs
    # get up-to-date resource status
    updated_resources = []
    for r in rs:
        try:
            # in-place update of resource status
            gc3libs.log.debug("Trying to update status of resource '%s' ...", r._resource.name)
            r.get_resource_status()
            updated_resources.append(r)
        except Exception, x:
            # ignore errors in update, assume resource has a problem
            # and just drop it
            gc3libs.log.error("Cannot update status of resource '%s', dropping it."
                              " See log file for details.",
                              r._resource.name)
            gc3libs.log.debug("Got error from get_resource_status(): %s: %s",
                              x.__class__.__name__, x.args, exc_info=True)
    return sorted(updated_resources, cmp=_cmp_resources)



## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="__init__",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
