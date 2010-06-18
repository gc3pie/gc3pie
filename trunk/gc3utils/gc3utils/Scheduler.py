import sys

import gc3utils
from Exceptions import *

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
            gc3utils.log.debug("Ignoring invalid LRMS object '%s'" % lrms)
            continue
        if not ( # check that Application requirements are within resource limits
            int(application.requested_cores) > int(lrms._resource.max_cores_per_job) 
            or int(application.requested_memory) > int(lrms._resource.max_memory_per_core) 
            or int(application.requested_walltime) > int(lrms._resource.max_walltime)
           ):
            _selected_lrms_list.append(lrms)
        else:
            gc3utils.log.info("Rejecting resource '%s':"
                              " no match with application requirements", 
                              lrms._resource.name)
    return _selected_lrms_list


def _cmp_resources(a,b):
    """
    Compare resources `a` and `b` and return -1,0,1 accordingly
    (see doc for the Python standard function `cmp`).

    Computational resource `a` is preferred over `b` if it has
    more free slots; failing that, if it has less queued jobs (in
    total); failing that, if it has less queued jobs from the same
    user; finally, should all preceding parameters compare equal,
    `a` is preferred over `b` if it has less running jobs from the
    same user.
    """
    a_ = (a._resource.free_slots, -a._resource.queued, 
          -a._resource.user_queued, a._resource.user_run)
    b_ = (b._resource.free_slots, -b._resource.queued, 
          -b._resource.user_queued, b._resource.user_run)
    return cmp(a_, b_)


def do_brokering(lrms_list, application):
    assert (application is not None), \
        "Scheduler.do_brokering(): expected valid `Application` object, got `None` instead."
    rs = _compatible_resources(lrms_list, application)
    # get up-to-date resource status
    for r in rs:
        # in-place update of resource status
        r.get_resource_status()
    return sorted(rs, cmp=_cmp_resources)
