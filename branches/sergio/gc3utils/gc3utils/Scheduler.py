import sys
from Exceptions import *

class Scheduler(object):

    def do_brokering(lrms_list,application):
        try:
            _selected_lrms_list = []
            for lrms in lrms_list:
                if (int(application.requested_cores) > int(lrms.max_cores_per_job)) | (int(application.requested_memory) > int(lrms.max_memory_per_core)) | (int(application.requested_walltime) > int(lrms.max_walltime)):
                    continue
                else:
                    # lrms is a good candidate
                    _selected_lrms_list.append(lrms)
            return _selected_lrms_list

        except AttributeError:
            # either lrms or application are not valid objects
            raise BrokerException(sys.exc_info()[1])
        except:
            raise BrokerException(sys.exc_info()[1])

    do_brokering = staticmethod(do_brokering)
