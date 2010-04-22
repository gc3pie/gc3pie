import sys

class Scheduler:
    @staticmethod
    def do_brokering(lrms_list,application):
        try:
            _selected_lrms_list = []
            for lrms in lrms_list:
                if (application.cores > lrms.max_cores_per_job) | (application.memory > lrms.max_memory_per_core) | (application.walltime > lrms.max_walltime):
                    continue
                else:
                    # lrms is a good candidate
                    _selected_lrms_list.append(lrms)
            return _selected_lrms_list

        except AttributeError:
            # either lrms or application are not valid objects
            raise BrokerExeption(sys.exc_info()[1])
        except:
            raise BrokerExeption(sys.exc_info()[1])
