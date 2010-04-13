class Scheduler:
    @staticmethod
    def do_brokering(lrms_list,application):
        try:
            for lrms in lrms_list:
                if (application.cores > lrms.max_cores_per_job) | (application.memory > lrms.max_memory_per_core) | (application.walltime > lrms.max_walltime) :
                    continue
                else:
                    return lrms
            raise Exception('Failed finding lrms that could fullfill the application requirements')
        except:
            raise
