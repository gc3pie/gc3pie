try:
    import stackless
    stackless_present = True
except:
    stackless_present = False
    print 'Not using stackless'

import cPickle
from time import sleep
import sys
sys.path.append('/home/mmonroe/apps/gorg')
from gorg_site.gorg_site.lib.mydb import Mydb
DEBUG_DO_NOT_USE_STACKLESS=True

class GFunction(object):
    LOG_FILENAME = '/tmp/python_scheduler_logger.out'
    # Time to sleep between polling job status
    SLEEP_TIME = 1
    TASK_FILE_PREFIX = 'task_'
    myChannel = None
    
    def __init__(self, options):
        self.options = options
        self.logger = self._create_logger()
        self.db= Mydb(self.options.db_name,self.options.db_loc).cdb()
    
    def preprocess(self):
        pass
    
    def process_loop(self, a_task, calculator):
        pass

    def calculate(self, to_execute, calculator):
        sys.path.append('/home/mmonroe/apps/gorg')
        from gorg.gridjobscheduler import GridjobScheduler
        job_scheduler = GridjobScheduler()
        result_list = calculator.calculate(to_execute)
        for a_result in result_list:
            self.logger.info('Submited job %s to batch system.'%(a_result.a_job.id))
        for a_result in result_list:
            job_done = False
            while not job_done:
                job_scheduler.run()
                if self.myChannel:
                    self.logger.info('Restart tasklet waiting for job %s.'%(a_result.a_job.id))
                    del self.logger
                    self.myChannel.receive()
                    self.logger=self._create_logger(options)
                else:
                    self.logger.info('Restart sleeping, waiting for job %s.'%(a_result.a_job.id))
                    sleep(self.SLEEP_TIME)
                job_done = a_result.wait(timeout=10)
            a_result.read()
        return result_list

    def postprocess(self, result_list):
        return result_list
        
    def run(self, myChannel=None):
        if myChannel:
            self.myChannel = myChannel
        a_task, calculator = self.preprocess()
        done = False
        while not done:
            done, result_list = self.process_loop(a_task, calculator)
        postprocess_result = self.postprocess(result_list)
        return postprocess_result
    
    def _create_logger(self):
        import logging
        import logging.handlers
        logger = logging.getLogger("restart_main")
        #Setup logger
        LOGGING_LEVELS = (logging.CRITICAL, logging.ERROR, 
                                        logging.WARNING, logging.INFO, logging.DEBUG)
        level = len(LOGGING_LEVELS) if len(self.options.verbose) > len(LOGGING_LEVELS) else len(self.options.verbose)
        logger.setLevel(level)    
        file_handler = logging.handlers.RotatingFileHandler(
                  self.LOG_FILENAME, maxBytes=100000, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger
    
def run_function(gfunction):
    if DEBUG_DO_NOT_USE_STACKLESS or not stackless_present:
        gfunction.run()
    else:
        myChannel = stackless.channel()
        t1 = stackless.tasklet(gfunction.run)(options, myChannel)
        t1.run()
        output = open('%s/%s'%(options.directory,'task_%s.pkl'%gfunction.task_id), 'wb')
        cPickle.dump(myChannel, output)
        output.close() 
        t1.kill()
        exit()



