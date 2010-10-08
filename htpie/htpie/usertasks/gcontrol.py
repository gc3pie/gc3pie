import htpie

from htpie.lib import utils
from htpie.lib.exceptions import *
from htpie import enginemodel as model
from htpie import statemachine

from htpie.usertasks.usertasks import *

class GControl(object):
    
    @staticmethod
    def kill(id):
        doc =  model.Task.objects.with_id(id)
        doc.kill()
        htpie.log.info('Task %s will be killed'%(id))
    
    @staticmethod
    def retry(id):
        doc =  model.Task.objects.with_id(id)
        doc.retry()
        htpie.log.info('Task %s will be retried'%(id))
    
    @staticmethod
    def info(id, long_format):
        doc = model.Task.objects.with_id(id)
        output = doc.display(long_format)
        sys.stdout.write(output)
        sys.stdout.flush()
